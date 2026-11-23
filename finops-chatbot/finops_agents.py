"""Dynamic, read-only Confluent MCP agents for the FinOps chatbot.

The tool catalogue is discovered from the live MCP server on every request.
Tool descriptions and input schemas are supplied to the planner. Only tools
explicitly marked readOnlyHint=True may be executed by this module.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any
import sys
import shutil
import logging


from dotenv import load_dotenv
from google import genai

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:  # pragma: no cover - reported clearly at runtime
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None


class AgentError(RuntimeError):
    """A safe, user-displayable MCP agent error."""


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if hasattr(value, "dict"):
        return value.dict()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _get_attr(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _tool_annotations(tool: Any) -> dict[str, Any]:
    raw = _get_attr(tool, "annotations", default={}) or {}
    return _jsonable(raw) if not isinstance(raw, dict) else raw


def _is_read_only(tool: Any) -> bool:
    annotations = _tool_annotations(tool)
    return annotations.get("readOnlyHint") is True or annotations.get("read_only_hint") is True


def _tool_schema(tool: Any) -> dict[str, Any]:
    schema = _get_attr(tool, "inputSchema", "input_schema", default={}) or {}
    return _jsonable(schema) if not isinstance(schema, dict) else schema


def _tool_catalog(tools: list[Any]) -> list[dict[str, Any]]:
    catalog = []
    for tool in tools:
        schema = _tool_schema(tool)
        annotations = _tool_annotations(tool)
        catalog.append({
            "name": _get_attr(tool, "name", default=""),
            "description": _get_attr(tool, "description", default="") or "",
            "inputSchema": schema,
            "annotations": annotations,
            "read_only": _is_read_only(tool),
        })
    return catalog


def _extract_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I)
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", raw)
        if not match:
            raise AgentError("The MCP tool planner did not return valid JSON.")
        value = json.loads(match.group(0))
    if not isinstance(value, dict):
        raise AgentError("The MCP tool planner returned an unexpected response.")
    return value


def _validate_schema(schema: dict[str, Any], args: dict[str, Any], tool_name: str) -> None:
    """Validate the practical JSON Schema constraints before MCP execution."""
    if not isinstance(args, dict):
        raise AgentError(f"Arguments for {tool_name} must be a JSON object.")
    props = schema.get("properties") or {}
    if schema.get("additionalProperties") is False:
        extra = set(args) - set(props)
        if extra:
            raise AgentError(f"Unsupported arguments for {tool_name}: {', '.join(sorted(extra))}")
    missing = set(schema.get("required") or []) - set(args)
    if missing:
        raise AgentError(f"Missing required arguments for {tool_name}: {', '.join(sorted(missing))}")
    for key, value in args.items():
        spec = props.get(key, {})
        expected = spec.get("type")
        valid = True
        if expected == "string":
            valid = isinstance(value, str)
        elif expected == "integer":
            valid = isinstance(value, int) and not isinstance(value, bool)
        elif expected == "number":
            valid = isinstance(value, (int, float)) and not isinstance(value, bool)
        elif expected == "boolean":
            valid = isinstance(value, bool)
        elif expected == "array":
            valid = isinstance(value, list)
        elif expected == "object":
            valid = isinstance(value, dict)
        if not valid:
            raise AgentError(f"Argument '{key}' has the wrong type for {tool_name}; expected {expected}.")
        if "enum" in spec and value not in spec["enum"]:
            raise AgentError(f"Argument '{key}' for {tool_name} must be one of: {spec['enum']}.")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if "maximum" in spec and value > spec["maximum"]:
                raise AgentError(f"Argument '{key}' exceeds the maximum for {tool_name}.")
            if "minimum" in spec and value < spec["minimum"]:
                raise AgentError(f"Argument '{key}' is below the minimum for {tool_name}.")
        if isinstance(value, str) and "pattern" in spec:
            if not re.fullmatch(spec["pattern"], value):
                raise AgentError(f"Argument '{key}' does not match the required format for {tool_name}.")


def _make_plan(question: str, start_date: str, end_date: str,
               catalog: list[dict[str, Any]], api_key: str, model_name: str,
               prior_results: list[dict[str, Any]] | None = None,
               already_called: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Ask Gemini to choose tools using their live descriptions and schemas."""
    if not api_key:
        raise AgentError("GOOGLE_API_KEY is missing; the MCP tool-planning agent cannot run.")
    # The Billing REST API remains the financial source of truth. We deliberately
    # exclude the MCP billing tool from the planner to avoid duplicate billing paths.
    excluded = {"list-billing-costs", "search-product-docs", "get-product-doc-page",
                "consume-messages", "produce-message"}
    candidates = [item for item in catalog
                  if item["read_only"] and item["name"] not in excluded]
    if not candidates:
        return []

    tool_reference = [{
        "name": item["name"],
        "description": item["description"],
        "inputSchema": item["inputSchema"],
    } for item in candidates]

    client = genai.Client(api_key=api_key)
    prior_results = prior_results or []
    already_called = already_called or []
    prompt = f"""You are the MCP Inventory Agent for a Confluent Cloud FinOps application.
Select only MCP tools needed to answer the user's question using the live tool descriptions and input schemas below.
The tool descriptions are the authoritative explanation of what each tool does. Do not invent tool names or arguments.
Only select tools from the supplied catalogue. These tools are marked read-only. Never select a tool that creates, updates, deletes, restarts, or otherwise changes resources.
Billing data is retrieved separately from the Confluent Billing REST API; do not select a billing tool.
For a broad account/resource cost overview, select relevant read-only inventory list tools for environments, Kafka clusters, topics, connectors, Flink compute pools, schemas/Schema Registry, ksqlDB, cluster links, tags, and other available resource types. Select only categories actually represented by the catalogue.
If the question asks about metrics, follow tool-description prerequisites; in particular, discover available metrics before querying a metric.
Use exact argument names and casing from inputSchema. Supply all required arguments. Do not guess IDs; first select a discovery tool when IDs are unknown. If an argument cannot be determined and no discovery tool can provide it, omit that call.
You are operating in a multi-step workflow. Previous tool results may contain IDs needed by later tools. Use those exact returned IDs when constructing dependent calls. Do not repeat an identical tool+arguments call already executed. If a prerequisite listing returned IDs, use them to call child-resource listing tools. If there are no useful additional calls, return an empty calls array.
Return ONLY JSON in this shape: {{"calls":[{{"tool":"exact-tool-name","arguments":{{}},"purpose":"short reason"}}]}}.
Use at most 10 calls in this round. Keep calls in dependency order. Do not include markdown.

USER QUESTION: {question}
BILLING PERIOD: {start_date} through {end_date} inclusive

TOOLS ALREADY EXECUTED (do not repeat identical calls):
{json.dumps(already_called, ensure_ascii=False, default=str)[:8000]}

RESULTS FROM PREVIOUS MCP CALLS (use exact IDs returned here):
{json.dumps(prior_results, ensure_ascii=False, default=str)[:14000]}

AVAILABLE READ-ONLY MCP TOOLS:
{json.dumps(tool_reference, ensure_ascii=False)}
"""
    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config={
            "temperature": 0,
            "max_output_tokens": 2200,
        },
    )
    text = getattr(response, "text", "") or ""
    payload = _extract_json(text)
    calls = payload.get("calls", [])
    if not isinstance(calls, list):
        raise AgentError("The MCP tool planner returned an invalid calls list.")
    return calls[:10]


def _decode_result(result: Any) -> Any:
    if result is None:
        return None
    structured = _get_attr(result, "structuredContent", "structured_content")
    if structured is not None:
        return _jsonable(structured)
    decoded = []
    for item in _get_attr(result, "content", default=[]) or []:
        text_value = _get_attr(item, "text")
        if not isinstance(text_value, str) or not text_value.strip():
            continue
        raw = text_value.strip()
        try:
            decoded.append(json.loads(raw))
        except json.JSONDecodeError:
            decoded.append(raw)
    if len(decoded) == 1:
        return decoded[0]
    return decoded



def _windows_server_params(config_path: Path) -> Any:
    # Load the parent environment file before launching MCP.
    env_file = config_path.parent / ".env"

    if env_file.is_file():
        load_dotenv(env_file, override=False)

    if os.name == "nt":
        node_path = shutil.which("node.exe")
        npx_path = shutil.which("npx.cmd")

        if not node_path:
            raise AgentError("Node.js executable was not found in PATH.")

        if not npx_path:
            raise AgentError("npx.cmd was not found in PATH.")

        npx_cli = (
            Path(npx_path).parent
            / "node_modules"
            / "npm"
            / "bin"
            / "npx-cli.js"
        )

        if not npx_cli.is_file():
            raise AgentError(f"npm CLI was not found: {npx_cli}")

        command = node_path
        args = [
            str(npx_cli),
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(config_path),
        ]

    else:
        command = os.getenv("CONFLUENT_MCP_COMMAND", "npx")
        args = [
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(config_path),
        ]

    return StdioServerParameters(
        command=command,
        args=args,
        env=dict(os.environ),
        cwd=str(config_path.parent),
    )



async def _run_agent_session(question: str, start_date: str, end_date: str,
                             config_path: Path, api_key: str, model_name: str,
                             timeout_seconds: int) -> dict[str, Any]:
    if ClientSession is None or stdio_client is None:
        raise AgentError("MCP Python SDK is missing. Install the requirements.txt dependencies.")
    if not config_path.is_file():
        raise AgentError(f"Existing Confluent MCP config was not found: {config_path}")

    async with stdio_client(
    _windows_server_params(config_path),
    errlog=sys.stderr,
) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            logger = logging.getLogger("confluent-finops-chatbot")
            logger.info("Starting Confluent MCP server")
            logger.info("MCP config: %s", config_path)
            logger.info("Node executable: %s", shutil.which("node"))
            logger.info("NPX executable: %s", shutil.which("npx.cmd"))
            await session.initialize()
            listed = await session.list_tools()
            tools = list(listed.tools)
            catalog = _tool_catalog(tools)
            tool_map = {_get_attr(tool, "name"): tool for tool in tools}

            all_plan: list[dict[str, Any]] = []
            results: list[dict[str, Any]] = []
            errors: list[dict[str, str]] = []
            executed_keys: set[str] = set()

            # Iterate so the agent can discover parent IDs first (environment,
            # cluster, compute pool) and then call child-resource tools using
            # those exact IDs. A single static plan cannot safely do that.
            for _round in range(3):
                round_plan = await asyncio.to_thread(
                    _make_plan, question, start_date, end_date, catalog,
                    api_key, model_name, results, all_plan
                )
                if not round_plan:
                    break

                names = [str(call.get("tool", "")) for call in round_plan if isinstance(call, dict)]
                if "query-metrics" in names and "list-available-metrics" in tool_map and "list-available-metrics" not in names and not any(p.get("tool") == "list-available-metrics" for p in all_plan):
                    round_plan.insert(0, {"tool": "list-available-metrics", "arguments": {},
                                          "purpose": "Required prerequisite: discover valid metric names and filters."})

                executed_this_round = 0
                for call in round_plan:
                    if not isinstance(call, dict):
                        continue
                    name = call.get("tool")
                    args = call.get("arguments", {})
                    purpose = str(call.get("purpose", ""))[:300]
                    tool = tool_map.get(name)
                    if tool is None:
                        errors.append({"tool": str(name), "error": "Tool is not exposed by the live MCP server."})
                        continue
                    if not _is_read_only(tool):
                        errors.append({"tool": str(name), "error": "Blocked: tool is not explicitly marked read-only."})
                        continue
                    if not isinstance(args, dict):
                        errors.append({"tool": str(name), "error": "Arguments must be a JSON object."})
                        continue
                    call_key = json.dumps({"tool": name, "arguments": args}, sort_keys=True, default=str)
                    if call_key in executed_keys:
                        continue
                    executed_keys.add(call_key)
                    schema = _tool_schema(tool)
                    try:
                        _validate_schema(schema, args, str(name))
                        result = await session.call_tool(str(name), args)
                        if _get_attr(result, "isError", "is_error", default=False):
                            raise AgentError(str(_decode_result(result)))
                        results.append({"tool": name, "purpose": purpose, "arguments": args,
                                        "result": _decode_result(result)})
                        executed_this_round += 1
                    except Exception as exc:
                        errors.append({"tool": str(name), "error": f"{type(exc).__name__}: {exc}"})
                all_plan.extend(round_plan)
                if executed_this_round == 0 or len(results) >= 10:
                    break

            return {
                "tool_count": len(catalog),
                "catalog": catalog,
                "plan": all_plan,
                "results": results,
                "errors": errors,
            }


def run_inventory_agents(question: str, start_date: str, end_date: str,
                         config_path: Path, api_key: str, model_name: str,
                         timeout_seconds: int = 120) -> dict[str, Any]:
    """Public synchronous entry point for Streamlit."""
    try:
        return asyncio.run(asyncio.wait_for(
            _run_agent_session(question, start_date, end_date, config_path,
                               api_key, model_name, timeout_seconds),
            timeout=timeout_seconds,
        ))
    except asyncio.TimeoutError as exc:
        raise AgentError(f"MCP agent workflow timed out after {timeout_seconds} seconds.") from exc
    except RuntimeError as exc:
        raise AgentError(f"Could not start MCP agent session: {exc}") from exc
