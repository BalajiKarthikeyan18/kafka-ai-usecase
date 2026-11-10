import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# Existing MCP server configuration. These paths are intentionally kept
# separate from the FinOps project so the existing MCP setup is not modified.
MCP_PROJECT_DIR = Path(r"D:\JLR\AI USECASE")
CONFIG_PATH = MCP_PROJECT_DIR / "config.yaml"
ENV_PATH = MCP_PROJECT_DIR / ".env"

# Output is written relative to this script.
RESULTS_DIR = Path(__file__).resolve().parent / "results"


def content_to_text(result: Any) -> str:
    """Combine textual MCP content blocks into one string."""
    parts = []
    for item in getattr(result, "content", []) or []:
        text = getattr(item, "text", None)
        if text:
            parts.append(text)
    return "\n".join(parts)


def result_to_jsonable(result: Any) -> dict:
    """Keep the complete MCP response in a JSON-serializable form."""
    return {
        "is_error": bool(getattr(result, "isError", False)),
        "structured_content": getattr(result, "structuredContent", None),
        "content": [
            {
                "type": getattr(item, "type", None),
                "text": getattr(item, "text", None),
            }
            for item in (getattr(result, "content", []) or [])
        ],
    }


def parse_json_from_text(text: str):
    """Parse a whole JSON response, or the first JSON object/array in text."""
    text = text.strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char in "{[":
            try:
                value, _ = decoder.raw_decode(text[index:])
                return value
            except json.JSONDecodeError:
                continue
    return None


def extract_topic_names(result: Any) -> list[str]:
    """Extract topic names from structured or text-based MCP responses."""
    names = set()

    structured = getattr(result, "structuredContent", None)
    if structured is None:
        structured = getattr(result, "structured_content", None)

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key.lower() in {"topic_name", "topicname"} and isinstance(child, str):
                    names.add(child.strip())
                elif key.lower() in {"topics", "data", "items"}:
                    walk(child)
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, str):
            parsed = parse_json_from_text(value)
            if parsed is not None:
                walk(parsed)

    if structured is not None:
        walk(structured)

    text = content_to_text(result)
    parsed = parse_json_from_text(text)
    if parsed is not None:
        walk(parsed)

    # Current MCP tool has returned: "Kafka topics: a,b,c"
    for line in text.splitlines():
        match = re.search(r"Kafka topics\s*:\s*(.*)", line, flags=re.IGNORECASE)
        if match:
            for name in match.group(1).split(","):
                name = name.strip()
                if name:
                    names.add(name)

    # Also support a plain list response such as "Topics: a, b".
    for line in text.splitlines():
        match = re.search(r"^\s*Topics?\s*:\s*(.*)$", line, flags=re.IGNORECASE)
        if match:
            for name in match.group(1).split(","):
                name = name.strip()
                if name and not name.lower().startswith(("pagination", "page ")):
                    names.add(name)

    return sorted(names)


def build_arguments(tool, context: dict) -> dict:
    """
    Build arguments from the tool's advertised input schema.
    Only schema properties are sent. Known environment/cluster values are
    supplied when the tool requests those fields.
    """
    schema = getattr(tool, "inputSchema", None) or {}
    properties = schema.get("properties", {}) or {}
    required = set(schema.get("required", []) or [])
    args = {}

    for key in properties:
        lower = key.lower()
        if lower in {"environment_id", "env_id", "environmentid"}:
            if context.get("environment_id"):
                args[key] = context["environment_id"]
        elif lower in {"cluster_id", "clusterid", "kafka_cluster_id"}:
            if context.get("cluster_id"):
                args[key] = context["cluster_id"]
        elif lower in {"topicname", "topic_name"}:
            if context.get("topic_name"):
                args[key] = context["topic_name"]
        elif lower in {"connector_name", "connectorname", "name"}:
            if context.get("connector_name"):
                args[key] = context["connector_name"]

    missing = required - set(args)
    if missing:
        raise ValueError(
            f"Cannot determine required arguments {sorted(missing)} "
            f"for MCP tool '{tool.name}'. Schema: {schema}"
        )
    return args


async def call_tool(session, tools_by_name, tool_name, context=None):
    """Call a tool if available and preserve errors in the output."""
    context = context or {}
    tool = tools_by_name.get(tool_name)
    if tool is None:
        return {
            "is_error": True,
            "error": f"MCP tool '{tool_name}' is not available",
            "content": [],
        }

    try:
        arguments = build_arguments(tool, context)
        result = await session.call_tool(tool_name, arguments=arguments)
        serialized = result_to_jsonable(result)
        serialized["arguments"] = arguments
        return serialized
    except Exception as exc:
        return {
            "is_error": True,
            "arguments": {},
            "error": f"{type(exc).__name__}: {exc}",
            "content": [],
        }


def first_environment_id(environments_result: dict) -> str | None:
    candidates = []
    structured = environments_result.get("structured_content")
    if structured is not None:
        candidates.append(structured)
    for item in environments_result.get("content", []):
        parsed = parse_json_from_text(item.get("text") or "")
        if parsed is not None:
            candidates.append(parsed)

    def walk(value):
        if isinstance(value, dict):
            if isinstance(value.get("id"), str) and (
                "environment" in str(value.get("resource_name", "")).lower()
                or "environment" in str(value.get("kind", "")).lower()
            ):
                return value["id"]
            for key in ("data", "environments", "items"):
                found = walk(value.get(key))
                if found:
                    return found
            for child in value.values():
                found = walk(child)
                if found:
                    return found
        elif isinstance(value, list):
            for child in value:
                found = walk(child)
                if found:
                    return found
        return None

    for candidate in candidates:
        found = walk(candidate)
        if found:
            return found

    # The current MCP server formats environments as plain text.
    for item in environments_result.get("content", []):
        text = item.get("text") or ""
        match = re.search(r"Environment:.*?\n\s*ID:\s*([A-Za-z0-9_-]+)", text)
        if match:
            return match.group(1)
    return None


def extract_cluster_ids(clusters_result: dict) -> list[str]:
    ids = set()
    for item in clusters_result.get("content", []):
        text = item.get("text") or ""
        ids.update(re.findall(r"^\s*ID:\s*(lkc-[A-Za-z0-9_-]+)\s*$", text, re.MULTILINE))
    structured = clusters_result.get("structured_content")
    if structured:
        def walk(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key.lower() in {"cluster_id", "id"} and isinstance(child, str) and child.startswith("lkc-"):
                        ids.add(child)
                    else:
                        walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
        walk(structured)
    return sorted(ids)


async def main():
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"MCP config file not found: {CONFIG_PATH}")
    if not ENV_PATH.is_file():
        raise FileNotFoundError(f"MCP .env file not found: {ENV_PATH}")

    load_dotenv(ENV_PATH, override=False)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    server_params = StdioServerParameters(
        command="cmd.exe",
        args=[
            "/c",
            "npx",
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(CONFIG_PATH),
        ],
        env=os.environ.copy(),
        cwd=str(MCP_PROJECT_DIR),
    )

    inventory = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Confluent MCP server",
        "resources": {},
        "topic_configs": {},
        "discovery_errors": [],
    }

    async with stdio_client(server_params, errlog=None) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            listed_tools = await session.list_tools()
            tools_by_name = {tool.name: tool for tool in listed_tools.tools}

            # Discover organization and environment first.
            for tool_name in ("list-organizations", "list-environments"):
                inventory["resources"][tool_name] = await call_tool(
                    session, tools_by_name, tool_name
                )

            environment_id = first_environment_id(
                inventory["resources"]["list-environments"]
            )

            # Discover resource collections. Environment-scoped tools receive
            # the environment ID automatically when their schema requests it.
            context = {"environment_id": environment_id}
            for tool_name in (
                "list-clusters",
                "list-topics",
                "list-connectors",
                "list-schemas",
                "list-compute-pools",
            ):
                inventory["resources"][tool_name] = await call_tool(
                    session, tools_by_name, tool_name, context
                )

            # Topic configuration is discovered dynamically from list-topics.
            topic_result = inventory["resources"]["list-topics"]
            topic_names = extract_topic_names_from_serialized(topic_result)
            inventory["topic_names"] = topic_names

            # A single cluster is the common case for this POC. If multiple
            # clusters are present, try each discovered cluster context.
            cluster_ids = extract_cluster_ids(
                inventory["resources"]["list-clusters"]
            )
            inventory["cluster_ids"] = cluster_ids

            for topic_name in topic_names:
                topic_context = {
                    "environment_id": environment_id,
                    "topic_name": topic_name,
                }
                topic_config_tool = tools_by_name.get("get-topic-config")
                if topic_config_tool:
                    # The MCP tool expects the exact camelCase argument
                    # "topicName". Its advertised schema may not expose the
                    # property in a form the generic argument builder can read.
                    topic_args = {"topicName": topic_name}
                    try:
                        result = await session.call_tool(
                            "get-topic-config", arguments=topic_args
                        )
                        serialized = result_to_jsonable(result)
                        serialized["arguments"] = topic_args

                        # Some MCP server-side validation failures are returned
                        # as text content rather than result.isError=True.
                        response_text = content_to_text(result)
                        if (
                            "MCP error" in response_text
                            or "Input validation error" in response_text
                        ):
                            serialized["is_error"] = True
                            serialized["error"] = response_text

                        inventory["topic_configs"][topic_name] = serialized
                    except Exception as exc:
                        inventory["topic_configs"][topic_name] = {
                            "is_error": True,
                            "arguments": topic_args,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                else:
                    inventory["topic_configs"][topic_name] = {
                        "is_error": True,
                        "error": "MCP tool 'get-topic-config' is not available",
                    }

    # Keep both a complete raw response and a concise run summary.
    output_path = RESULTS_DIR / "dynamic_resource_inventory.json"
    output_path.write_text(
        json.dumps(inventory, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    summary = {
        "generated_at_utc": inventory["generated_at_utc"],
        "output_file": str(output_path),
        "available_mcp_tools": sorted(tools_by_name.keys()),
        "environment_id_detected": environment_id,
        "cluster_ids_detected": cluster_ids,
        "topic_count": len(topic_names),
        "topic_config_success_count": sum(
            not item.get("is_error", False)
            for item in inventory["topic_configs"].values()
        ),
        "topic_config_failure_count": sum(
            item.get("is_error", False)
            for item in inventory["topic_configs"].values()
        ),
    }

    print(json.dumps(summary, indent=2))
    print("\nResource discovery results:")
    for name, result in inventory["resources"].items():
        print(
            f"- {name}: "
            f"{'ERROR' if result.get('is_error') else 'OK'}"
        )
        if result.get("error"):
            print(f"  {result['error']}")
    print(f"\nFull response saved to: {output_path}")


def extract_topic_names_from_serialized(result: dict) -> list[str]:
    """Adapter for the serialized MCP response stored in inventory."""
    names = set()

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key.lower() in {"topic_name", "topicname"} and isinstance(child, str):
                    names.add(child.strip())
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, str):
            parsed = parse_json_from_text(value)
            if parsed is not None:
                walk(parsed)
            for line in value.splitlines():
                match = re.search(r"Kafka topics\s*:\s*(.*)", line, flags=re.IGNORECASE)
                if match:
                    names.update(
                        name.strip()
                        for name in match.group(1).split(",")
                        if name.strip()
                    )
                match = re.search(r"^\s*Topics?\s*:\s*(.*)$", line, flags=re.IGNORECASE)
                if match:
                    names.update(
                        name.strip()
                        for name in match.group(1).split(",")
                        if name.strip()
                    )

    walk(result.get("structured_content"))
    walk(result.get("content", []))
    return sorted(names)


if __name__ == "__main__":
    asyncio.run(main())
