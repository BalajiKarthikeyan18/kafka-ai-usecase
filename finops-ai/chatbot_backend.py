"""
Confluent FinOps Chatbot — backend-connected edition
====================================================

Run from this folder:
    streamlit run chatbot_backend.py

Backend data sources:
- Billing: Confluent Cloud Billing REST API (Basic auth from parent .env)
- Inventory: Existing Confluent MCP server using the parent's config.yaml
- AI explanation: Gemini 2.5 Flash

No user file uploads, documentation crawling, FAISS, embeddings, or local
billing/inventory snapshot files are required.

Expected layout:
    AI USECASE/
      .env
      config.yaml
      finops-ai/                 # existing project; left untouched
      finops-chatbot/
        chatbot.py
        .env                     # GOOGLE_API_KEY and GEMINI_MODEL
        data_store/
          chat_history/

Parent .env:
    CONFLUENT_CLOUD_API_KEY=...
    CONFLUENT_CLOUD_API_SECRET=...

finops-chatbot/.env:
    GOOGLE_API_KEY=...
    GEMINI_MODEL=gemini-2.5-flash

The MCP server's tool names and input schemas are discovered at runtime.
The code selects inventory tools by name and supplies only schema-supported
arguments. If your MCP server exposes different tool names, the sidebar
shows the discovered tools so the mapping can be adjusted without changing
the MCP server configuration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
import google.generativeai as genai

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

APP_DIR = Path(__file__).resolve().parent
PARENT_DIR = APP_DIR.parent
PARENT_CONFIG = PARENT_DIR / "config.yaml"
PARENT_ENV = PARENT_DIR / ".env"

DATA_DIR = APP_DIR / "data_store"
HISTORY_DIR = DATA_DIR / "chat_history"
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(APP_DIR / ".env", override=False)
load_dotenv(PARENT_ENV, override=False)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

CONFLUENT_API_KEY = os.getenv("CONFLUENT_CLOUD_API_KEY", "").strip()
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_CLOUD_API_SECRET", "").strip()
BILLING_API_URL = os.getenv(
    "CONFLUENT_BILLING_URL",
    "https://api.confluent.cloud/billing/v1/costs",
).strip()

MCP_COMMAND = os.getenv("CONFLUENT_MCP_COMMAND", "npx")
MCP_PACKAGE = os.getenv("CONFLUENT_MCP_PACKAGE", "@confluentinc/mcp-confluent")
MCP_TIMEOUT = int(os.getenv("MCP_TIMEOUT_SECONDS", "90"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
MAX_DAYS_PER_BILLING_REQUEST = 31
MAX_CONTEXT_ROWS = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("confluent-finops-chatbot")


class FinOpsError(Exception):
    """Expected error safe to display in the Streamlit interface."""


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def configure_gemini() -> None:
    if not GOOGLE_API_KEY:
        raise FinOpsError(
            "GOOGLE_API_KEY is missing. Add it to finops-chatbot/.env."
        )
    genai.configure(api_key=GOOGLE_API_KEY)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False, default=str)
    temporary.replace(path)


def money(value: float) -> str:
    return f"${value:,.4f}"


def as_float(value: Any) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def flatten_mcp_content(result: Any) -> Any:
    """Decode MCP structuredContent or JSON/text Content blocks safely."""
    if result is None:
        return None
    structured = getattr(result, "structuredContent", None)
    if structured is None:
        structured = getattr(result, "structured_content", None)
    if structured is not None:
        return structured

    decoded: list[Any] = []
    for item in getattr(result, "content", []) or []:
        text_value = getattr(item, "text", None)
        if not isinstance(text_value, str) or not text_value.strip():
            continue
        raw = text_value.strip()
        try:
            decoded.append(json.loads(raw))
            continue
        except json.JSONDecodeError:
            pass
        # Some MCP tools prefix/suffix JSON with a short status message.
        parsed = None
        for pattern in (r"(\{[\s\S]*\})", r"(\[[\s\S]*\])"):
            match = re.search(pattern, raw)
            if match:
                try:
                    parsed = json.loads(match.group(1))
                    break
                except json.JSONDecodeError:
                    continue
        decoded.append(parsed if parsed is not None else raw)

    if len(decoded) == 1:
        return decoded[0]
    return decoded


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """Extract inventory objects from common envelopes without hiding text."""
    if isinstance(payload, list):
        records: list[dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                records.append(item)
            elif isinstance(item, list):
                records.extend(extract_records(item))
        return records
    if isinstance(payload, dict):
        for key in (
            "data", "items", "results", "resources", "clusters", "topics",
            "connectors", "schemas", "compute_pools", "computePools",
            "environments", "environmentsList", "subjects",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                return extract_records(value)
            if isinstance(value, dict):
                nested = extract_records(value)
                if nested:
                    return nested
        for key in ("result", "response"):
            if key in payload:
                nested = extract_records(payload[key])
                if nested:
                    return nested
        if any(k in payload for k in (
            "id", "name", "display_name", "cluster_id", "connector_id",
            "resource_id", "topic_name", "connector_name",
        )):
            return [payload]
    return []


def _tool_schema(tool: Any) -> tuple[dict[str, Any], set[str]]:
    schema = getattr(tool, "inputSchema", None) or {}
    return (schema.get("properties") or {}, set(schema.get("required") or []))


def _arguments_for_tool(tool: Any, candidates: dict[str, Any]) -> dict[str, Any]:
    """Pass only arguments accepted by this server's live tool schema."""
    properties, required = _tool_schema(tool)
    args = {k: v for k, v in candidates.items() if k in properties and v is not None}
    missing = required - set(args)
    if missing:
        raise FinOpsError(
            f"Missing required arguments for {getattr(tool, 'name', 'MCP tool')}: "
            f"{', '.join(sorted(missing))}"
        )
    return args


async def _call_mcp(session: Any, tool_map: dict[str, Any], name: str,
                    candidates: dict[str, Any] | None = None) -> Any:
    tool = tool_map.get(name)
    if tool is None:
        raise FinOpsError(f"MCP tool '{name}' is not exposed by this server.")
    args = _arguments_for_tool(tool, candidates or {})
    result = await session.call_tool(name, args)
    if getattr(result, "isError", False) or getattr(result, "is_error", False):
        raise FinOpsError(f"{name} returned an MCP error: {flatten_mcp_content(result)}")
    return flatten_mcp_content(result)


def _records_or_ids(payload: Any, category: str) -> list[dict[str, Any]]:
    """Normalize JSON envelopes, subject-keyed schema maps, and MCP text output."""
    if isinstance(payload, dict):
        # list-schemas commonly returns {subject: {version, id, schemaType, ...}}.
        if category == "schemas":
            schema_rows = []
            for subject, value in payload.items():
                if isinstance(value, dict):
                    schema_rows.append({"name": str(subject), "subject": str(subject), **value})
            if schema_rows:
                return schema_rows
        records = extract_records(payload)
        if records:
            return records

    if isinstance(payload, list):
        result: list[dict[str, Any]] = []
        for item in payload:
            result.extend(_records_or_ids(item, category))
        return result

    if not isinstance(payload, str):
        return []

    text_value = payload.strip()
    if not text_value:
        return []

    # Parse labelled blocks emitted by Confluent MCP, retaining useful metadata.
    if category == "environments":
        rows = []
        for block in re.split(r"(?=\bEnvironment:\s*)", text_value):
            name = re.search(r"\bEnvironment:\s*(.+)", block)
            rid = re.search(r"\bID:\s*(env-[A-Za-z0-9_-]+)", block)
            if rid:
                rows.append({"id": rid.group(1), "name": name.group(1).strip() if name else rid.group(1)})
        return rows

    if category == "kafka_clusters":
        rows = []
        for block in re.split(r"(?=\bCluster:\s*)", text_value):
            name = re.search(r"\bCluster:\s*(.+)", block)
            rid = re.search(r"\bID:\s*(lkc-[A-Za-z0-9_-]+)", block)
            env = re.search(r"\bEnvironment ID:\s*(env-[A-Za-z0-9_-]+)", block)
            if rid:
                rows.append({"id": rid.group(1), "name": name.group(1).strip() if name else rid.group(1),
                             "environment_id": env.group(1) if env else None})
        return rows

    if category == "flink_compute_pools":
        rows = []
        for block in re.split(r"(?=\bCompute Pool:\s*)", text_value):
            name = re.search(r"\bCompute Pool:\s*(.+)", block)
            rid = re.search(r"\bID:\s*(lfcp-[A-Za-z0-9_-]+)", block)
            if rid:
                rows.append({"id": rid.group(1), "name": name.group(1).strip() if name else rid.group(1)})
        return rows

    if category == "connectors":
        # list-connectors returns names only; do not manufacture connector IDs.
        rows = []
        for line in text_value.splitlines():
            candidate = line.strip()
            if candidate and not candidate.lower().startswith(("active connectors", "successfully", "pagination:")):
                rows.append({"name": candidate, "_needs_config_lookup": True})
        return rows

    if category == "topics":
        # MCP may return either one topic per line or a single line such as:
        # "Kafka topics: topic-a,topic-b,topic-c".
        rows: list[dict[str, Any]] = []
        for line in text_value.splitlines():
            cleaned = line.strip()
            if not cleaned or cleaned.lower().startswith(("successfully", "pagination")):
                continue
            if re.match(r"^(?:Kafka\s+)?topics?\s*:", cleaned, flags=re.IGNORECASE):
                cleaned = re.sub(r"^(?:Kafka\s+)?topics?\s*:\s*", "", cleaned, flags=re.IGNORECASE)
                for item in re.split(r"[,;]", cleaned):
                    name = item.strip().strip("'\"")
                    if name and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
                        rows.append({"name": name})
                continue
            match = re.search(r"(?:Topic:\s*|^\s*)([A-Za-z0-9][A-Za-z0-9._-]{1,})\s*$", cleaned)
            if match and match.group(1).lower() not in ("topics", "pagination"):
                rows.append({"name": match.group(1)})
        # Preserve order while avoiding duplicate topic rows.
        unique: dict[str, dict[str, Any]] = {}
        for row in rows:
            unique[row["name"]] = row
        return list(unique.values())

    return []


def discover_mcp_tools_sync() -> dict[str, Any]:
    """Retrieve inventory through the same Windows MCP session pattern as FinOps Explorer."""
    if ClientSession is None or stdio_client is None:
        raise FinOpsError("MCP SDK missing. Install it with: python -m pip install mcp")
    if not PARENT_CONFIG.exists():
        raise FinOpsError(f"Existing MCP config was not found: {PARENT_CONFIG}")

    # Load the same parent credentials/environment used by the working explorer.
    load_dotenv(PARENT_ENV, override=False)

    async def run() -> dict[str, Any]:
        if os.name == "nt":
            command = os.getenv("COMSPEC", "cmd.exe")
            args = ["/c", "npx", "-y", MCP_PACKAGE, "--config", str(PARENT_CONFIG)]
        else:
            command = MCP_COMMAND
            args = ["-y", MCP_PACKAGE, "--config", str(PARENT_CONFIG)]

        params = StdioServerParameters(
            command=command,
            args=args,
            env=os.environ.copy(),
            cwd=str(PARENT_DIR),
        )
        async with stdio_client(params, errlog=None) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                listed = await session.list_tools()
                tool_map = {tool.name: tool for tool in listed.tools}
                output: dict[str, Any] = {
                    "tools": [{
                        "name": tool.name,
                        "description": (tool.description or "")[:400],
                        "input_schema": getattr(tool, "inputSchema", {}) or {},
                    } for tool in listed.tools],
                    "inventory": {},
                    "errors": {},
                    "raw_responses": {},
                    "configured_connections": None,
                }

                async def fetch(category: str, tool_name: str,
                                candidates: dict[str, Any] | None = None) -> list[dict[str, Any]]:
                    try:
                        payload = await _call_mcp(session, tool_map, tool_name, candidates)
                        output["raw_responses"][category] = payload
                        rows = _records_or_ids(payload, category)
                        if not rows:
                            output["errors"].setdefault(category, []).append(
                                f"{tool_name} returned a response, but no resource records or IDs could be parsed."
                            )
                        return rows
                    except Exception as exc:
                        output["errors"].setdefault(category, []).append(
                            f"{tool_name}: {type(exc).__name__}: {exc}"
                        )
                        return []

                # This is diagnostic only. It does not require the chatbot/Gemini
                # to execute tools and does not mutate the Confluent account.
                if "list-configured-connections" in tool_map:
                    try:
                        output["configured_connections"] = await _call_mcp(
                            session, tool_map, "list-configured-connections", {}
                        )
                    except Exception as exc:
                        output["errors"].setdefault("configured_connections", []).append(str(exc))

                environments = await fetch("environments", "list-environments", {})
                output["inventory"]["environments"] = environments

                # The working FinOps Explorer first calls list-clusters with no
                # arguments. If the live schema requires an environment, supply it.
                clusters: list[dict[str, Any]] = []
                env_ids = [r.get("id") or r.get("environment_id") or r.get("environmentId")
                           for r in environments]
                env_ids = [str(x) for x in env_ids if x]
                if not env_ids:
                    env_ids = [None]
                for env_id in env_ids:
                    cluster_rows = await fetch(
                        "kafka_clusters", "list-clusters",
                        {"environmentId": env_id, "environment_id": env_id},
                    )
                    for row in cluster_rows:
                        if env_id:
                            row.setdefault("environment_id", env_id)
                    clusters.extend(cluster_rows)
                output["inventory"]["kafka_clusters"] = clusters

                topics: list[dict[str, Any]] = []
                connectors: list[dict[str, Any]] = []
                for cluster in clusters:
                    cluster_id = (cluster.get("id") or cluster.get("cluster_id")
                                  or cluster.get("clusterId"))
                    env_id = (cluster.get("environment_id") or cluster.get("environmentId"))
                    if not cluster_id:
                        continue
                    topic_rows = await fetch(
                        "topics", "list-topics",
                        {"cluster_id": str(cluster_id), "environment_id": str(env_id) if env_id else None,
                         "clusterId": str(cluster_id), "environmentId": str(env_id) if env_id else None},
                    )
                    for row in topic_rows:
                        row.setdefault("cluster_id", str(cluster_id))
                        if env_id:
                            row.setdefault("environment_id", str(env_id))
                    topics.extend(topic_rows)

                    connector_rows = await fetch(
                        "connectors", "list-connectors",
                        {"environmentId": str(env_id) if env_id else None,
                         "clusterId": str(cluster_id)},
                    )
                    # list-connectors may return names only. Resolve each name
                    # through get-connector-config, using that tool's live schema.
                    config_tool = tool_map.get("get-connector-config")
                    for row in connector_rows:
                        row.setdefault("cluster_id", str(cluster_id))
                        if env_id:
                            row.setdefault("environment_id", str(env_id))
                        if row.get("_needs_config_lookup") and config_tool is not None:
                            connector_name = row.get("name")
                            # The live MCP schema uses the exact camelCase key
                            # connectorName. Pass that key explicitly: do not
                            # derive it from a potentially incomplete schema wrapper.
                            args = {"connectorName": str(connector_name)}
                            if env_id:
                                args["environmentId"] = str(env_id)
                            if cluster_id:
                                args["clusterId"] = str(cluster_id)
                            try:
                                config_result = await session.call_tool("get-connector-config", args)
                                if getattr(config_result, "isError", False) or getattr(config_result, "is_error", False):
                                    raise FinOpsError(f"get-connector-config returned an MCP error: {flatten_mcp_content(config_result)}")
                                config_payload = flatten_mcp_content(config_result)
                                output["raw_responses"].setdefault("connector_configs", {})[str(connector_name)] = config_payload
                                config_records = extract_records(config_payload)
                                config_record = config_records[0] if config_records else (config_payload if isinstance(config_payload, dict) else {})
                                if isinstance(config_record, dict):
                                    row.update(config_record)
                                    row["name"] = connector_name
                                    row.pop("_needs_config_lookup", None)
                            except Exception as exc:
                                output["errors"].setdefault("connectors", []).append(
                                    f"get-connector-config ({connector_name}): {type(exc).__name__}: {exc}"
                                )
                    connectors.extend(connector_rows)
                output["inventory"]["topics"] = topics
                output["inventory"]["connectors"] = connectors

                compute_pools: list[dict[str, Any]] = []
                schemas: list[dict[str, Any]] = []
                for env_id in env_ids:
                    pools = await fetch(
                        "flink_compute_pools", "list-compute-pools",
                        {"environmentId": env_id, "pageSize": 1000},
                    )
                    for row in pools:
                        row.setdefault("environment_id", env_id)
                    compute_pools.extend(pools)
                    schema_rows = await fetch(
                        "schemas", "list-schemas",
                        {"environment_id": env_id, "latestOnly": True},
                    )
                    for row in schema_rows:
                        row.setdefault("environment_id", env_id)
                    schemas.extend(schema_rows)
                output["inventory"]["flink_compute_pools"] = compute_pools
                output["inventory"]["schemas"] = schemas

                # De-duplicate records by ID/name while preserving environment.
                for category, rows in output["inventory"].items():
                    unique: dict[str, dict[str, Any]] = {}
                    for row in rows:
                        identity = (row.get("id") or row.get("resource_id") or
                                    row.get("cluster_id") or row.get("connector_id") or
                                    row.get("name") or json.dumps(row, sort_keys=True, default=str))
                        key = f"{row.get('environment_id', '')}:{identity}"
                        unique[key] = row
                    output["inventory"][category] = list(unique.values())

                output["inventory_record_count"] = sum(
                    len(rows) for rows in output["inventory"].values() if isinstance(rows, list)
                )
                if output["inventory_record_count"] == 0:
                    output["errors"].setdefault("inventory", []).append(
                        "MCP tools were discovered, but no inventory records were parsed. "
                        "See raw_responses and configured_connections in diagnostics."
                    )
                return output

    try:
        return asyncio.run(asyncio.wait_for(run(), timeout=MCP_TIMEOUT))
    except asyncio.TimeoutError as exc:
        raise FinOpsError(f"Confluent MCP request timed out after {MCP_TIMEOUT} seconds.") from exc
    except RuntimeError as exc:
        raise FinOpsError(f"Could not start MCP async client: {exc}") from exc


@st.cache_data(ttl=300, show_spinner=False)
def get_backend_inventory() -> dict[str, Any]:
    """Cache inventory for five minutes to avoid launching MCP per question."""
    return discover_mcp_tools_sync()


# ---------------------------------------------------------------------------
# Billing API
# ---------------------------------------------------------------------------

def fetch_billing(
    start_date: date,
    end_date_exclusive: date,
) -> list[dict[str, Any]]:
    if not CONFLUENT_API_KEY or not CONFLUENT_API_SECRET:
        raise FinOpsError(
            "Confluent billing credentials were not found in the parent .env."
        )
    if end_date_exclusive <= start_date:
        raise FinOpsError("End date must be later than start date.")

    session = requests.Session()
    session.auth = (CONFLUENT_API_KEY, CONFLUENT_API_SECRET)
    session.headers.update({"Accept": "application/json"})

    all_rows: list[dict[str, Any]] = []
    current = start_date

    while current < end_date_exclusive:
        window_end = min(
            current + timedelta(days=MAX_DAYS_PER_BILLING_REQUEST),
            end_date_exclusive,
        )
        params: dict[str, Any] = {
            "start_date": current.isoformat(),
            "end_date": window_end.isoformat(),
            "page_size": 1000,
        }

        while True:
            response = session.get(
                BILLING_API_URL,
                params=params,
                timeout=HTTP_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            all_rows.extend(
                row for row in payload.get("data", [])
                if isinstance(row, dict)
            )
            next_token = (payload.get("metadata") or {}).get("next")
            if not next_token:
                break
            params["page_token"] = next_token

        current = window_end

    return all_rows


@st.cache_data(ttl=300, show_spinner=False)
def get_backend_billing(start_iso: str, end_exclusive_iso: str) -> list[dict[str, Any]]:
    return fetch_billing(
        date.fromisoformat(start_iso),
        date.fromisoformat(end_exclusive_iso),
    )


# ---------------------------------------------------------------------------
# Resource normalization and deterministic FinOps calculations
# ---------------------------------------------------------------------------

def inventory_resource_id(row: dict[str, Any]) -> str | None:
    value = (
        row.get("id")
        or row.get("resource_id")
        or row.get("cluster_id")
        or row.get("connector_id")
        or row.get("topic_id")
        or row.get("compute_pool_id")
        or row.get("schema_id")
    )
    return str(value) if value else None


def inventory_name(row: dict[str, Any]) -> str | None:
    return (
        row.get("name")
        or row.get("display_name")
        or row.get("resource_name")
    )


def flatten_inventory(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for resource_type, records in inventory.items():
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            rows.append({
                "resource_type": resource_type,
                "resource_id": inventory_resource_id(record),
                "name": inventory_name(record),
                "environment_id": (
                    record.get("environment_id")
                    or (record.get("environment", {}).get("id")
                        if isinstance(record.get("environment"), dict) else None)
                ),
                "raw": record,
            })
    return rows


def normalize_billing(row: dict[str, Any]) -> dict[str, Any]:
    resource = row.get("resource")
    if not isinstance(resource, dict):
        resource = {}

    environment = resource.get("environment")
    if not isinstance(environment, dict):
        environment = {}

    return {
        "resource_id": (
            resource.get("id")
            or row.get("resource_id")
            or row.get("resource.id")
        ),
        "resource_name": (
            resource.get("display_name")
            or row.get("resource_display_name")
            or row.get("resource.display_name")
        ),
        "environment_id": environment.get("id") or row.get("environment_id"),
        "product": row.get("product"),
        "line_type": row.get("line_type"),
        "amount": as_float(row.get("amount")),
        "quantity": row.get("quantity"),
        "unit": row.get("unit"),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
    }


def calculate_summary(
    billing_rows: list[dict[str, Any]],
    inventory_data: dict[str, Any],
) -> dict[str, Any]:
    billing = [normalize_billing(row) for row in billing_rows]
    inventory = flatten_inventory(inventory_data)

    inventory_ids = {
        row["resource_id"] for row in inventory if row["resource_id"]
    }

    promo_rows = [
        row for row in billing
        if str(row.get("line_type") or "").upper() == "PROMO_CREDIT"
    ]
    non_promo_rows = [
        row for row in billing
        if str(row.get("line_type") or "").upper() != "PROMO_CREDIT"
    ]

    grouped: dict[str, dict[str, Any]] = {}
    unallocated = []

    for row in billing:
        rid = row.get("resource_id")
        line_type = str(row.get("line_type") or "").upper()

        # Promo credits remain negative and unallocated.
        if line_type == "PROMO_CREDIT" or not rid:
            unallocated.append(row)
            continue

        rid = str(rid)
        if rid not in grouped:
            grouped[rid] = {
                "resource_id": rid,
                "resource_name": row.get("resource_name"),
                "amount": 0.0,
                "record_count": 0,
                "products": set(),
            }

        grouped[rid]["amount"] += row["amount"]
        grouped[rid]["record_count"] += 1
        if row.get("product"):
            grouped[rid]["products"].add(str(row["product"]))

    resources = []
    unmatched = []

    for item in grouped.values():
        rid = item["resource_id"]
        if not inventory_ids:
            status = "not verified (inventory IDs unavailable)"
        elif rid in inventory_ids:
            status = "matched by exact resource ID"
        else:
            status = "unmatched (possibly historical)"

        result = {
            **item,
            "products": sorted(item["products"]),
            "match_status": status,
        }
        resources.append(result)

        if inventory_ids and rid not in inventory_ids:
            unmatched.append(result)

    resources.sort(key=lambda row: row["amount"], reverse=True)

    return {
        "billing_count": len(billing),
        "inventory_count": len(inventory),
        "gross": sum(row["amount"] for row in non_promo_rows),
        "promo_credit": sum(row["amount"] for row in promo_rows),
        "net": sum(row["amount"] for row in billing),
        "resources": resources,
        "unallocated": unallocated,
        "unmatched": unmatched,
        "inventory": inventory,
    }


def build_finops_context(summary: dict[str, Any]) -> str:
    if not summary["billing_count"] and not summary["inventory_count"]:
        return (
            "No billing or inventory data was retrieved. "
            "Do not invent account-specific facts or costs."
        )

    sections = []

    if summary["billing_count"]:
        sections.append(
            "ACTUAL BILLING SUMMARY\n"
            f"Billing rows: {summary['billing_count']}\n"
            f"Gross non-promo amount: {money(summary['gross'])}\n"
            f"Promo credits (negative and unallocated): "
            f"{money(summary['promo_credit'])}\n"
            f"Net amount: {money(summary['net'])}\n"
            "These totals are calculated from Confluent billing line items."
        )

        resource_lines = []
        for item in summary["resources"][:MAX_CONTEXT_ROWS]:
            resource_lines.append(
                f"- Resource ID: {item['resource_id']}; "
                f"name: {item.get('resource_name')}; "
                f"amount: {money(item['amount'])}; "
                f"records: {item['record_count']}; "
                f"products: {', '.join(item['products']) or 'not specified'}; "
                f"status: {item['match_status']}"
            )

        sections.append(
            "RESOURCE COST BREAKDOWN (exact resource ID matching only)\n"
            + ("\n".join(resource_lines) or "No resource-attributed rows.")
        )

        unallocated_lines = [
            f"- line_type={row.get('line_type')}; "
            f"amount={money(row['amount'])}; "
            f"product={row.get('product')}; "
            f"resource_id={row.get('resource_id')}"
            for row in summary["unallocated"][:MAX_CONTEXT_ROWS]
        ]
        sections.append(
            "UNALLOCATED BILLING ROWS\n"
            + ("\n".join(unallocated_lines) or "None.")
        )

    if summary["inventory_count"]:
        inventory_lines = [
            f"- Type: {row['resource_type']}; "
            f"ID: {row.get('resource_id')}; "
            f"Name: {row.get('name')}; "
            f"Environment: {row.get('environment_id')}"
            for row in summary["inventory"][:MAX_CONTEXT_ROWS]
        ]
        sections.append("LIVE INVENTORY\n" + "\n".join(inventory_lines))

    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Gemini response generation — FinOps only
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a beginner-friendly Confluent Cloud FinOps assistant.
Your scope is ONLY FinOps: billing, expenditure, resource-level costs,
usage charges, promo credits, resource reconciliation, and cost optimization.

Use only the supplied live billing and inventory data for account-specific
facts. Do not use or refer to Confluent product documentation.

Rules:
- Never invent amounts, resource IDs, names, usage, or billing periods.
- Use the exact totals calculated by Python.
- Match billing and inventory only by exact resource IDs, never by names.
- Keep unmatched/historical resource IDs visible as unmatched.
- If inventory retrieval failed or returned no IDs, say matching is unverified.
- Keep promo credits negative and unallocated.
- Distinguish actual billed amounts from estimates and examples.
- Do not claim a specific saving without sufficient data.
- Give practical optimization suggestions, clearly labelled as suggestions.
- If data does not support an answer, state what is missing.
- Never reveal credentials or environment variable values.
Explain financial concepts in plain English.
"""


def generate_answer(question: str, context: str) -> str:
    if not GOOGLE_API_KEY:
        raise FinOpsError("GOOGLE_API_KEY is missing from finops-chatbot/.env.")

    configure_gemini()
    prompt = f"""{SYSTEM_PROMPT}

LIVE FINOPS ACCOUNT DATA:
{context}

USER QUESTION:
{question}

Answer directly and clearly. For cost questions, state whether the amount
comes from actual billing rows. Do not guess if information is unavailable.
"""
    model = genai.GenerativeModel(GEMINI_MODEL)
    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.2,
            "max_output_tokens": 1800,
        },
    )

    answer = getattr(response, "text", None)
    if not answer:
        raise FinOpsError("Gemini returned an empty response.")
    return answer.strip()


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

def render_sidebar() -> tuple[date, date]:
    with st.sidebar:
        st.header("Backend connection")
        st.caption("MCP configuration")
        st.code(str(PARENT_CONFIG), language=None)

        if not PARENT_CONFIG.exists():
            st.error("Parent config.yaml was not found.")

        st.divider()
        st.subheader("Billing period")

        today = date.today()
        start = st.date_input(
            "Start date (inclusive)",
            value=today.replace(day=1),
            key="billing_start",
        )
        end = st.date_input(
            "End date (inclusive)",
            value=today,
            key="billing_end",
        )

        st.caption(
            "Billing and inventory are retrieved by the backend. "
            "No file upload is needed."
        )

        if st.button("Refresh backend data", use_container_width=True):
            get_backend_billing.clear()
            get_backend_inventory.clear()
            st.rerun()

        return start, end


def render_summary(summary: dict[str, Any]) -> None:
    st.subheader("FinOps overview")

    if summary["billing_count"]:
        col1, col2, col3 = st.columns(3)
        col1.metric("Gross non-promo", money(summary["gross"]))
        col2.metric("Promo credits", money(summary["promo_credit"]))
        col3.metric("Net billing", money(summary["net"]))
        st.caption(f"{summary['billing_count']} billing line items")
    else:
        st.warning("No billing rows were returned for the selected period.")

    if summary["resources"]:
        st.subheader("Resource cost breakdown")
        table_rows = [
            {
                "Resource ID": item["resource_id"],
                "Resource name": item.get("resource_name"),
                "Amount": item["amount"],
                "Billing rows": item["record_count"],
                "Products": ", ".join(item["products"]),
                "Reconciliation": item["match_status"],
            }
            for item in summary["resources"]
        ]
        st.dataframe(
            pd.DataFrame(table_rows),
            use_container_width=True,
            hide_index=True,
        )

    if summary["unmatched"]:
        with st.expander(
            f"Unmatched resource IDs ({len(summary['unmatched'])})"
        ):
            st.dataframe(
                pd.DataFrame([
                    {
                        "Resource ID": item["resource_id"],
                        "Resource name": item.get("resource_name"),
                        "Amount": item["amount"],
                    }
                    for item in summary["unmatched"]
                ]),
                use_container_width=True,
                hide_index=True,
            )


def persist_history(messages: list[dict[str, Any]]) -> None:
    path = HISTORY_DIR / f"chat_{date.today().isoformat()}.json"
    write_json(path, messages)


def main() -> None:
    st.set_page_config(
        page_title="Confluent FinOps Chatbot",
        layout="wide",
    )
    st.title("Confluent FinOps Chatbot")
    st.caption(
        "Backend-connected billing and inventory analysis. "
        "No manual data uploads or documentation retrieval."
    )

    start_date, end_date = render_sidebar()

    if end_date < start_date:
        st.error("Billing end date cannot be earlier than start date.")
        return

    # Retrieve billing and inventory from backend. Both are cached for 5 min.
    billing_rows: list[dict[str, Any]] = []
    inventory_result: dict[str, Any] = {
        "tools": [],
        "inventory": {},
        "errors": {},
    }

    left, right = st.columns(2)

    with left:
        try:
            with st.spinner("Retrieving billing from Confluent Cloud..."):
                billing_rows = get_backend_billing(
                    start_date.isoformat(),
                    (end_date + timedelta(days=1)).isoformat(),
                )
            st.success(f"Billing connected: {len(billing_rows)} rows")
        except Exception as exc:
            st.error(f"Billing retrieval failed: {exc}")
            logger.exception("Billing retrieval failed")

    with right:
        try:
            with st.spinner("Retrieving inventory through Confluent MCP..."):
                inventory_result = get_backend_inventory()
            inventory_total = sum(
                len(records)
                for records in inventory_result.get("inventory", {}).values()
                if isinstance(records, list)
            )
            if inventory_total > 0:
                st.success(f"MCP inventory retrieved: {inventory_total} records")
            else:
                st.error("MCP tools discovered, but inventory retrieval returned zero records. See diagnostics.")
        except Exception as exc:
            st.error(f"MCP inventory retrieval failed: {exc}")
            logger.exception("MCP inventory retrieval failed")

    inventory_data = inventory_result.get("inventory", {})
    summary = calculate_summary(billing_rows, inventory_data)

    render_summary(summary)

    with st.expander("Backend diagnostics", expanded=False):
        st.write("MCP config:", str(PARENT_CONFIG))
        st.write("Discovered MCP tools:")
        tool_rows = inventory_result.get("tools", [])
        if tool_rows:
            st.dataframe(
                pd.DataFrame([
                    {"Tool": row["name"], "Description": row["description"]}
                    for row in tool_rows
                ]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.write("No MCP tools were discovered.")

        connections = inventory_result.get("configured_connections")
        if connections is not None:
            st.write("Configured MCP connections (non-secret response):")
            st.json(connections)
        errors = inventory_result.get("errors", {})
        st.write("Inventory tool errors / parsing warnings:")
        st.json(errors if errors else {"status": "No tool errors reported."})
        raw_responses = inventory_result.get("raw_responses", {})
        if raw_responses:
            with st.expander("Raw MCP inventory responses", expanded=False):
                st.json(raw_responses)

    if not GOOGLE_API_KEY:
        st.warning(
            "GOOGLE_API_KEY is missing. Add it to finops-chatbot/.env."
        )

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    question = st.chat_input(
        "Ask about expenditure, resource costs, billing, or optimization..."
    )
    if not question:
        return

    st.session_state.messages.append(
        {"role": "user", "content": question}
    )
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        try:
            context = build_finops_context(summary)
            with st.spinner("Analyzing live FinOps data..."):
                answer = generate_answer(question, context)
            st.markdown(answer)
            st.session_state.messages.append(
                {"role": "assistant", "content": answer}
            )
            persist_history(st.session_state.messages)
        except Exception as exc:
            error_message = f"Unable to answer: {exc}"
            st.error(error_message)
            logger.exception("Answer generation failed")
            st.session_state.messages.append(
                {"role": "assistant", "content": error_message}
            )
            persist_history(st.session_state.messages)


if __name__ == "__main__":
    main()
