import asyncio
import base64
import json
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from mcp_client import get_mcp_session


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent
PARENT_DIR = PROJECT_DIR.parent
RESULTS_DIR = PROJECT_DIR / "results"
LOCAL_ENV_FILE = PROJECT_DIR / ".env"
PARENT_ENV_FILE = PARENT_DIR / ".env"
CONFIG_PATH = PARENT_DIR / "config.yaml"

BILLING_API_URL = "https://api.confluent.cloud/billing/v1/costs"
DEFAULT_PAGE_SIZE = 1000
MAX_BILLING_RANGE_DAYS = 31
REQUEST_TIMEOUT = 60

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
load_dotenv(PARENT_ENV_FILE, override=False)
load_dotenv(LOCAL_ENV_FILE, override=False)

st.set_page_config(
    page_title="Confluent FinOps Explorer",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Confluent FinOps Explorer")
st.caption(
    "Organization-wide Confluent Cloud resource discovery, billing retrieval, "
    "resource correlation and usage visibility."
)


# ============================================================
# MCP TOOL DISCOVERY
# ============================================================

DISCOVERY_TOOL_NAMES = [
    "list-organizations",
    "list-environments",
    "list-clusters",
    "list-topics",
    "get-topic-config",
    "list-connectors",
    "get-connector-config",
    "get-connector-status",
    "list-schemas",
    "list-compute-pools",
    "list-consumer-groups",
    "list-tags",
    "list-available-metrics",
    "query-metrics",
]

READ_ONLY_TOOLS = DISCOVERY_TOOL_NAMES + [
    "read-environment",
    "get-connector-offsets",
    "get-connector-tasks",
    "get-connector-error-summary",
    "get-connector-error-recommendations",
    "get-connector-logs",
    "describe-consumer-group",
    "get-consumer-group-lag",
    "get-partition-offsets",
    "search-topics-by-name",
    "search-topics-by-tag",
    "list-configured-connections",
    "describe-configured-connection",
    "explain-disabled-tools",
    "config-help",
]

TOOL_DESCRIPTIONS = {
    "list-organizations": "Discover Confluent organizations.",
    "list-environments": "Discover environments.",
    "list-clusters": "Discover Kafka clusters.",
    "list-topics": "Discover topics in a Kafka cluster.",
    "get-topic-config": "Retrieve a topic configuration.",
    "list-connectors": "Discover connectors.",
    "get-connector-config": "Retrieve connector configuration.",
    "get-connector-status": "Retrieve connector status.",
    "list-schemas": "Discover Schema Registry subjects and schemas.",
    "list-compute-pools": "Discover Flink compute pools.",
    "list-consumer-groups": "Discover consumer groups.",
    "list-tags": "Discover available tags.",
    "list-available-metrics": "Discover queryable telemetry metrics.",
    "query-metrics": "Query Confluent Cloud usage metrics.",
}


def extract_mcp_result(result: Any) -> dict:
    output = {
        "is_error": bool(getattr(result, "isError", False)),
        "structured_content": getattr(result, "structuredContent", None),
        "content": [],
    }
    for item in getattr(result, "content", []):
        output["content"].append(
            item.text if hasattr(item, "text") else str(item)
        )
    return output


async def get_mcp_tools() -> list[dict]:
    async with get_mcp_session() as session:
        response = await session.list_tools()
        return [
            {
                "name": tool.name,
                "description": tool.description or "",
                "input_schema": getattr(tool, "inputSchema", {}) or {},
            }
            for tool in response.tools
        ]


async def execute_mcp_tool(tool_name: str, arguments: dict) -> dict:
    async with get_mcp_session() as session:
        result = await session.call_tool(tool_name, arguments=arguments)
        return extract_mcp_result(result)


def run_async(coroutine):
    try:
        return asyncio.run(coroutine)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def save_json(filename: str, data: Any) -> Path:
    path = RESULTS_DIR / filename
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False, default=str)
    return path


def result_text(result: dict) -> str:
    return "\n".join(result.get("content") or [])


def combined_result_payload(result: dict) -> Any:
    structured = result.get("structured_content")
    if structured not in (None, {}, []):
        return structured
    return result_text(result)


def schema_properties(tool: dict) -> tuple[dict, list]:
    schema = tool.get("input_schema") or {}
    return schema.get("properties", {}) or {}, schema.get("required", []) or []


def infer_argument_value(name: str, context: dict) -> Any:
    """Best-effort mapping for common Confluent MCP argument names."""
    key = name.lower()
    aliases = {
        "environment_id": ["environment_id", "env_id", "environment"],
        "env_id": ["env_id", "environment_id", "environment"],
        "cluster_id": ["cluster_id", "kafka_cluster_id", "cluster"],
        "kafka_cluster_id": ["kafka_cluster_id", "cluster_id", "cluster"],
        "topic_name": ["topic_name", "topic"],
        "topic": ["topic", "topic_name"],
        "connector_name": ["connector_name", "name", "connector"],
        "name": ["name", "connector_name", "topic_name"],
        "compute_pool_id": ["compute_pool_id", "pool_id"],
        "subject": ["subject", "subject_name"],
    }
    for candidate in aliases.get(key, [key]):
        if candidate in context and context[candidate] not in (None, ""):
            return context[candidate]
    return None


def build_arguments(tool: dict, context: dict | None = None) -> tuple[dict, list]:
    context = context or {}
    properties, required = schema_properties(tool)
    arguments = {}
    unresolved = []
    for name, spec in properties.items():
        value = infer_argument_value(name, context)
        if value is not None:
            arguments[name] = value
        elif name in required:
            unresolved.append(name)
        elif "default" in spec:
            arguments[name] = spec["default"]
    return arguments, unresolved


def call_discovery_tool(
    tool_map: dict,
    tool_name: str,
    context: dict | None = None,
) -> dict:
    if tool_name not in tool_map:
        return {
            "is_error": True,
            "content": [f"Tool is not available in this MCP server: {tool_name}"],
            "structured_content": None,
        }

    tool = tool_map[tool_name]
    arguments, unresolved = build_arguments(tool, context)
    if unresolved:
        return {
            "is_error": True,
            "content": [
                "Required arguments could not be inferred: "
                + ", ".join(unresolved)
            ],
            "structured_content": None,
        }

    try:
        return run_async(execute_mcp_tool(tool_name, arguments))
    except Exception as exc:
        return {
            "is_error": True,
            "content": [str(exc)],
            "structured_content": None,
        }


# ============================================================
# RESOURCE RESPONSE NORMALIZATION
# ============================================================

def walk_objects(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_objects(child)


def find_value(obj: dict, names: list[str]):
    lowered = {str(k).lower(): v for k, v in obj.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value not in (None, ""):
            return value
    return None


def extract_resource_objects(payload: Any) -> list[dict]:
    """Extract object records from MCP structured or textual responses."""
    if isinstance(payload, dict):
        candidates = []
        for key in (
            "data", "items", "results", "environments", "clusters",
            "topics", "connectors", "schemas", "subjects", "compute_pools",
            "consumer_groups", "organizations",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend(x for x in value if isinstance(x, dict))
        if candidates:
            return candidates
        return [payload]

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, str):
        return []

    text = payload.strip()
    # Some MCP tools return JSON embedded in a text block.
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char not in "[{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[index:])
            found = extract_resource_objects(parsed)
            if found:
                return found
        except json.JSONDecodeError:
            continue

    # Fallback for the known plain-text list-connectors/list-compute-pools output.
    records = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("Connector:"):
            records.append({"name": line.split(":", 1)[1].strip()})
        elif line.startswith("Compute Pool:"):
            records.append({"name": line.split(":", 1)[1].strip()})
    return records


def normalize_resource(
    resource_type: str,
    item: dict,
    environment_id: str | None = None,
    cluster_id: str | None = None,
) -> dict:
    resource_id = find_value(
        item,
        ["id", "resource_id", "cluster_id", "topic_id", "connector_id",
         "compute_pool_id", "subject_id", "link_id"],
    )
    name = find_value(
        item,
        ["name", "display_name", "topic_name", "connector_name",
         "subject", "subject_name", "cluster_name"],
    )
    resource_environment = item.get("environment") or {}
    if isinstance(resource_environment, dict):
        environment_id = (
            resource_environment.get("id")
            or item.get("environment_id")
            or environment_id
        )
    else:
        environment_id = item.get("environment_id") or environment_id

    return {
        "resource_type": resource_type,
        "resource_id": str(resource_id) if resource_id else None,
        "resource_name": str(name) if name else None,
        "environment_id": environment_id,
        "cluster_id": item.get("cluster_id") or cluster_id,
        "details": item,
    }


def resource_key(resource: dict) -> tuple:
    return (
        resource.get("resource_type"),
        resource.get("resource_id"),
        resource.get("resource_name"),
        resource.get("environment_id"),
        resource.get("cluster_id"),
    )


def discover_organization_resources(tool_map: dict) -> dict:
    """Discover organization resources through read-only MCP tools."""
    resources = []
    calls = []
    failures = []

    def invoke(tool_name: str, context: dict | None = None):
        result = call_discovery_tool(tool_map, tool_name, context)
        calls.append({
            "tool": tool_name,
            "arguments_context": context or {},
            "is_error": result.get("is_error", False),
            "content": result.get("content", []),
            "structured_content": result.get("structured_content"),
        })
        if result.get("is_error"):
            failures.append({
                "tool": tool_name,
                "context": context or {},
                "error": result_text(result),
            })
        return result

    def add(tool_name: str, resource_type: str, context=None,
            environment_id=None, cluster_id=None):
        result = invoke(tool_name, context)
        if result.get("is_error"):
            return []
        payload = combined_result_payload(result)
        items = extract_resource_objects(payload)
        normalized = [
            normalize_resource(resource_type, item, environment_id, cluster_id)
            for item in items
        ]
        resources.extend(normalized)
        return normalized

    # Organization and environment discovery.
    add("list-organizations", "organization")
    environments = add("list-environments", "environment")

    # If the tool returns text and the generic parser could not extract IDs,
    # preserve the raw result and continue with the known environment context
    # from the session's previously saved inventory, if available.
    saved_env_file = RESULTS_DIR / "environments.json"
    if not any(r["resource_type"] == "environment" for r in resources):
        if saved_env_file.exists():
            try:
                old = json.loads(saved_env_file.read_text(encoding="utf-8"))
                for item in extract_resource_objects(old):
                    resources.append(normalize_resource("environment", item))
            except Exception:
                pass

    env_resources = [r for r in resources if r["resource_type"] == "environment"]
    env_ids = sorted({
        r["resource_id"] for r in env_resources if r.get("resource_id")
    })

    # Discover clusters; the MCP list-clusters tool may be organization-wide.
    clusters = add("list-clusters", "kafka_cluster")
    cluster_resources = [r for r in resources if r["resource_type"] == "kafka_cluster"]

    # Recover cluster IDs from the existing discovery output if the current
    # server response is text-only and cannot be normalized.
    saved_cluster_file = RESULTS_DIR / "clusters.json"
    if not cluster_resources and saved_cluster_file.exists():
        try:
            old = json.loads(saved_cluster_file.read_text(encoding="utf-8"))
            for item in extract_resource_objects(old):
                resources.append(normalize_resource("kafka_cluster", item))
            cluster_resources = [
                r for r in resources if r["resource_type"] == "kafka_cluster"
            ]
        except Exception:
            pass

    # Use cluster/environment pairs. When environment is absent, use all known
    # environments as a fallback only if the MCP tool requires it.
    cluster_pairs = []
    for cluster in cluster_resources:
        cluster_id = cluster.get("resource_id")
        env_id = cluster.get("environment_id")
        if cluster_id:
            cluster_pairs.append((cluster_id, env_id))

    for cluster_id, env_id in cluster_pairs:
        context = {
            "cluster_id": cluster_id,
            "environment_id": env_id,
            "env_id": env_id,
        }
        add(
            "list-topics", "topic", context,
            environment_id=env_id, cluster_id=cluster_id
        )
        add(
            "list-consumer-groups", "consumer_group", context,
            environment_id=env_id, cluster_id=cluster_id
        )

    # Environment-scoped discovery.
    for env_id in env_ids or [None]:
        context = {"environment_id": env_id, "env_id": env_id}
        add("list-connectors", "connector", context, environment_id=env_id)
        add("list-schemas", "schema_subject", context, environment_id=env_id)
        add("list-compute-pools", "flink_compute_pool", context, environment_id=env_id)

    # De-duplicate without dropping different resources that share a name.
    unique = {}
    for resource in resources:
        unique[resource_key(resource)] = resource
    resources = list(unique.values())

    return {
        "discovered_at": date.today().isoformat(),
        "resource_count": len(resources),
        "resources": resources,
        "tool_calls": calls,
        "failures": failures,
    }


# ============================================================
# BILLING REST API
# ============================================================

def get_billing_credentials() -> tuple[str, str]:
    api_key = os.getenv("CONFLUENT_CLOUD_API_KEY")
    api_secret = os.getenv("CONFLUENT_CLOUD_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError(
            "Missing CONFLUENT_CLOUD_API_KEY or "
            "CONFLUENT_CLOUD_API_SECRET in the parent .env file."
        )
    return api_key, api_secret


def get_billing_headers() -> dict:
    api_key, api_secret = get_billing_credentials()
    encoded = base64.b64encode(
        f"{api_key}:{api_secret}".encode("utf-8")
    ).decode("ascii")
    return {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded}",
    }


def retrieve_billing(
    start_date: date,
    end_date: date,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """
    Retrieve organization-wide billing. end_date follows the Billing API's
    exclusive-end convention. Keep every raw line item, including records
    missing product/resource attributes.
    """
    if end_date <= start_date:
        raise ValueError("End date must be later than start date.")
    if (end_date - start_date).days > MAX_BILLING_RANGE_DAYS:
        raise ValueError(
            f"Billing date range cannot exceed {MAX_BILLING_RANGE_DAYS} days."
        )
    if not 1 <= page_size <= DEFAULT_PAGE_SIZE:
        raise ValueError(f"Page size must be between 1 and {DEFAULT_PAGE_SIZE}.")

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "page_size": page_size,
    }
    headers = get_billing_headers()
    records, pages, seen_tokens = [], [], set()

    while True:
        response = requests.get(
            BILLING_API_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Billing API HTTP {response.status_code}: {response.text}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Billing API returned invalid JSON.") from exc

        if payload.get("errors"):
            raise RuntimeError(json.dumps(payload["errors"], indent=2))
        if not isinstance(payload.get("data"), list):
            raise RuntimeError("Billing API response does not contain a data array.")

        records.extend(payload["data"])
        pages.append(payload)
        next_token = (payload.get("metadata") or {}).get("next")
        if not next_token:
            break
        next_token = str(next_token)
        if next_token in seen_tokens:
            raise RuntimeError("Repeated billing pagination token detected.")
        seen_tokens.add(next_token)
        params["page_token"] = next_token

    return {
        "start_date": start_date.isoformat(),
        "end_date_exclusive": end_date.isoformat(),
        "page_size": page_size,
        "record_count": len(records),
        "records": records,
        "pages": pages,
        "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }


# ============================================================
# BILLING NORMALIZATION AND RESOURCE CORRELATION
# ============================================================

def billing_dataframe(records: list[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        resource = record.get("resource") or {}
        environment = resource.get("environment") or {}
        rows.append({
            "product": record.get("product") or "UNKNOWN",
            "line_type": record.get("line_type") or record.get("type") or "UNKNOWN",
            "resource_id": resource.get("id"),
            "resource_name": resource.get("display_name"),
            "environment_id": environment.get("id"),
            "start_date": record.get("start_date"),
            "end_date": record.get("end_date"),
            "granularity": record.get("granularity"),
            "quantity": record.get("quantity"),
            "unit": record.get("unit"),
            "price": record.get("price"),
            "original_amount": record.get("original_amount", 0) or 0,
            "discount_amount": record.get("discount_amount", 0) or 0,
            "amount": record.get("amount", 0) or 0,
            "network_access_type": record.get("network_access_type"),
            "raw_record": record,
        })
    return pd.DataFrame(rows)


def resource_inventory_dataframe(inventory: dict) -> pd.DataFrame:
    rows = []
    for resource in inventory.get("resources", []):
        rows.append({
            "resource_type": resource.get("resource_type"),
            "resource_name": resource.get("resource_name"),
            "resource_id": resource.get("resource_id"),
            "environment_id": resource.get("environment_id"),
            "cluster_id": resource.get("cluster_id"),
        })
    return pd.DataFrame(rows)


def correlate_billing(
    billing_df: pd.DataFrame,
    inventory: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Match actual billing rows to discovered inventory by exact resource ID."""
    if billing_df.empty:
        return billing_df.copy(), pd.DataFrame()

    inventory_df = resource_inventory_dataframe(inventory)
    if inventory_df.empty:
        result = billing_df.copy()
        result["matched_resource_type"] = None
        result["match_status"] = "UNMATCHED"
        return result, inventory_df

    # Index only exact IDs. Do not guess a match from display names.
    id_map = {}
    for _, row in inventory_df.iterrows():
        resource_id = row.get("resource_id")
        if pd.notna(resource_id) and str(resource_id).strip():
            id_map.setdefault(str(resource_id), []).append(row.to_dict())

    matched_types, matched_names, statuses = [], [], []
    for _, billing_row in billing_df.iterrows():
        resource_id = billing_row.get("resource_id")
        candidates = id_map.get(str(resource_id), []) if pd.notna(resource_id) else []
        if candidates:
            # Prefer an exact resource ID and, where available, matching environment.
            env_id = billing_row.get("environment_id")
            selected = next(
                (
                    item for item in candidates
                    if env_id and item.get("environment_id") == env_id
                ),
                candidates[0],
            )
            matched_types.append(selected.get("resource_type"))
            matched_names.append(selected.get("resource_name"))
            statuses.append("DIRECT_RESOURCE_MATCH")
        elif pd.isna(resource_id) or not str(resource_id).strip():
            matched_types.append(None)
            matched_names.append(None)
            statuses.append("ORGANIZATION_LEVEL_OR_NO_RESOURCE_ID")
        else:
            matched_types.append(None)
            matched_names.append(None)
            statuses.append("RESOURCE_ID_NOT_IN_DISCOVERED_INVENTORY")

    result = billing_df.copy()
    result["matched_resource_type"] = matched_types
    result["matched_inventory_name"] = matched_names
    result["match_status"] = statuses
    return result, inventory_df


def make_resource_cost_table(correlated_df: pd.DataFrame) -> pd.DataFrame:
    if correlated_df.empty:
        return pd.DataFrame()
    resource_rows = correlated_df[
        correlated_df["resource_id"].notna()
        & (correlated_df["resource_id"].astype(str).str.strip() != "")
    ]
    if resource_rows.empty:
        return pd.DataFrame()
    return (
        resource_rows.groupby(
            [
                "product", "matched_resource_type", "resource_name",
                "resource_id", "environment_id", "match_status",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            amount=("amount", "sum"),
            original_amount=("original_amount", "sum"),
            discount_amount=("discount_amount", "sum"),
            billing_line_items=("amount", "size"),
        )
        .sort_values("amount", ascending=False)
    )


def make_resource_coverage(
    inventory_df: pd.DataFrame,
    correlated_df: pd.DataFrame,
) -> pd.DataFrame:
    if inventory_df.empty:
        return pd.DataFrame()
    billed_ids = set()
    if not correlated_df.empty:
        billed_ids = set(
            correlated_df.loc[
                correlated_df["resource_id"].notna(), "resource_id"
            ].astype(str)
        )
    result = inventory_df.copy()
    result["billing_status"] = result["resource_id"].apply(
        lambda value: (
            "DIRECT_BILLING_RECORD_FOUND"
            if pd.notna(value) and str(value) in billed_ids
            else "NO_INDIVIDUAL_BILLING_RECORD_FOUND"
        )
    )
    return result


def billing_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "gross_charges": 0.0,
            "credits": 0.0,
            "net_amount": 0.0,
            "line_items": 0,
        }
    amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return {
        "gross_charges": float(amount[amount > 0].sum()),
        "credits": float(amount[amount < 0].sum()),
        "net_amount": float(amount.sum()),
        "line_items": int(len(df)),
    }


# ============================================================
# PERSISTENCE
# ============================================================

def save_billing(billing: dict) -> Path:
    return save_json("billing.json", billing)


def save_inventory(inventory: dict) -> Path:
    return save_json("resource_inventory.json", inventory)


def save_correlation(correlated_df: pd.DataFrame, coverage_df: pd.DataFrame):
    corr_path = RESULTS_DIR / "billing_resource_correlation.csv"
    coverage_path = RESULTS_DIR / "resource_billing_coverage.csv"
    export_df = correlated_df.drop(columns=["raw_record"], errors="ignore")
    export_df.to_csv(corr_path, index=False)
    coverage_df.to_csv(coverage_path, index=False)
    return corr_path, coverage_path


# ============================================================
# SIDEBAR AND NAVIGATION
# ============================================================

st.sidebar.title("Navigation")
section = st.sidebar.radio(
    "Select section",
    ["Dashboard", "Resource Explorer", "Billing Explorer"],
)
st.sidebar.divider()
st.sidebar.caption("Connection status")
st.sidebar.write(f"MCP config: {'Available' if CONFIG_PATH.exists() else 'Not found'}")
st.sidebar.write(f"Parent .env: {'Available' if PARENT_ENV_FILE.exists() else 'Not found'}")
st.sidebar.write("Billing source: Confluent Cloud REST API")


# ============================================================
# DASHBOARD
# ============================================================

if section == "Dashboard":
    st.header("Organization Overview")
    st.write(
        "Discover resources through the existing Confluent MCP server and "
        "retrieve organization-wide billing directly from the Confluent Billing API."
    )

    inventory = st.session_state.get("resource_inventory")
    billing = st.session_state.get("billing_data")

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Discovered resources",
        inventory.get("resource_count", 0) if inventory else "Not loaded",
    )
    col2.metric(
        "Billing line items",
        billing.get("record_count", 0) if billing else "Not loaded",
    )
    col3.metric(
        "MCP tools loaded",
        len(st.session_state.get("mcp_tools", [])),
    )

    st.info(
        "Recommended workflow: load MCP tools, discover the organization inventory, "
        "retrieve billing for a date range, then review resource-level matches."
    )


# ============================================================
# RESOURCE EXPLORER
# ============================================================

elif section == "Resource Explorer":
    st.header("Organization Resource Explorer")
    st.write(
        "Discover environments, Kafka clusters, topics, connectors, schemas, "
        "Flink compute pools and consumer groups using read-only MCP tools."
    )

    if not CONFIG_PATH.exists():
        st.error(f"MCP configuration not found: {CONFIG_PATH}")
        st.stop()

    if st.button("Load Available MCP Tools", type="primary"):
        with st.spinner("Connecting to the existing Confluent MCP server..."):
            try:
                tools = run_async(get_mcp_tools())
                st.session_state["mcp_tools"] = tools
                st.success(f"Connected. {len(tools)} tools are available.")
                save_json("available_mcp_tools.json", tools)
            except Exception as exc:
                st.error(f"MCP connection failed: {exc}")

    tools = st.session_state.get("mcp_tools", [])
    tool_map = {tool["name"]: tool for tool in tools}

    if tools:
        st.subheader("Automated organization discovery")
        st.caption(
            "The discovery process uses only read-only tools. Some tools require "
            "cluster or environment identifiers; those are supplied from earlier results."
        )

        if st.button("Discover All Available Resources", type="primary"):
            with st.spinner(
                "Discovering organizations, environments, clusters, topics, "
                "connectors, schemas, compute pools and consumer groups..."
            ):
                try:
                    inventory = discover_organization_resources(tool_map)
                    st.session_state["resource_inventory"] = inventory
                    path = save_inventory(inventory)
                    st.success(
                        f"Discovery completed. {inventory['resource_count']} "
                        f"resource records saved."
                    )
                    st.caption(f"Inventory saved to: {path}")
                    if inventory.get("failures"):
                        st.warning(
                            f"{len(inventory['failures'])} discovery calls failed "
                            "or could not be executed. Review the failures below."
                        )
                except Exception as exc:
                    st.error(f"Resource discovery failed: {exc}")

        inventory = st.session_state.get("resource_inventory")
        if inventory:
            st.divider()
            st.subheader("Discovered inventory")
            inventory_df = resource_inventory_dataframe(inventory)
            if not inventory_df.empty:
                type_counts = (
                    inventory_df.groupby("resource_type")
                    .size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                )
                st.dataframe(type_counts, width="stretch", hide_index=True)
                selected_type = st.selectbox(
                    "Filter resource type",
                    ["All"] + sorted(inventory_df["resource_type"].dropna().unique()),
                )
                filtered = inventory_df
                if selected_type != "All":
                    filtered = filtered[
                        filtered["resource_type"] == selected_type
                    ]
                st.dataframe(filtered, width="stretch", hide_index=True)
                st.download_button(
                    "Download resource inventory CSV",
                    data=inventory_df.to_csv(index=False),
                    file_name="confluent_resource_inventory.csv",
                    mime="text/csv",
                    width="content",
                )
            else:
                st.warning(
                    "No structured resource records were extracted. Raw MCP results "
                    "are retained in the discovery output for inspection."
                )

            with st.expander("Discovery failures and raw tool responses"):
                st.json({
                    "failures": inventory.get("failures", []),
                    "tool_calls": inventory.get("tool_calls", []),
                })

    else:
        st.info("Load Available MCP Tools to begin organization discovery.")

    st.divider()
    st.subheader("Manual read-only MCP query")
    if tools:
        available = [name for name in READ_ONLY_TOOLS if name in tool_map]
        if available:
            selected = st.selectbox("Select MCP tool", available)
            st.caption(
                tool_map[selected].get("description")
                or TOOL_DESCRIPTIONS.get(selected, "")
            )
            st.json(tool_map[selected].get("input_schema", {}))
            args_text = st.text_area(
                "Arguments (JSON)",
                value="{}",
                height=120,
                key="manual_mcp_arguments",
            )
            if st.button("Execute MCP Query"):
                try:
                    args = json.loads(args_text)
                    if not isinstance(args, dict):
                        raise ValueError("Arguments must be a JSON object.")
                    with st.spinner(f"Executing {selected}..."):
                        result = run_async(execute_mcp_tool(selected, args))
                    st.session_state["manual_mcp_result"] = result
                    save_json(
                        f"manual_{selected.replace('-', '_')}_latest.json",
                        result,
                    )
                    if result.get("is_error"):
                        st.error("MCP tool returned an error.")
                    else:
                        st.success("Query completed.")
                    st.json(result)
                except Exception as exc:
                    st.error(f"Query failed: {exc}")


# ============================================================
# BILLING EXPLORER
# ============================================================

elif section == "Billing Explorer":
    st.header("Organization-wide Billing Explorer")
    st.write(
        "Retrieve all billing line items for the organization and correlate "
        "them with individually discovered Confluent resources."
    )
    st.info(
        "Billing is retrieved once at organization level. The application then "
        "matches each billing record to inventory using exact resource IDs. "
        "A resource without its own billing record is not assigned an invented cost."
    )

    today = date.today()
    default_start = today - timedelta(days=1)
    default_end = today

    date_col1, date_col2, date_col3 = st.columns(3)
    with date_col1:
        start_date = st.date_input("Start date (inclusive)", value=default_start)
    with date_col2:
        end_date = st.date_input("End date (exclusive)", value=default_end)
    with date_col3:
        page_size = st.number_input(
            "Page size",
            min_value=1,
            max_value=DEFAULT_PAGE_SIZE,
            value=DEFAULT_PAGE_SIZE,
            step=100,
        )

    date_valid = (
        end_date > start_date
        and (end_date - start_date).days <= MAX_BILLING_RANGE_DAYS
    )
    if not date_valid:
        st.error(
            f"Choose an exclusive end date after the start date and a range "
            f"of no more than {MAX_BILLING_RANGE_DAYS} days."
        )

    if st.button(
        "Retrieve Organization Billing",
        type="primary",
        disabled=not date_valid,
    ):
        try:
            with st.spinner("Retrieving all organization billing line items..."):
                billing_data = retrieve_billing(
                    start_date, end_date, int(page_size)
                )
                save_path = save_billing(billing_data)
            st.session_state["billing_data"] = billing_data
            st.success(
                f"Retrieved {billing_data['record_count']} billing line items."
            )
            st.caption(f"Raw billing saved to: {save_path}")
        except Exception as exc:
            st.error(f"Billing retrieval failed: {exc}")

    billing_data = st.session_state.get("billing_data")
    inventory = st.session_state.get("resource_inventory")

    if billing_data:
        raw_records = billing_data.get("records", [])
        billing_df = billing_dataframe(raw_records)
        summary = billing_summary(billing_df)

        st.divider()
        st.subheader("Organization billing summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Gross charges", f"${summary['gross_charges']:,.4f}")
        m2.metric("Credits / adjustments", f"${summary['credits']:,.4f}")
        m3.metric("Net amount", f"${summary['net_amount']:,.4f}")
        m4.metric("Billing line items", summary["line_items"])

        st.caption(
            f"Start: {billing_data['start_date']} | "
            f"End (exclusive): {billing_data['end_date_exclusive']}"
        )

        if not billing_df.empty:
            st.divider()
            st.subheader("Product breakdown")
            product_df = (
                billing_df.groupby("product", as_index=False)["amount"]
                .sum()
                .sort_values("amount", ascending=False)
            )
            st.dataframe(product_df, width="stretch", hide_index=True)
            st.bar_chart(product_df.set_index("product"))

            st.divider()
            st.subheader("Billing dimensions")
            dimension_df = (
                billing_df.groupby(
                    ["product", "line_type", "unit"],
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    amount=("amount", "sum"),
                    quantity=("quantity", "sum"),
                    line_items=("amount", "size"),
                )
                .sort_values("amount", ascending=False)
            )
            st.dataframe(dimension_df, width="stretch", hide_index=True)

            if inventory:
                correlated_df, inventory_df = correlate_billing(
                    billing_df, inventory
                )
                resource_costs = make_resource_cost_table(correlated_df)
                coverage_df = make_resource_coverage(
                    inventory_df, correlated_df
                )
                corr_path, coverage_path = save_correlation(
                    correlated_df, coverage_df
                )

                st.divider()
                st.subheader("Individual resource billing")
                direct_matches = correlated_df[
                    correlated_df["match_status"] == "DIRECT_RESOURCE_MATCH"
                ]
                if not resource_costs.empty:
                    st.dataframe(
                        resource_costs,
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.warning(
                        "No billing records matched discovered resource IDs. "
                        "Review unmatched records and discovery results."
                    )

                st.divider()
                st.subheader("Resource billing coverage")
                st.caption(
                    "This table distinguishes resources with direct billing records "
                    "from resources discovered in inventory but not separately billed "
                    "in the selected billing period."
                )
                if not coverage_df.empty:
                    st.dataframe(
                        coverage_df,
                        width="stretch",
                        hide_index=True,
                    )
                    coverage_counts = (
                        coverage_df.groupby("billing_status")
                        .size()
                        .reset_index(name="resources")
                    )
                    st.dataframe(
                        coverage_counts,
                        width="stretch",
                        hide_index=True,
                    )

                st.divider()
                st.subheader("Unmatched and organization-level billing")
                unmatched_df = correlated_df[
                    correlated_df["match_status"] != "DIRECT_RESOURCE_MATCH"
                ]
                if not unmatched_df.empty:
                    display_unmatched = unmatched_df.drop(
                        columns=["raw_record"], errors="ignore"
                    )
                    st.dataframe(
                        display_unmatched,
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.success("Every resource-associated billing record matched inventory.")

                st.caption(
                    f"Correlation CSV: {corr_path} | Coverage CSV: {coverage_path}"
                )

            else:
                st.warning(
                    "Resource inventory has not been loaded. Go to Resource Explorer, "
                    "load MCP tools and run Discover All Available Resources to enable "
                    "individual resource correlation."
                )

            st.divider()
            st.subheader("All billing line items")
            display_df = billing_df.drop(columns=["raw_record"], errors="ignore")
            st.dataframe(display_df, width="stretch", hide_index=True)

            st.divider()
            st.subheader("Exports")
            export_col1, export_col2 = st.columns(2)
            with export_col1:
                st.download_button(
                    "Download raw billing JSON",
                    data=json.dumps(
                        billing_data, indent=2, ensure_ascii=False, default=str
                    ),
                    file_name="confluent_billing.json",
                    mime="application/json",
                    width="content",
                )
            with export_col2:
                st.download_button(
                    "Download billing CSV",
                    data=display_df.to_csv(index=False),
                    file_name="confluent_billing.csv",
                    mime="text/csv",
                    width="content",
                )

        else:
            st.info("No billing line items were returned for this period.")
    else:
        st.info("Select a date range and retrieve organization billing to begin.")


# ============================================================
# FOOTER
# ============================================================

st.divider()
st.caption(
    "Confluent FinOps Explorer | Read-only MCP resource discovery | "
    "Organization-wide Billing REST API | Exact-ID cost correlation"
)
