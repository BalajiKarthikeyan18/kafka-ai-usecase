
import asyncio
import base64
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# ============================================================
# CONFIGURATION
# ============================================================

APP_DIR = Path(__file__).resolve().parent
PARENT_DIR = APP_DIR.parent

MCP_CONFIG_PATH = PARENT_DIR / "config.yaml"
PARENT_ENV_PATH = PARENT_DIR / ".env"
PROJECT_ENV_PATH = APP_DIR / ".env"

RESULTS_DIR = APP_DIR / "results"

BILLING_API = "https://api.confluent.cloud/billing/v1/costs"
CONNECT_API = "https://api.confluent.cloud/connect/v1"
PAGE_SIZE = 1000


# ============================================================
# ENVIRONMENT
# ============================================================

def load_environment():
    if PARENT_ENV_PATH.exists():
        load_dotenv(PARENT_ENV_PATH, override=False)

    if PROJECT_ENV_PATH.exists():
        load_dotenv(PROJECT_ENV_PATH, override=False)


def get_required_env(name):
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Missing environment variable: {name}"
        )

    return value


def get_api_auth():
    load_environment()

    api_key = get_required_env("CONFLUENT_CLOUD_API_KEY")
    api_secret = get_required_env("CONFLUENT_CLOUD_API_SECRET")

    encoded = base64.b64encode(
        f"{api_key}:{api_secret}".encode("utf-8")
    ).decode("utf-8")

    return f"Basic {encoded}"


def save_json(filename, data):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    path = RESULTS_DIR / filename

    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )

    return path


# ============================================================
# MCP CONNECTION
# ============================================================

@asynccontextmanager
async def get_mcp_session():

    if not MCP_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"MCP configuration not found: {MCP_CONFIG_PATH}"
        )

    load_environment()

    params = StdioServerParameters(
        command="cmd.exe",
        args=[
            "/c",
            "npx",
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(MCP_CONFIG_PATH)
        ],
        env=os.environ.copy(),
        cwd=str(PARENT_DIR)
    )

    async with stdio_client(
        params,
        errlog=None
    ) as (read, write):

        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


# ============================================================
# MCP RESPONSE HANDLING
# ============================================================

def mcp_result_to_python(result):

    structured = getattr(
        result,
        "structuredContent",
        None
    )

    if structured is None:
        structured = getattr(
            result,
            "structured_content",
            None
        )

    if structured is not None:
        return structured

    output = []

    for item in getattr(result, "content", []) or []:

        text_value = getattr(item, "text", None)

        if not text_value:
            continue

        stripped = text_value.strip()

        try:
            output.append(json.loads(stripped))
        except json.JSONDecodeError:
            output.append(stripped)

    if len(output) == 1:
        return output[0]

    return output


async def call_mcp_tool(session, tool_name, arguments=None):

    result = await session.call_tool(
        tool_name,
        arguments or {}
    )

    if (
        getattr(result, "isError", False)
        or getattr(result, "is_error", False)
    ):
        raise RuntimeError(
            str(mcp_result_to_python(result))
        )

    return mcp_result_to_python(result)


# ============================================================
# RESOURCE EXTRACTION
# ============================================================

def find_ids(payload, pattern):

    found = set()

    def walk(value):

        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, str):
                    if re.fullmatch(pattern, item):
                        found.add(item)
                walk(item)

        elif isinstance(value, list):
            for item in value:
                walk(item)

        elif isinstance(value, str):
            found.update(re.findall(pattern, value))

    walk(payload)

    return sorted(found)


def find_environment_ids(payload):
    return find_ids(
        payload,
        r"env-[A-Za-z0-9_-]+"
    )


def find_cluster_ids(payload):
    return find_ids(
        payload,
        r"lkc-[A-Za-z0-9_-]+"
    )


def extract_named_resources(payload, label):

    results = {}

    if isinstance(payload, dict):

        for key, value in payload.items():
            results.update(
                extract_named_resources(value, label)
            )

        return results

    if isinstance(payload, list):

        for value in payload:
            results.update(
                extract_named_resources(value, label)
            )

        return results

    if not isinstance(payload, str):
        return results

    # Example:
    # Cluster: cluster_0
    # ID: lkc-125nwo6

    pattern = (
        rf"{label}:\s*([^\r\n]+)"
        rf"[\r\n]+\s*ID:\s*"
        rf"({ 'lkc-[A-Za-z0-9_-]+' if label == 'Cluster' else 'env-[A-Za-z0-9_-]+' })"
    )

    for match in re.finditer(
        pattern,
        payload,
        re.IGNORECASE
    ):
        name = match.group(1).strip()
        resource_id = match.group(2).strip()

        results[resource_id] = name

    return results


def extract_connector_names(payload):

    names = set()

    if isinstance(payload, dict):

        for key in ("name", "connector_name", "connectorName"):
            value = payload.get(key)

            if isinstance(value, str):
                names.add(value)

        for value in payload.values():
            names.update(extract_connector_names(value))

    elif isinstance(payload, list):

        for value in payload:
            names.update(extract_connector_names(value))

    elif isinstance(payload, str):

        match = re.search(
            r"Active Connectors:\s*(.*)",
            payload,
            re.IGNORECASE | re.DOTALL
        )

        if match:

            content = match.group(1)

            for name in re.split(r"[\s,]+", content.strip()):

                name = name.strip()

                if re.fullmatch(
                    r"[A-Za-z0-9_.-]+",
                    name
                ):
                    names.add(name)

    return sorted(names)


# ============================================================
# CONFLUENT CONNECT REST API
# ============================================================

def fetch_connector_inventory(environment_id, cluster_id):

    if not environment_id or not cluster_id:
        raise RuntimeError(
            "Environment ID and Kafka cluster ID are required "
            "to retrieve connector IDs."
        )

    url = (
        f"{CONNECT_API}/environments/{environment_id}"
        f"/clusters/{cluster_id}/connectors"
    )

    response = requests.get(
        url,
        headers={
            "Authorization": get_api_auth(),
            "Accept": "application/json"
        },
        params={"expand": "id"},
        timeout=60
    )

    if not response.ok:
        raise RuntimeError(
            f"Connect API HTTP {response.status_code}: "
            f"{response.text[:2000]}"
        )

    payload = response.json()

    if not isinstance(payload, dict):
        raise RuntimeError(
            "Unexpected connector API response format."
        )

    connectors = []

    for connector_name, details in payload.items():

        if not isinstance(details, dict):
            continue

        id_details = details.get("id") or {}

        connector_id = None

        if isinstance(id_details, dict):
            connector_id = id_details.get("id")

        elif isinstance(id_details, str):
            connector_id = id_details

        connectors.append({
            "name": connector_name,
            "id": connector_id,
            "resource_type": "connector",
            "status": details.get("status"),
            "info": details.get("info"),
            "extensions": details.get("extensions")
        })

    return connectors


# ============================================================
# RESOURCE DISCOVERY
# ============================================================

async def discover_resources():

    inventory = {}
    errors = []

    async with get_mcp_session() as session:

        # ----------------------------------------------------
        # Base discovery
        # ----------------------------------------------------

        base_calls = [
            ("organizations", "list-organizations", {}),
            ("environments", "list-environments", {}),
            ("clusters", "list-clusters", {})
        ]

        for key, tool, arguments in base_calls:

            try:
                inventory[key] = await call_mcp_tool(
                    session,
                    tool,
                    arguments
                )

            except Exception as exc:

                inventory[key] = None

                errors.append({
                    "resource": key,
                    "error": str(exc)
                })

        # ----------------------------------------------------
        # Extract environment and cluster IDs
        # ----------------------------------------------------

        environment_ids = find_environment_ids(
            inventory.get("environments")
        )

        cluster_ids = find_cluster_ids(
            inventory.get("clusters")
        )

        environment_id = (
            environment_ids[0]
            if environment_ids
            else None
        )

        cluster_id = (
            cluster_ids[0]
            if cluster_ids
            else None
        )

        inventory["discovered_environment_id"] = environment_id
        inventory["discovered_cluster_id"] = cluster_id

        # ----------------------------------------------------
        # Environment and cluster name-to-ID mappings
        # ----------------------------------------------------

        inventory["environment_id_map"] = extract_named_resources(
            inventory.get("environments"),
            "Environment"
        )

        inventory["cluster_id_map"] = extract_named_resources(
            inventory.get("clusters"),
            "Cluster"
        )

        # ----------------------------------------------------
        # Other MCP resource discovery
        # ----------------------------------------------------

        resource_calls = [
            ("topics", "list-topics", {}),
            ("schemas", "list-schemas", {}),
            ("compute_pools", "list-compute-pools", {})
        ]

        for key, tool, arguments in resource_calls:

            try:
                inventory[key] = await call_mcp_tool(
                    session,
                    tool,
                    arguments
                )

            except Exception as exc:

                inventory[key] = None

                errors.append({
                    "resource": key,
                    "error": str(exc)
                })

        # ----------------------------------------------------
        # Connector names through MCP
        # ----------------------------------------------------

        connector_arguments = {}

        if environment_id:
            connector_arguments["environmentId"] = environment_id

        if cluster_id:
            connector_arguments["clusterId"] = cluster_id

        try:

            inventory["connectors"] = await call_mcp_tool(
                session,
                "list-connectors",
                connector_arguments
            )

        except Exception as exc:

            inventory["connectors"] = None

            errors.append({
                "resource": "connectors",
                "error": str(exc)
            })

        # ----------------------------------------------------
        # Connector IDs through Connect REST API
        # ----------------------------------------------------

        try:

            connector_resources = fetch_connector_inventory(
                environment_id,
                cluster_id
            )

            inventory["connector_resources"] = connector_resources

        except Exception as exc:

            inventory["connector_resources"] = []

            errors.append({
                "resource": "connector_ids",
                "error": str(exc)
            })

        # ----------------------------------------------------
        # Connector configuration through MCP
        # ----------------------------------------------------

        connector_names = extract_connector_names(
            inventory.get("connectors")
        )

        connector_configs = {}

        for connector_name in connector_names:

            try:

                connector_configs[connector_name] = (
                    await call_mcp_tool(
                        session,
                        "get-connector-config",
                        {
                            "connectorName": connector_name,
                            "environmentId": environment_id,
                            "clusterId": cluster_id
                        }
                    )
                )

            except Exception as exc:

                connector_configs[connector_name] = {
                    "retrieval_error": str(exc)
                }

        inventory["connector_configs"] = connector_configs

    return inventory, errors


# ============================================================
# RESOURCE COUNTING
# ============================================================

def count_resources(payload, resource_type):

    if payload is None:
        return None

    if resource_type == "connectors":

        if isinstance(payload, (dict, list)):
            return len(extract_connector_names(payload))

        return len(extract_connector_names(payload))

    if isinstance(payload, dict):

        for key in (
            "data",
            "items",
            "organizations",
            "environments",
            "clusters",
            "topics",
            "schemas",
            "compute_pools"
        ):

            value = payload.get(key)

            if isinstance(value, (dict, list)):
                return len(value)

        if any(
            key in payload
            for key in ("id", "resource_id", "cluster_id")
        ):
            return 1

        return len(payload)

    if isinstance(payload, list):
        return len(payload)

    if isinstance(payload, str):

        patterns = {
            "organizations": r"^\s*Organization:",
            "environments": r"^\s*Environment:",
            "clusters": r"^\s*Cluster:",
            "topics": r"^\s*Topic:",
            "schemas": r"^\s*Subject:",
            "compute_pools": r"^\s*Compute Pool:"
        }

        pattern = patterns.get(resource_type)

        if pattern:
            matches = re.findall(
                pattern,
                payload,
                re.MULTILINE
            )

            if matches:
                return len(matches)

        if resource_type == "topics":

            match = re.search(
                r"Kafka topics:\s*(.*)",
                payload,
                re.IGNORECASE | re.DOTALL
            )

            if match:
                return len([
                    item for item in match.group(1).split(",")
                    if item.strip()
                ])

        count_match = re.search(
            r"(?:Successfully retrieved|Retrieved)\s+(\d+)",
            payload,
            re.IGNORECASE
        )

        if count_match:
            return int(count_match.group(1))

    return None


# ============================================================
# DISPLAY MCP RESPONSE
# ============================================================

def display_mcp_response(payload):

    if payload is None:
        st.warning("No response received.")
        return

    if isinstance(payload, (dict, list)):
        st.json(payload)
        return

    if isinstance(payload, str):

        text = payload.strip()

        if not text:
            st.info("Empty response.")
            return

        try:
            parsed = json.loads(text)

            if isinstance(parsed, (dict, list)):
                st.json(parsed)
                return

        except json.JSONDecodeError:
            pass

        st.code(text, language="text")
        return

    st.write(payload)


# ============================================================
# BILLING API
# ============================================================

def get_next_page_token(payload):

    metadata = payload.get("metadata") or {}

    for key in (
        "next_page_token",
        "nextPageToken",
        "page_token"
    ):

        token = metadata.get(key)

        if token:
            return str(token)

    next_url = metadata.get("next")

    if isinstance(next_url, str):

        values = parse_qs(urlparse(next_url).query)

        if "page_token" in values:
            return values["page_token"][0]

    return None


def fetch_billing_costs(start_date, end_date_exclusive):

    if end_date_exclusive <= start_date:
        raise ValueError("Invalid date range.")

    if (end_date_exclusive - start_date).days > 31:
        raise ValueError(
            "Maximum billing date range is 31 days."
        )

    headers = {
        "Accept": "application/json",
        "Authorization": get_api_auth()
    }

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date_exclusive.isoformat(),
        "page_size": PAGE_SIZE
    }

    records = []
    pages = []
    page_token = None
    seen_tokens = set()

    while True:

        if page_token:
            params["page_token"] = page_token
        else:
            params.pop("page_token", None)

        response = requests.get(
            BILLING_API,
            headers=headers,
            params=params,
            timeout=60
        )

        if not response.ok:
            raise RuntimeError(
                f"Billing API HTTP {response.status_code}: "
                f"{response.text[:2000]}"
            )

        payload = response.json()

        page_records = payload.get("data", [])

        if not isinstance(page_records, list):
            raise RuntimeError(
                "Invalid billing API data response."
            )

        records.extend(page_records)

        pages.append({
            "record_count": len(page_records)
        })

        next_token = get_next_page_token(payload)

        if not next_token:
            break

        if next_token in seen_tokens:
            raise RuntimeError(
                "Repeated billing pagination token."
            )

        seen_tokens.add(next_token)
        page_token = next_token

        if len(pages) > 100:
            raise RuntimeError(
                "Pagination safety limit reached."
            )

    return records, pages


# ============================================================
# RESOURCE ID CORRELATION
# ============================================================

def collect_resource_id_map(inventory):

    id_map = {}

    def add_resource(resource_id, resource_type, name=""):

        if not isinstance(resource_id, str):
            return

        resource_id = resource_id.strip()

        if not resource_id:
            return

        id_map[resource_id] = {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "display_name": str(name or "")
        }

    def walk(value, resource_type, parent_name=""):

        if isinstance(value, dict):

            name = (
                value.get("name")
                or value.get("display_name")
                or value.get("topic_name")
                or value.get("connector_name")
                or value.get("connectorName")
                or parent_name
            )

            for key in (
                "id",
                "resource_id",
                "cluster_id",
                "environment_id",
                "connector_id"
            ):

                resource_id = value.get(key)

                if isinstance(resource_id, str):
                    add_resource(
                        resource_id,
                        resource_type,
                        name
                    )

                elif isinstance(resource_id, dict):

                    nested_id = resource_id.get("id")

                    if isinstance(nested_id, str):
                        add_resource(
                            nested_id,
                            resource_type,
                            name
                        )

            for child in value.values():
                walk(
                    child,
                    resource_type,
                    str(name or parent_name)
                )

        elif isinstance(value, list):

            for child in value:
                walk(child, resource_type, parent_name)

    type_map = {
        "organizations": "organization",
        "environments": "environment",
        "clusters": "kafka_cluster",
        "topics": "topic",
        "connectors": "connector",
        "schemas": "schema",
        "compute_pools": "flink_compute_pool"
    }

    for key, payload in inventory.items():

        if key in type_map:
            walk(payload, type_map[key])

    # --------------------------------------------------------
    # Extract IDs from plain-text MCP responses.
    # --------------------------------------------------------

    environment_map = inventory.get("environment_id_map") or {}

    for resource_id, name in environment_map.items():
        add_resource(
            resource_id,
            "environment",
            name
        )

    cluster_map = inventory.get("cluster_id_map") or {}

    for resource_id, name in cluster_map.items():
        add_resource(
            resource_id,
            "kafka_cluster",
            name
        )

    # Fallback: IDs can still be extracted even if the
    # MCP response does not contain a recognizable name.

    for resource_id in find_environment_ids(
        inventory.get("environments")
    ):
        add_resource(
            resource_id,
            "environment"
        )

    for resource_id in find_cluster_ids(
        inventory.get("clusters")
    ):
        add_resource(
            resource_id,
            "kafka_cluster"
        )

    # --------------------------------------------------------
    # Connector IDs from the Connect REST API.
    # --------------------------------------------------------

    for connector in inventory.get("connector_resources", []):

        add_resource(
            connector.get("id"),
            "connector",
            connector.get("name")
        )

    return id_map


def normalize_billing_records(records, resource_id_map):

    normalized = []

    for index, record in enumerate(records):

        resource = record.get("resource") or {}

        resource_id = (
            resource.get("id")
            if isinstance(resource, dict)
            else None
        )

        resource_name = (
            resource.get("display_name")
            if isinstance(resource, dict)
            else None
        )

        environment_id = None

        if isinstance(resource, dict):

            environment = resource.get("environment") or {}

            if isinstance(environment, dict):
                environment_id = environment.get("id")

        match = (
            resource_id_map.get(resource_id)
            if resource_id
            else None
        )

        if match:

            status = "Matched by exact resource ID"

            resource_type = match["resource_type"]

            matched_name = (
                match["display_name"]
                or resource_name
            )

        elif resource_id:

            status = "Billing resource ID not found in inventory"

            resource_type = "unmatched_resource"

            matched_name = resource_name

        else:

            status = "Organization-level or unallocated"

            resource_type = "unallocated"

            matched_name = None

        amount = record.get("amount")
        quantity = record.get("quantity")

        normalized.append({

            "record_number": index + 1,

            "start_date": record.get("start_date"),
            "end_date": record.get("end_date"),

            "product": (
                record.get("product")
                or "UNKNOWN / NOT PROVIDED"
            ),

            "line_type": (
                record.get("line_type")
                or record.get("type")
                or "UNKNOWN"
            ),

            "amount": (
                float(amount)
                if amount is not None
                else 0.0
            ),

            "original_amount": (
                float(record["original_amount"])
                if record.get("original_amount") is not None
                else None
            ),

            "discount_amount": (
                float(record["discount_amount"])
                if record.get("discount_amount") is not None
                else None
            ),

            "quantity": (
                float(quantity)
                if quantity is not None
                else None
            ),

            "unit": record.get("unit"),

            "resource_id": resource_id,
            "billing_resource_name": resource_name,
            "environment_id": environment_id,

            "matched_resource_type": resource_type,
            "matched_resource_name": matched_name,

            "attribution_status": status,

            "raw_record": record
        })

    return normalized


# ============================================================
# COST SUMMARY AND RECONCILIATION
# ============================================================

def build_resource_cost_summary(normalized):

    rows = []

    for item in normalized:

        rows.append({

            "Attribution": item["attribution_status"],

            "Resource type": item["matched_resource_type"],

            "Resource name": (
                item["matched_resource_name"]
                or item["billing_resource_name"]
                or "Not specified"
            ),

            "Resource ID": (
                item["resource_id"]
                or "Not provided"
            ),

            "Product": item["product"],

            "Line type": item["line_type"],

            "Amount": item["amount"]
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    return (
        df.groupby(
            [
                "Attribution",
                "Resource type",
                "Resource name",
                "Resource ID",
                "Product"
            ],
            dropna=False,
            as_index=False
        )
        .agg(
            Amount=("Amount", "sum"),
            Line_items=("Line type", "count"),
            Line_types=(
                "Line type",
                lambda x: ", ".join(
                    sorted(set(map(str, x)))
                )
            )
        )
        .sort_values("Amount", ascending=False)
    )


def calculate_reconciliation(normalized):

    total = sum(
        item["amount"]
        for item in normalized
    )

    matched = sum(
        item["amount"]
        for item in normalized
        if item["attribution_status"]
        == "Matched by exact resource ID"
    )

    unallocated = sum(
        item["amount"]
        for item in normalized
        if item["attribution_status"]
        == "Organization-level or unallocated"
    )

    unmatched = sum(
        item["amount"]
        for item in normalized
        if item["attribution_status"]
        == "Billing resource ID not found in inventory"
    )

    return {

        "billing_total": total,

        "matched_resource_total": matched,

        "unallocated_total": unallocated,

        "unmatched_resource_total": unmatched,

        "reconciliation_difference": (
            total - matched - unallocated - unmatched
        ),

        "record_count": len(normalized)
    }


# ============================================================
# STREAMLIT APPLICATION
# ============================================================

st.set_page_config(
    page_title="Confluent FinOps",
    layout="wide"
)

st.title("Confluent FinOps")

st.caption(
    "Resource inventory from the Confluent MCP server "
    "and actual billing line items from the "
    "Confluent Billing REST API."
)

load_environment()


# ============================================================
# SESSION STATE
# ============================================================

defaults = {
    "inventory": None,
    "discovery_errors": [],
    "billing_records": None,
    "normalized_records": None,
    "resource_summary": None,
    "reconciliation": None,
    "billing_pages": None
}

for key, value in defaults.items():

    if key not in st.session_state:
        st.session_state[key] = value


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.header("Billing period")

    default_end = date.today() - timedelta(days=1)
    default_start = default_end - timedelta(days=6)

    selected_range = st.date_input(
        "Select date range",
        value=(default_start, default_end),
        help=(
            "End date is inclusive in this UI. "
            "It is converted to an exclusive API end date."
        )
    )

    st.caption("Maximum date range: 31 days.")

    run_discovery = st.button(
        "Discover resources",
        use_container_width=True
    )

    run_billing = st.button(
        "Fetch billing and correlate",
        type="primary",
        use_container_width=True
    )

    st.divider()

    st.caption("MCP config")
    st.caption(str(MCP_CONFIG_PATH))


# ============================================================
# RUN DISCOVERY AND BILLING
# ============================================================

if run_discovery or run_billing:

    if (
        not isinstance(selected_range, (tuple, list))
        or len(selected_range) != 2
    ):
        st.error("Select both start and end dates.")
        st.stop()

    start_date, end_date_inclusive = selected_range

    end_date_exclusive = end_date_inclusive + timedelta(days=1)

    if end_date_inclusive < start_date:
        st.error("Invalid date range.")
        st.stop()

    if (end_date_exclusive - start_date).days > 31:
        st.error("Select a maximum of 31 days.")
        st.stop()

    # --------------------------------------------------------
    # Resource discovery
    # --------------------------------------------------------

    if run_discovery or st.session_state.inventory is None:

        with st.spinner("Discovering Confluent resources..."):

            try:

                inventory, errors = asyncio.run(
                    discover_resources()
                )

                st.session_state.inventory = inventory
                st.session_state.discovery_errors = errors

                save_json(
                    "resource_inventory.json",
                    inventory
                )

                save_json(
                    "resource_discovery_errors.json",
                    errors
                )

            except Exception as exc:

                st.error(
                    f"Resource discovery failed: {exc}"
                )

                st.stop()

    # --------------------------------------------------------
    # Billing retrieval and correlation
    # --------------------------------------------------------

    if run_billing:

        with st.spinner(
            "Fetching billing and correlating resources..."
        ):

            try:

                records, pages = fetch_billing_costs(
                    start_date,
                    end_date_exclusive
                )

                resource_id_map = collect_resource_id_map(
                    st.session_state.inventory or {}
                )

                normalized = normalize_billing_records(
                    records,
                    resource_id_map
                )

                summary = build_resource_cost_summary(
                    normalized
                )

                reconciliation = calculate_reconciliation(
                    normalized
                )

                st.session_state.billing_records = records
                st.session_state.normalized_records = normalized
                st.session_state.resource_summary = summary
                st.session_state.reconciliation = reconciliation
                st.session_state.billing_pages = pages

                save_json("billing_raw.json", records)

                save_json(
                    "billing_normalized.json",
                    normalized
                )

                save_json(
                    "resource_id_map.json",
                    resource_id_map
                )

                save_json(
                    "billing_reconciliation.json",
                    reconciliation
                )

                if not summary.empty:

                    summary.to_csv(
                        RESULTS_DIR / "resource_cost_summary.csv",
                        index=False
                    )

            except Exception as exc:

                st.error(
                    f"Billing processing failed: {exc}"
                )

                st.stop()


# ============================================================
# RESOURCE INVENTORY DASHBOARD
# ============================================================

if st.session_state.inventory is not None:

    st.subheader("Resource inventory")

    inventory = st.session_state.inventory

    resource_definitions = [
        ("Organizations", "organizations"),
        ("Environments", "environments"),
        ("Kafka clusters", "clusters"),
        ("Topics", "topics"),
        ("Connectors", "connectors"),
        ("Schemas", "schemas"),
        ("Compute pools", "compute_pools")
    ]

    cols = st.columns(4)

    for index, (label, key) in enumerate(resource_definitions):

        count = count_resources(
            inventory.get(key),
            key
        )

        if key == "connectors":
            connector_resources = inventory.get(
                "connector_resources", []
            )

            if connector_resources:
                count = len(connector_resources)

        display_count = (
            str(count)
            if count is not None
            else "Unavailable"
        )

        cols[index % 4].metric(
            label,
            display_count
        )

    # --------------------------------------------------------
    # Verified IDs
    # --------------------------------------------------------

    st.markdown("#### Discovered resource IDs")

    st.write(
        "Environment ID:",
        inventory.get("discovered_environment_id")
        or "Unavailable"
    )

    st.write(
        "Kafka cluster ID:",
        inventory.get("discovered_cluster_id")
        or "Unavailable"
    )

    connector_resources = inventory.get(
        "connector_resources", []
    )

    if connector_resources:

        connector_df = pd.DataFrame([
            {
                "Connector name": item.get("name"),
                "Connector ID": item.get("id"),
                "Resource type": item.get("resource_type")
            }
            for item in connector_resources
        ])

        st.dataframe(
            connector_df,
            use_container_width=True,
            hide_index=True
        )

    else:
        st.warning(
            "Connector IDs could not be retrieved. "
            "Check the Connect API discovery warning."
        )

    if st.session_state.discovery_errors:

        with st.expander("Resource discovery warnings"):

            st.dataframe(
                pd.DataFrame(
                    st.session_state.discovery_errors
                ),
                use_container_width=True
            )

    with st.expander("View raw resource inventory"):

        for key, payload in inventory.items():

            st.markdown(
                f"**{key.replace('_', ' ').title()}**"
            )

            display_mcp_response(payload)


# ============================================================
# BILLING DASHBOARD
# ============================================================

if st.session_state.normalized_records is not None:

    st.subheader("Billing and resource correlation")

    reconciliation = st.session_state.reconciliation or {}

    cols = st.columns(4)

    cols[0].metric(
        "Billing total",
        f"${reconciliation['billing_total']:,.4f}"
    )

    cols[1].metric(
        "Matched resources",
        f"${reconciliation['matched_resource_total']:,.4f}"
    )

    cols[2].metric(
        "Unallocated",
        f"${reconciliation['unallocated_total']:,.4f}"
    )

    cols[3].metric(
        "Unmatched resource IDs",
        f"${reconciliation['unmatched_resource_total']:,.4f}"
    )

    st.caption(
        f"{reconciliation['record_count']} "
        "billing line items retrieved."
    )

    difference = reconciliation[
        "reconciliation_difference"
    ]

    if abs(difference) < 0.000001:
        st.success("Billing reconciliation passed.")

    else:
        st.warning(
            f"Reconciliation difference: ${difference:,.6f}"
        )

    # --------------------------------------------------------
    # Resource cost summary
    # --------------------------------------------------------

    st.markdown("#### Resource cost summary")

    summary = st.session_state.resource_summary

    if summary is not None and not summary.empty:

        st.dataframe(
            summary.style.format({
                "Amount": "${:,.6f}"
            }),
            use_container_width=True,
            hide_index=True
        )

    else:
        st.info("No billing records found.")

    # --------------------------------------------------------
    # Billing line items
    # --------------------------------------------------------

    st.markdown("#### Billing line items")

    normalized_df = pd.DataFrame([
        {
            key: value
            for key, value in row.items()
            if key != "raw_record"
        }
        for row in st.session_state.normalized_records
    ])

    if not normalized_df.empty:

        st.dataframe(
            normalized_df.drop(
                columns=["record_number"],
                errors="ignore"
            ),
            use_container_width=True,
            hide_index=True
        )

    # --------------------------------------------------------
    # Unmatched resources
    # --------------------------------------------------------

    unmatched_df = normalized_df[
        normalized_df["attribution_status"]
        == "Billing resource ID not found in inventory"
    ]

    if not unmatched_df.empty:

        with st.expander("Investigate unmatched billing resources"):

            st.warning(
                "These billing resource IDs were not found "
                "in the currently discovered inventory. "
                "They may represent historical resources."
            )

            st.dataframe(
                unmatched_df[
                    [
                        "billing_resource_name",
                        "resource_id",
                        "product",
                        "line_type",
                        "amount"
                    ]
                ],
                use_container_width=True,
                hide_index=True
            )

    # --------------------------------------------------------
    # Raw billing data
    # --------------------------------------------------------

    with st.expander("View original billing records"):

        display_mcp_response(
            st.session_state.billing_records
        )

    with st.expander("View reconciliation details"):

        st.json(reconciliation)

    # --------------------------------------------------------
    # Downloads
    # --------------------------------------------------------

    st.download_button(
        "Download normalized billing CSV",
        data=normalized_df.to_csv(index=False).encode("utf-8"),
        file_name="billing_normalized.csv",
        mime="text/csv"
    )

    st.download_button(
        "Download raw billing JSON",
        data=json.dumps(
            st.session_state.billing_records,
            indent=2
        ).encode("utf-8"),
        file_name="billing_raw.json",
        mime="application/json"
    )


# ============================================================
# ATTRIBUTION POLICY
# ============================================================

st.divider()

st.caption(
    "Only exact billing resource-ID matches are treated as "
    "resource-level costs. Billing records without resource IDs "
    "remain unallocated. Billing resource IDs absent from the "
    "discovered inventory are reported as unmatched. "
    "Historical resource IDs are not attributed to current "
    "resources based on display names. Topic-level usage "
    "allocation will be treated separately as an estimate, "
    "not actual billing."
)
