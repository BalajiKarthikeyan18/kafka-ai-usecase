
import asyncio
import base64
import json
import os
from datetime import date, timedelta
from pathlib import Path

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

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Load parent environment variables first, then project variables.
load_dotenv(PARENT_ENV_FILE, override=False)
load_dotenv(LOCAL_ENV_FILE, override=False)


# ============================================================
# STREAMLIT PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Confluent FinOps Explorer",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Confluent FinOps Explorer")

st.caption(
    "Explore Confluent Cloud resources and billing information "
    "directly from your Confluent Cloud organization."
)


# ============================================================
# MCP RESOURCE DISCOVERY
# ============================================================

READ_ONLY_TOOLS = [
    "list-organizations",
    "list-environments",
    "read-environment",
    "list-clusters",
    "list-topics",
    "get-topic-config",
    "list-connectors",
    "get-connector-config",
    "get-connector-status",
    "get-connector-offsets",
    "get-connector-tasks",
    "get-connector-error-summary",
    "get-connector-error-recommendations",
    "get-connector-logs",
    "list-schemas",
    "list-compute-pools",
    "list-consumer-groups",
    "describe-consumer-group",
    "get-consumer-group-lag",
    "get-partition-offsets",
    "list-tags",
    "search-topics-by-name",
    "search-topics-by-tag",
    "list-available-metrics",
    "query-metrics",
    "list-configured-connections",
    "describe-configured-connection",
    "explain-disabled-tools",
    "config-help",
]


TOOL_DESCRIPTIONS = {
    "list-organizations": "Retrieve Confluent Cloud organizations.",
    "list-environments": "Retrieve environments in the organization.",
    "read-environment": "Retrieve details of a specific environment.",
    "list-clusters": "Retrieve Kafka clusters.",
    "list-topics": "Retrieve Kafka topics.",
    "get-topic-config": "Retrieve configuration for a Kafka topic.",
    "list-connectors": "Retrieve Kafka Connect connectors.",
    "get-connector-config": "Retrieve connector configuration.",
    "get-connector-status": "Retrieve connector status.",
    "get-connector-offsets": "Retrieve connector offsets.",
    "get-connector-tasks": "Retrieve connector tasks.",
    "get-connector-error-summary": "Retrieve connector error summary.",
    "get-connector-error-recommendations": "Retrieve connector error recommendations.",
    "get-connector-logs": "Retrieve connector logs.",
    "list-schemas": "Retrieve Schema Registry subjects and schemas.",
    "list-compute-pools": "Retrieve Flink compute pools.",
    "list-consumer-groups": "Retrieve Kafka consumer groups.",
    "describe-consumer-group": "Retrieve details of a consumer group.",
    "get-consumer-group-lag": "Retrieve consumer group lag.",
    "get-partition-offsets": "Retrieve partition offsets.",
    "list-tags": "Retrieve available tags.",
    "search-topics-by-name": "Search Kafka topics by name.",
    "search-topics-by-tag": "Search Kafka topics by tag.",
    "list-available-metrics": "Retrieve available Confluent metrics.",
    "query-metrics": "Query Confluent Cloud metrics.",
    "list-configured-connections": "List configured Confluent connections.",
    "describe-configured-connection": "Describe a configured connection.",
    "explain-disabled-tools": "Explain unavailable MCP tools.",
    "config-help": "Retrieve MCP configuration guidance.",
}


def extract_mcp_result(result):
    """Extract structured and textual content from an MCP result."""

    output = {
        "is_error": getattr(result, "isError", False),
        "structured_content": getattr(result, "structuredContent", None),
        "content": []
    }

    for item in getattr(result, "content", []):
        if hasattr(item, "text"):
            output["content"].append(item.text)
        else:
            output["content"].append(str(item))

    return output


async def get_mcp_tools():
    """Retrieve available tools from the existing MCP server."""

    async with get_mcp_session() as session:
        response = await session.list_tools()

        return [
            {
                "name": tool.name,
                "description": tool.description or "",
                "input_schema": getattr(tool, "inputSchema", {})
            }
            for tool in response.tools
        ]


async def execute_mcp_tool(tool_name, arguments):
    """Execute a read-only tool using the existing MCP connection."""

    async with get_mcp_session() as session:

        result = await session.call_tool(
            tool_name,
            arguments=arguments
        )

        return extract_mcp_result(result)


def run_async(coroutine):
    """Execute an asynchronous MCP operation from Streamlit."""

    try:
        return asyncio.run(coroutine)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def get_tool_schema(tool, schema):
    """Display the input schema returned by the MCP server."""

    if not isinstance(schema, dict):
        return {}

    properties = schema.get("properties", {})
    required = schema.get("required", [])

    return {
        "description": tool.get("description", ""),
        "required": required,
        "properties": properties
    }


# ============================================================
# BILLING API
# ============================================================

def get_billing_credentials():
    """Read Confluent Cloud credentials from the parent .env."""

    api_key = os.getenv("CONFLUENT_CLOUD_API_KEY")
    api_secret = os.getenv("CONFLUENT_CLOUD_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError(
            "Confluent Cloud credentials are missing. "
            "Check CONFLUENT_CLOUD_API_KEY and "
            "CONFLUENT_CLOUD_API_SECRET in the parent .env file."
        )

    return api_key, api_secret


def get_billing_headers():
    """Generate Basic authentication headers."""

    api_key, api_secret = get_billing_credentials()

    credential_string = f"{api_key}:{api_secret}"

    encoded_credentials = base64.b64encode(
        credential_string.encode("utf-8")
    ).decode("utf-8")

    return {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_credentials}"
    }


def retrieve_billing(
    start_date,
    end_date,
    page_size=DEFAULT_PAGE_SIZE
):
    """
    Retrieve billing records directly from Confluent Cloud.

    Records without a product field are preserved.
    Pagination is followed using metadata.next.
    """

    if end_date < start_date:
        raise ValueError("End date cannot be before start date.")

    date_difference = (end_date - start_date).days

    if date_difference > MAX_BILLING_RANGE_DAYS:
        raise ValueError(
            f"Billing date range cannot exceed "
            f"{MAX_BILLING_RANGE_DAYS} days."
        )

    if page_size < 1 or page_size > DEFAULT_PAGE_SIZE:
        raise ValueError(
            f"Page size must be between 1 and {DEFAULT_PAGE_SIZE}."
        )

    headers = get_billing_headers()

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "page_size": page_size
    }

    all_records = []
    raw_pages = []
    seen_tokens = set()

    while True:

        response = requests.get(
            BILLING_API_URL,
            headers=headers,
            params=params,
            timeout=60
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Billing API failed with HTTP "
                f"{response.status_code}: {response.text}"
            )

        try:
            response_data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "Billing API returned invalid JSON."
            ) from exc

        if "errors" in response_data:
            raise RuntimeError(
                json.dumps(response_data["errors"], indent=4)
            )

        if not isinstance(response_data.get("data"), list):
            raise RuntimeError(
                "Unexpected billing response: data is not an array."
            )

        records = response_data["data"]

        all_records.extend(records)

        raw_pages.append(response_data)

        metadata = response_data.get("metadata") or {}
        next_token = metadata.get("next")

        if not next_token:
            break

        next_token = str(next_token)

        if next_token in seen_tokens:
            raise RuntimeError(
                "Repeated pagination token detected. "
                "Stopping to avoid an infinite loop."
            )

        seen_tokens.add(next_token)

        params["page_token"] = next_token

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "page_size": page_size,
        "record_count": len(all_records),
        "records": all_records,
        "pages": raw_pages
    }


# ============================================================
# BILLING ANALYSIS
# ============================================================

def prepare_billing_dataframe(records):
    """Convert billing records into a DataFrame."""

    if not records:
        return pd.DataFrame()

    flattened_records = []

    for record in records:

        resource = record.get("resource") or {}
        environment = resource.get("environment") or {}

        flattened_records.append({
            "product": record.get("product") or "UNKNOWN",
            "line_type": record.get("line_type") or "UNKNOWN",
            "type": record.get("type") or "UNKNOWN",
            "resource_name": resource.get("display_name") or "UNALLOCATED",
            "resource_id": resource.get("id") or "UNALLOCATED",
            "environment_id": environment.get("id") or "UNALLOCATED",
            "start_date": record.get("start_date"),
            "end_date": record.get("end_date"),
            "granularity": record.get("granularity"),
            "quantity": record.get("quantity"),
            "unit": record.get("unit"),
            "price": record.get("price"),
            "original_amount": record.get("original_amount", 0),
            "discount_amount": record.get("discount_amount", 0),
            "amount": record.get("amount", 0),
            "network_access_type": record.get("network_access_type")
        })

    return pd.DataFrame(flattened_records)


def calculate_billing_summary(df):
    """Calculate billing totals without discarding credits."""

    if df.empty:
        return {
            "gross_charges": 0.0,
            "credits": 0.0,
            "net_amount": 0.0,
            "line_items": 0
        }

    gross_charges = df.loc[
        df["amount"] > 0,
        "amount"
    ].sum()

    credits = df.loc[
        df["amount"] < 0,
        "amount"
    ].sum()

    net_amount = df["amount"].sum()

    return {
        "gross_charges": float(gross_charges),
        "credits": float(credits),
        "net_amount": float(net_amount),
        "line_items": len(df)
    }


def save_billing_results(billing_data):
    """Save raw billing records and response pages."""

    output_path = RESULTS_DIR / "billing.json"

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            billing_data,
            file,
            indent=4,
            ensure_ascii=False
        )

    return output_path


def save_resource_result(tool_name, result):
    """Save the latest MCP resource query result."""

    safe_name = tool_name.replace("-", "_")

    output_path = RESULTS_DIR / f"{safe_name}_latest.json"

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            result,
            file,
            indent=4,
            ensure_ascii=False
        )

    return output_path


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("Navigation")

selected_section = st.sidebar.radio(
    "Select section",
    [
        "Dashboard",
        "Resource Explorer",
        "Billing Explorer"
    ]
)

st.sidebar.divider()

st.sidebar.caption("Confluent Cloud connection")

st.sidebar.write(
    f"Configuration: {'Available' if CONFIG_PATH.exists() else 'Not found'}"
)

st.sidebar.write(
    f"Parent .env: {'Available' if PARENT_ENV_FILE.exists() else 'Not found'}"
)


# ============================================================
# DASHBOARD
# ============================================================

if selected_section == "Dashboard":

    st.header("Organization Overview")

    st.write(
        "Use this dashboard to query your Confluent Cloud "
        "organization and explore billing information."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Resource Explorer")

        st.write(
            "Discover environments, Kafka clusters, topics, "
            "connectors, schemas and Flink compute pools."
        )

        if st.button(
            "Open Resource Explorer",
            width="content"
        ):
            st.session_state["navigation_target"] = "Resource Explorer"
            st.rerun()

    with col2:
        st.subheader("Billing Explorer")

        st.write(
            "Retrieve billing records, inspect resource-level "
            "costs and identify promotional credits."
        )

        if st.button(
            "Open Billing Explorer",
            width="content"
        ):
            st.session_state["navigation_target"] = "Billing Explorer"
            st.rerun()

    st.divider()

    st.subheader("Available capabilities")

    capability_col1, capability_col2, capability_col3 = st.columns(3)

    with capability_col1:
        st.metric("Resource integration", "MCP")

    with capability_col2:
        st.metric("Billing integration", "REST API")

    with capability_col3:
        st.metric("Report analysis", "Gemini - Next phase")


# ============================================================
# RESOURCE EXPLORER
# ============================================================

elif selected_section == "Resource Explorer":

    st.header("Resource Explorer")

    st.write(
        "Query Confluent Cloud resources through your existing "
        "Confluent MCP server."
    )

    if not CONFIG_PATH.exists():
        st.error(
            f"MCP configuration not found: {CONFIG_PATH}"
        )
        st.stop()

    if st.button(
        "Load Available MCP Tools",
        type="primary"
    ):

        with st.spinner("Connecting to Confluent MCP server..."):

            try:
                tools = run_async(get_mcp_tools())

                st.session_state["mcp_tools"] = tools

                st.success(
                    f"Connected successfully. "
                    f"Retrieved {len(tools)} MCP tools."
                )

            except Exception as exc:
                st.error(f"MCP connection failed: {exc}")

    available_tools = st.session_state.get("mcp_tools", [])

    if available_tools:

        available_tool_map = {
            tool["name"]: tool
            for tool in available_tools
        }

        usable_tools = [
            name for name in READ_ONLY_TOOLS
            if name in available_tool_map
        ]

        st.metric(
            "Available read-only tools",
            len(usable_tools)
        )

        selected_tool = st.selectbox(
            "Select resource query",
            usable_tools,
            format_func=lambda name: (
                f"{name} — {TOOL_DESCRIPTIONS.get(name, '')}"
            )
        )

        selected_tool_info = available_tool_map[selected_tool]

        schema = get_tool_schema(
            selected_tool_info,
            selected_tool_info.get("input_schema", {})
        )

        st.subheader("Tool information")

        st.write(
            selected_tool_info.get("description")
            or TOOL_DESCRIPTIONS.get(selected_tool, "")
        )

        with st.expander("View input schema"):
            st.json(selected_tool_info.get("input_schema", {}))

        st.subheader("Query parameters")

        default_arguments = st.session_state.get(
            f"args_{selected_tool}",
            "{}"
        )

        arguments_text = st.text_area(
            "Enter tool arguments as JSON",
            value=default_arguments,
            height=160,
            help=(
                "Use the exact argument names and types shown "
                "in the input schema. Use {} for tools requiring "
                "no arguments."
            )
        )

        required_fields = schema.get("required", [])

        if required_fields:
            st.caption(
                "Required parameters: "
                + ", ".join(required_fields)
            )

        if st.button(
            "Execute Resource Query",
            type="primary"
        ):

            try:
                arguments = json.loads(arguments_text)

                if not isinstance(arguments, dict):
                    raise ValueError(
                        "Arguments must be a JSON object."
                    )

                missing_fields = [
                    field for field in required_fields
                    if field not in arguments
                ]

                if missing_fields:
                    raise ValueError(
                        "Missing required parameters: "
                        + ", ".join(missing_fields)
                    )

                with st.spinner(
                    f"Executing {selected_tool}..."
                ):

                    result = run_async(
                        execute_mcp_tool(
                            selected_tool,
                            arguments
                        )
                    )

                st.session_state["last_resource_result"] = result
                st.session_state["last_resource_tool"] = selected_tool

                output_path = save_resource_result(
                    selected_tool,
                    result
                )

                if result["is_error"]:
                    st.error("The MCP tool returned an error.")
                else:
                    st.success("Resource query completed.")

                st.caption(f"Saved result: {output_path}")

            except json.JSONDecodeError as exc:
                st.error(
                    f"Invalid JSON arguments: {exc}"
                )

            except Exception as exc:
                st.error(
                    f"Resource query failed: {exc}"
                )

    else:
        st.info(
            "Click 'Load Available MCP Tools' to connect "
            "to your Confluent MCP server."
        )

    if "last_resource_result" in st.session_state:

        st.divider()

        st.subheader("Query Result")

        last_result = st.session_state["last_resource_result"]

        structured_content = last_result.get(
            "structured_content"
        )

        if structured_content is not None:
            st.subheader("Structured response")
            st.json(structured_content)

        content = last_result.get("content", [])

        if content:
            st.subheader("Text response")

            for item in content:
                st.code(item, language="text")

        if not structured_content and not content:
            st.info("The tool returned an empty response.")


# ============================================================
# BILLING EXPLORER
# ============================================================

elif selected_section == "Billing Explorer":

    st.header("Billing Explorer")

    st.write(
        "Retrieve original Confluent Cloud billing records "
        "directly from the Billing REST API."
    )

    st.info(
        "Billing retrieval uses the Confluent Cloud API credentials "
        "in your parent .env file. It does not use the MCP billing "
        "handler, so records without product are preserved."
    )

    today = date.today()

    default_start = today - timedelta(days=1)
    default_end = today

    st.subheader("Billing date range")

    date_col1, date_col2, date_col3 = st.columns(3)

    with date_col1:
        start_date = st.date_input(
            "Start date",
            value=default_start
        )

    with date_col2:
        end_date = st.date_input(
            "End date",
            value=default_end
        )

    with date_col3:
        page_size = st.number_input(
            "Page size",
            min_value=1,
            max_value=DEFAULT_PAGE_SIZE,
            value=DEFAULT_PAGE_SIZE,
            step=100
        )

    if end_date < start_date:
        st.error("End date cannot be before start date.")

    elif (end_date - start_date).days > MAX_BILLING_RANGE_DAYS:
        st.error(
            f"Billing date range cannot exceed "
            f"{MAX_BILLING_RANGE_DAYS} days."
        )

    if st.button(
        "Retrieve Billing Data",
        type="primary",
        disabled=(
            end_date < start_date
            or (end_date - start_date).days > MAX_BILLING_RANGE_DAYS
        )
    ):

        try:
            with st.spinner(
                "Retrieving billing data from Confluent Cloud..."
            ):

                billing_data = retrieve_billing(
                    start_date=start_date,
                    end_date=end_date,
                    page_size=int(page_size)
                )

                output_path = save_billing_results(
                    billing_data
                )

            st.session_state["billing_data"] = billing_data

            st.success(
                f"Retrieved {billing_data['record_count']} billing records."
            )

            st.caption(
                f"Raw billing data saved to: {output_path}"
            )

        except Exception as exc:
            st.error(
                f"Billing retrieval failed: {exc}"
            )

    # --------------------------------------------------------
    # DISPLAY BILLING RESULTS
    # --------------------------------------------------------

    billing_data = st.session_state.get("billing_data")

    if billing_data:

        records = billing_data.get("records", [])

        df = prepare_billing_dataframe(records)

        st.divider()

        st.subheader("Billing Summary")

        summary = calculate_billing_summary(df)

        metric1, metric2, metric3, metric4 = st.columns(4)

        metric1.metric(
            "Gross Charges",
            f"${summary['gross_charges']:.4f}"
        )

        metric2.metric(
            "Promotional Credits / Adjustments",
            f"${summary['credits']:.4f}"
        )

        metric3.metric(
            "Net Amount",
            f"${summary['net_amount']:.4f}"
        )

        metric4.metric(
            "Line Items",
            summary["line_items"]
        )

        st.caption(
            f"Period: {billing_data['start_date']} "
            f"to {billing_data['end_date']}"
        )

        # ----------------------------------------------------
        # PRODUCT BREAKDOWN
        # ----------------------------------------------------

        st.divider()

        st.subheader("Product Breakdown")

        if not df.empty:

            product_df = (
                df.groupby("product", as_index=False)["amount"]
                .sum()
                .sort_values("amount", ascending=False)
            )

            st.dataframe(
                product_df,
                width="content",
                hide_index=True
            )

            chart_data = product_df.set_index("product")

            st.bar_chart(chart_data)

        # ----------------------------------------------------
        # RESOURCE BREAKDOWN
        # ----------------------------------------------------

        st.divider()

        st.subheader("Resource-level Cost Breakdown")

        if not df.empty:

            resource_df = (
                df.groupby(
                    [
                        "product",
                        "resource_name",
                        "resource_id",
                        "environment_id"
                    ],
                    as_index=False
                )["amount"]
                .sum()
                .sort_values("amount", ascending=False)
            )

            st.dataframe(
                resource_df,
                width="content",
                hide_index=True
            )

        # ----------------------------------------------------
        # BILLING LINE ITEMS
        # ----------------------------------------------------

        st.divider()

        st.subheader("Billing Line Items")

        if not df.empty:

            st.dataframe(
                df,
                width="content",
                hide_index=True
            )

        else:
            st.info("No billing records found for this period.")

        # ----------------------------------------------------
        # UNALLOCATED RECORDS
        # ----------------------------------------------------

        st.divider()

        st.subheader("Unallocated Billing Records")

        if not df.empty:

            unallocated_df = df[
                (df["resource_id"] == "UNALLOCATED")
                | (df["product"] == "UNKNOWN")
            ]

            if not unallocated_df.empty:

                st.warning(
                    f"{len(unallocated_df)} records "
                    "have missing product or resource information."
                )

                st.dataframe(
                    unallocated_df,
                    width="content",
                    hide_index=True
                )

            else:
                st.success(
                    "All records have product and resource information."
                )

        # ----------------------------------------------------
        # RAW RESPONSE
        # ----------------------------------------------------

        st.divider()

        st.subheader("Raw Billing API Response")

        with st.expander("View raw API response"):

            st.json(billing_data)

        # ----------------------------------------------------
        # DOWNLOAD
        # ----------------------------------------------------

        st.divider()

        st.subheader("Export Billing Data")

        download_col1, download_col2 = st.columns(2)

        with download_col1:

            json_content = json.dumps(
                billing_data,
                indent=4,
                ensure_ascii=False
            )

            st.download_button(
                "Download JSON",
                data=json_content,
                file_name="confluent_billing.json",
                mime="application/json",
                width="content"
            )

        with download_col2:

            csv_content = df.to_csv(index=False)

            st.download_button(
                "Download CSV",
                data=csv_content,
                file_name="confluent_billing.csv",
                mime="text/csv",
                width="content"
            )

    else:

        st.info(
            "Select a billing date range and click "
            "'Retrieve Billing Data' to begin."
        )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Confluent FinOps Explorer | Resource discovery through MCP | "
    "Billing retrieval through Confluent Cloud REST API"
)
