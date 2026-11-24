"""Confluent FinOps Intelligence Chatbot.

Backend-only application: Confluent Billing REST API + existing Confluent MCP
server + Gemini agents. No uploads, documentation crawling, FAISS, or changes
to the existing finops-ai / MCP Explorer project.

Run: streamlit run chatbot.py
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from google import genai
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from finops_agents import AgentError, run_inventory_agents
from team_cost_allocation import build_team_cost_report

# load_dotenv(APP_ENV, override=False)
# load_dotenv(PARENT_ENV, override=False)

APP_DIR = Path(__file__).resolve().parent
PARENT_DIR = APP_DIR.parent
PARENT_ENV = PARENT_DIR / ".env"
APP_ENV = APP_DIR / ".env"
load_dotenv(APP_ENV, override=False)
load_dotenv(PARENT_ENV, override=False)

CONFIG_PATH = Path(os.getenv("CONFLUENT_MCP_CONFIG", str(PARENT_DIR / "config.yaml"))).expanduser()
if not CONFIG_PATH.is_absolute():
    CONFIG_PATH = (PARENT_DIR / CONFIG_PATH).resolve()
DATA_DIR = APP_DIR / "data_store"
HISTORY_DIR = DATA_DIR / "chat_history"
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_API_KEY = (
    os.getenv("GOOGLE_API_KEY")
    or os.getenv("GEMINI_API_KEY")
    or ""
).strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash").strip()
CONFLUENT_API_KEY = os.getenv("CONFLUENT_CLOUD_API_KEY", "").strip()
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_CLOUD_API_SECRET", "").strip()
BILLING_API_URL = os.getenv("CONFLUENT_BILLING_URL", "https://api.confluent.cloud/billing/v1/costs").strip()
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
MCP_TIMEOUT = int(os.getenv("MCP_TIMEOUT_SECONDS", "800"))
MAX_DAYS_PER_REQUEST = 31
MAX_CONTEXT_ROWS = 250

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("confluent-finops-chatbot")


class FinOpsError(RuntimeError):
    pass


def money(value: float) -> str:
    return f"${value:,.4f}"


def as_float(value: Any) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def render_team_cost_report(report: dict[str, Any]) -> None:
    st.subheader("Team Cost Allocation")
    st.caption(
        "Direct costs are actual billing amounts matched by exact resource ID. "
        "Tag ownership comes from MCP and Stream Catalog metadata. Shared and "
        "unallocated charges are not automatically distributed."
    )

    catalog = report.get("catalog_discovery", {})
    if not catalog.get("available"):
        st.warning(
            "Stream Catalog discovery is unavailable. "
            + "; ".join(catalog.get("errors", []))
        )

    st.markdown("**Tag definitions discovered**")
    st.write(report.get("tag_definitions_discovered", []))
    st.markdown("**Team tags discovered**")
    st.write(report.get("team_tags_discovered", []))
    st.markdown("**Shared/platform tags discovered**")
    st.write(report.get("shared_tags_discovered", []))

    # Show real entity assignments returned by Stream Catalog.
    catalog_entities = catalog.get("entities", [])
    st.markdown("**Stream Catalog tag assignments**")
    if catalog_entities:
        st.dataframe(
            pd.DataFrame([
                {
                    "Resource type": item.get("resource_type"),
                    "Catalog entity type": item.get("catalog_entity_type"),
                    "Resource name": item.get("resource_name"),
                    "Resource ID": item.get("resource_id"),
                    "Qualified name": item.get("qualified_name"),
                    "Tags": ", ".join(item.get("tags", [])),
                    "GUID": item.get("guid"),
                    "Status": item.get("status"),
                }
                for item in catalog_entities
            ]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(
            "Stream Catalog returned no entities for the searched tags. "
            "Review the coverage details below before concluding that resources "
            "are untagged."
        )

    checks = catalog.get("checks", [])
    if checks:
        with st.expander("Stream Catalog search coverage", expanded=False):
            st.dataframe(
                pd.DataFrame([
                    {
                        "Tag": item.get("tag"),
                        "Entity type": item.get("entity_type"),
                        "Status": item.get("status"),
                        "HTTP status": item.get("http_status"),
                        "Results": item.get("result_count", 0),
                    }
                    for item in checks
                ]),
                use_container_width=True,
                hide_index=True,
            )
    if catalog.get("errors"):
        with st.expander("Stream Catalog errors", expanded=False):
            st.write(catalog["errors"])

    st.markdown("**Team-wise direct actual costs**")
    team_rows = [
        {
            "Team": item["team"],
            "Direct actual cost": item["direct_actual_cost"],
            "Matched billing resources": item["matched_billing_resources"],
            "Assigned inventory resources": len(item.get("assigned_inventory_resources", [])),
        }
        for item in report.get("teams", [])
    ]
    if team_rows:
        st.dataframe(
            pd.DataFrame(team_rows),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No team ownership tags were discovered in the available metadata.")

    st.markdown("**Merged resource ownership**")
    ownership_rows = [
        {
            "Resource type": item.get("resource_type"),
            "Resource name": item.get("resource_name"),
            "Resource ID": item.get("resource_id"),
            "Tags": ", ".join(item.get("tags", [])),
            "Owner": item.get("owner") or "Unassigned",
            "Ownership status": item.get("ownership_status"),
            "Metadata source": ", ".join(item.get("sources", [])),
            "Qualified name": item.get("qualified_name"),
            "Billing match": item.get("billing_match_status"),
            "Direct actual cost": item.get("billing_actual_cost"),
        }
        for item in report.get("inventory", [])
    ]
    if ownership_rows:
        st.dataframe(
            pd.DataFrame(ownership_rows),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No resource inventory records were returned.")

    with st.expander(
        f"Shared infrastructure billing ({len(report.get('shared_billing_rows', []))})"
    ):
        st.json(report.get("shared_billing_rows", []))

    with st.expander(
        f"Unallocated billing ({len(report.get('unallocated_billing_rows', []))})"
    ):
        st.json(report.get("unallocated_billing_rows", []))

    with st.expander(
        f"Unmatched billing ({len(report.get('unmatched_billing_rows', []))})"
    ):
        st.json(report.get("unmatched_billing_rows", []))

    with st.expander(
        f"Promotional credits ({len(report.get('promotional_credit_rows', []))})"
    ):
        st.json(report.get("promotional_credit_rows", []))

    st.info(report.get("allocation_status", "No allocation status was returned."))



def fetch_billing(start_date: date, end_exclusive: date) -> list[dict[str, Any]]:
    """Retrieve all billing rows, using bounded date windows and pagination."""
    if not CONFLUENT_API_KEY or not CONFLUENT_API_SECRET:
        raise FinOpsError("Confluent billing credentials are missing from the parent .env.")
    if end_exclusive <= start_date:
        raise FinOpsError("Billing end date must be later than start date.")

    http = requests.Session()
    http.auth = (CONFLUENT_API_KEY, CONFLUENT_API_SECRET)
    http.headers.update({"Accept": "application/json"})
    rows: list[dict[str, Any]] = []
    current = start_date
    while current < end_exclusive:
        window_end = min(current + timedelta(days=MAX_DAYS_PER_REQUEST), end_exclusive)
        params: dict[str, Any] = {
            "start_date": current.isoformat(),
            "end_date": window_end.isoformat(),
            "page_size": 1000,
        }
        while True:
            response = http.get(BILLING_API_URL, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            page_rows = payload.get("data", [])
            rows.extend(row for row in page_rows if isinstance(row, dict))
            next_token = (payload.get("metadata") or {}).get("next")
            if not next_token:
                break
            params["page_token"] = next_token
        current = window_end
    return rows


@st.cache_data(ttl=300, show_spinner=False)
def get_billing(start_iso: str, end_exclusive_iso: str) -> list[dict[str, Any]]:
    return fetch_billing(date.fromisoformat(start_iso), date.fromisoformat(end_exclusive_iso))


def normalize_billing(row: dict[str, Any]) -> dict[str, Any]:
    resource = row.get("resource") if isinstance(row.get("resource"), dict) else {}
    environment = resource.get("environment") if isinstance(resource.get("environment"), dict) else {}
    return {
        "resource_id": resource.get("id") or row.get("resource_id") or row.get("resource.id"),
        "resource_name": resource.get("display_name") or row.get("resource_display_name") or row.get("resource.display_name"),
        "environment_id": environment.get("id") or row.get("environment_id"),
        "product": row.get("product"),
        "line_type": row.get("line_type"),
        "amount": as_float(row.get("amount")),
        "original_amount": as_float(row.get("original_amount")),
        "quantity": row.get("quantity"),
        "unit": row.get("unit"),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "raw": row,
    }


def _walk_records(value: Any):
    if isinstance(value, dict):
        # Yield a resource-shaped record and continue through nested envelopes.
        if any(k in value for k in ("id", "resource_id", "cluster_id", "connector_id", "topic_id", "compute_pool_id", "name", "display_name")):
            yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from _walk_records(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_records(child)


def inventory_records(agent_result: dict[str, Any]) -> list[dict[str, Any]]:
    records = []
    seen = set()
    for tool_result in agent_result.get("results", []):
        tool_name = tool_result.get("tool", "unknown")
        for record in _walk_records(tool_result.get("result")):
            rid = (record.get("id") or record.get("resource_id") or record.get("cluster_id") or
                   record.get("connector_id") or record.get("topic_id") or record.get("compute_pool_id"))
            identity = (tool_name, str(rid) if rid else json.dumps(record, sort_keys=True, default=str))
            if identity in seen:
                continue
            seen.add(identity)
            records.append({
                "source_tool": tool_name,
                "resource_id": str(rid) if rid else None,
                "name": record.get("name") or record.get("display_name") or record.get("resource_name"),
                "environment_id": record.get("environment_id") or record.get("environmentId"),
                "raw": record,
            })
    return records


def calculate_summary(billing_rows: list[dict[str, Any]], agent_result: dict[str, Any]) -> dict[str, Any]:
    billing = [normalize_billing(row) for row in billing_rows]
    inventory = inventory_records(agent_result)
    inventory_ids = {row["resource_id"] for row in inventory if row.get("resource_id")}

    promo = [r for r in billing if str(r.get("line_type") or "").upper() == "PROMO_CREDIT"]
    non_promo = [r for r in billing if str(r.get("line_type") or "").upper() != "PROMO_CREDIT"]
    resource_groups: dict[str, dict[str, Any]] = {}
    unallocated = []
    for row in billing:
        rid = row.get("resource_id")
        if not rid or str(row.get("line_type") or "").upper() == "PROMO_CREDIT":
            unallocated.append(row)
            continue
        key = str(rid)
        group = resource_groups.setdefault(key, {
            "resource_id": key, "resource_name": row.get("resource_name"),
            "amount": 0.0, "record_count": 0, "products": set(), "line_types": set(),
        })
        group["amount"] += row["amount"]
        group["record_count"] += 1
        if row.get("product"):
            group["products"].add(str(row["product"]))
        if row.get("line_type"):
            group["line_types"].add(str(row["line_type"]))

    resources = []
    for item in resource_groups.values():
        matched = item["resource_id"] in inventory_ids
        resources.append({
            **item,
            "products": sorted(item["products"]),
            "line_types": sorted(item["line_types"]),
            "match_status": "matched by exact resource ID" if matched else (
                "unmatched / historical" if inventory_ids else "not verified: inventory IDs unavailable"
            ),
        })
    resources.sort(key=lambda x: x["amount"], reverse=True)

    by_product: dict[str, float] = {}
    by_line_type: dict[str, float] = {}
    by_unit: dict[str, float] = {}
    for row in billing:
        product = str(row.get("product") or "Unspecified")
        line_type = str(row.get("line_type") or "Unspecified")
        unit = str(row.get("unit") or "Unspecified")
        by_product[product] = by_product.get(product, 0.0) + row["amount"]
        by_line_type[line_type] = by_line_type.get(line_type, 0.0) + row["amount"]
        by_unit[unit] = by_unit.get(unit, 0.0) + row["amount"]

    unmatched = [r for r in resources if inventory_ids and r["resource_id"] not in inventory_ids]
    return {
        "billing_count": len(billing),
        "inventory_count": len(inventory),
        "gross": sum(r["amount"] for r in non_promo),
        "promo_credit": sum(r["amount"] for r in promo),
        "net": sum(r["amount"] for r in billing),
        "by_product": dict(sorted(by_product.items(), key=lambda x: x[1], reverse=True)),
        "by_line_type": dict(sorted(by_line_type.items(), key=lambda x: x[1], reverse=True)),
        "by_unit": dict(sorted(by_unit.items(), key=lambda x: x[1], reverse=True)),
        "resources": resources,
        "unallocated": unallocated,
        "unmatched": unmatched,
        "inventory": inventory,
        "agent_result": agent_result,
    }



def _summary_context(summary: dict[str, Any]) -> str:
    lines = [
        "FINANCIAL SOURCE OF TRUTH: Confluent Billing REST API line items.",
        f"Billing rows: {summary['billing_count']}",
        f"Gross non-promo amount: {money(summary['gross'])}",
        f"Promotional credits (preserved as negative/unallocated): {money(summary['promo_credit'])}",
        f"Net billed amount: {money(summary['net'])}",
        "Product totals:",
    ]

    lines.extend(
        f"- {name}: {money(amount)}"
        for name, amount in summary["by_product"].items()
    )

    lines.append("Billing line-type totals:")
    lines.extend(
        f"- {name}: {money(amount)}"
        for name, amount in summary["by_line_type"].items()
    )

    lines.append("Resource totals (exact resource ID matching only):")

    for row in summary["resources"][:MAX_CONTEXT_ROWS]:
        lines.append(
            f"- ID={row['resource_id']}; "
            f"name={row.get('resource_name')}; "
            f"amount={money(row['amount'])}; "
            f"products={','.join(row['products'])}; "
            f"match={row['match_status']}"
        )

    lines.append("Unallocated billing rows:")

    for row in summary["unallocated"][:MAX_CONTEXT_ROWS]:
        lines.append(
            f"- line_type={row.get('line_type')}; "
            f"product={row.get('product')}; "
            f"amount={money(row['amount'])}; "
            f"resource_id={row.get('resource_id')}"
        )

    lines.append(
        "MCP inventory records (inventory and metadata evidence, not billing authority):"
    )

    for row in summary["inventory"][:MAX_CONTEXT_ROWS]:
        lines.append(
            f"- source_tool={row['source_tool']}; "
            f"id={row.get('resource_id')}; "
            f"name={row.get('name')}; "
            f"environment={row.get('environment_id')}; "
            f"raw_metadata={json.dumps(row.get('raw', {}), ensure_ascii=False, default=str)[:3000]}"
        )

    team_report = summary.get("team_cost_report")
    if team_report:
        lines.append("TEAM OWNERSHIP REPORT (Python-generated; use these exact results):")
        report_payload = {
            "tag_definitions_discovered": team_report.get("tag_definitions_discovered", []),
            "team_tags_discovered": team_report.get("team_tags_discovered", []),
            "shared_tags_discovered": team_report.get("shared_tags_discovered", []),
            "inventory": team_report.get("inventory", []),
            "catalog_discovery": team_report.get("catalog_discovery", {}),
            "teams": team_report.get("teams", []),
            "shared_billing_rows": team_report.get("shared_billing_rows", []),
            "unmatched_billing_rows": team_report.get("unmatched_billing_rows", []),
            "unallocated_billing_rows": team_report.get("unallocated_billing_rows", []),
            "allocation_status": team_report.get("allocation_status"),
            "limitations": team_report.get("limitations", []),
        }
        report_text = json.dumps(report_payload, ensure_ascii=False, default=str)
        lines.append(report_text[:50000])

    agent = summary.get("agent_result", {})

    lines.append(f"MCP tools discovered: {agent.get('tool_count', 0)}")
    lines.append("MCP tool execution results:")

    for call in agent.get("results", []):
        result_text = json.dumps(
            call.get("result"),
            ensure_ascii=False,
            default=str,
        )

        # Keep useful tool output while preventing excessive prompt size.
        max_result_chars = 12000
        if len(result_text) > max_result_chars:
            result_text = (
                result_text[:max_result_chars]
                + "... [RESULT TRUNCATED]"
            )

        lines.append(
            f"\nTOOL: {call.get('tool')}\n"
            f"PURPOSE: {call.get('purpose')}\n"
            f"ARGUMENTS: {json.dumps(call.get('arguments', {}), ensure_ascii=False, default=str)}\n"
            f"ACTUAL MCP RESULT: {result_text}\n"
        )

    if agent.get("errors"):
        lines.append("MCP tool errors:")
        lines.extend(
            f"- {e.get('tool')}: {e.get('error')}"
            for e in agent["errors"]
        )

    return "\n".join(lines)


SYSTEM_PROMPT = """You are a Confluent Cloud FinOps Analyst Agent.
Answer only from the supplied billing, MCP, and Stream Catalog results. Billing REST API line items are the financial source of truth. MCP and Stream Catalog results describe inventory and metadata only; never treat inventory as a charge. Python-calculated totals are authoritative and must not be recomputed or changed by you.
For tag ownership questions, use the Python-generated TEAM OWNERSHIP REPORT and its catalog_discovery.entities as the authoritative tag-assignment evidence. Do not ignore Stream Catalog results and do not claim that no resources have a tag when matching entities are present there. Distinguish a tag definition from an entity assignment. A zero-result query for one entity type is not proof that the tag is absent from all resource types. Unsupported/invalid entity-type queries are coverage gaps, not zero-result evidence.
Never invent resource IDs, names, costs, quantities, dates, credits, or savings. Match billing and inventory only by exact resource IDs; never match by display name. Keep promo credits negative and separate. Keep billing-only or unmatched IDs visible. If inventory tool calls fail or a category is not exposed/enabled, state that coverage gap instead of claiming the resource type does not exist. Distinguish measured costs from suggestions or estimates. Do not assign a Kafka cluster's full charge to its topics. Give concise tables when useful. Never reveal credentials or environment variables."""


def generate_answer(question: str, context: str) -> str:
    if not GOOGLE_API_KEY:
        raise FinOpsError(
            "Google Gemini API key is missing. "
            "Set GOOGLE_API_KEY or GEMINI_API_KEY in the parent .env."
        )

    client = genai.Client(api_key=GOOGLE_API_KEY)

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=(
            f"{SYSTEM_PROMPT}\n\n"
            f"LIVE ACCOUNT DATA:\n{context}\n\n"
            f"USER QUESTION:\n{question}\n\n"
            "Answer directly. Clearly state when data is missing."
        ),
        config={
            "temperature": 0.15,
            "max_output_tokens": 2400,
        },
    )

    answer = getattr(response, "text", None)

    if not answer:
        raise FinOpsError("Gemini returned an empty response.")

    return answer.strip()


def persist_history(messages: list[dict[str, str]]) -> None:
    path = HISTORY_DIR / f"chat_{date.today().isoformat()}.json"
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def render_sidebar() -> tuple[date, date]:
    with st.sidebar:
        st.header("FinOps settings")
        st.caption("Existing Confluent MCP configuration")
        st.code(str(CONFIG_PATH), language=None)
        st.write("MCP config:", "Found" if CONFIG_PATH.is_file() else "Not found")
        today = date.today()
        start = st.date_input("Start date (inclusive)", value=today.replace(day=1), key="start_date")
        end = st.date_input("End date (inclusive)", value=today, key="end_date")
        if st.button("Refresh backend data", use_container_width=True):
            get_billing.clear()
            st.rerun()
        st.caption("Backend-only data. No file uploads. Existing finops-ai / Explorer is unchanged.")
        return start, end


def render_overview(summary: dict[str, Any]) -> None:
    st.subheader("Billing overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gross charges", money(summary["gross"]))
    c2.metric("Promotional credits", money(summary["promo_credit"]))
    c3.metric("Net billing", money(summary["net"]))
    c4.metric("Billing line items", f"{summary['billing_count']:,}")
    st.caption(f"MCP inventory records returned for this question: {summary['inventory_count']:,}")

    left, right = st.columns(2)
    with left:
        st.markdown("**Cost by product**")
        if summary["by_product"]:
            st.dataframe(pd.DataFrame([{"Product": k, "Amount": v} for k, v in summary["by_product"].items()]),
                         use_container_width=True, hide_index=True)
    with right:
        st.markdown("**Cost by billing line type**")
        if summary["by_line_type"]:
            st.dataframe(pd.DataFrame([{"Line type": k, "Amount": v} for k, v in summary["by_line_type"].items()]),
                         use_container_width=True, hide_index=True)

    if summary["resources"]:
        st.markdown("**Cost by resource ID**")
        st.dataframe(pd.DataFrame([{
            "Resource ID": r["resource_id"], "Resource name": r.get("resource_name"),
            "Amount": r["amount"], "Billing rows": r["record_count"],
            "Products": ", ".join(r["products"]), "Reconciliation": r["match_status"],
        } for r in summary["resources"]]), use_container_width=True, hide_index=True)
    with st.expander(f"Unallocated billing rows ({len(summary['unallocated'])})"):
        if summary["unallocated"]:
            st.dataframe(pd.DataFrame([{
                "Date": r.get("start_date"), "Product": r.get("product"),
                "Line type": r.get("line_type"), "Amount": r["amount"],
                "Resource ID": r.get("resource_id"), "Unit": r.get("unit"),
            } for r in summary["unallocated"]]), use_container_width=True, hide_index=True)
        else:
            st.write("No unallocated rows in the selected period.")


def main() -> None:
    st.set_page_config(page_title="Confluent FinOps Intelligence", layout="wide")
    st.title("Confluent FinOps Intelligence Chatbot")
    st.caption("Multi-agent billing and inventory analysis using Confluent Billing REST API, live MCP tool descriptions and Gemini.")
    start_date, end_date = render_sidebar()
    if end_date < start_date:
        st.error("End date cannot be earlier than start date.")
        return

    if "messages" not in st.session_state:
        st.session_state.messages = []
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    question = st.chat_input("Ask about total spend, clusters, topics, Flink CFUs, ksqlDB, connectors, tags, or optimization...")
    if not question:
        return
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Billing Agent: retrieving authoritative billing line items..."):
                billing_rows = get_billing(start_date.isoformat(), (end_date + timedelta(days=1)).isoformat())
            with st.spinner("Supervisor: discovering MCP tools and reading their descriptions and schemas..."):
                agent_result = run_inventory_agents(
                    question=question,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    config_path=CONFIG_PATH,
                    api_key=GOOGLE_API_KEY,
                    model_name=GEMINI_MODEL,
                    timeout_seconds=MCP_TIMEOUT,
                )
            summary = calculate_summary(billing_rows, agent_result)

            team_report = build_team_cost_report(
                billing_rows=billing_rows,
                agent_result=agent_result,
            )

            summary["team_cost_report"] = team_report

            render_overview(summary)
            render_team_cost_report(team_report)

            with st.expander("Agent execution trace", expanded=False):
                st.write(f"Discovered {agent_result['tool_count']} MCP tools.")
                st.markdown("**Selected tool plan**")
                st.json(agent_result.get("plan", []))
                st.markdown("**Executed tool results (truncated for display)**")
                st.json(agent_result.get("results", []))
                if agent_result.get("errors"):
                    st.markdown("**Tool errors**")
                    st.json(agent_result["errors"])
                st.markdown("**Read-only tool catalogue**")
                st.dataframe(pd.DataFrame([{
                    "Tool": t["name"], "Description": t["description"],
                    "Read only": t["read_only"], "Input schema": json.dumps(t["inputSchema"], ensure_ascii=False),
                } for t in agent_result.get("catalog", [])]), use_container_width=True, hide_index=True)

            with st.spinner("FinOps Analyst: reconciling results and preparing the answer..."):
                answer = generate_answer(question, _summary_context(summary))
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})
            persist_history(st.session_state.messages)
        except (FinOpsError, AgentError, requests.RequestException) as exc:
            message = f"Unable to complete the FinOps analysis: {exc}"
            st.error(message)
            logger.exception("FinOps request failed")
            st.session_state.messages.append({"role": "assistant", "content": message})
            persist_history(st.session_state.messages)
        except Exception as exc:
            message = f"Unexpected error during FinOps analysis: {type(exc).__name__}: {exc}"
            st.error(message)
            logger.exception("Unexpected FinOps request failure")
            st.session_state.messages.append({"role": "assistant", "content": message})
            persist_history(st.session_state.messages)


if __name__ == "__main__":
    main()
