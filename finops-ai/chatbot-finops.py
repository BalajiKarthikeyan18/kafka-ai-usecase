"""
Confluent FinOps-only Chatbot
Run: streamlit run chatbot.py

No Confluent documentation crawling, embeddings, FAISS, confluence.index,
or metadata.pkl. The chatbot uses billing and inventory data only.

Project .env:
    GOOGLE_API_KEY=your_google_ai_studio_key
    GEMINI_MODEL=gemini-2.5-flash

Parent .env:
    CONFLUENT_CLOUD_API_KEY=...
    CONFLUENT_CLOUD_API_SECRET=...
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
import google.generativeai as genai


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

APP_DIR = Path(__file__).resolve().parent
PARENT_DIR = APP_DIR.parent

DATA_DIR = APP_DIR / "data_store"
BILLING_DIR = DATA_DIR / "billing"
INVENTORY_DIR = DATA_DIR / "inventory"
HISTORY_DIR = DATA_DIR / "chat_history"

for directory in (BILLING_DIR, INVENTORY_DIR, HISTORY_DIR):
    directory.mkdir(parents=True, exist_ok=True)

load_dotenv(APP_DIR / ".env", override=False)
load_dotenv(PARENT_DIR / ".env", override=False)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

CONFLUENT_API_KEY = os.getenv("CONFLUENT_CLOUD_API_KEY", "").strip()
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_CLOUD_API_SECRET", "").strip()
BILLING_API_URL = os.getenv(
    "CONFLUENT_BILLING_URL",
    "https://api.confluent.cloud/billing/v1/costs",
).strip()

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
MAX_DAYS_PER_REQUEST = 31
MAX_CONTEXT_ROWS = 150

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("finops-chatbot")


class FinOpsError(Exception):
    pass


# ---------------------------------------------------------------------
# File and data helpers
# ---------------------------------------------------------------------

def configure_gemini() -> None:
    if not GOOGLE_API_KEY:
        raise FinOpsError(
            "GOOGLE_API_KEY is missing. Add it to finops-chatbot/.env."
        )
    genai.configure(api_key=GOOGLE_API_KEY)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2, ensure_ascii=False, default=str)
    temp_path.replace(path)


def load_records_from_file(path: Path) -> list[dict[str, Any]]:
    try:
        suffix = path.suffix.lower()

        if suffix == ".json":
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)

            if isinstance(payload, list):
                return [row for row in payload if isinstance(row, dict)]

            if isinstance(payload, dict):
                for key in (
                    "data", "items", "results", "costs",
                    "billing", "resources", "inventory",
                ):
                    if isinstance(payload.get(key), list):
                        return [
                            row for row in payload[key]
                            if isinstance(row, dict)
                        ]
                return [payload]

        elif suffix == ".jsonl":
            records = []
            with path.open("r", encoding="utf-8") as file:
                for line in file:
                    if line.strip():
                        row = json.loads(line)
                        if isinstance(row, dict):
                            records.append(row)
            return records

        elif suffix == ".csv":
            frame = pd.read_csv(path)
            frame = frame.astype(object).where(pd.notna(frame), None)
            return frame.to_dict(orient="records")

    except Exception as exc:
        logger.exception("Unable to read %s", path)
        st.warning(f"Could not read {path.name}: {exc}")

    return []


def load_folder(folder: Path) -> list[dict[str, Any]]:
    records = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.suffix.lower() in {
            ".json", ".jsonl", ".csv"
        }:
            records.extend(load_records_from_file(path))
    return records


def save_upload(uploaded_file, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / Path(uploaded_file.name).name
    path.write_bytes(uploaded_file.getvalue())
    return path


def as_float(value: Any) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def money(value: float) -> str:
    return f"${value:,.4f}"


# ---------------------------------------------------------------------
# Confluent Cloud Billing API
# ---------------------------------------------------------------------

def fetch_billing(
    start_date: date,
    end_date_exclusive: date,
) -> list[dict[str, Any]]:
    """Fetch billing rows; end_date is exclusive and each window <= 31 days."""
    if not CONFLUENT_API_KEY or not CONFLUENT_API_SECRET:
        raise FinOpsError(
            "Confluent billing credentials are missing. Add "
            "CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET "
            "to the parent .env file."
        )

    if end_date_exclusive <= start_date:
        raise FinOpsError("End date must be later than start date.")

    session = requests.Session()
    session.auth = (CONFLUENT_API_KEY, CONFLUENT_API_SECRET)
    session.headers.update({"Accept": "application/json"})

    rows: list[dict[str, Any]] = []
    current = start_date

    while current < end_date_exclusive:
        window_end = min(
            current + timedelta(days=MAX_DAYS_PER_REQUEST),
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
            rows.extend(
                row for row in payload.get("data", [])
                if isinstance(row, dict)
            )

            next_token = (payload.get("metadata") or {}).get("next")
            if not next_token:
                break
            params["page_token"] = next_token

        current = window_end

    return rows


def save_billing_snapshot(
    rows: list[dict[str, Any]],
    start: date,
    end_inclusive: date,
) -> Path:
    path = BILLING_DIR / f"billing_{start}_{end_inclusive}.json"
    write_json(
        path,
        {
            "data": rows,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "start_date_inclusive": start.isoformat(),
            "end_date_inclusive": end_inclusive.isoformat(),
            "source": "Confluent Cloud Billing API",
        },
    )
    return path


# ---------------------------------------------------------------------
# Deterministic FinOps calculations
# ---------------------------------------------------------------------

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
        "raw": row,
    }


def normalize_inventory(row: dict[str, Any]) -> dict[str, Any]:
    resource_id = (
        row.get("id")
        or row.get("resource_id")
        or row.get("cluster_id")
        or row.get("connector_id")
    )
    return {
        "resource_id": str(resource_id) if resource_id else None,
        "name": row.get("name") or row.get("display_name") or row.get("resource_name"),
        "resource_type": row.get("resource_type") or row.get("type") or row.get("kind"),
        "environment_id": row.get("environment_id"),
        "raw": row,
    }


def calculate_summary(
    raw_billing: list[dict[str, Any]],
    raw_inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    billing = [normalize_billing(row) for row in raw_billing]
    inventory = [normalize_inventory(row) for row in raw_inventory]

    inventory_ids = {
        item["resource_id"] for item in inventory if item["resource_id"]
    }

    promo = [
        row for row in billing
        if str(row.get("line_type") or "").upper() == "PROMO_CREDIT"
    ]
    non_promo = [
        row for row in billing
        if str(row.get("line_type") or "").upper() != "PROMO_CREDIT"
    ]

    gross = sum(row["amount"] for row in non_promo)
    promo_total = sum(row["amount"] for row in promo)
    net = sum(row["amount"] for row in billing)

    grouped: dict[str, dict[str, Any]] = {}
    unallocated = []

    for row in billing:
        resource_id = row.get("resource_id")
        line_type = str(row.get("line_type") or "").upper()

        # Credits remain unallocated. Rows without IDs are also unallocated.
        if line_type == "PROMO_CREDIT" or not resource_id:
            unallocated.append(row)
            continue

        resource_id = str(resource_id)
        if resource_id not in grouped:
            grouped[resource_id] = {
                "resource_id": resource_id,
                "resource_name": row.get("resource_name"),
                "amount": 0.0,
                "record_count": 0,
                "products": set(),
            }

        item = grouped[resource_id]
        item["amount"] += row["amount"]
        item["record_count"] += 1
        if row.get("product"):
            item["products"].add(str(row["product"]))

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

    resources.sort(key=lambda x: x["amount"], reverse=True)

    return {
        "billing_count": len(billing),
        "inventory_count": len(inventory),
        "gross": gross,
        "promo_credit": promo_total,
        "net": net,
        "resources": resources,
        "unallocated": unallocated,
        "promo_rows": promo,
        "unmatched": unmatched,
        "inventory": inventory,
    }


def build_finops_context(summary: dict[str, Any]) -> str:
    if summary["billing_count"] == 0 and summary["inventory_count"] == 0:
        return (
            "No billing or inventory data is loaded. Do not make "
            "account-specific claims or invent costs."
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
            "These totals are calculated from the supplied billing rows."
        )

        resource_rows = []
        for item in summary["resources"][:MAX_CONTEXT_ROWS]:
            resource_rows.append(
                f"- ID={item['resource_id']}; "
                f"name={item.get('resource_name')}; "
                f"amount={money(item['amount'])}; "
                f"records={item['record_count']}; "
                f"products={', '.join(item['products']) or 'not specified'}; "
                f"status={item['match_status']}"
            )
        sections.append(
            "RESOURCE COSTS (grouped by exact resource ID only)\n"
            + ("\n".join(resource_rows) or "No resource-attributed rows.")
        )

        unallocated_rows = []
        for item in summary["unallocated"][:MAX_CONTEXT_ROWS]:
            unallocated_rows.append(
                f"- line_type={item.get('line_type')}; "
                f"amount={money(item['amount'])}; "
                f"product={item.get('product')}; "
                f"resource_id={item.get('resource_id')}"
            )
        sections.append(
            "UNALLOCATED BILLING ROWS\n"
            + ("\n".join(unallocated_rows) or "None.")
        )

    if summary["inventory_count"]:
        inventory_rows = []
        for item in summary["inventory"][:MAX_CONTEXT_ROWS]:
            inventory_rows.append(
                f"- ID={item.get('resource_id')}; "
                f"name={item.get('name')}; "
                f"type={item.get('resource_type')}; "
                f"environment={item.get('environment_id')}"
            )
        sections.append("INVENTORY SNAPSHOT\n" + "\n".join(inventory_rows))

    return "\n\n".join(sections)


# ---------------------------------------------------------------------
# Gemini answer generation (FinOps context only)
# ---------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a beginner-friendly FinOps assistant for Confluent Cloud.
Your scope is strictly FinOps: billing, expenditure, resource cost breakdown,
usage charges, promo credits, resource reconciliation, and cost optimization.

Use ONLY the supplied account billing and inventory data for account-specific
facts. Do not use Confluent documentation or claim to have consulted it.

Rules:
- Never invent billing amounts, resource IDs, names, usage, or billing periods.
- Use the exact totals calculated by Python.
- Reconcile billing and inventory only by exact resource IDs, never names.
- Keep unmatched or historical resource IDs visible as unmatched.
- If inventory is missing, say that resource matching cannot be verified.
- Keep promo credits negative and unallocated.
- Clearly distinguish actual billed amounts from estimates and examples.
- Do not claim a specific saving without enough data to calculate it.
- Give practical optimization suggestions, but label them as suggestions.
- If the supplied data does not answer a question, say what is missing.
- Never reveal credentials, API keys, or environment variable values.
Explain concepts in plain English and define FinOps terms when useful.
"""


def generate_answer(question: str, finops_context: str) -> str:
    configure_gemini()
    prompt = f"""{SYSTEM_PROMPT}

FINOPS ACCOUNT DATA:
{finops_context}

USER QUESTION:
{question}

Answer directly and clearly. Use bullets where useful. For cost questions,
state whether the answer is supported by actual billing data. If data is
missing, say so rather than guessing.
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


# ---------------------------------------------------------------------
# Streamlit interface
# ---------------------------------------------------------------------

def render_sidebar() -> None:
    with st.sidebar:
        st.header("FinOps data")
        st.caption("Billing folder")
        st.code(str(BILLING_DIR), language=None)
        st.caption("Inventory folder")
        st.code(str(INVENTORY_DIR), language=None)

        st.divider()
        st.subheader("Fetch live Confluent billing")

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

        if st.button("Fetch billing", use_container_width=True):
            try:
                if end < start:
                    raise FinOpsError("End date cannot be earlier than start date.")

                with st.spinner("Fetching billing from Confluent Cloud..."):
                    rows = fetch_billing(start, end + timedelta(days=1))
                    path = save_billing_snapshot(rows, start, end)

                st.success(f"Fetched {len(rows)} rows. Saved {path.name}.")
                st.rerun()

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "unknown"
                st.error(
                    f"Billing API returned HTTP {status}. Check API credentials, "
                    "permissions, and the selected date range."
                )
                logger.exception("Billing API request failed")
            except Exception as exc:
                st.error(str(exc))
                logger.exception("Billing fetch failed")

        st.divider()
        st.subheader("Upload data snapshots")

        billing_file = st.file_uploader(
            "Billing file",
            type=["json", "jsonl", "csv"],
            key="billing_file",
        )
        if billing_file and st.button("Save billing snapshot"):
            saved = save_upload(billing_file, BILLING_DIR)
            st.success(f"Saved {saved.name}")
            st.rerun()

        inventory_file = st.file_uploader(
            "Inventory file",
            type=["json", "jsonl", "csv"],
            key="inventory_file",
        )
        if inventory_file and st.button("Save inventory snapshot"):
            saved = save_upload(inventory_file, INVENTORY_DIR)
            st.success(f"Saved {saved.name}")
            st.rerun()

        st.divider()
        st.caption(
            "FinOps-only mode: no documentation crawler, embeddings, "
            "FAISS index, or documentation metadata."
        )


def render_overview(summary: dict[str, Any]) -> None:
    st.subheader("Billing overview")

    if summary["billing_count"] == 0:
        st.info(
            "No billing data is loaded. Fetch billing from Confluent Cloud "
            "or upload a billing JSON/CSV file."
        )
    else:
        first, second, third = st.columns(3)
        first.metric("Gross non-promo", money(summary["gross"]))
        second.metric("Promo credits", money(summary["promo_credit"]))
        third.metric("Net billing", money(summary["net"]))
        st.caption(
            f"{summary['billing_count']} billing rows | "
            f"{summary['inventory_count']} inventory rows"
        )

    if summary["resources"]:
        st.subheader("Resource cost breakdown")
        rows = [
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
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("Loaded data files"):
        billing_files = sorted(p.name for p in BILLING_DIR.iterdir() if p.is_file())
        inventory_files = sorted(p.name for p in INVENTORY_DIR.iterdir() if p.is_file())
        st.write("Billing:", billing_files or "No billing files")
        st.write("Inventory:", inventory_files or "No inventory files")


def persist_history(messages: list[dict[str, Any]]) -> None:
    path = HISTORY_DIR / f"chat_{date.today().isoformat()}.json"
    write_json(path, messages)


def main() -> None:
    st.set_page_config(
        page_title="Confluent FinOps Chatbot",
        layout="wide",
    )
    st.title("Confluent FinOps Chatbot")
    st.write(
        "Ask questions about your Confluent Cloud expenditure, resource costs, "
        "billing breakdown, promo credits, and cost optimization."
    )

    render_sidebar()

    raw_billing = load_folder(BILLING_DIR)
    raw_inventory = load_folder(INVENTORY_DIR)
    summary = calculate_summary(raw_billing, raw_inventory)

    render_overview(summary)

    if not GOOGLE_API_KEY:
        st.warning(
            "GOOGLE_API_KEY is missing. Add it to finops-chatbot/.env "
            "and restart the application. Do not paste your key into chat."
        )

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    question = st.chat_input(
        "Example: Which resource contributes the highest cost?"
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
            with st.spinner("Analyzing your FinOps data..."):
                answer = generate_answer(question, context)
            st.markdown(answer)

            st.session_state.messages.append(
                {"role": "assistant", "content": answer}
            )
            persist_history(st.session_state.messages)

        except Exception as exc:
            error_text = f"Unable to answer: {exc}"
            st.error(error_text)
            logger.exception("Chat response failed")
            st.session_state.messages.append(
                {"role": "assistant", "content": error_text}
            )
            persist_history(st.session_state.messages)


if __name__ == "__main__":
    main()
