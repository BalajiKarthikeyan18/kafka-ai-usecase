"""
FinOps + Confluent Documentation Chatbot
Run: streamlit run chatbot.py

The documentation index is built automatically on first run and refreshed
from the public Confluent documentation sitemap. Local index files are created
under knowledge_base/. FinOps snapshots belong under data_store/billing/ and
data_store/inventory/. Live billing can be fetched using credentials in the
parent .env file.
"""
from __future__ import annotations

import json
import os
import pickle
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import faiss
import google.generativeai as genai
import numpy as np
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Paths: finops-chatbot is independent of the existing finops-ai project.
ROOT = Path(__file__).resolve().parent
PARENT = ROOT.parent
KB = ROOT / "knowledge_base"
DATA = ROOT / "data_store"
BILLING_DIR = DATA / "billing"
INVENTORY_DIR = DATA / "inventory"
HISTORY_DIR = DATA / "chat_history"
INDEX_FILE = KB / "confluent.index"
META_FILE = KB / "metadata.pkl"
MANIFEST_FILE = KB / "manifest.json"

for folder in (KB, BILLING_DIR, INVENTORY_DIR, HISTORY_DIR):
    folder.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT / ".env", override=False)
load_dotenv(PARENT / ".env", override=False)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
EMBED_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "models/gemini-embedding-001").strip()
CONFLUENT_KEY = os.getenv("CONFLUENT_CLOUD_API_KEY", "").strip()
CONFLUENT_SECRET = os.getenv("CONFLUENT_CLOUD_API_SECRET", "").strip()
BILLING_API = os.getenv("CONFLUENT_BILLING_URL", "https://api.confluent.cloud/billing/v1/costs")
SITEMAP_URL = os.getenv("CONFLUENT_DOC_SITEMAP_URL", "https://docs.confluent.io/sitemap.xml")
URL_FILTER = os.getenv("CONFLUENT_DOC_URL_FILTER", "").lower().strip()
MAX_PAGES = int(os.getenv("MAX_DOC_PAGES", "250"))
CHUNK_WORDS = int(os.getenv("CHUNK_WORDS", "220"))
OVERLAP = int(os.getenv("CHUNK_OVERLAP", "40"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))

INSTRUCTIONS = """
You are a beginner-friendly Confluent and FinOps assistant. Explain technical
terms in plain English. Use documentation excerpts for product behavior and
the supplied account data for account-specific facts. Never invent billing
amounts. Calculate totals in Python, not by guessing. Match billing resources
only by exact resource IDs, never display names. Keep unmatched/historical
resource rows visible. Keep promo credits negative and unallocated. Clearly
distinguish actual billed amounts from estimates or examples. If evidence is
missing, say so. Cite documentation page title and URL when supplied. Never
reveal credentials or environment variables.
"""


class AppError(Exception):
    pass


def configure_gemini():
    if not GOOGLE_API_KEY:
        raise AppError("GOOGLE_API_KEY is missing from finops-chatbot/.env.")
    genai.configure(api_key=GOOGLE_API_KEY)


def get(url, **kwargs):
    response = requests.get(
        url, timeout=TIMEOUT,
        headers={"User-Agent": "FinOps-Confluent-Chatbot/1.0"}, **kwargs
    )
    response.raise_for_status()
    return response


def parse_sitemap(content):
    root = ET.fromstring(content)
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    pages, nested = [], []
    for node in root.findall(f".//{ns}url/{ns}loc"):
        if node.text:
            pages.append(node.text.strip())
    for node in root.findall(f".//{ns}sitemap/{ns}loc"):
        if node.text:
            nested.append(node.text.strip())
    return pages, nested


def discover_pages():
    queue, visited, pages = [SITEMAP_URL], set(), []
    while queue and len(pages) < MAX_PAGES:
        sitemap = queue.pop(0)
        if sitemap in visited:
            continue
        visited.add(sitemap)
        page_urls, child_maps = parse_sitemap(get(sitemap).content)
        queue.extend(x for x in child_maps if x not in visited)
        for url in page_urls:
            if "confluent.io" not in urlparse(url).netloc.lower():
                continue
            if URL_FILTER and URL_FILTER not in url.lower():
                continue
            if url not in pages:
                pages.append(url)
                if len(pages) >= MAX_PAGES:
                    break
    return pages


def page_text(url):
    soup = BeautifulSoup(get(url).text, "html.parser")
    for node in soup(["script", "style", "nav", "footer", "header", "noscript", "svg"]):
        node.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else url
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = re.sub(r"\s+", " ", main.get_text(" ", strip=True)).strip()
    return {"title": title, "url": url, "text": text} if len(text) > 250 else None


def chunks(text):
    words = text.split()
    step = max(1, CHUNK_WORDS - OVERLAP)
    for i in range(0, len(words), step):
        part = " ".join(words[i:i + CHUNK_WORDS])
        if part:
            yield part
        if i + CHUNK_WORDS >= len(words):
            break


def embed(texts, task="RETRIEVAL_DOCUMENT"):
    configure_gemini()
    vectors = []
    for i in range(0, len(texts), 64):
        result = genai.embed_content(
            model=EMBED_MODEL, content=texts[i:i + 64], task_type=task
        )
        values = result["embedding"]
        if values and isinstance(values[0], (int, float)):
            values = [values]
        vectors.extend(values)
        time.sleep(0.05)
    matrix = np.asarray(vectors, dtype=np.float32)
    matrix /= np.maximum(np.linalg.norm(matrix, axis=1, keepdims=True), 1e-12)
    return matrix


def kb_ready():
    return INDEX_FILE.exists() and META_FILE.exists() and MANIFEST_FILE.exists()


def build_kb(force=False):
    configure_gemini()
    if kb_ready() and not force:
        manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
        built = datetime.fromisoformat(manifest["built_at"].replace("Z", "+00:00"))
        if (datetime.now(timezone.utc) - built).total_seconds() < 86400:
            with META_FILE.open("rb") as f:
                return len(pickle.load(f)), manifest["pages"]

    urls = discover_pages()
    if not urls:
        raise AppError("No Confluent documentation pages found in the sitemap.")
    records = []
    progress = st.progress(0, text="Downloading Confluent documentation...")
    for n, url in enumerate(urls, 1):
        try:
            page = page_text(url)
            if page:
                for number, part in enumerate(chunks(page["text"])):
                    records.append({
                        "title": page["title"], "url": page["url"],
                        "chunk": part, "chunk_number": number
                    })
        except Exception as exc:
            st.warning(f"Skipped {url}: {exc}")
        progress.progress(n / len(urls), text=f"Read {n}/{len(urls)} pages")
    progress.empty()
    if not records:
        raise AppError("Pages were found, but no readable documentation text was extracted.")

    vectors = embed([r["chunk"] for r in records])
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(vectors)
    faiss.write_index(index, str(INDEX_FILE))
    with META_FILE.open("wb") as f:
        pickle.dump(records, f)
    manifest = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "pages": len(urls), "chunks": len(records), "sitemap": SITEMAP_URL,
        "embedding_model": EMBED_MODEL
    }
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return len(records), len(urls)


def search_docs(question, k=5):
    if not kb_ready():
        return []
    vector = embed([question], task="RETRIEVAL_QUERY")
    index = faiss.read_index(str(INDEX_FILE))
    scores, ids = index.search(vector, k)
    with META_FILE.open("rb") as f:
        metadata = pickle.load(f)
    results = []
    for score, idx in zip(scores[0], ids[0]):
        if 0 <= idx < len(metadata):
            item = dict(metadata[idx])
            item["score"] = float(score)
            results.append(item)
    return results


def read_data_file(path):
    try:
        if path.suffix.lower() == ".json":
            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
            if isinstance(obj, dict):
                for key in ("data", "items", "results", "costs", "resources"):
                    if isinstance(obj.get(key), list):
                        return [x for x in obj[key] if isinstance(x, dict)]
                return [obj]
        if path.suffix.lower() == ".jsonl":
            return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path).where(pd.notna(pd.read_csv(path)), None).to_dict("records")
    except Exception as exc:
        st.warning(f"Could not read {path.name}: {exc}")
    return []


def load_folder(folder):
    rows = []
    for path in folder.glob("*"):
        if path.suffix.lower() in (".json", ".jsonl", ".csv"):
            rows.extend(read_data_file(path))
    return rows


def fetch_billing(start, end_exclusive):
    if not CONFLUENT_KEY or not CONFLUENT_SECRET:
        raise AppError("Confluent billing credentials are missing from the parent .env.")
    session = requests.Session()
    session.auth = (CONFLUENT_KEY, CONFLUENT_SECRET)
    session.headers["Accept"] = "application/json"
    all_rows, current = [], start
    while current < end_exclusive:
        stop = min(current + timedelta(days=31), end_exclusive)
        params = {"start_date": current.isoformat(), "end_date": stop.isoformat(), "page_size": 1000}
        while True:
            response = session.get(BILLING_API, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            all_rows.extend(payload.get("data", []))
            token = (payload.get("metadata") or {}).get("next")
            if not token:
                break
            params["page_token"] = token
        current = stop
    return all_rows


def billing_context():
    raw_rows = load_folder(BILLING_DIR)
    inventory = load_folder(INVENTORY_DIR)
    if not raw_rows and not inventory:
        return "No FinOps data loaded. Fetch billing in the sidebar or add JSON/CSV exports to data_store/billing and data_store/inventory."

    def normalized(row):
        resource = row.get("resource") or {}
        try:
            amount = float(row.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        env = resource.get("environment")
        return {
            "id": resource.get("id") or row.get("resource_id"),
            "name": resource.get("display_name") or row.get("resource_display_name"),
            "product": row.get("product"),
            "line_type": row.get("line_type"),
            "amount": amount,
            "environment_id": env.get("id") if isinstance(env, dict) else None
        }

    rows = [normalized(r) for r in raw_rows]
    if not rows:
        return "Inventory snapshot loaded, but no billing rows are available.\n" + json.dumps(inventory[:100], default=str)

    promo = [r for r in rows if str(r["line_type"]).upper() == "PROMO_CREDIT"]
    normal = [r for r in rows if str(r["line_type"]).upper() != "PROMO_CREDIT"]
    totals = {}
    for row in normal:
        rid = row["id"]
        if not rid:
            continue
        if rid not in totals:
            totals[rid] = {"id": rid, "name": row["name"], "amount": 0.0, "records": 0}
        totals[rid]["amount"] += row["amount"]
        totals[rid]["records"] += 1

    inventory_ids = {
        str(x.get("id") or x.get("resource_id") or x.get("cluster_id"))
        for x in inventory if x.get("id") or x.get("resource_id") or x.get("cluster_id")
    }
    summary = []
    for item in totals.values():
        status = "matched by exact resource ID" if item["id"] in inventory_ids else "unmatched (possibly historical)"
        summary.append({**item, "status": status})

    gross = sum(r["amount"] for r in normal)
    credit = sum(r["amount"] for r in promo)
    net = sum(r["amount"] for r in rows)
    return (
        f"Actual billing line items: {len(rows)}\n"
        f"Gross non-promo amount: ${gross:,.4f}\n"
        f"Promo credits (negative and unallocated): ${credit:,.4f}\n"
        f"Net amount: ${net:,.4f}\n"
        "Resource totals grouped strictly by exact resource ID:\n"
        + json.dumps(summary, indent=2, default=str)
        + "\nPromo credit rows, kept unallocated:\n"
        + json.dumps(promo, indent=2, default=str)
        + "\nInventory snapshot:\n"
        + json.dumps(inventory[:100], indent=2, default=str)
    )


def answer_question(question, docs, finance):
    configure_gemini()
    doc_context = "\n\n".join(
        f"Title: {d['title']}\nURL: {d['url']}\nExcerpt: {d['chunk']}"
        for d in docs
    ) or "No relevant documentation excerpts were retrieved."
    prompt = f"""{INSTRUCTIONS}

DOCUMENTATION EXCERPTS:
{doc_context}

FINOPS DATA:
{finance}

USER QUESTION:
{question}

Answer in beginner-friendly language. If the answer uses documentation, add a
Sources section with the exact page titles and URLs above. Do not invent missing facts.
"""
    model = genai.GenerativeModel(GEMINI_MODEL)
    response = model.generate_content(
        prompt, generation_config={"temperature": 0.2, "max_output_tokens": 1800}
    )
    if not response.text:
        raise AppError("Gemini returned an empty answer.")
    return response.text.strip()


def main():
    st.set_page_config(page_title="FinOps + Confluent Chatbot", layout="wide")
    st.title("FinOps + Confluent Documentation Chatbot")
    st.caption("Ask beginner-friendly questions about Confluent and your account's FinOps data.")

    with st.sidebar:
        st.header("FinOps")
        st.write("Billing snapshots:", str(BILLING_DIR))
        st.write("Inventory snapshots:", str(INVENTORY_DIR))
        st.subheader("Fetch live billing")
        start = st.date_input("Start date (inclusive)", date.today().replace(day=1))
        end = st.date_input("End date (inclusive)", date.today())
        if st.button("Fetch Confluent billing", use_container_width=True):
            try:
                with st.spinner("Fetching billing..."):
                    rows = fetch_billing(start, end + timedelta(days=1))
                    target = BILLING_DIR / f"billing_{start}_{end}.json"
                    target.write_text(json.dumps({"data": rows, "fetched_at": datetime.now(timezone.utc).isoformat()}, indent=2), encoding="utf-8")
                st.success(f"Saved {len(rows)} billing rows.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

        st.divider()
        st.header("Documentation")
        if st.button("Build / refresh documentation index", use_container_width=True):
            try:
                with st.spinner("Crawling Confluent documentation and creating embeddings..."):
                    count, pages = build_kb(force=True)
                st.success(f"Indexed {pages} pages and {count} text chunks.")
            except Exception as exc:
                st.error(str(exc))
        if MANIFEST_FILE.exists():
            try:
                manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
                st.caption(f"Last built: {manifest['built_at']}\nPages: {manifest['pages']} | Chunks: {manifest['chunks']}")
            except Exception:
                pass

    if not GOOGLE_API_KEY:
        st.warning("Add GOOGLE_API_KEY to finops-chatbot/.env. Do not paste your key into this chat.")

    if "messages" not in st.session_state:
        st.session_state.messages = []
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander("Documentation sources"):
                    for src in msg["sources"]:
                        st.markdown(f"- [{src['title']}]({src['url']})")

    question = st.chat_input("Example: What is a Kafka topic, and what am I paying for?")
    if not question:
        return
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        try:
            if not kb_ready():
                with st.spinner("First run: building the documentation knowledge base..."):
                    count, pages = build_kb(force=True)
                st.success(f"Created documentation index from {pages} pages ({count} chunks).")
            with st.spinner("Searching documentation and FinOps data..."):
                docs = search_docs(question)
                finance = billing_context()
                reply = answer_question(question, docs, finance)
            st.markdown(reply)
            with st.expander("Retrieved documentation sources"):
                if docs:
                    for src in docs:
                        st.markdown(f"- [{src['title']}]({src['url']})")
                else:
                    st.write("No relevant documentation pages were retrieved.")
            st.session_state.messages.append({"role": "assistant", "content": reply, "sources": docs})
        except Exception as exc:
            reply = f"Unable to answer: {exc}"
            st.error(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply, "sources": []})

    history = HISTORY_DIR / f"chat_{date.today().isoformat()}.json"
    history.write_text(json.dumps(st.session_state.messages, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
