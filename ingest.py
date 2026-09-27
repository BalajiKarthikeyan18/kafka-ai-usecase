import os
import re
import pickle
import requests
import faiss
import numpy as np

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
EMAIL = os.getenv("CONFLUENCE_EMAIL")
API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

PAGE_ID = "131075"

client = genai.Client(api_key=GEMINI_API_KEY)


# --------------------------------------------------
# Fetch Confluence page
# --------------------------------------------------

def fetch_confluence_page(page_id):

    url = f"{BASE_URL}/wiki/rest/api/content/{page_id}"

    params = {
        "expand": "body.storage,version,space"
    }

    response = requests.get(
        url,
        params=params,
        auth=(EMAIL, API_TOKEN),
        headers={
            "Accept": "application/json"
        }
    )

    response.raise_for_status()

    return response.json()


# --------------------------------------------------
# Clean Confluence HTML
# --------------------------------------------------

def clean_html(html):

    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts/styles
    for element in soup(["script", "style"]):
        element.decompose()

    text = soup.get_text("\n")

    # Remove excessive whitespace
    lines = []

    for line in text.splitlines():

        line = line.strip()

        if not line:
            continue

        # Remove common Confluence editor artifacts
        if line in ["wide", "760", "true"]:
            continue

        lines.append(line)

    text = "\n".join(lines)

    # Remove excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


# --------------------------------------------------
# Split document into chunks
# --------------------------------------------------

def create_chunks(text, chunk_size=1000, overlap=150):

    chunks = []

    start = 0

    while start < len(text):

        end = start + chunk_size

        chunk = text[start:end]

        chunks.append(chunk)

        start += chunk_size - overlap

    return chunks


# --------------------------------------------------
# Generate Gemini embedding
# --------------------------------------------------

def generate_embedding(text):

    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )

    return np.array(
        response.embeddings[0].values,
        dtype="float32"
    )


# --------------------------------------------------
# Main ingestion process
# --------------------------------------------------

def main():

    print("Fetching Confluence page...")

    page = fetch_confluence_page(PAGE_ID)

    title = page["title"]

    html = (
        page["body"]
        ["storage"]
        ["value"]
    )

    print("Page:", title)

    print("Cleaning content...")

    text = clean_html(html)

    print("Creating chunks...")

    chunks = create_chunks(text)

    print("Number of chunks:", len(chunks))

    print("Generating embeddings...")

    embeddings = []

    for i, chunk in enumerate(chunks):

        print(
            f"Embedding chunk {i + 1}/{len(chunks)}"
        )

        embedding = generate_embedding(chunk)

        embeddings.append(embedding)

    embeddings = np.vstack(embeddings)

    # --------------------------------------------------
    # Create FAISS index
    # --------------------------------------------------

    dimension = embeddings.shape[1]

    index = faiss.IndexFlatL2(dimension)

    index.add(embeddings)

    # --------------------------------------------------
    # Save knowledge base
    # --------------------------------------------------

    os.makedirs("knowledge_base", exist_ok=True)

    faiss.write_index(
        index,
        "knowledge_base/confluence.index"
    )

    metadata = []

    for i, chunk in enumerate(chunks):

        metadata.append({
            "id": i,
            "text": chunk,
            "title": title,
            "page_id": PAGE_ID,
            "url": (
                f"{BASE_URL}/wiki/spaces/"
                f"Kafka/pages/{PAGE_ID}"
            )
        })

    with open(
        "knowledge_base/metadata.pkl",
        "wb"
    ) as f:

        pickle.dump(metadata, f)

    print("\nKnowledge base created successfully.")

    print(
        "Index:",
        "knowledge_base/confluence.index"
    )

    print(
        "Metadata:",
        "knowledge_base/metadata.pkl"
    )


if __name__ == "__main__":
    main()