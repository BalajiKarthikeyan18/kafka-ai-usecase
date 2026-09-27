import os
import pickle

import faiss
import numpy as np
from dotenv import load_dotenv
from google import genai


load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

client = genai.Client(
    api_key=GEMINI_API_KEY
)


# Load FAISS index
index = faiss.read_index(
    "knowledge_base/confluence.index"
)


# Load metadata
with open(
    "knowledge_base/metadata.pkl",
    "rb"
) as f:

    metadata = pickle.load(f)


def generate_embedding(text):

    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )

    return np.array(
        response.embeddings[0].values,
        dtype="float32"
    )


def search(query, top_k=3):

    query_embedding = generate_embedding(query)

    query_embedding = query_embedding.reshape(
        1, -1
    )

    distances, indices = index.search(
        query_embedding,
        top_k
    )

    print("\nQuery:")
    print(query)

    print("\nRelevant Confluence sections:")
    print("=" * 70)

    for rank, idx in enumerate(indices[0], start=1):

        result = metadata[idx]

        print(f"\nResult {rank}")
        print("-" * 70)

        print("Title:")
        print(result["title"])

        print("\nSource:")
        print(result["url"])

        print("\nContent:")
        print(result["text"])

        print("\nDistance:")
        print(distances[0][rank - 1])


if __name__ == "__main__":

    question = input(
        "\nAsk a question about the Confluence documentation: "
    )

    search(question)