import os
import pickle

import faiss
import numpy as np
from dotenv import load_dotenv
from google import genai


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

client = genai.Client(
    api_key=GEMINI_API_KEY
)


# --------------------------------------------------
# Load knowledge base
# --------------------------------------------------

index = faiss.read_index(
    "knowledge_base/confluence.index"
)

with open(
    "knowledge_base/metadata.pkl",
    "rb"
) as f:

    metadata = pickle.load(f)


# --------------------------------------------------
# Embedding
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
# Retrieve relevant documents
# --------------------------------------------------

def retrieve(query, top_k=3):

    query_embedding = generate_embedding(query)

    query_embedding = query_embedding.reshape(
        1, -1
    )

    distances, indices = index.search(
        query_embedding,
        top_k
    )

    results = []

    for idx in indices[0]:

        results.append(
            metadata[idx]
        )

    return results


# --------------------------------------------------
# Generate answer
# --------------------------------------------------

def generate_answer(question, documents):

    context = "\n\n".join(
        document["text"]
        for document in documents
    )

    prompt = f"""
You are a helpful assistant for an internal
Kafka and Confluent documentation knowledge base.

Answer the user's question using ONLY the
provided Confluence documentation.

If the answer cannot be found in the documentation,
say:

"I couldn't find this information in the
Confluence knowledge base."

Do not invent information.

Keep the answer clear and concise.

CONFLUENCE DOCUMENTATION:

{context}


USER QUESTION:

{question}
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return response.text


# --------------------------------------------------
# Chatbot
# --------------------------------------------------

def main():

    print("=" * 70)
    print("Confluence Documentation Chatbot")
    print("=" * 70)

    while True:

        question = input(
            "\nYou: "
        ).strip()

        if question.lower() in [
            "exit",
            "quit"
        ]:

            print("Goodbye.")
            break

        if not question:
            continue

        print("\nSearching Confluence knowledge base...")

        documents = retrieve(
            question,
            top_k=3
        )

        print("Generating answer...")

        answer = generate_answer(
            question,
            documents
        )

        print("\nAssistant:")
        print(answer)

        print("\nSources:")

        sources = set(
            document["url"]
            for document in documents
        )

        for source in sources:
            print(source)


if __name__ == "__main__":
    main()