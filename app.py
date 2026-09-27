import os
import pickle

import faiss
import numpy as np
import streamlit as st
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

@st.cache_resource
def load_knowledge_base():

    index = faiss.read_index(
        "knowledge_base/confluence.index"
    )

    with open(
        "knowledge_base/metadata.pkl",
        "rb"
    ) as f:

        metadata = pickle.load(f)

    return index, metadata


index, metadata = load_knowledge_base()


# --------------------------------------------------
# Generate embedding
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
# Retrieve documents
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
You are an internal documentation assistant.

Answer the user's question using ONLY the
provided Confluence documentation.

Do not use outside knowledge.

If the answer is not available in the provided
documentation, say:

"I couldn't find this information in the
Confluence knowledge base."

Do not invent information.

Provide a clear and concise answer.

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
# Streamlit UI
# --------------------------------------------------

st.set_page_config(
    page_title="Confluence Knowledge Assistant",
    page_icon="None",
    layout="centered"
)

st.title("Confluence Knowledge Assistant")

st.caption(
    "Ask questions about the Confluence documentation."
)


# --------------------------------------------------
# Chat history
# --------------------------------------------------

if "messages" not in st.session_state:

    st.session_state.messages = []


for message in st.session_state.messages:

    with st.chat_message(
        message["role"]
    ):

        st.markdown(
            message["content"]
        )


# --------------------------------------------------
# Chat input
# --------------------------------------------------

question = st.chat_input(
    "Ask a question..."
)


if question:

    # Display user message

    st.session_state.messages.append({
        "role": "user",
        "content": question
    })

    with st.chat_message("user"):

        st.markdown(question)


    # Generate response

    with st.chat_message("assistant"):

        with st.spinner(
            "Searching Confluence..."
        ):

            documents = retrieve(
                question,
                top_k=3
            )

            answer = generate_answer(
                question,
                documents
            )

            st.markdown(answer)


            # Sources

            st.markdown(
                "**Sources:**"
            )

            sources = set(
                document["url"]
                for document in documents
            )

            for source in sources:

                st.markdown(
                    f"- {source}"
                )


    st.session_state.messages.append({
        "role": "assistant",
        "content": answer
    })