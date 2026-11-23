# Confluent FinOps Intelligence Chatbot

This upgrade adds a dynamic MCP inventory agent to the existing backend-connected Streamlit FinOps chatbot.

## Design rules

- Confluent Billing REST API is the financial source of truth.
- The existing Confluent MCP server is used for live inventory and metadata only.
- MCP tool names, descriptions, input schemas and read-only annotations are discovered at runtime.
- Gemini selects tools using their actual descriptions and schemas; arguments are validated before execution.
- Only MCP tools explicitly marked `readOnlyHint=true` can execute.
- Python performs all financial aggregation and exact-resource-ID reconciliation.
- Promotional credits remain negative and unallocated. Unmatched and billing-only resources remain visible.
- No manual uploads, documentation crawling, embeddings, FAISS, or local billing/inventory snapshots.
- Existing `finops-ai` and the existing MCP Explorer/configuration are not modified.

## Expected layout

```text
D:\JLR\AI USECASE\
  .env                         # existing Confluent billing credentials
  config.yaml                  # existing MCP configuration
  finops-ai\                   # unchanged
  finops-chatbot\
    chatbot.py                 # replace the current chatbot entry point with this file
    finops_agents.py            # add this module beside chatbot.py
    requirements.txt
    .env                        # existing Google AI Studio key, or copy .env.example
    data_store\chat_history\
```

The parent `.env` must contain the existing billing credentials:

```env
CONFLUENT_CLOUD_API_KEY=...
CONFLUENT_CLOUD_API_SECRET=...
```

The chatbot `.env` must contain:

```env
GOOGLE_API_KEY=...
GEMINI_MODEL=gemini-2.5-flash
```

## Installation

Run from `D:\JLR\AI USECASE\finops-chatbot`:

```powershell
python -m pip install -r requirements.txt
streamlit run chatbot.py
```

The application reuses `..\config.yaml` and the parent `.env`; it does not create or edit either one.

## Agent workflow

1. Billing Agent retrieves paginated billing line items from `/billing/v1/costs`.
2. MCP Supervisor opens the existing MCP server and discovers the live tool catalogue.
3. Inventory Agent gives Gemini the actual read-only tool descriptions and JSON input schemas.
4. Gemini proposes up to ten read-only calls. The application blocks non-read-only tools and validates each call against its live schema.
5. Python normalizes and aggregates billing by product, line type, unit and exact resource ID; it reconciles only exact IDs against returned inventory.
6. FinOps Analyst explains the computed results and calls out missing inventory coverage or failed tools.

## Important limitations / validation

- This code has not been tested against the user's live Confluent account or local Windows environment in this workspace. Run it locally and inspect the Agent execution trace.
- A resource category is only covered when the connected MCP server exposes an appropriate enabled read-only tool and the planner can supply its required arguments.
- ksqlDB, cluster links, tags, and other resource categories are not assumed to exist as enabled tools; their coverage is based on the live tool catalogue and call results.
- Billing resource IDs may not map one-to-one to every inventory object (for example, topic-level costs may be represented under a parent cluster). No name-based or inferred allocation is performed.
- If your installed MCP Python SDK has a different `stdio_client` signature, adjust the MCP transport call to the installed SDK version; do not change `config.yaml` or credentials.
- The legacy `google-generativeai` SDK is retained to match the existing chatbot implementation.
