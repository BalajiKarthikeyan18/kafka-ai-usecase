# Confluent FinOps Intelligence Chatbot

A Streamlit-based proof of concept for analyzing Confluent Cloud
billing, discovering live resource inventory through the existing
Confluent MCP server, identifying resource ownership through tags and
Stream Catalog, and producing FinOps answers with Gemini.

The application combines three sources:

  -----------------------------------------------------------------------
  Source                              Responsibility
  ----------------------------------- -----------------------------------
  Confluent Billing REST API          Financial source of truth: actual
                                      billing line items, product totals,
                                      resource costs and promotional
                                      credits

  Existing Confluent MCP server       Live inventory and metadata
                                      discovery using the tools exposed
                                      by the configured MCP server

  Confluent Stream Catalog API        Tag-to-entity discovery for
                                      supported Catalog entity types

  Gemini                              Plans read-only MCP calls from the
                                      live tool descriptions and schemas,
                                      then explains the reconciled
                                      results
  -----------------------------------------------------------------------

**Important:** Inventory metadata is not billing data. The application
matches costs to resources using resource IDs. It does not assume that a
topic inherits the full cost of its Kafka cluster.

------------------------------------------------------------------------

## 1. Features

### Billing analysis

-   Retrieves billing line items for a selected date range.
-   Requests billing data in bounded date windows and follows
    pagination.
-   Displays gross charges, promotional credits and net billing.
-   Breaks costs down by product, billing line type and resource ID.
-   Keeps promotional credits separate from resource costs.

### Dynamic MCP inventory discovery

-   Starts the existing Confluent MCP server using the configured
    `config.yaml`.
-   Discovers the live tool catalogue, descriptions, input schemas and
    read-only annotations.
-   Uses Gemini to select relevant tools based on the user's question.
-   Executes only tools explicitly marked read-only.
-   Supports multi-round discovery so parent resource IDs can be
    discovered before child-resource calls.
-   Records tool plans, results and errors in the Streamlit execution
    trace.

### Tag and team ownership

-   Reads tag definitions from the MCP `list-tags` result.
-   Uses Stream Catalog search to discover entities associated with
    team/shared tags.
-   Merges MCP inventory with Stream Catalog entity metadata.
-   Recognizes ownership tags matching `TEAM_*`, for example `TEAM_A`,
    `TEAM_B` and `TEAM_C`.
-   Recognizes `Management`, `shared` and `platform` as shared/platform
    ownership tags.
-   Flags resources with multiple team tags as conflicting ownership.
-   Reports unassigned or untagged inventory separately.

### Billing reconciliation

-   Normalizes tenant-prefixed Catalog qualified names for supported
    resource types before matching billing IDs.
-   Retains Catalog qualified names and entity metadata for
    traceability.
-   Matches billing and inventory using canonical resource IDs, not
    display names.
-   Aggregates multiple billing line items for a resource.
-   Reports reconciliation outcomes such as matched, no billing record,
    unmatched, ambiguous or missing resource ID, as represented by the
    current implementation.
-   Keeps shared/platform costs separate from team direct costs.
-   Does not calculate usage-based allocations unless suitable usage
    data and an allocation method are available.

### Gemini FinOps answers

-   Receives billing summaries, MCP results and the team
    ownership/reconciliation report.
-   Answers questions about costs, inventory, tags, ownership and
    potential optimization.
-   Must distinguish measured billing costs from estimates and
    recommendations.
-   Must report missing tool coverage rather than claiming a resource
    category does not exist.

------------------------------------------------------------------------

## 2. Architecture

``` text
                         Streamlit Chat UI
                                |
                                v
                         User's question
                                |
              +-----------------+------------------+
              |                                    |
              v                                    v
      Billing Agent                         MCP Supervisor
              |                                    |
              v                                    v
   Confluent Billing API                 Existing Confluent MCP
              |                                    |
              v                                    v
     Billing line items                 Live tools + descriptions
                                                   |
                                                   v
                                         Gemini tool planner
                                                   |
                                                   v
                                      Read-only MCP tool calls
                                                   |
                                                   v
                                         Inventory results
              |                                    |
              +-----------------+------------------+
                                |
                                v
                       Stream Catalog search
                                |
                                v
                    Resource tags and ownership
                                |
                                v
                  Python ID normalization and
                     billing reconciliation
                                |
                                v
                     Team cost ownership report
                                |
                                v
                       Gemini FinOps analyst
                                |
                                v
                         Answer in Streamlit
```

### Source-of-truth rules

1.  Billing REST API line items are authoritative for actual charges.
2.  MCP and Stream Catalog provide inventory and metadata, not financial
    amounts.
3.  Billing-to-inventory reconciliation uses resource IDs. Display-name
    matching is not used as a substitute.
4.  A Kafka topic's team tag does not mean that the Kafka cluster's
    entire bill belongs to that team.
5.  Shared/platform resources remain separate unless a transparent
    allocation policy and comparable usage measurements are available.
6.  Missing, conflicting or unmatched data must remain visible.

------------------------------------------------------------------------

## 3. Project structure

The expected project layout is:

``` text
D:\AI USECASE\
|
+-- .env
+-- config.yaml
|
+-- finops-chatbot\
    |
    +-- chatbot.py
    +-- finops_agents.py
    +-- team_cost_allocation.py
    +-- requirements.txt
    +-- mcp_smoke_test.py
    +-- data_store\
        +-- chat_history\
```

The chatbot loads `.env` from its own directory and then from the parent
directory. The existing parent `.env` and `config.yaml` can therefore be
reused.

The application does not require changing the separate `finops-ai` / MCP
Explorer project.

------------------------------------------------------------------------

## 4. Prerequisites

-   Windows 10/11 or another supported Python environment.
-   Python 3.10 or newer (the code uses modern Python type syntax).
-   Node.js and npm/npx, because the configured Confluent MCP server is
    launched through `npx`.
-   Access to a Confluent Cloud organization and the required API
    credentials.
-   A Google Gemini API key.
-   Network access to the Confluent Billing API, Schema Registry/Stream
    Catalog endpoint and the MCP server's required Confluent APIs.

The current Windows setup was tested with Node.js `v24.21.0`. Other
compatible Node.js versions may work.

------------------------------------------------------------------------

## 5. Environment configuration

Create or update the parent `.env` file:

``` dotenv
# Gemini
GEMINI_API_KEY=your_gemini_api_key
# Optional alternative name:
# GOOGLE_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.5-flash

# Confluent Cloud Billing API credentials
CONFLUENT_CLOUD_API_KEY=your_confluent_cloud_api_key
CONFLUENT_CLOUD_API_SECRET=your_confluent_cloud_api_secret

# Optional: billing endpoint default
# CONFLUENT_BILLING_URL=https://api.confluent.cloud/billing/v1/costs

# Schema Registry / Stream Catalog credentials
SCHEMA_REGISTRY_ENDPOINT=https://your-schema-registry-endpoint
SCHEMA_REGISTRY_API_KEY=your_schema_registry_api_key
SCHEMA_REGISTRY_API_SECRET=your_schema_registry_api_secret

# Optional runtime settings
# CONFLUENT_MCP_CONFIG=D:\AI USECASE\config.yaml
# HTTP_TIMEOUT_SECONDS=30
# MCP_TIMEOUT_SECONDS=800
```

Use the actual endpoint and credentials from your Confluent Cloud
environment. Do not commit `.env`, API keys, API secrets or
service-account credentials to source control.

The Schema Registry API key/secret must have permission to perform the
Stream Catalog searches used by the application. The Billing API
credentials must have the required billing read permissions.

### MCP configuration

The application expects the existing Confluent MCP configuration file
at:

``` text
D:\AI USECASE\config.yaml
```

By default, `chatbot.py` resolves `config.yaml` from the parent
directory of the project. You can override this with
`CONFLUENT_MCP_CONFIG`.

Keep the MCP configuration and its credentials secured. Do not paste
secrets into prompts or commit them to Git.

------------------------------------------------------------------------

## 6. Installation

Open PowerShell and move to the project directory:

``` powershell
cd "D:\AI USECASE\finops-chatbot"
```

Activate the existing virtual environment if you already have one:

``` powershell
& "D:\AI USECASE\.venv\Scripts\Activate.ps1"
```

If you need to create it:

``` powershell
py -3.12 -m venv "D:\AI USECASE\.venv"
& "D:\AI USECASE\.venv\Scripts\Activate.ps1"
```

Install dependencies:

``` powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If you are creating `requirements.txt`, the application imports these
packages:

``` text
google-genai
mcp
pandas
python-dotenv
requests
streamlit
```

Install the requirements file from the project root. Pin package
versions after validating a known-good environment.

------------------------------------------------------------------------

## 7. Validate the MCP connection

Before launching the Streamlit app, verify that Node.js and npx are
available:

``` powershell
node --version
npx.cmd --version
```

The project includes `mcp_smoke_test.py` for checking MCP initialization
and tool discovery, if present in your local project.

Run:

``` powershell
python mcp_smoke_test.py
```

A successful test should initialize the configured MCP server and list
its available tools. The tool count can change with MCP server versions
and configuration; do not hardcode an expected count.

If the smoke test fails: - Confirm `config.yaml` exists at the
configured path. - Confirm Node.js and npm/npx are installed and
available. - Confirm the MCP credentials and configuration are valid. -
Confirm network access to Confluent Cloud. - Check the full error output
before changing the MCP configuration.

The chatbot's MCP planner only executes tools marked with
`readOnlyHint=True`. Tools without that explicit annotation are blocked.

------------------------------------------------------------------------

## 8. Run the chatbot

From the project directory, with the virtual environment activated:

``` powershell
streamlit run chatbot.py
```

Streamlit prints a local URL in the terminal, normally:

``` text
http://localhost:8501
```

Open that address in a browser.

Use the sidebar to select the billing start date and end date. The end
date is inclusive in the UI. Use **Refresh backend data** to clear the
cached billing response and reload it.

Ask a question in the chat input. For each request, the application: 1.
Retrieves billing rows for the selected date range. 2. Starts the MCP
server and discovers its live read-only tool catalogue. 3. Uses Gemini
to plan and execute relevant MCP inventory calls. 4. Searches Stream
Catalog for discovered ownership/shared tags. 5. Merges inventory and
tag metadata. 6. Reconciles billing against inventory using resource
IDs. 7. Renders billing and ownership reports. 8. Sends the consolidated
context to Gemini for the final answer.

The **Agent execution trace** expander shows the selected plan, executed
tool results, errors and discovered read-only tool catalogue.

------------------------------------------------------------------------

## 9. Team ownership and tagging convention

The current ownership logic recognizes team tags matching:

``` text
TEAM_A
TEAM_B
TEAM_C
TEAM_PLATFORM
```

The pattern is `TEAM_` followed by letters, numbers, underscores or
hyphens.

Shared/platform tags currently recognized include:

``` text
Management
shared
platform
```

Other tags, such as `dev` and `sample_tag`, are descriptive tags and do
not independently assign team ownership.

### Recommended tagging rules

-   Assign exactly one `TEAM_*` ownership tag to each team-owned
    resource.
-   Add descriptive tags such as `dev`, `production`, `critical` or
    `sample_tag` as needed.
-   Use `Management` or the agreed shared/platform tag for shared
    infrastructure.
-   Avoid assigning multiple team ownership tags to one resource unless
    the ownership conflict is intentional and handled by a defined
    policy.
-   Review untagged resources regularly.

### Same tag on multiple topics

Multiple topics can share the same team tag. For example:

  Resource            Tags                     Ownership
  ------------------- ------------------------ -----------
  `payment_topics`    `TEAM_A`, `dev`          TEAM_A
  `dev.sap.raw.tpc`   `TEAM_A`, `sample_tag`   TEAM_A
  `mcp-test-topic`    `TEAM_A`                 TEAM_A
  `order_topic`       `TEAM_B`                 TEAM_B

Each topic is a separate inventory resource, but all three TEAM_A topics
belong to the same owner.

### Conflicting team tags

If a resource has both `TEAM_A` and `TEAM_B`, it is treated as
conflicting ownership. It should not be assigned to either team's direct
cost until the tags are corrected or an explicit shared-ownership
allocation policy is implemented.

------------------------------------------------------------------------

## 10. Cost allocation and reconciliation rules

### Direct actual cost

A billing amount can be attributed to a team only when: - the billing
resource ID is available; - the inventory resource ID can be normalized
reliably; - the IDs match; - ownership is unambiguous; and - the
resource is not classified as shared/platform infrastructure.

Multiple billing line items for the same resource are aggregated for the
selected billing period.

### Resource ID normalization

Some Stream Catalog qualified names include a tenant prefix. For
supported entity types, the application extracts the final resource-ID
segment for billing reconciliation while preserving the original
qualified name.

Example:

``` text
Catalog qualified name: lsrc-example:lcc-123abc
Canonical resource ID:  lcc-123abc
Billing resource ID:    lcc-123abc
```

Normalization must only be applied to known qualified-name formats. Do
not match resources by display name or by an arbitrary suffix when the
identifier format is unknown.

### Reconciliation outcomes

The report distinguishes outcomes such as:

  -----------------------------------------------------------------------
  Status                              Meaning
  ----------------------------------- -----------------------------------
  Matched                             Inventory resource was matched to
                                      billing using its canonical
                                      resource ID

  No billing record                   Inventory resource has no matching
                                      billing resource in the selected
                                      billing data

  Unmatched                           A billing resource ID has no
                                      matching inventory resource

  Ambiguous                           More than one possible inventory
                                      match exists

  Missing resource ID                 A source record does not contain a
                                      usable resource ID
  -----------------------------------------------------------------------

A resource with no billing record is not necessarily free. It may be
outside the selected billing period, not billable as a separate
resource, or represented under a parent resource.

### Shared Kafka cluster costs

Kafka billing may be reported at cluster level, while team ownership is
recorded at topic level. In that case: - the cluster's billing cost is
shown as shared/platform cost if the cluster is tagged accordingly; -
topic ownership is reported independently; - the full cluster cost is
not assigned to every topic or team; - usage-based allocation requires a
valid metric with appropriate topic/team dimensions, compatible units
and an agreed allocation formula.

### Promotional credits

Promotional credits are kept separate from direct resource costs. They
should not be assigned to individual teams unless a separate, explicit
credit-allocation policy is introduced.

------------------------------------------------------------------------

## 11. Example prompts

### Billing

-   What is my total Confluent Cloud spend for this billing period?
-   Show gross charges, promotional credits and net billing.
-   Break down the bill by product and billing line type.
-   Show the top five most expensive resources.

### Kafka

-   List all Kafka clusters with their IDs and regions.
-   Show all topics in `cluster_0`.
-   What are the Kafka CKU, storage, network read and network write
    charges?
-   Which Kafka cluster has the highest cost?

### Team ownership

-   Show all resources tagged `TEAM_A`.
-   List resources belonging to TEAM_B and TEAM_C.
-   Which resources are tagged `Management`?
-   Show team-wise direct actual costs and reconciliation status.
-   Identify resources with conflicting team tags.
-   List all unassigned or untagged resources.

### Connectors and Flink

-   List all active connectors and their costs.
-   Show all Flink compute pools and their billing records.
-   List Flink statements and available compute metrics.
-   Identify failed connectors or potentially underutilized compute
    pools.

### Cross-resource analysis

-   Find the most expensive Kafka cluster, list its topics and show
    topic ownership.
-   Reconcile account-wide billing against the discovered inventory.
-   Show shared, team-owned, unmatched and unallocated costs separately.
-   Generate an executive FinOps summary with optimization
    recommendations.

The exact answer depends on which tools and metrics are exposed by the
configured MCP server and which data is available in the selected
billing period.

------------------------------------------------------------------------

## 12. Troubleshooting

### Gemini API key missing

Check that `GEMINI_API_KEY` or `GOOGLE_API_KEY` is set in the parent
`.env` and that the app is loading the expected file.

### Confluent billing authentication failure

Check `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET`, the
API key's billing permissions, and the billing endpoint.

### Stream Catalog authentication or search failure

Check: - `SCHEMA_REGISTRY_ENDPOINT` - `SCHEMA_REGISTRY_API_KEY` -
`SCHEMA_REGISTRY_API_SECRET` - API key permissions - endpoint format and
network access

Unsupported Catalog entity types are coverage gaps. A failed or
unsupported search must not be interpreted as proof that no resources of
that type exist.

### MCP config not found

Check the sidebar's displayed MCP config path and verify that the file
exists. Set `CONFLUENT_MCP_CONFIG` if the config is stored elsewhere.

### Node.js or npx not found

Install Node.js/npm and make sure `node.exe` and `npx.cmd` are
discoverable in the PowerShell PATH. Restart the terminal after changing
PATH.

### MCP planner returns malformed or incomplete JSON

Check the Gemini model setting, API response and planner error in the
agent trace. The application records planner failures rather than
executing an invalid tool plan.

### Team cost shows \$0.00

Inspect the resource's reconciliation status: - Check whether the
billing period includes the charge. - Compare the Catalog qualified name
with the billing resource ID. - Confirm that the resource ID
normalization rule is valid for that entity type. - Check whether the
billing charge is recorded against a parent resource. - Do not assign a
different resource's billing amount based on a similar name.

### A resource appears unassigned

Check the actual Stream Catalog classification/tag assignments and
confirm that the resource has exactly one recognized `TEAM_*` tag or an
agreed shared/platform tag.

------------------------------------------------------------------------

## 13. Security and operational notes

-   Keep `.env` and `config.yaml` out of public repositories if they
    contain credentials.
-   Do not log or display API secrets.
-   Use read-only Confluent API credentials wherever possible.
-   The MCP agent blocks tools that are not explicitly marked read-only.
-   Review the execution trace when validating new MCP tools or resource
    categories.
-   Treat Gemini output as an explanation of source data, not as a
    replacement for billing records.
-   Validate any optimization recommendation before changing production
    resources.
-   The chat history is written under `data_store/chat_history`; protect
    this directory because conversations may contain operational
    details.

------------------------------------------------------------------------

## 14. Current limitations

-   Inventory coverage depends on the tools enabled in the configured
    Confluent MCP server.
-   The current MCP catalogue may not expose every Confluent resource
    category. For example, ksqlDB inventory must not be assumed
    available unless a suitable tool is exposed.
-   Stream Catalog searches use candidate entity types; support and tag
    coverage can vary by entity type and environment.
-   Billing may be reported at a parent resource level rather than at
    topic or statement level.
-   Topic-level actual cost cannot be claimed when the Billing API does
    not provide topic-level billing.
-   Shared-cost allocation is not calculated without suitable usage
    measurements and an explicit allocation policy.
-   A resource absent from the selected billing period is not
    automatically free.
-   Cost comparisons across periods require comparable date ranges and
    billing data.

------------------------------------------------------------------------

## 15. Suggested next improvements

1.  Validate Stream Catalog entity-type coverage for each resource
    category in the target Confluent environment.
2.  Add resource-specific usage metrics where the MCP metrics tools
    expose suitable dimensions.
3.  Implement a documented shared-cost allocation policy only after
    validating metric granularity and units.
4.  Add month-over-month cost trends and budget thresholds.
5.  Add ownership-tag governance checks and periodic reports for
    untagged resources.
6.  Add automated tests using representative billing, MCP and Stream
    Catalog response fixtures.

------------------------------------------------------------------------

## 16. License and project status

This is an internal proof of concept. Review security, permissions, API
coverage, allocation assumptions and operational controls before using
it for financial reporting or production decisions.
