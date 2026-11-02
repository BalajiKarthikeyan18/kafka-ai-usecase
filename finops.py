
import asyncio
import json
import os
from pathlib import Path

from dotenv import dotenv_values
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# --------------------------------------------------
# Configuration
# --------------------------------------------------

ENV_FILE = ".env"
CONFIG_FILE = "./config.yaml"
OUTPUT_DIR = Path("mcp_output")

# Tools relevant to our initial FinOps implementation
TARGET_TOOLS = [
    "list-environments",
    "read-environment",
    "list-clusters",
    "list-topics",
    "list-connectors",
    "list-compute-pools",
    "list-schemas",
    "list-billing-costs",
    "list-available-metrics",
    "query-metrics",
    "list-tags",
    "search-topics-by-tag",
    "get-connector-config"
]

OUTPUT_DIR.mkdir(exist_ok=True)

# Load environment variables
env_values = {
    key: value
    for key, value in dotenv_values(ENV_FILE).items()
    if value is not None
}

server_env = {**os.environ, **env_values}


# --------------------------------------------------
# MCP Server Configuration
# --------------------------------------------------

server_params = StdioServerParameters(
    command="npx",
    args=[
        "-y",
        "@confluentinc/mcp-confluent",
        "--config",
        CONFIG_FILE
    ],
    env=server_env
)


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def save_response(tool_name, response):
    """Save MCP response as JSON."""

    output_file = OUTPUT_DIR / f"{tool_name}.json"

    try:
        data = response.model_dump(mode="json")
    except AttributeError:
        data = str(response)

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, default=str)

    print(f"\nSaved response: {output_file}")


def display_schema(tool):
    """Display MCP tool description and input schema."""

    print("\n" + "=" * 65)
    print(f"TOOL: {tool.name}")
    print("=" * 65)

    print("Description:")
    print(tool.description or "No description available")

    print("\nInput Schema:")
    print(json.dumps(tool.input_schema, indent=4))


# --------------------------------------------------
# Main Application
# --------------------------------------------------

async def main():

    print("\nCONFLUENT FINOPS MCP EXPLORER")
    print("=" * 65)

    async with stdio_client(server_params) as (read, write):

        async with ClientSession(read, write) as session:

            await session.initialize()

            # Retrieve all available MCP tools
            result = await session.list_tools()

            available_tools = {
                tool.name: tool
                for tool in result.tools
            }

            print(f"\nTotal available MCP tools: {len(available_tools)}")

            print("\nAvailable FinOps-related tools:")

            for name in TARGET_TOOLS:
                if name in available_tools:
                    print(f"  [AVAILABLE] {name}")
                else:
                    print(f"  [NOT FOUND] {name}")

            # Save tool definitions
            schemas = {}

            for name in TARGET_TOOLS:
                if name in available_tools:
                    tool = available_tools[name]
                    schemas[name] = {
                        "description": tool.description,
                        "inputSchema": tool.input_schema
                    }

            schema_file = OUTPUT_DIR / "tool_schemas.json"

            with open(schema_file, "w", encoding="utf-8") as file:
                json.dump(schemas, file, indent=4)

            print(f"\nTool schemas saved: {schema_file}")

            # Interactive tool execution
            while True:

                print("\n" + "-" * 65)
                print("SELECT A TOOL")
                print("-" * 65)

                available_names = [
                    name for name in TARGET_TOOLS
                    if name in available_tools
                ]

                for index, name in enumerate(available_names, 1):
                    print(f"{index}. {name}")

                print("0. Exit")

                choice = input("\nEnter your choice: ").strip()

                if choice == "0":
                    break

                if not choice.isdigit():
                    print("Invalid selection.")
                    continue

                index = int(choice) - 1

                if index < 0 or index >= len(available_names):
                    print("Invalid selection.")
                    continue

                tool_name = available_names[index]
                tool = available_tools[tool_name]

                # Display exact argument requirements
                display_schema(tool)

                print("\nEnter arguments as JSON.")
                print("Use {} if the tool requires no arguments.")
                print("Example: {\"environment_id\": \"...\"}")

                raw_args = input("\nArguments: ").strip()

                try:
                    arguments = json.loads(raw_args or "{}")

                    if not isinstance(arguments, dict):
                        raise ValueError("Arguments must be a JSON object.")

                except (json.JSONDecodeError, ValueError) as error:
                    print(f"Invalid arguments: {error}")
                    continue

                print(f"\nExecuting: {tool_name}")

                try:
                    response = await session.call_tool(
                        tool_name,
                        arguments=arguments
                    )

                    if response.is_error:
                        print("\nMCP tool returned an error.")
                    else:
                        print("\nMCP tool executed successfully.")

                    print("\nResponse:")

                    for item in response.content:
                        if hasattr(item, "text"):
                            print(item.text)
                        else:
                            print(str(item))

                    save_response(tool_name, response)

                except Exception as error:
                    print(f"\nExecution failed: {error}")

    print("\nMCP exploration completed.")


if __name__ == "__main__":
    asyncio.run(main())
