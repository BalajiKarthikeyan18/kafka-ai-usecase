import asyncio
import json
import os

from dotenv import load_dotenv

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# ============================================================
# LOAD ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# MCP SERVER CONFIGURATION
# ============================================================

MCP_COMMAND = "npx"

MCP_ARGS = [
    "dotenv",
    "-e",
    ".env",
    "--",
    "npx",
    "@confluentinc/mcp-confluent",
    "--config",
    "./config.yaml",
]


# ============================================================
# MAIN
# ============================================================

async def main():

    server_params = StdioServerParameters(
        command=MCP_COMMAND,
        args=MCP_ARGS,
        env=os.environ.copy(),
    )

    print()
    print("Starting Confluent MCP Server...")
    print()

    # --------------------------------------------------------
    # Connect to MCP
    # --------------------------------------------------------

    async with stdio_client(
        server_params
    ) as (
        read_stream,
        write_stream,
    ):

        async with ClientSession(
            read_stream,
            write_stream,
        ) as mcp:

            # ------------------------------------------------
            # Initialize MCP
            # ------------------------------------------------

            await mcp.initialize()

            print(
                "Connected to Confluent MCP Server"
            )

            print()

            # ------------------------------------------------
            # Get all tools
            # ------------------------------------------------

            result = await mcp.list_tools()

            tools = result.tools

            print(
                f"Available MCP tools: {len(tools)}"
            )

            print()
            print("=" * 70)
            print("SERVICE-ACCOUNT TOOL DISCOVERY")
            print("=" * 70)

            found = False

            # ------------------------------------------------
            # Search for service-account related tools
            # ------------------------------------------------

            for tool in tools:

                tool_name = (
                    tool.name or ""
                )

                description = (
                    tool.description or ""
                )

                search_text = (
                    tool_name + " " + description
                ).lower()

                if (
                    "service" in search_text
                    or "account" in search_text
                ):

                    found = True

                    print()
                    print("-" * 70)

                    print(
                        f"Tool name: {tool_name}"
                    )

                    print()
                    print(
                        "Description:"
                    )

                    print(
                        description
                    )

                    print()
                    print(
                        "Input schema:"
                    )

                    print(
                        json.dumps(
                            tool.input_schema,
                            indent=2,
                            default=str,
                        )
                    )

            # ------------------------------------------------
            # No matching tools
            # ------------------------------------------------

            if not found:

                print()
                print(
                    "NO SERVICE-ACCOUNT RELATED "
                    "TOOLS FOUND."
                )

                print()
                print(
                    "The current Confluent MCP server "
                    "does not expose a tool whose name "
                    "or description contains "
                    "'service' or 'account'."
                )

            # ------------------------------------------------
            # Print all tool names for reference
            # ------------------------------------------------

            print()
            print("=" * 70)
            print("ALL MCP TOOLS")
            print("=" * 70)

            for index, tool in enumerate(
                tools,
                start=1,
            ):

                print(
                    f"{index:02d}. {tool.name}"
                )

            print()
            print("=" * 70)
            print("DISCOVERY COMPLETE")
            print("=" * 70)
            print()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "Stopped."
        )