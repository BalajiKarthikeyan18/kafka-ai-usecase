import asyncio
import json
import os

from dotenv import load_dotenv
from google import genai
from google.genai import types

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


load_dotenv()


MODEL = "gemini-3.6-flash"


async def main():

    # -----------------------------
    # Gemini
    # -----------------------------
    gemini = genai.Client(
        api_key=os.getenv("GEMINI_API_KEY")
    )

    # -----------------------------
    # Confluent MCP
    # -----------------------------
    server_params = StdioServerParameters(
        command="npx",
        args=[
            "dotenv",
            "-e",
            ".env",
            "--",
            "npx",
            "@confluentinc/mcp-confluent",
            "--config",
            "./config.yaml",
        ],
    )

    async with stdio_client(server_params) as (read, write):

        async with ClientSession(read, write) as mcp:

            await mcp.initialize()

            # Get MCP tools
            mcp_result = await mcp.list_tools()

            print()
            print("Connected to Confluent MCP Server")
            print("Available MCP tools:", len(mcp_result.tools))
            print()

            # ------------------------------------------------
            # Convert MCP tools into Gemini function declarations
            # ------------------------------------------------

            function_declarations = []

            for tool in mcp_result.tools:

                # For now, expose only read-only tools
                read_only_tools = {
                    "list-topics",
                    "get-partition-offsets",
                    "list-consumer-groups",
                    "describe-consumer-group",
                    "get-consumer-group-lag",
                    "list-compute-pools",
                    "list-connectors",
                    "get-connector-config",
                    "get-connector-offsets",
                    "get-connector-status",
                    "get-connector-tasks",
                    "get-connector-error-summary",
                    "get-connector-error-recommendations",
                    "get-connector-logs",
                    "search-topics-by-tag",
                    "search-topics-by-name",
                    "list-tags",
                    "get-topic-config",
                    "list-clusters",
                    "list-environments",
                    "read-environment",
                    "list-schemas",
                    "list-billing-costs",
                    "query-metrics",
                    "list-available-metrics",
                    "search-product-docs",
                    "get-product-doc-page",
                    "list-organizations",
                    "list-configured-connections",
                    "describe-configured-connection",
                }

                if tool.name not in read_only_tools:
                    continue

                function_declarations.append(
                    types.FunctionDeclaration(
                        name=tool.name.replace("-", "_"),
                        description=tool.description or tool.name,
                        parameters_json_schema=tool.input_schema,
                    )
                )

            gemini_tools = [
                types.Tool(
                    function_declarations=function_declarations
                )
            ]

            print("Read-only tools exposed to Gemini:",
                  len(function_declarations))

            print()

            # -----------------------------
            # Conversation
            # -----------------------------

            history = []

            while True:

                user_input = input("You: ").strip()

                if user_input.lower() in {"exit", "quit", "q"}:
                    print("Goodbye.")
                    break

                if not user_input:
                    continue

                history.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_text(
                                text=user_input
                            )
                        ],
                    )
                )

                try:

                    response = await gemini.aio.models.generate_content(
                        model=MODEL,
                        contents=history,
                        config=types.GenerateContentConfig(
                            system_instruction="""
You are an AI assistant for managing Confluent Cloud.

Use the available Confluent MCP tools when the
user asks for information about Confluent Cloud.

Only use read-only tools.

Never create, delete, update, pause, resume,
restart, alter, or produce resources.

If a requested operation requires a write operation,
tell the user that write operations are currently disabled.

Do not invent Confluent Cloud information.
""",
                            tools=gemini_tools,
                            temperature=0,
                        ),
                    )

                    # -----------------------------
                    # Check whether Gemini requested
                    # an MCP function
                    # -----------------------------

                    function_calls = response.function_calls

                    if function_calls:

                        # Add Gemini response to conversation
                        history.append(response.candidates[0].content)

                        for call in function_calls:

                            tool_name = call.name.replace("_", "-")

                            arguments = call.args or {}

                            print(
                                f"\nCalling MCP tool: "
                                f"{tool_name}"
                            )

                            print(
                                f"Arguments: {arguments}"
                            )

                            # Execute MCP tool
                            tool_result = await mcp.call_tool(
                                tool_name,
                                arguments,
                            )

                            # Convert MCP result to JSON-safe text
                            result_text = ""

                            for content in tool_result.content:

                                if hasattr(content, "text"):
                                    result_text += content.text

                            if not result_text:
                                result_text = str(tool_result)

                            # Return tool result to Gemini
                            history.append(
                                types.Content(
                                    role="user",
                                    parts=[
                                        types.Part.from_function_response(
                                            name=call.name,
                                            response={
                                                "result": result_text
                                            },
                                        )
                                    ],
                                )
                            )

                        # Ask Gemini to interpret the MCP result
                        final_response = await gemini.aio.models.generate_content(
                            model=MODEL,
                            contents=history,
                            config=types.GenerateContentConfig(
                                system_instruction="""
Explain the MCP tool result clearly and concisely
to the user. Do not invent information.
""",
                                temperature=0,
                            ),
                        )

                        print()
                        print("Assistant:")
                        print(final_response.text)
                        print()

                        history.append(
                            final_response.candidates[0].content
                        )

                    else:

                        print()
                        print("Assistant:")
                        print(response.text)
                        print()

                        history.append(
                            response.candidates[0].content
                        )

                except Exception as e:

                    print()
                    print("ERROR:")
                    print(type(e).__name__)
                    print(e)
                    print()


if __name__ == "__main__":
    asyncio.run(main())