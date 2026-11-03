
import asyncio
import os
import sys

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

PROJECT_DIR = r"D:\JLR\AI USECASE"
CONFIG_PATH = os.path.join(PROJECT_DIR, "config.yaml")
ENV_PATH = os.path.join(PROJECT_DIR, ".env")


async def connect_to_mcp():

    if not os.path.isfile(CONFIG_PATH):
        raise FileNotFoundError(CONFIG_PATH)

    if not os.path.isfile(ENV_PATH):
        raise FileNotFoundError(ENV_PATH)

    # Load the same environment variables used by your original setup.
    load_dotenv(ENV_PATH, override=False)

    server_params = StdioServerParameters(
        command="cmd.exe",
        args=[
            "/c",
            "npx",
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            CONFIG_PATH
        ],
        env=os.environ.copy(),
        cwd=PROJECT_DIR
    )

    print("Starting Confluent MCP server...")

    async with stdio_client(
        server_params,
        errlog=sys.stderr
    ) as (read, write):

        print("MCP process started.")

        async with ClientSession(read, write) as session:

            await session.initialize()

            print("Connected successfully!")

            response = await session.list_tools()

            print(f"\nAvailable tools: {len(response.tools)}")

            for tool in response.tools:
                print(f"- {tool.name}")


if __name__ == "__main__":
    try:
        asyncio.run(connect_to_mcp())

    except Exception as exc:
        print(f"\nConnection failed: {exc!r}")
        sys.exit(1)
