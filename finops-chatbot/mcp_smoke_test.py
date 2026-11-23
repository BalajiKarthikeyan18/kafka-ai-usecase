
import asyncio
import logging
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

logging.basicConfig(level=logging.INFO)

APP_DIR = Path(__file__).resolve().parent
PARENT_DIR = APP_DIR.parent
ENV_FILE = PARENT_DIR / ".env"
CONFIG_FILE = PARENT_DIR / "config.yaml"


async def main():
    print("Loading environment:", ENV_FILE)
    print("Config:", CONFIG_FILE)

    load_dotenv(ENV_FILE, override=True)

    for key in [
        "KAFKA_BOOTSTRAP_SERVER",
        "KAFKA_REST_ENDPOINT",
        "SCHEMA_REGISTRY_ENDPOINT",
    ]:
        print(f"{key}: {'PRESENT' if os.getenv(key) else 'MISSING'}")

    node_path = shutil.which("node.exe")
    npx_path = shutil.which("npx.cmd")

    if not node_path or not npx_path:
        raise RuntimeError("Node.js or npx.cmd not found.")

    npx_cli = (
        Path(npx_path).parent
        / "node_modules"
        / "npm"
        / "bin"
        / "npx-cli.js"
    )

    if not npx_cli.is_file():
        raise RuntimeError(f"NPX CLI not found: {npx_cli}")

    params = StdioServerParameters(
        command=node_path,
        args=[
            str(npx_cli),
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(CONFIG_FILE),
        ],
        env=dict(os.environ),
        cwd=str(PARENT_DIR),
    )

    print("\nStarting MCP using Node.js directly")
    print("Node:", node_path)
    print("NPX CLI:", npx_cli)
    print("Arguments:", params.args)

    async with stdio_client(
        params,
        errlog=sys.stderr,
    ) as (read_stream, write_stream):

        print("\nMCP subprocess started.")

        async with ClientSession(
            read_stream,
            write_stream,
        ) as session:

            print("Initializing MCP session...")

            initialization = await session.initialize()

            print("\nMCP INITIALIZATION SUCCESSFUL")
            print("Server information:", initialization.server_info)

            result = await session.list_tools()

            print("\nTOOLS DISCOVERED:", len(result.tools))

            for tool in result.tools:
                print("-", tool.name)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logging.exception("MCP test failed")
        raise
