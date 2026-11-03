
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

PROJECT_DIR = r"D:\JLR\AI USECASE"
CONFIG_PATH = os.path.join(PROJECT_DIR, "config.yaml")
ENV_PATH = os.path.join(PROJECT_DIR, ".env")


@asynccontextmanager
async def get_mcp_session():

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

    async with stdio_client(
        server_params,
        errlog=None
    ) as (read, write):

        async with ClientSession(read, write) as session:

            await session.initialize()

            yield session
