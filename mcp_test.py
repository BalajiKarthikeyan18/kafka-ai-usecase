import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():

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

        async with ClientSession(read, write) as session:

            await session.initialize()

            result = await session.list_tools()

            print("\nConfluent MCP tools available:")
            print("--------------------------------")

            for tool in result.tools:
                print(tool.name)

            print("\nTotal tools:", len(result.tools))


if __name__ == "__main__":
    asyncio.run(main())