
import asyncio
import json
import os
from datetime import date, timedelta
from pathlib import Path

from mcp_client import get_mcp_session


PROJECT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = PROJECT_DIR / "results"

# Previous calendar day through today.
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=1)

PAGE_SIZE = 1000


def extract_content(result):
    """Extract text and other content returned by the MCP tool."""
    extracted = []

    for item in getattr(result, "content", []):
        if hasattr(item, "text"):
            extracted.append(item.text)
        else:
            extracted.append(str(item))

    return extracted


async def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Confluent Billing Discovery")
    print(f"Start date: {START_DATE}")
    print(f"End date:   {END_DATE}")
    print(f"Page size:  {PAGE_SIZE}")
    print()

    async with get_mcp_session() as session:

        tools = await session.list_tools()
        billing_tool = next(
            (tool for tool in tools.tools
             if tool.name == "list-billing-costs"),
            None
        )

        if billing_tool is None:
            raise RuntimeError(
                "MCP tool 'list-billing-costs' was not found."
            )

        print("Calling list-billing-costs...")

        result = await session.call_tool(
            "list-billing-costs",
            arguments={
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "pageSize": PAGE_SIZE
            }
        )

        content = extract_content(result)

        output = {
            "start_date": START_DATE.isoformat(),
            "end_date": END_DATE.isoformat(),
            "page_size": PAGE_SIZE,
            "is_error": getattr(result, "isError", False),
            "content": content
        }

        output_path = RESULTS_DIR / "billing_last_1_day.json"

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=4, ensure_ascii=False)

        print()
        print(f"MCP error: {output['is_error']}")
        print(f"Response saved to: {output_path}")

        for index, item in enumerate(content, start=1):
            print(f"\nResponse {index}:")
            print(item)

        print("\nBilling discovery completed.")


if __name__ == "__main__":
    asyncio.run(main())
