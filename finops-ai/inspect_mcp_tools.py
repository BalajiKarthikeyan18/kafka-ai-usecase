"""
Confluent MCP tool inspector with detailed exception reporting.

Run:
    python inspect_mcp_tools.py

Uses:
    ..\.env
    ..\config.yaml

Writes:
    mcp_tools.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent

load_dotenv(PARENT / ".env", override=False)
load_dotenv(HERE / ".env", override=False)

CONFIG_PATH = Path(
    os.getenv("CONFLUENT_MCP_CONFIG", str(PARENT / "config.yaml"))
).expanduser().resolve()


def to_jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if hasattr(value, "dict"):
        return value.dict()
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def make_server_parameters() -> StdioServerParameters:
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"MCP config file not found: {CONFIG_PATH}")

    if os.name == "nt":
        # Use the same Windows launch pattern as the existing working setup.
        command = os.getenv("COMSPEC", "cmd.exe")
        args = [
            "/c",
            "npx",
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(CONFIG_PATH),
        ]
    else:
        command = "npx"
        args = [
            "-y",
            "@confluentinc/mcp-confluent",
            "--config",
            str(CONFIG_PATH),
        ]

    return StdioServerParameters(
        command=command,
        args=args,
        env=dict(os.environ),
    )


def print_exception_tree(exc: BaseException, indent: int = 0) -> None:
    prefix = " " * indent
    print(
        f"{prefix}{type(exc).__name__}: {exc}",
        file=sys.stderr,
    )
    if isinstance(exc, BaseExceptionGroup):
        for index, child in enumerate(exc.exceptions, start=1):
            print(f"{prefix}  Sub-exception {index}:", file=sys.stderr)
            print_exception_tree(child, indent + 4)
    elif exc.__cause__ is not None:
        print(f"{prefix}Caused by:", file=sys.stderr)
        print_exception_tree(exc.__cause__, indent + 2)
    elif exc.__context__ is not None:
        print(f"{prefix}During handling:", file=sys.stderr)
        print_exception_tree(exc.__context__, indent + 2)


async def main() -> None:
    params = make_server_parameters()
    print(f"MCP config: {CONFIG_PATH}")
    print(f"Platform: {sys.platform}")
    print(f"Command: {params.command}")
    print(f"Arguments: {params.args}")
    print("Starting MCP server...")

    async with stdio_client(params, errlog=sys.stderr) as (read_stream, write_stream):
        print("MCP process started. Initializing client session...")
        async with ClientSession(read_stream, write_stream) as session:
            init_result = await session.initialize()
            print("MCP session initialized.")
            print(f"Server info: {to_jsonable(getattr(init_result, 'serverInfo', None))}")

            response = await session.list_tools()
            tools = []
            for tool in response.tools:
                tools.append({
                    "name": tool.name,
                    "description": tool.description or "",
                    "raw_tool_definition": tool.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=False
                    ),
                    "model_fields_set": list(
                        getattr(tool, "model_fields_set", [])
                    ),
                })
    output = {
        "mcp_config_path": str(CONFIG_PATH),
        "tool_count": len(tools),
        "tools": tools,
    }
    output_path = HERE / "mcp_tools.json"
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\nSaved JSON to: {output_path}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except BaseException as exc:
        print("\nMCP INSPECTOR FAILED. Full exception details:", file=sys.stderr)
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
        print("\nNested exception summary:", file=sys.stderr)
        print_exception_tree(exc)
        raise SystemExit(1)
