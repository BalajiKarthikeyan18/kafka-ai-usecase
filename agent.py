import asyncio
import json
import os
import re

from dotenv import load_dotenv
from google import genai
from google.genai import types

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# ============================================================
# LOAD ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

MODEL = "gemini-2.5-flash"

CLUSTER_ID = "lkc-125nwo6"
ENVIRONMENT_ID = "env-rgk1k0"

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
# GEMINI API
# ============================================================

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise RuntimeError(
        "GEMINI_API_KEY is missing from .env"
    )

gemini = genai.Client(
    api_key=GEMINI_API_KEY
)


# ============================================================
# TOPIC POLICY
# ============================================================

TOPIC_POLICY = {
    "dev": {
        "cleanup.policy": "compact",
        "partitions": 3,
    },
    "test": {
        "cleanup.policy": "compact",
        "partitions": 3,
    },
    "prod": {
        "cleanup.policy": "compact",
        "partitions": 6,
    },
}


# ============================================================
# TOPIC NAME FORMAT
#
# {environment}.{app_name}.{type}.tpc
#
# Example:
# dev.sap.raw.tpc
# test.orders.events.tpc
# prod.sap.raw.tpc
# ============================================================

TOPIC_NAME_PATTERN = re.compile(
    r"^(dev|test|prod)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.tpc$"
)


# ============================================================
# WRITE TOOLS
# ============================================================

WRITE_TOOLS = {
    "create-topics",
    "delete-topics",
    "produce-message",
    "create-connector",
    "delete-connector",
    "update-connector-config",
    "pause-connector",
    "resume-connector",
    "restart-connector",
    "create-schema",
    "delete-schema",
    "alter-topic-config",
    "create-topic-tags",
    "delete-tag",
    "remove-tag-from-entity",
    "add-tags-to-topic",
}


# ============================================================
# SYSTEM INSTRUCTION
#
# IMPORTANT:
# Gemini must NOT ask for confirmation.
# Python handles confirmation.
# ============================================================

SYSTEM_INSTRUCTION = """
You are a Confluent Cloud resource-management AI assistant.

You operate through the official Confluent MCP server.

IMPORTANT RULES:

1. Use MCP tools to obtain real Confluent Cloud information.

2. Do not invent resource IDs, topic names, configurations,
   or operation results.

3. Python is responsible for confirmation of write operations.

4. NEVER ask the user for confirmation yourself.

5. NEVER say:
   - "Do you want to continue?"
   - "Should I proceed?"
   - "Are you sure?"
   - "Please confirm"
   - or any similar confirmation question.

6. When the user requests a write operation, call the
   appropriate MCP write tool immediately.

7. Python will intercept the write operation and ask the user
   for confirmation before actually executing it.

8. Read-only operations can be executed normally.

9. Topic names must follow exactly:

   {environment}.{app_name}.{type}.tpc

10. Valid logical environments are:

   dev
   test
   prod

11. Mandatory topic policy:

   dev:
       cleanup.policy = compact
       partitions = 3

   test:
       cleanup.policy = compact
       partitions = 3

   prod:
       cleanup.policy = compact
       partitions = 6

12. Python is authoritative for topic configuration.

13. Do not override Python-provided topic configuration.

14. There is one Kafka cluster:

   lkc-125nwo6

15. There is one Confluent Cloud environment:

   env-rgk1k0

16. Topic creation is a multi-step operation:

   create topic
   configure cleanup.policy=compact
   verify configuration

17. Never claim a resource was created unless the MCP
   operation actually succeeded.

18. When a tool returns an error, report the actual error.
"""


# ============================================================
# MCP TOOL -> GEMINI TOOL
# ============================================================

def mcp_tool_to_gemini(tool):
    """
    Convert an MCP tool definition into a Gemini
    FunctionDeclaration.
    """

    description = tool.description or tool.name

    if tool.name in WRITE_TOOLS:
        description += (
            "\n\nIMPORTANT: This is a write operation. "
            "Call this tool when the user requests the operation. "
            "Do NOT ask the user for confirmation. "
            "The Python application handles confirmation."
        )

    return types.FunctionDeclaration(
        name=tool.name.replace("-", "_"),
        description=description,
        parameters_json_schema=tool.input_schema,
    )


# ============================================================
# EXTRACT TOPIC NAMES
# ============================================================

def extract_topic_names(arguments):
    """
    Extract topic names from MCP arguments.

    Handles:
        topicName
        topic_name
        topic
        name

    and:

        topics: [...]
    """

    names = []

    for key in [
        "topicName",
        "topic_name",
        "topic",
        "name",
    ]:

        value = arguments.get(key)

        if isinstance(value, str):
            names.append(value)

    topics = arguments.get("topics")

    if isinstance(topics, list):

        for item in topics:

            if isinstance(item, str):
                names.append(item)

            elif isinstance(item, dict):

                for key in [
                    "topic",
                    "topicName",
                    "topic_name",
                    "name",
                ]:

                    value = item.get(key)

                    if isinstance(value, str):
                        names.append(value)

    return list(dict.fromkeys(names))


# ============================================================
# VALIDATE TOPIC NAME
# ============================================================

def validate_topic_name(topic_name):
    """
    Validate:

        {env}.{app_name}.{type}.tpc
    """

    match = TOPIC_NAME_PATTERN.fullmatch(
        topic_name
    )

    if not match:
        return None

    environment = match.group(1)
    app_name = match.group(2)
    topic_type = match.group(3)

    return {
        "name": topic_name,
        "environment": environment,
        "app_name": app_name,
        "type": topic_type,
    }


# ============================================================
# ENFORCE TOPIC POLICY
# ============================================================

def enforce_topic_policy(arguments):
    """
    Python is authoritative here.

    Gemini cannot decide:
        partitions
        cleanup.policy
        cluster
        environment
    """

    topic_names = extract_topic_names(
        arguments
    )

    if not topic_names:

        raise ValueError(
            "No topic name was found."
        )

    validated_topics = []

    for topic_name in topic_names:

        parsed = validate_topic_name(
            topic_name
        )

        if not parsed:

            raise ValueError(
                f"Invalid topic name: {topic_name}\n\n"
                "Required format:\n"
                "{environment}.{app_name}.{type}.tpc\n\n"
                "Example:\n"
                "dev.sap.raw.tpc"
            )

        environment = parsed["environment"]

        policy = TOPIC_POLICY[
            environment
        ]

        validated_topics.append(
            {
                "name": topic_name,
                "environment": environment,
                "app_name": parsed["app_name"],
                "type": parsed["type"],
                "cleanup.policy": policy[
                    "cleanup.policy"
                ],
                "partitions": policy[
                    "partitions"
                ],
            }
        )

    # --------------------------------------------------------
    # Construct the ACTUAL MCP create-topics arguments.
    #
    # Note:
    # cleanup.policy is NOT supported by create-topics.
    # It is configured afterward using alter-topic-config.
    # --------------------------------------------------------

    normalized = {
        "topics": [],
        "cluster_id": CLUSTER_ID,
        "environment_id": ENVIRONMENT_ID,
    }

    for topic in validated_topics:

        normalized["topics"].append(
            {
                "topic": topic["name"],
                "numPartitions": topic[
                    "partitions"
                ],
            }
        )

    return normalized, validated_topics


# ============================================================
# PYTHON CONFIRMATION
# ============================================================

def ask_confirmation(
    operation,
    validated_topics,
):
    """
    Confirmation is handled entirely by Python.

    Gemini never sees the yes/no interaction as a
    conversational confirmation.
    """

    print()
    print("=" * 65)
    print("CONFIRMATION REQUIRED")
    print("=" * 65)

    print(
        f"Operation: {operation}"
    )

    print()

    for topic in validated_topics:

        print(
            f"Topic          : "
            f"{topic['name']}"
        )

        print(
            f"Environment    : "
            f"{topic['environment']}"
        )

        print(
            f"Cluster        : "
            f"{CLUSTER_ID}"
        )

        print(
            f"Cleanup Policy : "
            f"{topic['cleanup.policy']}"
        )

        print(
            f"Partitions     : "
            f"{topic['partitions']}"
        )

        print()

    print(
        "The topic will be:"
    )

    print(
        "1. Created"
    )

    print(
        "2. Configured with "
        "cleanup.policy=compact"
    )

    print(
        "3. Verified against the required policy"
    )

    print()

    while True:

        answer = input(
            "Proceed? (yes/no): "
        ).strip().lower()

        if answer in {
            "yes",
            "y",
        }:

            return True

        if answer in {
            "no",
            "n",
        }:

            return False

        print(
            "Please enter yes or no."
        )


# ============================================================
# CONFIGURE CLEANUP POLICY
# ============================================================

async def configure_topic_cleanup(
    mcp,
    topic_name,
):
    """
    Apply:

        cleanup.policy=compact

    using alter-topic-config.
    """

    arguments = {
        "topicName": topic_name,

        "topicConfigs": [
            {
                "name": "cleanup.policy",
                "value": "compact",
                "operation": "SET",
            }
        ],

        "validateOnly": False,

        "clusterId": CLUSTER_ID,

        "environmentId": ENVIRONMENT_ID,
    }

    print()
    print(
        f"Applying cleanup.policy=compact "
        f"to {topic_name}..."
    )

    result = await mcp.call_tool(
        "alter-topic-config",
        arguments=arguments,
    )

    return result


# ============================================================
# MCP RESULT -> TEXT
# ============================================================

def extract_result_text(result):
    """
    Convert MCP result into readable text.
    """

    content = getattr(
        result,
        "content",
        None,
    )

    if content:

        parts = []

        for item in content:

            text_value = getattr(
                item,
                "text",
                None,
            )

            if text_value:
                parts.append(
                    text_value
                )

        if parts:
            return "\n".join(parts)

    return str(result)


# ============================================================
# VERIFY TOPIC POLICY
# ============================================================

async def verify_topic_policy(
    mcp,
    topic_name,
    expected_partitions,
):
    """
    Verify:

        partitions_count == expected_partitions
        cleanup.policy == compact
    """

    print()
    print(
        f"Verifying configuration for "
        f"{topic_name}..."
    )

    result = await mcp.call_tool(
        "get-topic-config",
        arguments={
            "topicName": topic_name,
            "clusterId": CLUSTER_ID,
            "environmentId": ENVIRONMENT_ID,
        },
    )

    result_text = extract_result_text(
        result
    )

    print()
    print("Verification result:")
    print(result_text)

    cleanup_ok = False
    partitions_ok = False

    # --------------------------------------------------------
    # Parse JSON if possible
    # --------------------------------------------------------

    try:

        parsed = json.loads(
            result_text
        )

        json_text = json.dumps(
            parsed
        ).lower()

        cleanup_ok = (
            '"cleanup.policy"' in json_text
            and '"compact"' in json_text
        )

        partitions_ok = (
            '"partitions_count"' in json_text
            and str(expected_partitions)
            in json_text
        )

    except Exception:
        pass

    # --------------------------------------------------------
    # Text fallback
    # --------------------------------------------------------

    if not cleanup_ok:

        cleanup_ok = bool(
            re.search(
                r"cleanup\.policy"
                r".{0,200}"
                r"compact",
                result_text,
                re.IGNORECASE |
                re.DOTALL,
            )
        )

    if not partitions_ok:

        partitions_ok = bool(
            re.search(
                rf"partitions_count"
                rf".{{0,100}}"
                rf"{expected_partitions}",
                result_text,
                re.IGNORECASE |
                re.DOTALL,
            )
        )

    return (
        cleanup_ok and partitions_ok,
        result,
    )


# ============================================================
# CREATE TOPIC WORKFLOW
# ============================================================

async def create_topic_workflow(
    mcp,
    arguments,
):
    """
    Complete governed workflow:

        validate
            ↓
        derive policy
            ↓
        Python confirmation
            ↓
        create
            ↓
        configure
            ↓
        verify
    """

    # --------------------------------------------------------
    # STEP 1: Validate + enforce policy
    # --------------------------------------------------------

    try:

        (
            normalized_arguments,
            validated_topics,
        ) = enforce_topic_policy(
            arguments
        )

    except ValueError as exc:

        print()
        print("=" * 65)
        print("TOPIC POLICY ERROR")
        print("=" * 65)

        print(exc)

        return {
            "status": "policy_error",
            "error": str(exc),
        }

    # --------------------------------------------------------
    # STEP 2: Python confirmation
    # --------------------------------------------------------

    confirmed = ask_confirmation(
        "create-topics",
        validated_topics,
    )

    if not confirmed:

        print()
        print(
            "Topic creation cancelled."
        )

        return {
            "status": "cancelled",
            "message": (
                "The user cancelled "
                "topic creation."
            ),
        }

    # --------------------------------------------------------
    # STEP 3: CREATE TOPIC
    # --------------------------------------------------------

    print()
    print(
        "Creating topic(s)..."
    )

    try:

        create_result = await mcp.call_tool(
            "create-topics",
            arguments=normalized_arguments,
        )

    except Exception as exc:

        print()
        print(
            "ERROR: Topic creation failed."
        )

        print(
            f"Reason: {exc}"
        )

        return {
            "status": "error",
            "error": str(exc),
        }

    print()
    print("Create result:")

    print(
        extract_result_text(
            create_result
        )
    )

    # --------------------------------------------------------
    # STEP 4: Configure + verify each topic
    # --------------------------------------------------------

    final_results = []

    for topic in validated_topics:

        topic_name = topic["name"]

        # ----------------------------------------------------
        # Configure cleanup policy
        # ----------------------------------------------------

        try:

            config_result = (
                await configure_topic_cleanup(
                    mcp,
                    topic_name,
                )
            )

            print()
            print(
                "Configuration result:"
            )

            print(
                extract_result_text(
                    config_result
                )
            )

        except Exception as exc:

            print()
            print("=" * 65)
            print(
                "PARTIAL FAILURE"
            )
            print("=" * 65)

            print(
                f"Topic created: "
                f"{topic_name}"
            )

            print(
                "cleanup.policy configuration failed."
            )

            print(
                f"Reason: {exc}"
            )

            final_results.append(
                {
                    "topic": topic_name,
                    "status": (
                        "created_but_configuration_failed"
                    ),
                    "error": str(exc),
                }
            )

            continue

        # ----------------------------------------------------
        # Verify
        # ----------------------------------------------------

        try:

            (
                verified,
                verification_result,
            ) = await verify_topic_policy(
                mcp,
                topic_name,
                topic["partitions"],
            )

        except Exception as exc:

            print()
            print(
                "Verification failed:"
            )

            print(exc)

            final_results.append(
                {
                    "topic": topic_name,
                    "status": (
                        "created_but_verification_failed"
                    ),
                    "error": str(exc),
                }
            )

            continue

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        if verified:

            print()
            print("=" * 65)
            print(
                "TOPIC CREATION SUCCESSFUL"
            )
            print("=" * 65)

            print(
                f"Topic          : "
                f"{topic_name}"
            )

            print(
                f"Environment    : "
                f"{topic['environment']}"
            )

            print(
                f"Cluster        : "
                f"{CLUSTER_ID}"
            )

            print(
                f"Partitions     : "
                f"{topic['partitions']}"
            )

            print(
                f"Cleanup Policy : "
                f"{topic['cleanup.policy']}"
            )

            print(
                "Verification   : PASSED"
            )

            print("=" * 65)

            final_results.append(
                {
                    "topic": topic_name,
                    "status": "success",
                    "partitions": topic[
                        "partitions"
                    ],
                    "cleanup.policy": "compact",
                }
            )

        else:

            print()
            print("=" * 65)
            print(
                "POLICY VERIFICATION FAILED"
            )
            print("=" * 65)

            print(
                f"Topic: {topic_name}"
            )

            print(
                f"Expected partitions: "
                f"{topic['partitions']}"
            )

            print(
                "Expected cleanup.policy: compact"
            )

            print("=" * 65)

            final_results.append(
                {
                    "topic": topic_name,
                    "status": (
                        "created_but_policy_verification_failed"
                    ),
                }
            )

    return {
        "status": "completed",
        "results": final_results,
    }


# ============================================================
# GENERIC WRITE CONFIRMATION
# ============================================================

def ask_generic_write_confirmation(
    tool_name,
    arguments,
):
    """
    Confirmation for all write operations other than
    create-topics.
    """

    print()
    print("=" * 65)
    print(
        "CONFIRMATION REQUIRED"
    )
    print("=" * 65)

    print(
        f"Operation: {tool_name}"
    )

    print()
    print("Arguments:")

    print(
        json.dumps(
            arguments,
            indent=2,
            default=str,
        )
    )

    print()

    while True:

        answer = input(
            "Proceed? (yes/no): "
        ).strip().lower()

        if answer in {
            "yes",
            "y",
        }:

            return True

        if answer in {
            "no",
            "n",
        }:

            return False

        print(
            "Please enter yes or no."
        )


# ============================================================
# EXECUTE MCP TOOL
# ============================================================

async def execute_mcp_tool(
    mcp,
    tool_name,
    arguments,
):
    """
    Central Python governance layer.
    """

    # --------------------------------------------------------
    # CREATE TOPICS
    # --------------------------------------------------------

    if tool_name == "create-topics":

        return await create_topic_workflow(
            mcp,
            arguments,
        )

    # --------------------------------------------------------
    # OTHER WRITE OPERATIONS
    # --------------------------------------------------------

    if tool_name in WRITE_TOOLS:

        confirmed = (
            ask_generic_write_confirmation(
                tool_name,
                arguments,
            )
        )

        if not confirmed:

            print()
            print(
                "Operation cancelled."
            )

            return {
                "status": "cancelled",
                "message": (
                    "The user cancelled "
                    "the operation."
                ),
            }

    # --------------------------------------------------------
    # Force cluster/environment for topic config
    # --------------------------------------------------------

    if tool_name == "alter-topic-config":

        arguments["clusterId"] = (
            CLUSTER_ID
        )

        arguments["environmentId"] = (
            ENVIRONMENT_ID
        )

    # --------------------------------------------------------
    # Execute actual MCP operation
    # --------------------------------------------------------

    result = await mcp.call_tool(
        tool_name,
        arguments=arguments,
    )

    return result


# ============================================================
# MAIN
# ============================================================

async def main():

    # --------------------------------------------------------
    # MCP server configuration
    # --------------------------------------------------------

    server_params = StdioServerParameters(
        command=MCP_COMMAND,
        args=MCP_ARGS,
        env=os.environ.copy(),
    )

    # --------------------------------------------------------
    # Connect to official Confluent MCP
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
            # Initialize
            # ------------------------------------------------

            await mcp.initialize()

            print()
            print(
                "Connected to Confluent MCP Server"
            )

            # ------------------------------------------------
            # Discover all 49 tools
            # ------------------------------------------------

            mcp_tools_result = (
                await mcp.list_tools()
            )

            mcp_tools = (
                mcp_tools_result.tools
            )

            print()
            print(
                f"Available MCP tools: "
                f"{len(mcp_tools)}"
            )

            # ------------------------------------------------
            # Expose ALL tools to Gemini
            # ------------------------------------------------

            gemini_tools = []

            for tool in mcp_tools:

                declaration = (
                    mcp_tool_to_gemini(
                        tool
                    )
                )

                gemini_tools.append(
                    types.Tool(
                        function_declarations=[
                            declaration
                        ]
                    )
                )

            print(
                f"Tools exposed to Gemini: "
                f"{len(mcp_tools)}"
            )

            # ------------------------------------------------
            # Display policy
            # ------------------------------------------------

            print()
            print(
                "Topic creation policy:"
            )

            print(
                "  dev  -> "
                "compact / 3 partitions"
            )

            print(
                "  test -> "
                "compact / 3 partitions"
            )

            print(
                "  prod -> "
                "compact / 6 partitions"
            )

            print()
            print(
                f"Cluster: {CLUSTER_ID}"
            )

            print(
                f"Environment: "
                f"{ENVIRONMENT_ID}"
            )

            print()
            print(
                "All write operations require confirmation."
            )

            print()
            print(
                "Type 'exit' to quit."
            )

            # ------------------------------------------------
            # Gemini chat
            # ------------------------------------------------

            chat = gemini.aio.chats.create(
                model=MODEL,
                config=types.GenerateContentConfig(
                    system_instruction=(
                        SYSTEM_INSTRUCTION
                    ),
                    tools=gemini_tools,
                    temperature=0,
                ),
            )

            # ------------------------------------------------
            # Interactive loop
            # ------------------------------------------------

            while True:

                try:

                    user_input = input(
                        "\nYou: "
                    ).strip()

                except (
                    EOFError,
                    KeyboardInterrupt,
                ):

                    print()
                    print(
                        "Exiting..."
                    )

                    break

                if not user_input:
                    continue

                if user_input.lower() in {
                    "exit",
                    "quit",
                }:

                    print(
                        "Exiting..."
                    )

                    break

                # ------------------------------------------------
                # Send user request to Gemini
                # ------------------------------------------------

                try:

                    response = (
                        await chat.send_message(
                            user_input
                        )
                    )

                except Exception as exc:

                    print()
                    print(
                        "Gemini error:"
                    )

                    print(exc)

                    continue

                # ------------------------------------------------
                # TOOL CALL LOOP
                # ------------------------------------------------

                while True:

                    function_calls = (
                        getattr(
                            response,
                            "function_calls",
                            None,
                        )
                    )

                    if not function_calls:
                        break

                    tool_responses = []

                    # ------------------------------------------------
                    # Process every requested MCP tool
                    # ------------------------------------------------

                    for function_call in function_calls:

                        gemini_tool_name = (
                            function_call.name
                        )

                        # --------------------------------------------
                        # Convert Gemini name to MCP name
                        # --------------------------------------------

                        mcp_tool_name = (
                            gemini_tool_name.replace(
                                "_",
                                "-"
                            )
                        )

                        # --------------------------------------------
                        # Get arguments
                        # --------------------------------------------

                        if function_call.args:

                            arguments = dict(
                                function_call.args
                            )

                        else:

                            arguments = {}

                        print()
                        print(
                            f"Gemini requested tool: "
                            f"{mcp_tool_name}"
                        )

                        if arguments:

                            print()
                            print(
                                "Arguments:"
                            )

                            print(
                                json.dumps(
                                    arguments,
                                    indent=2,
                                    default=str,
                                )
                            )

                        # --------------------------------------------
                        # Execute through Python governance layer
                        # --------------------------------------------

                        try:

                            result = (
                                await execute_mcp_tool(
                                    mcp,
                                    mcp_tool_name,
                                    arguments,
                                )
                            )

                            if isinstance(
                                result,
                                dict,
                            ):

                                result_for_gemini = (
                                    result
                                )

                            else:

                                result_for_gemini = (
                                    extract_result_text(
                                        result
                                    )
                                )

                        except Exception as exc:

                            print()
                            print(
                                f"MCP tool error "
                                f"({mcp_tool_name}):"
                            )

                            print(exc)

                            result_for_gemini = {
                                "status": "error",
                                "error": str(exc),
                            }

                        # --------------------------------------------
                        # Return result to Gemini
                        # --------------------------------------------

                        tool_responses.append(
                            types.Part.from_function_response(
                                name=gemini_tool_name,
                                response={
                                    "result": (
                                        result_for_gemini
                                    )
                                },
                            )
                        )

                    # ------------------------------------------------
                    # Send tool result back to Gemini
                    # ------------------------------------------------

                    try:

                        response = (
                            await chat.send_message(
                                tool_responses
                            )
                        )

                    except Exception as exc:

                        print()
                        print(
                            "Gemini error while "
                            "processing tool result:"
                        )

                        print(exc)

                        break

                # ------------------------------------------------
                # Final Gemini response
                # ------------------------------------------------

                if response.text:

                    print()
                    print(
                        f"Assistant: "
                        f"{response.text}"
                    )


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
            "Application stopped."
        )