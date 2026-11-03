
import asyncio
import json
import re
from pathlib import Path

from mcp_client import get_mcp_session


# ==================================================
# PROJECT CONFIGURATION
# ==================================================

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ==================================================
# COMMON UTILITIES
# ==================================================

def save_json(filename, data):
    """Save discovery results as JSON."""

    output_file = RESULTS_DIR / filename

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=4,
            ensure_ascii=False
        )

    print(f"Saved: {output_file}")


def get_text_content(response):
    """Extract text blocks from an MCP response."""

    return [
        item.text
        for item in response.content
        if hasattr(item, "text") and item.text
    ]


# ==================================================
# ENVIRONMENT DISCOVERY
# ==================================================

def extract_environment_details(raw_text):

    environments = []
    current_environment = None

    for line in raw_text.splitlines():

        name_match = re.match(
            r"\s*Environment:\s*(.+)", line
        )

        id_match = re.match(
            r"\s*ID:\s*(\S+)", line
        )

        resource_match = re.match(
            r"\s*Resource Name:\s*(\S+)", line
        )

        created_match = re.match(
            r"\s*Created At:\s*(\S+)", line
        )

        updated_match = re.match(
            r"\s*Updated At:\s*(\S+)", line
        )

        governance_match = re.match(
            r"\s*Stream Governance Package:\s*(.+)", line
        )

        if name_match:

            if current_environment:
                environments.append(current_environment)

            current_environment = {
                "name": name_match.group(1).strip()
            }

        elif current_environment and id_match:
            current_environment["id"] = id_match.group(1)

        elif current_environment and resource_match:
            current_environment["resource_name"] = resource_match.group(1)

        elif current_environment and created_match:
            current_environment["created_at"] = created_match.group(1)

        elif current_environment and updated_match:
            current_environment["updated_at"] = updated_match.group(1)

        elif current_environment and governance_match:
            current_environment["stream_governance_package"] = (
                governance_match.group(1).strip()
            )

    if current_environment:
        environments.append(current_environment)

    return environments


async def discover_environments(session):

    print("\nDiscovering Confluent environments...")

    response = await session.call_tool(
        "list-environments",
        arguments={}
    )

    if response.is_error:
        raise RuntimeError(
            f"Environment discovery failed: {response.content}"
        )

    text_blocks = get_text_content(response)
    raw_text = "\n".join(text_blocks)

    environments = extract_environment_details(raw_text)

    result = {
        "total_environments": len(environments),
        "environments": environments,
        "raw_response": text_blocks
    }

    save_json("environments.json", result)

    print(f"\nDiscovered {len(environments)} environment(s).")

    for environment in environments:
        print(
            f"Environment: {environment.get('name')} "
            f"| ID: {environment.get('id')}"
        )

    return environments


# ==================================================
# CLUSTER DISCOVERY
# ==================================================

def extract_cluster_details(raw_text, environment):

    clusters = []

    cluster_sections = re.split(
        r"(?=^\s*Cluster:\s*)",
        raw_text,
        flags=re.MULTILINE
    )

    for section in cluster_sections:

        name_match = re.search(
            r"^\s*Cluster:\s*(.+)",
            section,
            re.MULTILINE
        )

        id_match = re.search(
            r"^\s*ID:\s*(\S+)",
            section,
            re.MULTILINE
        )

        if not name_match or not id_match:
            continue

        cluster = {
            "cluster_name": name_match.group(1).strip(),
            "cluster_id": id_match.group(1).strip(),
            "environment_name": environment["name"],
            "environment_id": environment["id"]
        }

        optional_fields = {
            "status": r"^\s*Status:\s*(.+)",
            "availability": r"^\s*Availability:\s*(.+)",
            "cloud": r"^\s*Cloud:\s*(.+)",
            "region": r"^\s*Region:\s*(.+)",
            "cku": r"^\s*CKU:\s*(.+)",
            "kind": r"^\s*Kind:\s*(.+)",
            "zones": r"^\s*Zones:\s*(.*)"
        }

        for field, pattern in optional_fields.items():

            match = re.search(
                pattern,
                section,
                re.MULTILINE
            )

            if match:

                value = match.group(1).strip()

                if field == "cku":
                    try:
                        value = float(value)

                        if value.is_integer():
                            value = int(value)

                    except ValueError:
                        pass

                cluster[field] = value

        endpoints = {}

        http_match = re.search(
            r"^\s*HTTP:\s*(\S+)",
            section,
            re.MULTILINE
        )

        bootstrap_match = re.search(
            r"^\s*Bootstrap:\s*(\S+)",
            section,
            re.MULTILINE
        )

        if http_match:
            endpoints["http"] = http_match.group(1)

        if bootstrap_match:
            endpoints["bootstrap"] = bootstrap_match.group(1)

        cluster["endpoints"] = endpoints

        clusters.append(cluster)

    return clusters


async def discover_clusters(session, environment):

    environment_id = environment["id"]

    print(
        f"\nDiscovering clusters in environment: "
        f"{environment['name']} ({environment_id})"
    )

    response = await session.call_tool(
        "list-clusters",
        arguments={
            "environmentId": environment_id
        }
    )

    if response.is_error:
        raise RuntimeError(
            f"Cluster discovery failed for {environment_id}: "
            f"{response.content}"
        )

    text_blocks = get_text_content(response)
    raw_text = "\n".join(text_blocks)

    clusters = extract_cluster_details(
        raw_text,
        environment
    )

    print(f"Discovered {len(clusters)} cluster(s).")

    for cluster in clusters:
        print(
            f"  Cluster: {cluster['cluster_name']} "
            f"| ID: {cluster['cluster_id']}"
        )

    return {
        "environment_name": environment["name"],
        "environment_id": environment_id,
        "total_clusters": len(clusters),
        "clusters": clusters,
        "raw_response": text_blocks
    }


# ==================================================
# TOPIC DISCOVERY
# ==================================================

async def discover_topics(session, environment, cluster):

    environment_id = environment["id"]
    cluster_id = cluster["cluster_id"]

    print(
        f"\nDiscovering topics in cluster: "
        f"{cluster['cluster_name']} ({cluster_id})"
    )

    try:

        response = await session.call_tool(
            "list-topics",
            arguments={
                "cluster_id": cluster_id,
                "environment_id": environment_id
            }
        )

        if response.is_error:

            error_message = str(response.content)

            print(
                f"Topic discovery failed for {cluster_id}: "
                f"{error_message}"
            )

            return {
                "environment_name": environment["name"],
                "environment_id": environment_id,
                "cluster_name": cluster["cluster_name"],
                "cluster_id": cluster_id,
                "status": "failed",
                "error": error_message,
                "raw_response": []
            }

        text_blocks = get_text_content(response)

        print("\nTopic response:")

        for text_block in text_blocks:
            print(text_block)

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "success",
            "raw_response": text_blocks
        }

    except Exception as error:

        error_message = str(error)

        print(
            f"Unexpected topic discovery error for "
            f"{cluster_id}: {error_message}"
        )

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "failed",
            "error": error_message,
            "raw_response": []
        }


# ==================================================
# MAIN DISCOVERY WORKFLOW
# ==================================================

async def discover_all_resources():

    async with get_mcp_session() as session:

        # STEP 1: Discover environments

        environments = await discover_environments(session)

        if not environments:

            print("\nNo environments found.")

            save_json(
                "clusters.json",
                {
                    "total_environments": 0,
                    "total_clusters": 0,
                    "cluster_results": []
                }
            )

            save_json(
                "topics.json",
                {
                    "total_clusters": 0,
                    "successful_clusters": 0,
                    "failed_clusters": 0,
                    "topic_results": []
                }
            )

            return

        # STEP 2: Discover clusters

        all_cluster_results = []
        all_clusters = []

        for environment in environments:

            if not environment.get("id"):
                print(
                    f"Skipping environment without ID: {environment}"
                )
                continue

            cluster_result = await discover_clusters(
                session,
                environment
            )

            all_cluster_results.append(cluster_result)

            all_clusters.extend(
                cluster_result["clusters"]
            )

        save_json(
            "clusters.json",
            {
                "total_environments": len(environments),
                "total_clusters": len(all_clusters),
                "cluster_results": all_cluster_results
            }
        )

        # STEP 3: Discover topics

        all_topic_results = []

        for cluster in all_clusters:

            environment = next(
                (
                    env for env in environments
                    if env["id"] == cluster["environment_id"]
                ),
                None
            )

            if not environment:
                continue

            topic_result = await discover_topics(
                session,
                environment,
                cluster
            )

            all_topic_results.append(topic_result)

        # STEP 4: Save topic results

        successful_clusters = sum(
            1 for result in all_topic_results
            if result["status"] == "success"
        )

        failed_clusters = sum(
            1 for result in all_topic_results
            if result["status"] == "failed"
        )

        save_json(
            "topics.json",
            {
                "total_clusters": len(all_topic_results),
                "successful_clusters": successful_clusters,
                "failed_clusters": failed_clusters,
                "topic_results": all_topic_results
            }
        )

        # STEP 5: Summary

        print("\n======================================")
        print("RESOURCE DISCOVERY SUMMARY")
        print("======================================")

        print(f"Environments discovered: {len(environments)}")
        print(f"Clusters discovered: {len(all_clusters)}")
        print(f"Topic discovery successful: {successful_clusters}")
        print(f"Topic discovery failed: {failed_clusters}")

        print("\nDiscovery workflow completed.")


# ==================================================
# APPLICATION ENTRY POINT
# ==================================================

if __name__ == "__main__":

    asyncio.run(discover_all_resources())
