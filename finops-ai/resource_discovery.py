
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
    current = None

    patterns = {
        "name": r"^\s*Environment:\s*(.+)",
        "id": r"^\s*ID:\s*(\S+)",
        "resource_name": r"^\s*Resource Name:\s*(\S+)",
        "created_at": r"^\s*Created At:\s*(\S+)",
        "updated_at": r"^\s*Updated At:\s*(\S+)",
        "stream_governance_package":
            r"^\s*Stream Governance Package:\s*(.+)"
    }

    for line in raw_text.splitlines():

        for field, pattern in patterns.items():

            match = re.match(pattern, line)

            if not match:
                continue

            if field == "name":

                if current:
                    environments.append(current)

                current = {
                    "name": match.group(1).strip()
                }

            elif current:
                current[field] = match.group(1).strip()

            break

    if current:
        environments.append(current)

    return environments


async def discover_environments(session):

    print("\nDiscovering Confluent environments...")

    response = await session.call_tool(
        "list-environments",
        arguments={}
    )

    if response.is_error:
        raise RuntimeError(str(response.content))

    text_blocks = get_text_content(response)

    environments = extract_environment_details(
        "\n".join(text_blocks)
    )

    save_json(
        "environments.json",
        {
            "total_environments": len(environments),
            "environments": environments,
            "raw_response": text_blocks
        }
    )

    print(f"Discovered {len(environments)} environment(s).")

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

    sections = re.split(
        r"(?=^\s*Cluster:\s*)",
        raw_text,
        flags=re.MULTILINE
    )

    for section in sections:

        name = re.search(
            r"^\s*Cluster:\s*(.+)",
            section,
            re.MULTILINE
        )

        cluster_id = re.search(
            r"^\s*ID:\s*(\S+)",
            section,
            re.MULTILINE
        )

        if not name or not cluster_id:
            continue

        cluster = {
            "cluster_name": name.group(1).strip(),
            "cluster_id": cluster_id.group(1).strip(),
            "environment_name": environment["name"],
            "environment_id": environment["id"]
        }

        fields = {
            "status": r"^\s*Status:\s*(.+)",
            "availability": r"^\s*Availability:\s*(.+)",
            "cloud": r"^\s*Cloud:\s*(.+)",
            "region": r"^\s*Region:\s*(.+)",
            "cku": r"^\s*CKU:\s*(.+)",
            "kind": r"^\s*Kind:\s*(.+)",
            "zones": r"^\s*Zones:\s*(.*)"
        }

        for field, pattern in fields.items():

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

        endpoint_patterns = {
            "http": r"^\s*HTTP:\s*(\S+)",
            "bootstrap": r"^\s*Bootstrap:\s*(\S+)"
        }

        for field, pattern in endpoint_patterns.items():

            match = re.search(
                pattern,
                section,
                re.MULTILINE
            )

            if match:
                endpoints[field] = match.group(1)

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
        raise RuntimeError(str(response.content))

    text_blocks = get_text_content(response)

    clusters = extract_cluster_details(
        "\n".join(text_blocks),
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
            raise RuntimeError(str(response.content))

        text_blocks = get_text_content(response)

        for block in text_blocks:
            print(block)

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "success",
            "raw_response": text_blocks
        }

    except Exception as error:

        print(f"Topic discovery failed: {error}")

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "failed",
            "error": str(error),
            "raw_response": []
        }


# ==================================================
# CONNECTOR DISCOVERY
# ==================================================

def extract_connector_names(raw_text):

    connector_names = []
    lines = raw_text.splitlines()

    for index, line in enumerate(lines):

        match = re.match(
            r"^\s*Active Connectors:\s*(.*)$",
            line,
            re.IGNORECASE
        )

        if not match:
            continue

        same_line_names = match.group(1).strip()

        if same_line_names:
            candidates = same_line_names.split(",")

        else:

            candidates = []

            for next_line in lines[index + 1:]:

                value = next_line.strip()

                if not value:
                    continue

                if (
                    value.lower().startswith("pagination:")
                    or re.match(r"^[A-Za-z][A-Za-z ]+:", value)
                ):
                    break

                candidates.extend(value.split(","))

        for candidate in candidates:

            name = candidate.strip()

            if (
                name
                and name.lower() not in {
                    "none",
                    "no connectors",
                    "null"
                }
                and name not in connector_names
            ):
                connector_names.append(name)

    return connector_names


async def discover_connectors(session, environment, cluster):

    environment_id = environment["id"]
    cluster_id = cluster["cluster_id"]

    print(
        f"\nDiscovering connectors in cluster: "
        f"{cluster['cluster_name']} ({cluster_id})"
    )

    try:

        response = await session.call_tool(
            "list-connectors",
            arguments={
                "environmentId": environment_id,
                "clusterId": cluster_id
            }
        )

        if response.is_error:
            raise RuntimeError(str(response.content))

        text_blocks = get_text_content(response)
        raw_text = "\n".join(text_blocks)

        print("\nConnector listing response:")
        print(raw_text)

        connector_names = extract_connector_names(raw_text)

        print(
            f"\nDiscovered {len(connector_names)} connector(s)."
        )

        connector_details = []

        for connector_name in connector_names:

            print(
                f"\nRetrieving configuration for: "
                f"{connector_name}"
            )

            try:

                config_response = await session.call_tool(
                    "get-connector-config",
                    arguments={
                        "environmentId": environment_id,
                        "clusterId": cluster_id,
                        "connectorName": connector_name
                    }
                )

                if config_response.is_error:
                    raise RuntimeError(
                        str(config_response.content)
                    )

                config_text = get_text_content(config_response)

                connector_details.append({
                    "connector_name": connector_name,
                    "status": "success",
                    "configuration_response": config_text
                })

                print(
                    f"Configuration retrieved: {connector_name}"
                )

            except Exception as error:

                print(
                    f"Configuration retrieval failed for "
                    f"{connector_name}: {error}"
                )

                connector_details.append({
                    "connector_name": connector_name,
                    "status": "failed",
                    "error": str(error),
                    "configuration_response": []
                })

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "success",
            "total_connectors": len(connector_names),
            "connectors": connector_details,
            "raw_response": text_blocks
        }

    except Exception as error:

        print(f"Connector discovery failed: {error}")

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "cluster_name": cluster["cluster_name"],
            "cluster_id": cluster_id,
            "status": "failed",
            "total_connectors": 0,
            "connectors": [],
            "error": str(error),
            "raw_response": []
        }


# ==================================================
# SCHEMA DISCOVERY
# ==================================================

async def discover_schemas(session, environment):

    environment_id = environment["id"]

    print(
        f"\nDiscovering schemas in environment: "
        f"{environment['name']} ({environment_id})"
    )

    try:

        response = await session.call_tool(
            "list-schemas",
            arguments={
                "environment_id": environment_id,
                "latestOnly": True
            }
        )

        if response.is_error:
            raise RuntimeError(str(response.content))

        text_blocks = get_text_content(response)

        for block in text_blocks:
            print(block)

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "status": "success",
            "raw_response": text_blocks
        }

    except Exception as error:

        print(f"Schema discovery failed: {error}")

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "status": "failed",
            "error": str(error),
            "raw_response": []
        }


# ==================================================
# COMPUTE POOL DISCOVERY
# ==================================================

def extract_compute_pool_details(raw_text, environment):
    """
    Extract compute pool details from the MCP response.

    The parser supports the text format returned by
    list-compute-pools.
    """

    compute_pools = []

    sections = re.split(
        r"(?=^\s*(?:Compute Pool|Pool):\s*)",
        raw_text,
        flags=re.MULTILINE | re.IGNORECASE
    )

    for section in sections:

        name_match = re.search(
            r"^\s*(?:Compute Pool|Pool):\s*(.+)",
            section,
            re.MULTILINE | re.IGNORECASE
        )

        id_match = re.search(
            r"^\s*ID:\s*(lfcp-[A-Za-z0-9]+)",
            section,
            re.MULTILINE
        )

        # Some responses may use a compact format.
        if not name_match or not id_match:
            continue

        pool = {
            "compute_pool_name": name_match.group(1).strip(),
            "compute_pool_id": id_match.group(1).strip(),
            "environment_name": environment["name"],
            "environment_id": environment["id"]
        }

        fields = {
            "cloud": r"^\s*Cloud:\s*(.+)",
            "region": r"^\s*Region:\s*(.+)",
            "status": r"^\s*Status:\s*(.+)",
            "max_cfu": r"^\s*Max CFU:\s*(.+)",
            "current_cfu": r"^\s*Current CFU:\s*(.+)"
        }

        for field, pattern in fields.items():

            match = re.search(
                pattern,
                section,
                re.MULTILINE | re.IGNORECASE
            )

            if match:
                pool[field] = match.group(1).strip()

        compute_pools.append(pool)

    return compute_pools


async def discover_compute_pools(session, environment):

    environment_id = environment["id"]

    print(
        f"\nDiscovering compute pools in environment: "
        f"{environment['name']} ({environment_id})"
    )

    try:

        response = await session.call_tool(
            "list-compute-pools",
            arguments={
                "environmentId": environment_id,
                "pageSize": 100
            }
        )

        if response.is_error:
            raise RuntimeError(str(response.content))

        text_blocks = get_text_content(response)
        raw_text = "\n".join(text_blocks)

        print("\nCompute pool response:")
        print(raw_text)

        compute_pools = extract_compute_pool_details(
            raw_text,
            environment
        )

        print(
            f"\nDiscovered {len(compute_pools)} compute pool(s)."
        )

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "status": "success",
            "total_compute_pools": len(compute_pools),
            "compute_pools": compute_pools,
            "raw_response": text_blocks
        }

    except Exception as error:

        print(f"Compute pool discovery failed: {error}")

        return {
            "environment_name": environment["name"],
            "environment_id": environment_id,
            "status": "failed",
            "total_compute_pools": 0,
            "compute_pools": [],
            "error": str(error),
            "raw_response": []
        }


# ==================================================
# MAIN DISCOVERY WORKFLOW
# ==================================================

async def discover_all_resources():

    async with get_mcp_session() as session:

        # STEP 1: ENVIRONMENTS

        environments = await discover_environments(session)

        if not environments:
            print("\nNo environments found.")
            return

        # STEP 2: CLUSTERS

        all_cluster_results = []
        all_clusters = []

        for environment in environments:

            if not environment.get("id"):
                continue

            result = await discover_clusters(
                session,
                environment
            )

            all_cluster_results.append(result)
            all_clusters.extend(result["clusters"])

        save_json(
            "clusters.json",
            {
                "total_environments": len(environments),
                "total_clusters": len(all_clusters),
                "cluster_results": all_cluster_results
            }
        )

        # STEP 3: TOPICS

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

            result = await discover_topics(
                session,
                environment,
                cluster
            )

            all_topic_results.append(result)

        save_json(
            "topics.json",
            {
                "total_clusters": len(all_topic_results),
                "successful_clusters": sum(
                    r["status"] == "success"
                    for r in all_topic_results
                ),
                "failed_clusters": sum(
                    r["status"] == "failed"
                    for r in all_topic_results
                ),
                "topic_results": all_topic_results
            }
        )

        # STEP 4: CONNECTORS

        all_connector_results = []

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

            result = await discover_connectors(
                session,
                environment,
                cluster
            )

            all_connector_results.append(result)

        total_connectors = sum(
            result["total_connectors"]
            for result in all_connector_results
        )

        successful_configs = sum(
            connector["status"] == "success"
            for result in all_connector_results
            for connector in result["connectors"]
        )

        failed_configs = sum(
            connector["status"] == "failed"
            for result in all_connector_results
            for connector in result["connectors"]
        )

        save_json(
            "connectors.json",
            {
                "total_clusters_checked": len(all_connector_results),
                "total_connectors": total_connectors,
                "successful_configurations": successful_configs,
                "failed_configurations": failed_configs,
                "connector_results": all_connector_results
            }
        )

        # STEP 5: SCHEMAS

        all_schema_results = []

        for environment in environments:

            if not environment.get("id"):
                continue

            result = await discover_schemas(
                session,
                environment
            )

            all_schema_results.append(result)

        save_json(
            "schemas.json",
            {
                "total_environments_checked": len(all_schema_results),
                "successful_environments": sum(
                    r["status"] == "success"
                    for r in all_schema_results
                ),
                "failed_environments": sum(
                    r["status"] == "failed"
                    for r in all_schema_results
                ),
                "schema_results": all_schema_results
            }
        )

        # STEP 6: COMPUTE POOLS

        all_compute_pool_results = []
        all_compute_pools = []

        for environment in environments:

            if not environment.get("id"):
                continue

            result = await discover_compute_pools(
                session,
                environment
            )

            all_compute_pool_results.append(result)

            all_compute_pools.extend(
                result["compute_pools"]
            )

        successful_pool_environments = sum(
            r["status"] == "success"
            for r in all_compute_pool_results
        )

        failed_pool_environments = sum(
            r["status"] == "failed"
            for r in all_compute_pool_results
        )

        save_json(
            "compute_pools.json",
            {
                "total_environments_checked": len(
                    all_compute_pool_results
                ),
                "total_compute_pools": len(all_compute_pools),
                "successful_environments": successful_pool_environments,
                "failed_environments": failed_pool_environments,
                "compute_pool_results": all_compute_pool_results
            }
        )

        # STEP 7: FINAL SUMMARY

        print("\n======================================")
        print("RESOURCE DISCOVERY SUMMARY")
        print("======================================")

        print(f"Environments: {len(environments)}")
        print(f"Kafka clusters: {len(all_clusters)}")

        print(
            f"Topic discovery successful: "
            f"{sum(r['status'] == 'success' for r in all_topic_results)}"
        )

        print(f"Connectors: {total_connectors}")
        print(f"Connector configurations retrieved: {successful_configs}")

        print(
            f"Schema discovery successful: "
            f"{sum(r['status'] == 'success' for r in all_schema_results)}"
        )

        print(f"Compute pools: {len(all_compute_pools)}")

        print(
            f"Compute pool discovery successful: "
            f"{successful_pool_environments}"
        )

        print(
            f"Compute pool discovery failed: "
            f"{failed_pool_environments}"
        )

        print("\nDiscovery workflow completed.")


# ==================================================
# APPLICATION ENTRY POINT
# ==================================================

if __name__ == "__main__":

    asyncio.run(discover_all_resources())
