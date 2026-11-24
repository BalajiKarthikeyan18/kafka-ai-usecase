"""
Team ownership discovery and conservative cost reconciliation for Confluent FinOps.

Sources:
- Confluent MCP inventory and topic-tag search results.
- Confluent Stream Catalog /catalog/v1/search/basic for tag-to-entity associations.
- Confluent Billing REST API line items supplied by chatbot.py.

The module never assigns a cluster's full charge to its topics or invents
usage-based allocations. Direct actual cost requires exact resource-ID matching.
"""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv


TEAM_TAG_RE = re.compile(r"^TEAM_[A-Z0-9_-]+$", re.IGNORECASE)
SHARED_TAGS = {"management", "shared", "platform"}

# These are candidate Stream Catalog entity types, not a guarantee that every
# entity type is supported or has tag assignments in a particular environment.
CATALOG_ENTITY_TYPES = (
    "kafka_logical_cluster",
    "kafka_topic",
    "cn_connector",
    "flink_compute_pool",
    "schema",
)


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _tag_names(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, str):
        if value.strip():
            found.add(value.strip())
    elif isinstance(value, dict):
        for key in ("name", "tag", "tag_name", "tagName", "value"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                found.add(candidate.strip())
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            found.update(_tag_names(item))
    return found


def _record_tags(record: dict[str, Any]) -> set[str]:
    tags: set[str] = set()
    for key in ("tags", "tag", "tag_names", "tagNames", "metadata_tags"):
        if key in record:
            tags.update(_tag_names(record[key]))
    metadata = record.get("metadata")
    if isinstance(metadata, dict):
        for key in ("tags", "tag", "labels"):
            tags.update(_tag_names(metadata.get(key)))
    return tags


def _catalog_tag_definitions(agent_result: dict[str, Any]) -> set[str]:
    """Extract tag definitions from common MCP response envelope shapes."""
    tags: set[str] = set()
    tag_keys = {
        "name", "tag", "tag_name", "tagname",
        "tag_names", "tagnames", "classificationnames",
    }

    def visit(value: Any, parent_key: str = "") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = str(key).replace("-", "_").lower()
                if normalized_key in tag_keys:
                    tags.update(_tag_names(child))
                elif isinstance(child, (dict, list)):
                    visit(child, normalized_key)
        elif isinstance(value, list):
            for child in value:
                if isinstance(child, (dict, list)):
                    visit(child, parent_key)
                elif parent_key in tag_keys and isinstance(child, str) and child.strip():
                    tags.add(child.strip())

    for call in agent_result.get("results", []):
        if "list-tags" in str(call.get("tool", "")).lower():
            visit(call.get("result"))
    return tags


def _mcp_resource_id(record: dict[str, Any]) -> str | None:
    for key in (
        "id", "resource_id", "resourceId", "cluster_id", "clusterId",
        "connector_id", "connectorId", "compute_pool_id", "computePoolId",
        "topic_id", "topicId", "schema_id", "schemaId",
    ):
        value = record.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return None


def _mcp_resource_name(record: dict[str, Any]) -> str | None:
    for key in ("name", "display_name", "displayName", "resource_name", "topic_name"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _mcp_resource_type(tool_name: str, record: dict[str, Any]) -> str:
    tool = tool_name.lower()
    explicit = str(
        record.get("entity_type") or record.get("entityType")
        or record.get("resource_type") or record.get("resourceType")
        or record.get("kind") or ""
    ).lower()
    combined = f"{tool} {explicit}"
    if "topic" in combined:
        return "kafka_topic"
    if "connector" in combined:
        return "connector"
    if "compute" in combined or "flink" in combined:
        return "flink_compute_pool"
    if "schema" in combined:
        return "schema"
    if "cluster" in combined:
        return "kafka_cluster"
    if "environment" in combined:
        return "environment"
    return "other"


def _qualified_name_to_resource_id(entity_type: str, qualified_name: str) -> str:
    """
    Convert Catalog qualified names to Confluent resource IDs where the
    Catalog format is known. Observed resource-qualified names use the
    Schema Registry tenant as a prefix, e.g. lsrc-...:lcc-... or
    lsrc-...:lfcp-.... Preserve the full qualified name separately.
    """
    id_bearing_types = {
        "kafka_logical_cluster",
        "cn_connector",
        "flink_compute_pool",
    }
    if entity_type in id_bearing_types and ":" in qualified_name:
        return qualified_name.rsplit(":", 1)[-1]
    return qualified_name


def _canonical_resource_id(value: Any) -> str | None:
    """Normalize only known Catalog tenant-prefixed IDs for matching."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Confluent resource IDs are the final component in the observed
    # <schema-registry-tenant>:<resource-id> Catalog qualified-name format.
    if ":" in text and text.split(":", 1)[0].startswith("lsrc-"):
        return text.rsplit(":", 1)[-1]
    return text


def _catalog_type_to_resource_type(entity_type: str) -> str:
    mapping = {
        "kafka_logical_cluster": "kafka_cluster",
        "kafka_topic": "kafka_topic",
        "cn_connector": "connector",
        "flink_compute_pool": "flink_compute_pool",
        "schema": "schema",
    }
    return mapping.get(entity_type, entity_type)


def discover_catalog_tag_entities(
    tag_names: list[str],
    endpoint: str | None = None,
    api_key: str | None = None,
    api_secret: str | None = None,
    timeout: int = 30,
    entity_types: tuple[str, ...] = CATALOG_ENTITY_TYPES,
) -> dict[str, Any]:
    """
    Search Stream Catalog for each tag and candidate entity type.

    A zero-result response is recorded as zero results for that specific
    type/tag pair. Unsupported entity types (4xx) are recorded as coverage
    gaps; other HTTP/network failures are recorded as errors.
    """
    endpoint = (endpoint or os.getenv("SCHEMA_REGISTRY_ENDPOINT", "")).strip().rstrip("/")
    api_key = (api_key or os.getenv("SCHEMA_REGISTRY_API_KEY", "")).strip()
    api_secret = (api_secret or os.getenv("SCHEMA_REGISTRY_API_SECRET", "")).strip()

    if not endpoint or not api_key or not api_secret:
        return {
            "available": False,
            "entities": [],
            "checks": [],
            "errors": ["Schema Registry endpoint or API credentials are missing."],
        }

    # The endpoint is the Schema Registry base URL, e.g. https://...confluent.cloud.
    # The API route is relative to that base URL.
    url = f"{endpoint}/catalog/v1/search/basic"
    session = requests.Session()
    session.auth = (api_key, api_secret)
    session.headers.update({"Accept": "application/json"})

    entities: dict[tuple[str, str], dict[str, Any]] = {}
    checks: list[dict[str, Any]] = []
    errors: list[str] = []

    for tag in sorted(set(tag_names)):
        for entity_type in entity_types:
            offset = 0
            limit = 100
            total_for_check = 0
            pages = 0

            while True:
                params = {
                    "type": entity_type,
                    "tag": tag,
                    "limit": limit,
                    "offset": offset,
                }
                try:
                    response = session.get(url, params=params, timeout=timeout)
                except requests.RequestException as exc:
                    errors.append(
                        f"{entity_type}/{tag}: {type(exc).__name__}: {exc}"
                    )
                    break

                if response.status_code in (400, 404, 422):
                    # Record as a coverage gap; don't claim no assignment.
                    checks.append({
                        "tag": tag,
                        "entity_type": entity_type,
                        "status": "unsupported_or_invalid_query",
                        "http_status": response.status_code,
                        "result_count": total_for_check,
                    })
                    break

                try:
                    response.raise_for_status()
                    payload = response.json()
                except (requests.RequestException, ValueError) as exc:
                    errors.append(
                        f"{entity_type}/{tag}: {type(exc).__name__}: {exc}"
                    )
                    break

                page_entities = payload.get("entities") or []
                if not isinstance(page_entities, list):
                    page_entities = []

                for entity in page_entities:
                    if not isinstance(entity, dict):
                        continue
                    attrs = entity.get("attributes") or {}
                    qname = attrs.get("qualifiedName")
                    name = attrs.get("name") or entity.get("displayText")
                    if not qname:
                        continue

                    key = (entity_type, str(qname))
                    item = entities.setdefault(key, {
                        "resource_type": _catalog_type_to_resource_type(entity_type),
                        "catalog_entity_type": entity_type,
                        "qualified_name": str(qname),
                        "resource_id": _qualified_name_to_resource_id(
                            entity_type, str(qname)
                        ),
                        "resource_name": name,
                        "guid": entity.get("guid"),
                        "status": entity.get("status"),
                        "tags": set(),
                        "source": "stream_catalog",
                    })
                    item["tags"].update(entity.get("classificationNames") or [])
                    # The queried tag is authoritative evidence of this assignment.
                    item["tags"].add(tag)
                    if not item.get("resource_name") and name:
                        item["resource_name"] = name
                    total_for_check += 1

                pages += 1
                search_parameters = payload.get("searchParameters") or {}
                response_limit = int(search_parameters.get("limit") or limit)
                response_offset = int(search_parameters.get("offset") or offset)

                if len(page_entities) < response_limit:
                    break
                offset = response_offset + response_limit

            # Only add a success/zero-result check if no unsupported check was
            # already recorded for this tag/type.
            if not any(
                c["tag"] == tag and c["entity_type"] == entity_type
                and c["status"] == "unsupported_or_invalid_query"
                for c in checks
            ):
                checks.append({
                    "tag": tag,
                    "entity_type": entity_type,
                    "status": "results_found" if total_for_check else "zero_results",
                    "result_count": total_for_check,
                    "pages": pages,
                })

    normalized = []
    for item in entities.values():
        item["tags"] = sorted(item["tags"])
        normalized.append(item)

    return {
        "available": True,
        "entities": sorted(
            normalized,
            key=lambda x: (x["resource_type"], x.get("resource_name") or ""),
        ),
        "checks": checks,
        "errors": errors,
        "endpoint_host": urlparse(endpoint).netloc,
    }


def _ownership(item: dict[str, Any]) -> tuple[str | None, str]:
    tags = item.get("tags") or []
    owners = sorted(
        tag for tag in tags if TEAM_TAG_RE.fullmatch(str(tag))
    )
    shared = sorted(
        tag for tag in tags if str(tag).lower() in SHARED_TAGS
    )
    if len(owners) == 1:
        return owners[0], "team_tagged"
    if len(owners) > 1:
        return None, "conflicting_team_tags"
    if shared:
        return shared[0], "shared_or_platform"
    return None, "unassigned"


def _build_mcp_inventory(agent_result: dict[str, Any]) -> list[dict[str, Any]]:
    resources: dict[tuple[str, str], dict[str, Any]] = {}

    for call in agent_result.get("results", []):
        tool_name = str(call.get("tool", "unknown"))
        tool_lower = tool_name.lower()
        if "list-tags" in tool_lower:
            continue

        args = call.get("arguments") or {}
        searched_tag = None
        if "tag" in tool_lower:
            for value in args.values():
                if isinstance(value, str) and (
                    TEAM_TAG_RE.fullmatch(value)
                    or value.lower() in SHARED_TAGS
                ):
                    searched_tag = value
                    break

        for record in _walk(call.get("result")):
            rid = _mcp_resource_id(record)
            name = _mcp_resource_name(record)
            rtype = _mcp_resource_type(tool_name, record)
            if not rid and not name:
                continue
            if rtype == "other":
                continue

            tags = _record_tags(record)
            if searched_tag:
                tags.add(searched_tag)

            identity = (rtype, rid or name or json.dumps(record, sort_keys=True, default=str))
            current = resources.setdefault(identity, {
                "resource_type": rtype,
                "resource_id": rid,
                "resource_name": name,
                "tags": set(),
                "source": "mcp",
                "source_tools": set(),
                "raw": record,
            })
            current["tags"].update(tags)
            current["source_tools"].add(tool_name)

    output = []
    for item in resources.values():
        item["tags"] = sorted(item["tags"])
        item["source_tools"] = sorted(item["source_tools"])
        item["owner"], item["ownership_status"] = _ownership(item)
        output.append(item)
    return output


def _merge_inventory(
    mcp_inventory: list[dict[str, Any]],
    catalog_inventory: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    for item in mcp_inventory + catalog_inventory:
        rid = item.get("resource_id")
        qname = item.get("qualified_name")
        name = item.get("resource_name")
        identity_value = rid or qname or name or json.dumps(item, sort_keys=True, default=str)
        key = (item.get("resource_type", "other"), str(identity_value))

        if key not in merged:
            merged[key] = {
                **item,
                "tags": set(item.get("tags") or []),
                "sources": {item.get("source", "unknown")},
            }
        else:
            current = merged[key]
            current["tags"].update(item.get("tags") or [])
            current["sources"].add(item.get("source", "unknown"))
            if not current.get("resource_name") and name:
                current["resource_name"] = name
            if not current.get("resource_id") and rid:
                current["resource_id"] = rid
            if qname:
                current["qualified_name"] = qname
            if item.get("guid"):
                current["guid"] = item["guid"]

    output = []
    for item in merged.values():
        item["tags"] = sorted(item["tags"])
        item["sources"] = sorted(item["sources"])
        item["owner"], item["ownership_status"] = _ownership(item)
        output.append(item)

    return sorted(
        output,
        key=lambda x: (
            x.get("resource_type", ""),
            x.get("resource_name") or "",
            x.get("resource_id") or "",
        ),
    )


def _normalize_billing_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        resource = row.get("resource") if isinstance(row.get("resource"), dict) else {}
        env = resource.get("environment") if isinstance(resource.get("environment"), dict) else {}
        rid = resource.get("id") or row.get("resource_id") or row.get("resource.id")
        try:
            amount = float(row.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        normalized.append({
            "resource_id": str(rid) if rid else None,
            "resource_name": (
                resource.get("display_name")
                or row.get("resource_display_name")
                or row.get("resource.display_name")
            ),
            "environment_id": env.get("id") or row.get("environment_id"),
            "product": row.get("product") or "Unspecified",
            "line_type": row.get("line_type") or "Unspecified",
            "amount": amount,
            "unit": row.get("unit"),
            "raw": row,
        })
    return normalized


def build_team_cost_report(
    billing_rows: list[dict[str, Any]],
    agent_result: dict[str, Any],
    catalog_endpoint: str | None = None,
    catalog_api_key: str | None = None,
    catalog_api_secret: str | None = None,
    catalog_timeout: int = 30,
) -> dict[str, Any]:
    """
    Discover tags from MCP definitions, search Stream Catalog for each tag
    across candidate entity types, merge with MCP inventory, and reconcile
    billing only by exact resource IDs.
    """
    # Load parent .env if this module is called outside the normal chatbot boot.
    load_dotenv(override=False)

    tag_definitions = _catalog_tag_definitions(agent_result)
    # Ensure known ownership/shared tags discovered by MCP are searched too.
    known_tags = {
        tag for tag in tag_definitions
        if TEAM_TAG_RE.fullmatch(tag) or tag.lower() in SHARED_TAGS
    }

    # MCP tag-search calls may reveal tags even if list-tags response shape differs.
    # Also collect explicit classification/tag arrays from returned resource metadata.
    for call in agent_result.get("results", []):
        for value in (call.get("arguments") or {}).values():
            if isinstance(value, str) and (
                TEAM_TAG_RE.fullmatch(value) or value.lower() in SHARED_TAGS
            ):
                known_tags.add(value)
        for record in _walk(call.get("result")):
            for key in ("classificationNames", "classification_names", "tags", "tagNames", "tag_names"):
                if key in record:
                    for tag in _tag_names(record[key]):
                        if TEAM_TAG_RE.fullmatch(tag) or tag.lower() in SHARED_TAGS:
                            known_tags.add(tag)

    catalog_result = discover_catalog_tag_entities(
        tag_names=sorted(known_tags),
        endpoint=catalog_endpoint,
        api_key=catalog_api_key,
        api_secret=catalog_api_secret,
        timeout=catalog_timeout,
    )

    mcp_inventory = _build_mcp_inventory(agent_result)
    inventory = _merge_inventory(
        mcp_inventory,
        catalog_result.get("entities", []),
    )

    team_tags = sorted({
        tag
        for item in inventory
        for tag in item.get("tags", [])
        if TEAM_TAG_RE.fullmatch(str(tag))
    } | known_tags.intersection({
        tag for tag in known_tags if TEAM_TAG_RE.fullmatch(tag)
    }))

    shared_tags = sorted({
        tag
        for item in inventory
        for tag in item.get("tags", [])
        if str(tag).lower() in SHARED_TAGS
    } | {
        tag for tag in known_tags if tag.lower() in SHARED_TAGS
    })

    billing = _normalize_billing_rows(billing_rows)

    # Build an exact-ID billing index after normalizing Catalog tenant prefixes.
    # Multiple billing line items for one resource are aggregated, not counted
    # as multiple matched resources in the team summary.
    billing_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in billing:
        if str(row["line_type"]).upper() == "PROMO_CREDIT":
            continue
        canonical_id = _canonical_resource_id(row.get("resource_id"))
        if canonical_id:
            billing_by_id[canonical_id].append(row)

    inventory_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in inventory:
        canonical_id = _canonical_resource_id(item.get("resource_id"))
        if canonical_id:
            inventory_by_id[canonical_id].append(item)

    # Resource-level reconciliation: attach billing totals to inventory records.
    matched_inventory_ids: set[str] = set()
    for item in inventory:
        canonical_id = _canonical_resource_id(item.get("resource_id"))
        matched_rows = billing_by_id.get(canonical_id, []) if canonical_id else []
        unique_inventory_matches = inventory_by_id.get(canonical_id, []) if canonical_id else []
        if canonical_id and len(unique_inventory_matches) > 1:
            item["billing_match_status"] = "ambiguous_inventory_match"
            item["billing_actual_cost"] = None
            item["matched_billing_resource_id"] = canonical_id
        elif matched_rows:
            item["billing_match_status"] = "matched"
            item["billing_actual_cost"] = round(sum(r["amount"] for r in matched_rows), 8)
            item["matched_billing_resource_id"] = canonical_id
            matched_inventory_ids.add(canonical_id)
        elif canonical_id:
            item["billing_match_status"] = "no_billing_record"
            item["billing_actual_cost"] = None
            item["matched_billing_resource_id"] = None
        else:
            item["billing_match_status"] = "missing_resource_id"
            item["billing_actual_cost"] = None
            item["matched_billing_resource_id"] = None

    team_totals: dict[str, float] = defaultdict(float)
    team_direct_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    team_matched_ids: dict[str, set[str]] = defaultdict(set)
    shared_rows: list[dict[str, Any]] = []
    unallocated_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []
    promo_rows: list[dict[str, Any]] = []

    for row in billing:
        if str(row["line_type"]).upper() == "PROMO_CREDIT":
            promo_rows.append(row)
            continue

        canonical_id = _canonical_resource_id(row.get("resource_id"))
        matches = inventory_by_id.get(canonical_id, []) if canonical_id else []
        owners = {
            item["owner"] for item in matches
            if item["ownership_status"] == "team_tagged" and item["owner"]
        }
        shared_matches = [
            item for item in matches
            if item["ownership_status"] == "shared_or_platform"
        ]
        conflict = any(
            item["ownership_status"] == "conflicting_team_tags"
            for item in matches
        )
        ambiguous = len(matches) > 1 and len({
            (m.get("resource_type"), m.get("resource_name"), m.get("owner"))
            for m in matches
        }) > 1

        enriched = {
            **row,
            "canonical_resource_id": canonical_id,
            "reconciliation_status": (
                "ambiguous" if ambiguous or conflict or len(owners) > 1
                else "matched" if matches
                else "unmatched"
            ),
            "inventory_matches": [
                {
                    "resource_type": item["resource_type"],
                    "resource_name": item.get("resource_name"),
                    "resource_id": item.get("resource_id"),
                    "qualified_name": item.get("qualified_name"),
                    "owner": item.get("owner"),
                    "tags": item.get("tags", []),
                    "ownership_status": item["ownership_status"],
                    "billing_match_status": item.get("billing_match_status"),
                    "sources": item.get("sources", []),
                }
                for item in matches
            ],
        }

        if len(owners) == 1 and not conflict and not shared_matches and not ambiguous:
            owner = next(iter(owners))
            team_totals[owner] += row["amount"]
            team_direct_rows[owner].append(enriched)
            if canonical_id:
                team_matched_ids[owner].add(canonical_id)
        elif shared_matches and not owners and not conflict and not ambiguous:
            shared_rows.append(enriched)
        else:
            unallocated_rows.append(enriched)
            if not matches:
                unmatched_rows.append(enriched)

    teams = [
        {
            "team": team,
            "direct_actual_cost": round(team_totals.get(team, 0.0), 8),
            "matched_billing_resources": len(team_matched_ids.get(team, set())),
            "assigned_inventory_resources": [
                {
                    "resource_type": item.get("resource_type"),
                    "resource_name": item.get("resource_name"),
                    "resource_id": item.get("resource_id"),
                    "qualified_name": item.get("qualified_name"),
                    "tags": item.get("tags", []),
                    "billing_match_status": item.get("billing_match_status"),
                    "billing_actual_cost": item.get("billing_actual_cost"),
                }
                for item in inventory
                if item.get("owner") == team and item.get("ownership_status") == "team_tagged"
            ],
            "resources": team_direct_rows.get(team, []),
        }
        for team in sorted(set(team_tags) | set(team_totals))
    ]

    # Summarise tag search coverage so "zero results" isn't misrepresented
    # as proof that no tag assignment exists.
    coverage_counts = defaultdict(int)
    for check in catalog_result.get("checks", []):
        coverage_counts[check["status"]] += 1

    return {
        "team_tags_discovered": team_tags,
        "shared_tags_discovered": shared_tags,
        "tag_definitions_discovered": sorted(tag_definitions),
        "teams": teams,
        "inventory": inventory,
        "billing_rows": billing,
        "shared_billing_rows": shared_rows,
        "unallocated_billing_rows": unallocated_rows,
        "unmatched_billing_rows": unmatched_rows,
        "promotional_credit_rows": promo_rows,
        "catalog_discovery": catalog_result,
        "catalog_coverage_counts": dict(coverage_counts),
        "topic_level_actual_cost_available": False,
        "allocation_status": (
            "No usage-based shared-cost allocation was calculated. "
            "A compatible usage metric with team/resource dimensions and "
            "matching units is required."
        ),
        "limitations": [
            "Billing amounts are actual only at the billing resource-ID level.",
            "Topic tags do not inherit the parent Kafka cluster's full charge.",
            "Resources with missing, conflicting, or ambiguous ownership remain unallocated.",
            "Catalog tenant prefixes are stripped only for known resource-ID matching; full qualified names are retained for traceability.",
            "Promotional credits are kept separate from resource costs.",
            "Candidate Catalog entity types can return unsupported-query coverage gaps.",
            "No proportional allocation is calculated without comparable measured usage.",
        ],
    }
