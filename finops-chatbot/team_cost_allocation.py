"""
Dynamic team ownership and conservative cost allocation for Confluent FinOps.

This module does not assume that every resource type is taggable and does not
invent resource ownership. It uses live MCP tool results and exact billing
resource IDs. Topic-level costs are not reported as actual charges unless the
billing API supplies a matching resource ID.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any


TEAM_TAG_RE = re.compile(r"^TEAM_[A-Z0-9_-]+$", re.IGNORECASE)
SHARED_TAGS = {"management", "shared", "platform"}


def _get(obj: Any, *keys: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        for key in keys:
            if key in obj:
                return obj[key]
    return default


def _walk(value: Any):
    """Yield dictionaries recursively from common MCP response envelopes."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _as_tag_names(value: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(value, str):
        names.add(value.strip())
    elif isinstance(value, dict):
        for key in ("name", "tag", "tag_name", "tagName", "value"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                names.add(candidate.strip())
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            names.update(_as_tag_names(item))
    return {name for name in names if name}


def _record_tags(record: dict[str, Any]) -> set[str]:
    tags: set[str] = set()
    for key in ("tags", "tag", "tag_names", "tagNames", "metadata_tags"):
        if key in record:
            tags.update(_as_tag_names(record[key]))
    # Some APIs represent tags as a metadata object.
    metadata = record.get("metadata")
    if isinstance(metadata, dict):
        for key in ("tags", "tag", "labels"):
            tags.update(_as_tag_names(metadata.get(key)))
    return tags


def _resource_id(record: dict[str, Any]) -> str | None:
    for key in (
        "id", "resource_id", "resourceId", "cluster_id", "clusterId",
        "connector_id", "connectorId", "compute_pool_id", "computePoolId",
        "topic_id", "topicId", "schema_id", "schemaId",
    ):
        value = record.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return None


def _resource_name(record: dict[str, Any]) -> str | None:
    for key in ("name", "display_name", "displayName", "resource_name", "topic_name"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _resource_type(tool_name: str, record: dict[str, Any]) -> str:
    name = tool_name.lower()
    if "topic" in name or any(k in record for k in ("topic_name", "topicName", "partitions")):
        return "topic"
    if "connector" in name or any(k in record for k in ("connector_id", "connectorId")):
        return "connector"
    if "compute-pool" in name or "compute_pool" in name or any(
        k in record for k in ("compute_pool_id", "computePoolId", "cfu")
    ):
        return "flink_compute_pool"
    if "schema" in name or any(k in record for k in ("subject", "schema_type")):
        return "schema"
    if "cluster" in name or any(k in record for k in ("cluster_id", "clusterId", "bootstrap_endpoint")):
        return "kafka_cluster"
    if "environment" in name or "environment_id" in record:
        return "environment"
    return "other"


def _extract_tag_catalog(agent_result: dict[str, Any]) -> set[str]:
    discovered: set[str] = set()
    for call in agent_result.get("results", []):
        tool = str(call.get("tool", "")).lower()
        if "list-tags" not in tool and "tag" not in tool:
            continue
        for record in _walk(call.get("result")):
            for key in ("name", "tag", "tag_name", "tagName", "value"):
                value = record.get(key)
                if isinstance(value, str) and value.strip():
                    discovered.add(value.strip())
    return discovered


def _extract_inventory_and_ownership(agent_result: dict[str, Any]):
    """Return unique resources and inferred owners backed by MCP tag evidence."""
    catalog_tags = _extract_tag_catalog(agent_result)
    team_tags = {tag for tag in catalog_tags if TEAM_TAG_RE.fullmatch(tag)}
    shared_tags = {tag for tag in catalog_tags if tag.lower() in SHARED_TAGS}

    resources: dict[tuple[str, str], dict[str, Any]] = {}

    for call in agent_result.get("results", []):
        tool_name = str(call.get("tool", "unknown"))
        tool_lower = tool_name.lower()
        result = call.get("result")
        args = call.get("arguments") or {}

        # search-topics-by-tag may return topic records without embedding tags.
        searched_tag = None
        if "search-topics-by-tag" in tool_lower:
            for value in args.values():
                if isinstance(value, str) and (
                    value in team_tags or value in shared_tags
                ):
                    searched_tag = value
                    break

        for record in _walk(result):
            rid = _resource_id(record)
            name = _resource_name(record)
            tags = _record_tags(record)

            # Ignore envelope dictionaries that are not identifiable resources.
            if not rid and not name:
                continue

            tags.update(
                tag for tag in (team_tags | shared_tags)
                if tag in tags
            )
            if searched_tag:
                tags.add(searched_tag)

            rtype = _resource_type(tool_name, record)
            identity = (rtype, rid or name or json.dumps(record, sort_keys=True, default=str))
            current = resources.setdefault(identity, {
                "resource_type": rtype,
                "resource_id": rid,
                "resource_name": name,
                "source_tools": set(),
                "tags": set(),
                "raw": record,
            })
            current["source_tools"].add(tool_name)
            current["tags"].update(tags)
            if not current.get("resource_id") and rid:
                current["resource_id"] = rid
            if not current.get("resource_name") and name:
                current["resource_name"] = name

    normalized = []
    for item in resources.values():
        tags = item["tags"]
        owners = sorted(tag for tag in tags if TEAM_TAG_RE.fullmatch(tag))
        shared = sorted(tag for tag in tags if tag.lower() in SHARED_TAGS)
        if len(owners) == 1:
            ownership = owners[0]
            ownership_status = "team_tagged"
        elif len(owners) > 1:
            ownership = None
            ownership_status = "conflicting_team_tags"
        elif shared:
            ownership = shared[0]
            ownership_status = "shared_or_platform"
        else:
            ownership = None
            ownership_status = "unassigned"

        normalized.append({
            "resource_type": item["resource_type"],
            "resource_id": item["resource_id"],
            "resource_name": item["resource_name"],
            "tags": sorted(tags),
            "team_tags": owners,
            "shared_tags": shared,
            "owner": ownership,
            "ownership_status": ownership_status,
            "source_tools": sorted(item["source_tools"]),
            "raw": item["raw"],
        })

    return sorted(team_tags), sorted(shared_tags), normalized


def _normalize_billing_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        resource = row.get("resource") if isinstance(row.get("resource"), dict) else {}
        env = resource.get("environment") if isinstance(resource.get("environment"), dict) else {}
        rid = (
            resource.get("id")
            or row.get("resource_id")
            or row.get("resource.id")
        )
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
) -> dict[str, Any]:
    """
    Build a conservative team report.

    Direct actual costs are assigned only when an MCP resource has one
    unambiguous owner tag and its exact resource ID matches a billing ID.
    Topic tags alone do not inherit the parent cluster's full cost.
    """
    team_tags, shared_tags, inventory = _extract_inventory_and_ownership(agent_result)
    billing = _normalize_billing_rows(billing_rows)

    inventory_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in inventory:
        if item.get("resource_id"):
            inventory_by_id[item["resource_id"]].append(item)

    team_totals: dict[str, float] = defaultdict(float)
    team_direct_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    shared_rows: list[dict[str, Any]] = []
    unallocated_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []
    promo_rows: list[dict[str, Any]] = []

    for row in billing:
        if str(row["line_type"]).upper() == "PROMO_CREDIT":
            promo_rows.append(row)
            continue

        rid = row["resource_id"]
        matches = inventory_by_id.get(rid, []) if rid else []

        owners = {
            item["owner"]
            for item in matches
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

        enriched = {
            **row,
            "inventory_matches": [
                {
                    "resource_type": item["resource_type"],
                    "resource_name": item["resource_name"],
                    "owner": item["owner"],
                    "tags": item["tags"],
                    "ownership_status": item["ownership_status"],
                }
                for item in matches
            ],
        }

        if len(owners) == 1 and not conflict and not shared_matches:
            owner = next(iter(owners))
            team_totals[owner] += row["amount"]
            team_direct_rows[owner].append(enriched)
        elif shared_matches and not owners and not conflict:
            shared_rows.append(enriched)
        elif not matches:
            unmatched_rows.append(enriched)
            unallocated_rows.append(enriched)
        else:
            unallocated_rows.append(enriched)

    all_teams = sorted(set(team_tags) | set(team_totals))
    teams = []
    for team in all_teams:
        teams.append({
            "team": team,
            "direct_actual_cost": round(team_totals.get(team, 0.0), 8),
            "matched_billing_resources": len(team_direct_rows.get(team, [])),
            "resources": team_direct_rows.get(team, []),
        })

    return {
        "team_tags_discovered": team_tags,
        "shared_tags_discovered": shared_tags,
        "teams": teams,
        "inventory": inventory,
        "billing_rows": billing,
        "shared_billing_rows": shared_rows,
        "unallocated_billing_rows": unallocated_rows,
        "unmatched_billing_rows": unmatched_rows,
        "promotional_credit_rows": promo_rows,
        "topic_level_actual_cost_available": False,
        "allocation_status": (
            "No usage-based shared-cost allocation was calculated. "
            "A compatible usage metric with team/resource dimensions and "
            "matching units is required."
        ),
        "limitations": [
            "Billing amounts are treated as actual only at the billing resource-ID level.",
            "Topic tags do not automatically inherit the parent Kafka cluster's full charge.",
            "Resources with missing, conflicting, or ambiguous ownership remain unallocated.",
            "Promotional credits are kept separate from resource costs.",
            "No proportional allocation is calculated without comparable measured usage.",
        ],
    }
