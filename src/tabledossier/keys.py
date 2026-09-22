"""Deep level, part II: exact uniqueness of keys.

Part of the embedded runtime (standard library only). Like ``planning`` and
``deep``, this module only *describes* work; the Spark adapter executes it.

A key is one or more columns of a table. Keys come from the configuration
(``deep.uniqueness.keys``), from declared PRIMARY KEY/UNIQUE constraints
(``declared_keys``) and, within the budget, from the identifier candidates of
the standard level (``identifier_candidates``). Every key is checked exactly
over the analysed scope with a grouped aggregation; several keys share one
Spark action (each row is exploded once per key it checks), so the number of
actions per table is bounded by ``deep.uniqueness.max_passes``.

Only counts leave the engine: duplicated values are never collected.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from tabledossier.jsonutil import short_hash
from tabledossier.metrics import measured, not_measured
from tabledossier.paths import (
    IdentifierError,
    column_reference_segments,
    display_path,
    field_id,
    field_segment,
    parse_table_identifier,
    table_lookup_key,
)
from tabledossier.planning import ORDERABLE_KINDS, iter_nodes

KEY_ORIGINS = ("configured", "declared_primary_key", "declared_unique", "identifier_candidate")
# Atomic kinds whose values can be grouped exactly (maps, structs and variants cannot).
KEY_KINDS = ORDERABLE_KINDS
KEY_OUTCOMES = ("unique", "unique_non_null", "duplicates", "empty")
NULL_KEY_SEMANTICS = (
    "A row with NULL in any key column is counted in rows_with_null_key and excluded from the "
    "distinct and duplicate counts: NULLs are not treated as equal, as in a SQL UNIQUE "
    "constraint. A PRIMARY KEY also forbids NULLs, so it holds only when the outcome is 'unique'."
)
_KY_FLOAT_NOTE = (
    "Floating-point key columns are compared as Spark groups them: NaN equals NaN and -0.0 equals "
    "0.0."
)


def _ky_segments_of_config(columns: Sequence[Any]) -> list[list[dict[str, Any]]] | None:
    try:
        return [column_reference_segments(column) for column in columns]
    except IdentifierError:
        return None


def requested_keys(
    config: Mapping[str, Any],
    table_lookup: str,
    constraints: Sequence[Mapping[str, Any]],
    candidate_paths: Sequence[Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """Return the keys requested for one table, in priority order.

    Configured keys come first, then declared PRIMARY KEY and UNIQUE
    constraints (when ``declared_keys``), then single-column identifier
    candidates (when ``identifier_candidates``). ``candidate_paths`` are the
    typed paths of the table's identifier candidates, in schema order.
    """
    settings = config["deep"]["uniqueness"]
    out: list[dict[str, Any]] = []
    for item in settings["keys"]:
        try:
            if table_lookup_key(parse_table_identifier(item["table"])) != table_lookup:
                continue
        except IdentifierError:
            continue
        out.append(
            {
                "origin": "configured",
                "name": item.get("id"),
                "segments": _ky_segments_of_config(item["columns"]),
                "requested": [
                    column if isinstance(column, str) else ".".join(column)
                    for column in item["columns"]
                ],
            }
        )
    if settings["declared_keys"]:
        for kind, origin in (
            ("primary_key", "declared_primary_key"),
            ("unique", "declared_unique"),
        ):
            for constraint in constraints:
                if constraint.get("constraint_type") != kind:
                    continue
                columns = [str(column) for column in constraint.get("columns", [])]
                out.append(
                    {
                        "origin": origin,
                        "name": constraint.get("name"),
                        "segments": [[field_segment(column)] for column in columns] or None,
                        "requested": columns,
                    }
                )
    if settings["identifier_candidates"]:
        for path in candidate_paths:
            out.append(
                {
                    "origin": "identifier_candidate",
                    "name": None,
                    "segments": [[dict(segment) for segment in path]],
                    "requested": [display_path([dict(segment) for segment in path])],
                }
            )
    return out


def key_columns_problem(nodes: Sequence[Mapping[str, Any] | None], requested: Sequence[str]) -> str:
    """Return why these schema nodes cannot form a key ('' when they can)."""
    for node, text in zip(nodes, requested, strict=False):
        if node is None:
            return f"column {text!r} is not in the documented schema tree"
        if any(segment["kind"] != "field" for segment in node["path"]):
            return f"column {node['display_path']!r} is inside an array or map"
        if node["type"]["kind"] not in KEY_KINDS:
            return (
                f"column {node['display_path']!r} has type {node['type']['physical_type']}, which "
                "cannot be compared exactly as a key part"
            )
    ids = [node["field_id"] for node in nodes if node is not None]
    if len(ids) != len(set(ids)):
        return "a column is listed more than once"
    return ""


def plan_uniqueness(
    tree: Mapping[str, Any], requested: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> dict[str, Any]:
    """Resolve requested keys against the schema tree and apply the budget.

    Keys with the same set of columns are merged (their origins are kept).
    The first ``max_keys`` eligible keys are planned and split into at most
    ``max_passes`` passes of contiguous keys; the others are recorded as
    ``not_computed`` (budget) or ``not_eligible`` with a reason.
    """
    settings = config["deep"]["uniqueness"]
    by_id = {node["field_id"]: node for node in iter_nodes(list(tree.get("fields", [])))}
    keys: list[dict[str, Any]] = []
    index_by_set: dict[tuple[str, ...], int] = {}
    for item in requested:
        segments = item["segments"]
        if not segments:
            keys.append(
                _ky_key(item, [], [], "not_eligible", "the key lists no valid column reference")
            )
            continue
        nodes = [by_id.get(field_id(list(path))) for path in segments]
        reason = key_columns_problem(nodes, item["requested"])
        columns = [
            node["display_path"] if node is not None else text
            for node, text in zip(nodes, item["requested"], strict=False)
        ]
        ids = [node["field_id"] for node in nodes if node is not None]
        if reason:
            keys.append(_ky_key(item, columns, ids, "not_eligible", reason))
            continue
        identity = tuple(sorted(ids))
        if identity in index_by_set:
            existing = keys[index_by_set[identity]]
            if item["origin"] not in existing["origins"]:
                existing["origins"].append(item["origin"])
            if item.get("name") and item["name"] not in existing["names"]:
                existing["names"].append(item["name"])
            continue
        index_by_set[identity] = len(keys)
        keys.append(_ky_key(item, columns, ids, "planned", None))
        keys[-1]["kinds"] = [by_id[fid]["type"]["kind"] for fid in ids]
    planned = [position for position, key in enumerate(keys) if key["status"] == "planned"]
    limited: list[dict[str, str]] = []
    for position in planned[settings["max_keys"] :]:
        key = keys[position]
        key["status"] = "not_computed"
        key["reason"] = f"beyond deep.uniqueness.max_keys = {settings['max_keys']}"
        limited.append(
            {
                "item": ", ".join(key["columns"]),
                "reason": "uniqueness_budget",
                "detail": key["reason"],
            }
        )
    planned = planned[: settings["max_keys"]]
    passes: list[list[int]] = []
    if planned:
        count = min(settings["max_passes"], len(planned))
        size = -(-len(planned) // count)
        passes = [planned[start : start + size] for start in range(0, len(planned), size)]
    for number, members in enumerate(passes, start=1):
        for position in members:
            keys[position]["operation_id"] = f"op_uniqueness_{number}"
    return {"keys": keys, "passes": passes, "limited": limited}


def _ky_key(
    item: Mapping[str, Any], columns: list[str], ids: list[str], status: str, reason: str | None
) -> dict[str, Any]:
    return {
        "key_id": "k_" + short_hash(sorted(ids) or list(item["requested"])),
        "origins": [item["origin"]],
        "names": [item["name"]] if item.get("name") else [],
        "columns": columns,
        "field_ids": ids,
        "status": status,
        "reason": reason,
        "operation_id": None,
        "kinds": [],
    }


def uniqueness_metrics(raw: Mapping[str, Any], scope: str) -> tuple[list[dict[str, Any]], str]:
    """Build the metrics and the outcome of one key from the counts of its pass.

    ``raw`` holds ``rows``, ``null_rows``, ``distinct``, ``dup_groups``,
    ``dup_rows`` and ``max_n`` (largest number of rows sharing one complete key).
    """
    rows = int(raw.get("rows") or 0)
    null_rows = int(raw.get("null_rows") or 0)
    distinct = int(raw.get("distinct") or 0)
    dup_groups = int(raw.get("dup_groups") or 0)
    dup_rows = int(raw.get("dup_rows") or 0)
    complete = rows - null_rows

    def count(name: str, value: int, unit: str, method: str, **extra: Any) -> dict[str, Any]:
        source = extra.pop("source", "aggregate")
        return measured(
            name,
            value,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source=source,
            method=method,
            **extra,
        )

    metrics = [
        count("rows_in_scope", rows, "rows", "rows read by the uniqueness pass (analysed scope)"),
        count(
            "rows_with_null_key",
            null_rows,
            "rows",
            "rows with NULL in at least one key column",
            **({"denominator": rows, "denominator_unit": "rows"} if rows else {}),
        ),
        count(
            "rows_with_complete_key",
            complete,
            "rows",
            "rows_in_scope - rows_with_null_key",
            source="derived",
            **({"denominator": rows, "denominator_unit": "rows"} if rows else {}),
        ),
        count(
            "distinct_keys",
            distinct,
            "keys",
            "exact count of distinct key values among rows with a complete key (grouped "
            "aggregation)",
        ),
        count(
            "duplicate_key_groups",
            dup_groups,
            "keys",
            "key values that occur in more than one row",
            **({"denominator": distinct, "denominator_unit": "keys"} if distinct else {}),
        ),
        count(
            "rows_in_duplicate_groups",
            dup_rows,
            "rows",
            "rows whose key value occurs in more than one row",
            **({"denominator": complete, "denominator_unit": "rows"} if complete else {}),
        ),
        count(
            "surplus_duplicate_rows",
            complete - distinct,
            "rows",
            "rows_with_complete_key - distinct_keys (rows beyond the first of each key value)",
            source="derived",
            **({"denominator": complete, "denominator_unit": "rows"} if complete else {}),
        ),
    ]
    if complete and raw.get("max_n") is not None:
        metrics.append(
            count(
                "max_rows_per_key",
                int(raw["max_n"]),
                "rows",
                "largest number of rows sharing one complete key value",
            )
        )
    else:
        metrics.append(
            not_measured(
                "max_rows_per_key",
                "insufficient_data",
                "no rows with a complete key in scope",
                scope=scope,
                source="aggregate",
            )
        )
    if complete == 0:
        outcome = "empty"
    elif dup_groups:
        outcome = "duplicates"
    elif null_rows:
        outcome = "unique_non_null"
    else:
        outcome = "unique"
    return metrics, outcome


def uniqueness_record(
    plan: Mapping[str, Any],
    results: Mapping[int, Mapping[str, Any]],
    errors: Mapping[int, str],
    *,
    config: Mapping[str, Any],
    scope: str,
    requested_any: bool,
) -> dict[str, Any]:
    """Return the table's ``uniqueness`` record (contract 1.2).

    ``results`` maps key positions to raw counts; ``errors`` maps key
    positions of failed passes to a sanitized reason.
    """
    settings = config["deep"]["uniqueness"]
    keys = []
    for position, key in enumerate(plan["keys"]):
        record: dict[str, Any] = {
            "key_id": key["key_id"],
            "origins": list(key["origins"]),
            "names": list(key["names"]),
            "columns": list(key["columns"]),
            "field_ids": list(key["field_ids"]),
            "status": key["status"],
            "reason": key["reason"],
            "outcome": None,
            "scope": scope,
            "operation_id": key["operation_id"],
            "metrics": [],
            "limitations": [],
        }
        if key["status"] == "planned":
            if position in errors:
                record["status"] = "error"
                record["reason"] = errors[position]
            else:
                metrics, outcome = uniqueness_metrics(results.get(position, {}), scope)
                record.update({"status": "measured", "metrics": metrics, "outcome": outcome})
                if "float" in key["kinds"]:
                    record["limitations"].append(_KY_FLOAT_NOTE)
        keys.append(record)
    notes = []
    if not requested_any:
        notes.append(
            "No key was requested for this table (deep.uniqueness.keys, declared_keys, "
            "identifier_candidates)."
        )
    return {
        "keys": keys,
        "sources": {
            "configured": any("configured" in key["origins"] for key in plan["keys"]),
            "declared_keys": bool(settings["declared_keys"]),
            "identifier_candidates": bool(settings["identifier_candidates"]),
        },
        "budget": {"max_keys": settings["max_keys"], "max_passes": settings["max_passes"]},
        "passes": {"budget": settings["max_passes"], "planned": len(plan["passes"])},
        "limited": list(plan["limited"]),
        "null_semantics": NULL_KEY_SEMANTICS,
        "notes": notes,
    }


def measured_keys(table: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return the measured key records of a table profile (empty before contract 1.2)."""
    return [
        key
        for key in (table.get("uniqueness") or {}).get("keys", [])
        if key["status"] == "measured"
    ]
