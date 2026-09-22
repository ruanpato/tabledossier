"""Deep level, part II: referential validation and relationship hypotheses.

Part of the embedded runtime (standard library only). The Spark adapter runs
one inclusion check per relationship or hypothesis pair; this module decides
what is checked (within the budgets), which column types can be compared and
how the counts become contract records.

* **Referential validation** checks known relationships (declared FOREIGN
  KEYs, relationships in the configuration): source rows with a complete key
  whose key is absent from the target are *orphans*. Both tables are read at
  the Delta versions recorded when they were profiled. A full-scope check can
  validate or violate a relationship; a sample can only violate it.
* **Relationship hypotheses** are data-driven and kept apart from known
  relationships. Only single-column keys measured exactly unique are targets;
  source columns are chosen by type compatibility and by the value (or length)
  ranges measured at the standard level, never by their names. A hypothesis
  never gets a cardinality and is never drawn as an ER edge.

Only counts leave the engine: orphan or matching values are never collected.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from tabledossier.keys import declared_column_segments
from tabledossier.metrics import measured, metric_value, numeric_value, ratio
from tabledossier.paths import parse_display_path, table_lookup_key

HYPOTHESIS_KINDS = ("integer", "decimal", "string", "date")
TARGET_SCOPE_NOTE = (
    "The target is read in full at its recorded version (the target table's filters are not "
    "applied): a reference is valid when its key exists anywhere in the target table."
)
_IG_EXACT_NUMERIC = ("integer", "decimal")


def _ig_scale(node: Mapping[str, Any]) -> int | None:
    scale = node["type"].get("scale")
    return int(scale) if isinstance(scale, int) else None


def compatible_kinds(left: Mapping[str, Any], right: Mapping[str, Any]) -> tuple[bool, str]:
    """Return ``(compatible, rule)`` for comparing two key columns by equality.

    Same kinds compare (except structures); integers and decimals compare as
    exact numbers. Floating point only compares with floating point, and
    timestamps with and without time zone never compare.
    """
    a, b = left["type"]["kind"], right["type"]["kind"]
    if a in ("struct", "array", "map", "variant", "interval", "null", "other") or b in (
        "struct",
        "array",
        "map",
        "variant",
        "interval",
        "null",
        "other",
    ):
        return False, f"{a} and {b} values are not compared as keys"
    if a == b:
        return True, f"same type kind ({a})"
    if a in _IG_EXACT_NUMERIC and b in _IG_EXACT_NUMERIC:
        return True, "exact numbers (integer and decimal)"
    return False, f"{a} and {b} are not compared by equality (no implicit conversion)"


def type_compatibility(
    from_nodes: Sequence[Mapping[str, Any]], to_nodes: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Return one compatibility record per pair of key columns."""
    out = []
    for left, right in zip(from_nodes, to_nodes, strict=False):
        ok, rule = compatible_kinds(left, right)
        out.append(
            {
                "from_column": left["display_path"],
                "from_type": left["type"]["physical_type"],
                "to_column": right["display_path"],
                "to_type": right["type"]["physical_type"],
                "compatible": ok,
                "rule": rule,
            }
        )
    return out


def end_segments(
    relationship: Mapping[str, Any], end: str, fields: Sequence[Mapping[str, Any]] = ()
) -> list[list[dict[str, Any]]]:
    """Typed paths of one end of a relationship record.

    Declared constraints list literal top-level column names, matched to the
    top-level columns of ``fields`` (the schema tree of that end's table)
    case-insensitively when the exact name is absent; configured and annotated
    relationships list display paths.
    """
    columns = relationship[end]["columns"]
    if relationship["origin"] == "declared_constraint":
        return declared_column_segments(columns, fields)
    return [parse_display_path(column) for column in columns]


def referential_requested(relationship: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    """Return why a relationship is not requested for validation ('' when it is)."""
    if config.get("analysis_level") != "deep":
        return "referential validation runs only at the deep level"
    settings = config["deep"]["referential"]
    if relationship["origin"] == "declared_constraint" and not settings["declared"]:
        return "declared foreign keys are not validated (deep.referential.declared = false)"
    if relationship["origin"] == "configuration" and not settings["configured"]:
        return "configured relationships are not validated (deep.referential.configured = false)"
    if relationship["origin"] not in ("declared_constraint", "configuration"):
        return "only declared and configured relationships are validated"
    return ""


def not_validated_detail(reason: str, **known: Any) -> dict[str, Any]:
    """Return the validation detail of a relationship not checked against the data."""
    detail: dict[str, Any] = {
        "status": "not_validated",
        "reason": reason,
        "mode": None,
        "from": None,
        "to": None,
        "operation_id": None,
        "type_compatibility": [],
        "target_key_unique": None,
        "metrics": [],
        "limitations": [],
    }
    detail.update(known)
    return detail


def _ig_side_metrics(raw: Mapping[str, Any], scope: str, target_scope: str) -> list[dict[str, Any]]:
    rows = int(raw.get("rows") or 0)
    nulls = int(raw.get("null_rows") or 0)
    complete = rows - nulls
    t_rows = int(raw.get("t_rows") or 0)
    t_nulls = int(raw.get("t_null_rows") or 0)
    t_distinct = int(raw.get("t_distinct") or 0)

    def count(name: str, value: int, unit: str, method: str, where: str, **extra: Any) -> Any:
        source = extra.pop("source", "aggregate")
        return measured(
            name,
            value,
            unit=unit,
            scope=where,
            accuracy="exact",
            source=source,
            method=method,
            **extra,
        )

    def denominator(value: int, unit: str = "rows") -> dict[str, Any]:
        return {"denominator": value, "denominator_unit": unit} if value else {}

    return [
        count("source_rows", rows, "rows", "source rows examined", scope),
        count(
            "source_rows_with_null_key",
            nulls,
            "rows",
            "source rows with NULL in at least one key column (never orphans)",
            scope,
            **denominator(rows),
        ),
        count(
            "source_rows_with_complete_key",
            complete,
            "rows",
            "source_rows - source_rows_with_null_key",
            scope,
            source="derived",
            **denominator(rows),
        ),
        count("target_rows", t_rows, "rows", "rows of the target table read", target_scope),
        count(
            "target_rows_with_null_key",
            t_nulls,
            "rows",
            "target rows with NULL in at least one key column (never matched)",
            target_scope,
            **denominator(t_rows),
        ),
        count(
            "target_distinct_keys",
            t_distinct,
            "keys",
            "exact count of distinct complete key values in the target",
            target_scope,
        ),
        count(
            "target_duplicate_key_groups",
            int(raw.get("t_dup_groups") or 0),
            "keys",
            "target key values that occur in more than one row",
            target_scope,
            **denominator(t_distinct, "keys"),
        ),
    ]


def validation_detail(
    raw: Mapping[str, Any],
    *,
    mode: str,
    sample_rows: int | None,
    from_info: Mapping[str, Any],
    to_info: Mapping[str, Any],
    compatibility: list[dict[str, Any]],
    operation_id: str,
) -> dict[str, Any]:
    """Turn the counts of one referential check into a validation detail.

    A full-scope check with no orphan validates the relationship and any orphan
    violates it. A sample can prove a violation (an orphan of the sample is an
    orphan of the table) but never validates the whole scope.
    """
    scope = "sample" if mode == "sample" else from_info["scope"]
    metrics = _ig_side_metrics(raw, scope, to_info["scope"])
    complete = int(raw.get("rows") or 0) - int(raw.get("null_rows") or 0)
    orphans = int(raw.get("orphans") or 0)
    metrics[3:3] = [
        measured(
            "orphan_rows",
            orphans,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method="source rows with a complete key that no target row has (anti join)",
            denominator=complete or None,
            denominator_unit="rows" if complete else None,
        ),
        ratio(
            "orphan_ratio",
            orphans,
            complete,
            scope=scope,
            source="derived",
            method="orphan_rows / source_rows_with_complete_key",
            denominator_unit="rows",
        ),
    ]
    limitations = [TARGET_SCOPE_NOTE]
    reason = None
    if complete == 0:
        status = "not_validated"
        reason = "no source row with a complete key was examined"
    elif orphans:
        status = "violated"
    elif mode == "sample":
        status = "not_validated"
        reason = (
            f"no orphan in a {mode} of at most {sample_rows} source rows; a sample cannot validate "
            "the whole scope"
        )
    else:
        status = "validated"
    if mode == "sample":
        limitations.append(
            "Source rows come from a bounded sample (potentially biased for prefix samples); the "
            "counts describe the sample only."
        )
    for side in (from_info, to_info):
        if side.get("consistency_mode") != "pinned_delta_version":
            limitations.append(
                f"{side['table']} is not pinned to a Delta version; it was read in its current "
                "state."
            )
    t_dups = int(raw.get("t_dup_groups") or 0)
    return {
        "status": status,
        "reason": reason,
        "mode": mode,
        "from": dict(from_info),
        "to": dict(to_info),
        "operation_id": operation_id,
        "type_compatibility": compatibility,
        "target_key_unique": t_dups == 0 if int(raw.get("t_distinct") or 0) else None,
        "metrics": metrics,
        "limitations": limitations,
    }


def referential_summary(
    relationships: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    planned: int,
    limited: list[dict[str, str]],
) -> dict[str, Any]:
    """Run-level record of referential validation (contract 1.2)."""
    settings = config["deep"]["referential"]
    statuses = [rel["validation"] for rel in relationships]
    return {
        "requested": {"configured": settings["configured"], "declared": settings["declared"]},
        "mode": settings["mode"],
        "budget": {
            "max_relationships": settings["max_relationships"],
            "max_sample_rows": settings["max_sample_rows"],
        },
        "planned": planned,
        "validated": statuses.count("validated"),
        "violated": statuses.count("violated"),
        "not_validated": statuses.count("not_validated"),
        "limited": limited,
        "notes": [
            TARGET_SCOPE_NOTE,
            "Orphan values are never collected; only counts are recorded.",
        ],
    }


# --------------------------------------------------------------------------- hypotheses


def _ig_profile_nodes(table: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    nodes: dict[str, Mapping[str, Any]] = {}
    stack = list((table.get("schema") or {}).get("fields", []))
    while stack:
        node = stack.pop()
        nodes[node["field_id"]] = node
        stack.extend(node.get("children", []))
    return nodes


def _ig_hypothesis_kind(node: Mapping[str, Any]) -> bool:
    kind = node["type"]["kind"]
    if kind == "decimal":
        return _ig_scale(node) == 0
    return kind in HYPOTHESIS_KINDS


def _ig_range(metrics: Sequence[Mapping[str, Any]], kind: str) -> tuple[Any, Any] | None:
    if kind == "string":
        low, high = metric_value(metrics, "min_length"), metric_value(metrics, "max_length")
    elif kind == "date":
        low, high = metric_value(metrics, "min"), metric_value(metrics, "max")
    else:
        low, high = numeric_value(metrics, "min"), numeric_value(metrics, "max")
    return None if low is None or high is None else (low, high)


def _ig_compare_ranges(source: tuple[Any, Any] | None, target: tuple[Any, Any] | None) -> str:
    if source is None or target is None:
        return "not_compared"
    if source[1] < target[0] or source[0] > target[1]:
        return "disjoint"
    if target[0] <= source[0] and source[1] <= target[1]:
        return "contained"
    return "overlapping"


def plan_hypotheses(
    tables: Sequence[Mapping[str, Any]],
    known: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Choose the column pairs whose inclusion is measured, within ``max_pairs``.

    Targets are single-column keys measured exactly unique in this run.
    Sources are profiled, non-empty columns of a compatible type whose measured
    range (values for numbers and dates, lengths for strings) is not disjoint
    from the target's. Column names are never used. Pairs already known as
    relationships are skipped. Pairs whose source range lies inside the target
    range come first, then schema order.
    """
    settings = config["deep"]["relationship_hypotheses"]
    targets = []
    for t_index, table in enumerate(tables):
        nodes = _ig_profile_nodes(table)
        fields = {f["field_id"]: f for f in table.get("field_profiles", [])}
        for key in (table.get("uniqueness") or {}).get("keys", []):
            if key["status"] != "measured" or key["outcome"] not in ("unique", "unique_non_null"):
                continue
            if len(key["field_ids"]) != 1 or key["field_ids"][0] not in nodes:
                continue
            node = nodes[key["field_ids"][0]]
            if _ig_hypothesis_kind(node):
                targets.append((t_index, table, node, fields.get(node["field_id"]), key))
    counts = {"known": 0, "disjoint": 0}
    pairs = []
    for t_index, target_table, target, target_field, key in targets:
        target_range = _ig_range((target_field or {}).get("metrics", []), target["type"]["kind"])
        for s_index, table in enumerate(tables):
            nodes = _ig_profile_nodes(table)
            for field in table.get("field_profiles", []):
                if not field.get("profiled") or field.get("element_context"):
                    continue
                found = nodes.get(field["field_id"])
                if found is None or any(s["kind"] != "field" for s in found["path"]):
                    continue
                node = found
                if s_index == t_index and node["field_id"] == target["field_id"]:
                    continue
                if not _ig_hypothesis_kind(node) or not compatible_kinds(node, target)[0]:
                    continue
                if metric_value(field["metrics"], "non_null_count") == 0:
                    continue
                identity = (
                    table_lookup_key(table["identifier"]["parts"]),
                    (node["field_id"],),
                    table_lookup_key(target_table["identifier"]["parts"]),
                    (target["field_id"],),
                )
                if identity in known:
                    counts["known"] += 1
                    continue
                relation = _ig_compare_ranges(
                    _ig_range(field["metrics"], node["type"]["kind"]), target_range
                )
                if relation == "disjoint":
                    counts["disjoint"] += 1
                    continue
                pairs.append(
                    {
                        "from_table_index": s_index,
                        "from_node": node,
                        "to_table_index": t_index,
                        "to_node": target,
                        "target_key_id": key["key_id"],
                        "range_relation": relation,
                        "range_basis": "lengths" if node["type"]["kind"] == "string" else "values",
                    }
                )
    ranked = sorted(pairs, key=lambda pair: 0 if pair["range_relation"] == "contained" else 1)
    selected = ranked[: settings["max_pairs"]]
    return {
        "targets": len(targets),
        "pairs": selected,
        "considered": len(pairs),
        "not_evaluated": len(pairs) - len(selected),
        "known_excluded": counts["known"],
        "disjoint_excluded": counts["disjoint"],
    }


def hypothesis_evidence(
    raw: Mapping[str, Any],
    *,
    scope: str,
    target_scope: str,
) -> tuple[list[dict[str, Any]], float | None]:
    """Inclusion metrics of one evaluated pair and the inclusion ratio (None if undefined)."""
    metrics = _ig_side_metrics(raw, scope, target_scope)
    complete = int(raw.get("rows") or 0) - int(raw.get("null_rows") or 0)
    included = complete - int(raw.get("orphans") or 0)
    inclusion = ratio(
        "inclusion_ratio",
        included,
        complete,
        scope=scope,
        source="derived",
        method="included_rows / source_rows_with_complete_key",
        denominator_unit="rows",
    )
    metrics[3:3] = [
        measured(
            "included_rows",
            included,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method="source rows with a complete key found among the target key values",
            denominator=complete or None,
            denominator_unit="rows" if complete else None,
        ),
        inclusion,
    ]
    value = inclusion["value"] if inclusion["status"] == "measured" else None
    return metrics, value


HYPOTHESIS_LIMITATIONS = (
    "Inclusion shows that source values occur among the target key values; it does not prove "
    "that the columns mean the same thing.",
    "No cardinality is asserted and the hypothesis is never drawn in the ER diagram; declare the "
    "relationship in the configuration once a person confirms it.",
)


def hypothesis_item(
    number: int,
    pair: Mapping[str, Any],
    *,
    source_table: Mapping[str, Any],
    target_table: Mapping[str, Any],
    evidence: Mapping[str, Any],
    operation_id: str,
    sample_rows: int | None,
) -> dict[str, Any]:
    """Return one hypothesis record (always ``status: hypothesis``, never a cardinality)."""
    limitations = list(HYPOTHESIS_LIMITATIONS)
    if sample_rows is not None:
        limitations.append(
            f"Inclusion was measured on at most {sample_rows} source rows (a bounded sample); it "
            "is not extrapolated to the table."
        )
    return {
        "hypothesis_id": f"hyp_{number}",
        "status": "hypothesis",
        "from": {
            "table": source_table["table_key"],
            "columns": [pair["from_node"]["display_path"]],
        },
        "to": {"table": target_table["table_key"], "columns": [pair["to_node"]["display_path"]]},
        "cardinality": None,
        "evidence": dict(evidence),
        "operation_id": operation_id,
        "limitations": limitations,
    }


def hypotheses_record(
    config: Mapping[str, Any],
    plan: Mapping[str, Any] | None,
    hypotheses: list[dict[str, Any]],
    *,
    evaluated: int,
    rejected: Mapping[str, int],
    reason: str | None,
) -> dict[str, Any]:
    """Run-level record of relationship hypotheses (contract 1.2)."""
    settings = config["deep"]["relationship_hypotheses"]
    return {
        "enabled": bool(settings["enabled"]),
        "reason": reason,
        "budget": {
            "max_pairs": settings["max_pairs"],
            "max_sample_rows": settings["max_sample_rows"],
            "inclusion_scope": settings["inclusion_scope"],
            "min_inclusion_ratio": settings["min_inclusion_ratio"],
        },
        "targets": plan["targets"] if plan else 0,
        "pairs_considered": plan["considered"] if plan else 0,
        "pairs_evaluated": evaluated,
        "pairs_not_evaluated": plan["not_evaluated"] if plan else 0,
        "pairs_known_excluded": plan["known_excluded"] if plan else 0,
        "pairs_disjoint_excluded": plan["disjoint_excluded"] if plan else 0,
        "pairs_rejected": dict(rejected),
        "hypotheses": hypotheses,
        "method": (
            "Targets: single-column keys measured exactly unique in this run. Sources: profiled "
            "columns of a compatible type whose measured value or length range is not disjoint "
            "from the target's; column names are not used. Each pair is one inclusion check "
            "(left join against the distinct target keys)."
        ),
        "limitations": [
            "Only single-column keys measured exactly unique are targets; composite relationships "
            "are not hypothesized.",
            "Pairs beyond max_pairs and pairs whose measured ranges are disjoint are not measured: "
            "a missing hypothesis is not evidence that no relationship exists.",
        ],
    }
