"""Engine-neutral planning: schema tree, field selection, sample and aggregate plans.

Part of the embedded runtime (standard library only).

The planner only *describes* work as declarative specs (``op`` names with
parameters). An engine adapter (``tabledossier.runtime.spark``) compiles the
specs into expressions and performs the actions. Adding metrics therefore adds
expressions to a bounded number of shared aggregation passes; it never adds one
Spark action per column. A single aggregation call is an organizational goal,
not a promise of a single physical scan or of a cost similar to ``count(*)``.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from tabledossier.metrics import not_measured
from tabledossier.paths import display_path, field_id, field_segment

NUMERIC_KINDS = ("integer", "float", "decimal")
TEMPORAL_KINDS = ("date", "timestamp", "timestamp_ntz")
ORDERABLE_KINDS = (*NUMERIC_KINDS, "string", "boolean", *TEMPORAL_KINDS, "binary")
EXTREME_METRICS = ("min", "max", "mean", "stddev", "quantiles")

# (metric name, op, tier, expression cost). Lower tiers are kept first when the
# expression budget is exceeded. Tier 0 is reserved for the table row count.
_PL_COMMON = [("null_count", "count_null", 1, 1)]
_PL_ORDERABLE = [
    ("approx_distinct_count", "approx_distinct", 3, 1),
    ("all_values_equal", "all_values_equal", 3, 2),
]
_PL_NUMERIC = [
    ("min", "min_value", 2, 1),
    ("max", "max_value", 2, 1),
    ("mean", "mean_value", 2, 1),
    ("zero_count", "count_zero", 2, 1),
    ("negative_count", "count_negative", 2, 1),
    ("positive_count", "count_positive", 2, 1),
    ("stddev", "stddev_value", 4, 1),
    ("quantiles", "quantiles", 4, 1),
]
_PL_FLOAT_EXTRA = [
    ("nan_count", "count_nan", 2, 1),
    ("positive_infinity_count", "count_pos_inf", 2, 1),
    ("negative_infinity_count", "count_neg_inf", 2, 1),
    ("finite_count", "count_finite", 2, 1),
]
_PL_STRING = [
    ("empty_count", "count_empty_string", 2, 1),
    ("min_length", "min_length", 2, 1),
    ("max_length", "max_length", 2, 1),
    ("mean_length", "mean_length", 2, 1),
    ("whitespace_only_count", "count_whitespace_only", 4, 1),
    ("length_quantiles", "length_quantiles", 4, 1),
]
_PL_BOOLEAN = [("true_count", "count_true", 2, 1), ("false_count", "count_false", 2, 1)]
_PL_TEMPORAL = [("min", "min_value", 2, 1), ("max", "max_value", 2, 1)]
_PL_BINARY = [("min_length", "min_length", 2, 1), ("max_length", "max_length", 2, 1)]
_PL_ARRAY = [
    ("empty_count", "count_empty_collection", 2, 1),
    ("min_size", "min_size", 2, 1),
    ("max_size", "max_size", 2, 1),
    ("mean_size", "mean_size", 2, 1),
    ("total_element_count", "sum_size", 2, 1),
    ("null_element_count", "count_null_elements", 3, 1),
]
_PL_MAP = [
    ("empty_count", "count_empty_collection", 2, 1),
    ("min_size", "min_size", 2, 1),
    ("max_size", "max_size", 2, 1),
    ("mean_size", "mean_size", 2, 1),
    ("total_entry_count", "sum_size", 2, 1),
    ("null_value_count", "count_null_map_values", 3, 1),
]


def scope_label(pinned: bool, filtered: bool) -> str:
    """Return the metric scope for a population (snapshot pinned or not, filtered or not)."""
    if pinned:
        return "filtered_snapshot" if filtered else "full_snapshot"
    return "filtered_table" if filtered else "full_table"


def _pl_shallow_type(ntype: Mapping[str, Any]) -> dict[str, Any]:
    shallow = {"kind": ntype["kind"], "physical_type": ntype.get("physical_type", ntype["kind"])}
    for key in ("precision", "scale"):
        if key in ntype:
            shallow[key] = ntype[key]
    return shallow


def _pl_children(ntype: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return child descriptors ``(segment, type, nullable, comment)`` of a type."""
    kind = ntype["kind"]
    if kind == "struct":
        return [
            {
                "segment": field_segment(child["name"]),
                "type": child["type"],
                "nullable": child.get("nullable"),
                "comment": child.get("comment"),
            }
            for child in ntype.get("fields", [])
        ]
    if kind == "array":
        return [
            {
                "segment": {"kind": "array_element"},
                "type": ntype["element_type"],
                "nullable": ntype.get("contains_null"),
                "comment": None,
            }
        ]
    if kind == "map":
        return [
            {
                "segment": {"kind": "map_key"},
                "type": ntype["key_type"],
                "nullable": False,
                "comment": None,
            },
            {
                "segment": {"kind": "map_value"},
                "type": ntype["value_type"],
                "nullable": ntype.get("value_contains_null"),
                "comment": None,
            },
        ]
    return []


def _pl_make_node(
    parent: Mapping[str, Any] | None, descriptor: Mapping[str, Any], depth: int
) -> dict[str, Any]:
    path = [*(parent["path"] if parent else []), descriptor["segment"]]
    segment = descriptor["segment"]
    return {
        "field_id": field_id(path),
        "name": segment.get("name"),
        "segment_kind": segment["kind"],
        "path": path,
        "display_path": display_path(path),
        "parent_field_id": parent["field_id"] if parent else None,
        "depth": depth,
        "type": _pl_shallow_type(descriptor["type"]),
        "nullable": descriptor.get("nullable"),
        "comment": descriptor.get("comment"),
        "children": [],
        "children_omitted": 0,
        "children_omitted_reason": None,
        "_ntype": descriptor["type"],
    }


def build_schema_tree(
    fields: Sequence[Mapping[str, Any]], *, max_depth: int, max_fields: int
) -> dict[str, Any]:
    """Build the documented schema tree breadth-first within depth and field limits.

    ``fields`` are neutral top-level field descriptors
    (``name``/``type``/``nullable``/``comment``) produced by an engine adapter.
    Breadth-first order keeps every top-level column before nested fields when
    the field budget is reached.
    """
    top: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = []
    count = 0
    omitted = 0
    top_omitted = 0
    for field in fields:
        if count >= max_fields:
            top_omitted += 1
            omitted += 1
            continue
        descriptor = {
            "segment": field_segment(field["name"]),
            "type": field["type"],
            "nullable": field.get("nullable"),
            "comment": field.get("comment"),
        }
        node = _pl_make_node(None, descriptor, 1)
        top.append(node)
        queue.append(node)
        count += 1
    position = 0
    while position < len(queue):
        node = queue[position]
        position += 1
        children = _pl_children(node["_ntype"])
        if not children:
            continue
        if node["depth"] >= max_depth:
            node["children_omitted"] = len(children)
            node["children_omitted_reason"] = "max_depth"
            omitted += len(children)
            continue
        for descriptor in children:
            if count >= max_fields:
                node["children_omitted"] += 1
                node["children_omitted_reason"] = "max_fields"
                omitted += 1
                continue
            child = _pl_make_node(node, descriptor, node["depth"] + 1)
            node["children"].append(child)
            queue.append(child)
            count += 1
    for node in queue:
        node.pop("_ntype", None)
    return {
        "fields": top,
        "nodes_total": count,
        "nodes_omitted": omitted,
        "top_level_omitted": top_omitted,
        "limits": {"max_depth": max_depth, "max_fields": max_fields},
    }


def iter_nodes(nodes: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Yield schema tree nodes depth-first in schema order."""
    for node in nodes:
        yield node
        yield from iter_nodes(node["children"])


def _pl_in_collection(node: Mapping[str, Any]) -> bool:
    return any(segment["kind"] != "field" for segment in node["path"])


def select_profile_fields(
    tree: Mapping[str, Any], selected_columns: list[str] | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(profiled_nodes, omissions)`` for the standard level.

    Fields inside array elements or map entries are documented in the schema
    tree but not profiled (element-level profiling belongs to ``deep``).
    """
    wanted = None if selected_columns is None else {name for name in selected_columns}
    profiled: list[dict[str, Any]] = []
    omissions: list[dict[str, Any]] = []
    for node in iter_nodes(tree["fields"]):
        top_name = node["path"][0]["name"]
        if wanted is not None and top_name not in wanted:
            if node["depth"] == 1:
                omissions.append(
                    _pl_omission(node, "not_selected", "column not in the configured selection")
                )
            continue
        if _pl_in_collection(node):
            parent_segment_kinds = [s["kind"] for s in node["path"][:-1]]
            if all(kind == "field" for kind in parent_segment_kinds):
                omissions.append(
                    _pl_omission(
                        node,
                        "inside_collection",
                        "array elements and map entries are profiled at collection level only; "
                        "element-level profiling is planned for the 'deep' level",
                    )
                )
            continue
        profiled.append(node)
        if node["children_omitted"]:
            omissions.append(
                _pl_omission(
                    node,
                    node["children_omitted_reason"] or "limit",
                    f"{node['children_omitted']} child field(s) not documented or profiled "
                    f"({node['children_omitted_reason']} limit)",
                )
            )
    if tree.get("top_level_omitted"):
        omissions.append(
            {
                "field_id": None,
                "display_path": None,
                "reason": "max_fields",
                "detail": (
                    f"{tree['top_level_omitted']} top-level column(s) beyond limits.max_fields"
                ),
                "metrics": [],
            }
        )
    return profiled, omissions


def _pl_omission(node: Mapping[str, Any], reason: str, detail: str) -> dict[str, Any]:
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "reason": reason,
        "detail": detail,
        "metrics": [],
    }


def plan_sample(profiled: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> dict[str, Any]:
    """Plan the transient sample used for semantic and JSON inference (strings only)."""
    sampling = config["sampling"]
    strings = [node for node in profiled if node["type"]["kind"] == "string"]
    plan: dict[str, Any] = {
        "enabled": False,
        "method": sampling["method"],
        "max_rows": sampling["max_rows"],
        "max_bytes": sampling["max_bytes"],
        "max_value_chars": sampling["max_value_chars"],
        "random_fraction": sampling.get("random_fraction"),
        "seed": sampling.get("seed"),
        "field_ids": [],
        "omitted_field_ids": [],
        "reason": None,
    }
    if config["analysis_level"] != "standard":
        plan["reason"] = "the metadata level does not read table rows"
    elif sampling["method"] == "none":
        plan["reason"] = "sampling disabled by configuration (sampling.method = none)"
    elif not strings:
        plan["reason"] = "no string fields to inspect"
    else:
        plan["enabled"] = True
        limit = sampling["max_columns"]
        plan["field_ids"] = [node["field_id"] for node in strings[:limit]]
        plan["omitted_field_ids"] = [node["field_id"] for node in strings[limit:]]
    return plan


def _pl_catalog(kind: str) -> list[tuple[str, str, int, int]]:
    specs = list(_PL_COMMON)
    if kind in ORDERABLE_KINDS:
        specs += _PL_ORDERABLE
    if kind == "float":
        specs += _PL_FLOAT_EXTRA
    if kind in NUMERIC_KINDS:
        specs += _PL_NUMERIC
    elif kind == "string":
        specs += _PL_STRING
    elif kind == "boolean":
        specs += _PL_BOOLEAN
    elif kind in TEMPORAL_KINDS:
        specs += _PL_TEMPORAL
    elif kind == "binary":
        specs += _PL_BINARY
    elif kind == "array":
        specs += _PL_ARRAY
    elif kind == "map":
        specs += _PL_MAP
    return specs


def plan_aggregates(
    profiled: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any],
    capabilities: Mapping[str, bool],
    scope: str,
    json_field_ids: set[str] | frozenset[str] = frozenset(),
    redacted_field_ids: set[str] | frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Plan shared aggregation passes for the ``standard`` level.

    Returns a dict with ``specs`` (alias, metric, op, field, tier, cost,
    params), ``passes`` (lists of aliases), ``static_metrics`` (metrics that
    are known up front not to be computed, with reasons) and ``omissions``.
    """
    metrics_cfg = config["metrics"]
    limits = config["limits"]
    by_id = {node["field_id"]: node for node in profiled}
    candidates: list[dict[str, Any]] = [
        {
            "metric": "row_count",
            "op": "count_all",
            "field_id": None,
            "tier": 0,
            "cost": 1,
            "params": {},
        }
    ]
    static: list[dict[str, Any]] = []
    for node in profiled:
        kind = node["type"]["kind"]
        fid = node["field_id"]
        for metric, op, tier, cost in _pl_catalog(kind):
            params: dict[str, Any] = {}
            if metric in EXTREME_METRICS and fid in redacted_field_ids:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            metric,
                            "redacted",
                            "value policy redacts extremes and distribution values for this field",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
                continue
            if op in ("quantiles", "length_quantiles"):
                params = {
                    "probabilities": list(metrics_cfg["quantiles"]),
                    "accuracy": metrics_cfg["quantile_accuracy"],
                }
                if not params["probabilities"]:
                    continue
            if op == "approx_distinct":
                params = {"rsd": metrics_cfg["approx_distinct_rsd"]}
            candidates.append(
                {
                    "metric": metric,
                    "op": op,
                    "field_id": fid,
                    "tier": tier,
                    "cost": cost,
                    "params": params,
                }
            )
        parent = by_id.get(node["parent_field_id"] or "")
        if parent is not None and parent["type"]["kind"] == "struct":
            candidates.append(
                {
                    "metric": "null_count_parent_present",
                    "op": "count_null_parent_present",
                    "field_id": fid,
                    "tier": 1,
                    "cost": 1,
                    "params": {"parent_field_id": parent["field_id"]},
                }
            )
        if kind in ("date", "timestamp"):
            candidates.append(
                {
                    "metric": "after_reference_count",
                    "op": "count_after_reference",
                    "field_id": fid,
                    "tier": 3,
                    "cost": 1,
                    "params": {},
                }
            )
        elif kind == "timestamp_ntz":
            static.append(
                {
                    "field_id": fid,
                    "metric": not_measured(
                        "after_reference_count",
                        "not_computed",
                        "timezone-less timestamps have no unambiguous reference instant",
                        scope=scope,
                        source="aggregate",
                    ),
                }
            )
        if kind == "string" and fid in json_field_ids:
            if not metrics_cfg.get("json_full_scope_validation", True):
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            "json_invalid_count",
                            "not_computed",
                            "full-scope JSON validation disabled by configuration",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
            elif capabilities.get("try_parse_json"):
                candidates.append(
                    {
                        "metric": "json_invalid_count",
                        "op": "count_json_invalid",
                        "field_id": fid,
                        "tier": 3,
                        "cost": 1,
                        "params": {},
                    }
                )
            else:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            "json_invalid_count",
                            "unsupported",
                            "try_parse_json is not available in this runtime; "
                            "JSON validity is reported for the sample only",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )

    capacity = limits["max_expressions_per_pass"] * limits["max_aggregate_passes"]
    kept, dropped = _pl_apply_budget(candidates, capacity)
    omissions: dict[str, dict[str, Any]] = {}
    for spec in dropped:
        fid = spec["field_id"]
        node = by_id[fid]
        entry = omissions.setdefault(
            fid,
            {
                "field_id": fid,
                "display_path": node["display_path"],
                "reason": "expression_budget",
                "detail": (
                    f"aggregate expression budget of {capacity} "
                    f"({limits['max_expressions_per_pass']} per pass x "
                    f"{limits['max_aggregate_passes']} passes) exceeded"
                ),
                "metrics": [],
            },
        )
        entry["metrics"].append(spec["metric"])
        static.append(
            {
                "field_id": fid,
                "metric": not_measured(
                    spec["metric"],
                    "not_computed",
                    "omitted to respect the aggregate expression budget",
                    scope=scope,
                    source="aggregate",
                ),
            }
        )
    for index, spec in enumerate(kept):
        spec["alias"] = f"a{index:04d}"
    passes: list[list[str]] = []
    current: list[str] = []
    used = 0
    for spec in kept:
        if current and used + spec["cost"] > limits["max_expressions_per_pass"]:
            passes.append(current)
            current, used = [], 0
        current.append(spec["alias"])
        used += spec["cost"]
    if current:
        passes.append(current)
    return {
        "specs": kept,
        "passes": passes,
        "static_metrics": static,
        "omissions": list(omissions.values()),
        "expression_count": sum(spec["cost"] for spec in kept),
        "expression_capacity": capacity,
    }


def _pl_apply_budget(
    candidates: list[dict[str, Any]], capacity: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    total = sum(spec["cost"] for spec in candidates)
    if total <= capacity:
        return candidates, []
    dropped_ids: set[int] = set()
    for tier in (4, 3, 2, 1):
        for index in range(len(candidates) - 1, -1, -1):
            if total <= capacity:
                break
            spec = candidates[index]
            if spec["tier"] == tier:
                dropped_ids.add(index)
                total -= spec["cost"]
        if total <= capacity:
            break
    kept = [spec for index, spec in enumerate(candidates) if index not in dropped_ids]
    dropped = [spec for index, spec in enumerate(candidates) if index in dropped_ids]
    return kept, dropped


def operation(
    op_id: str, kind: str, description: str, *, reads_user_data: bool, **details: Any
) -> dict[str, Any]:
    """Return a planned-operation record (what will be asked of the engine)."""
    record: dict[str, Any] = {
        "operation_id": op_id,
        "kind": kind,
        "description": description,
        "reads_user_data": reads_user_data,
    }
    if details:
        record["details"] = details
    return record
