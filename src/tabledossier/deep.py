"""Deep level, part I: elements of arrays and maps, and JSON paths of string fields.

Part of the embedded runtime (standard library only). Like ``planning``, this
module only *describes* work; the Spark adapter compiles and executes it.

``deep`` is ``standard`` plus opt-in operations, each bounded by the ``deep``
section of the configuration and declared in ``operations.planned``:

* **Element metrics** (``items[]``, ``items[].sku``, ``attrs{key}``,
  ``attrs{value}``) are per-row aggregations with higher-order functions
  (``filter``, ``transform``, ``array_min``...) summed over rows. They join the
  shared aggregation passes (lowest priority), are exact over the analysed
  scope and never use ``explode``. Their denominators are elements or entries,
  never rows.
* **Element distinct counts** need one row per element, so they run in one
  separate pass that explodes a bounded sample (or the full scope when its
  measured size fits the element budget). Scope and accuracy say so.
* **JSON paths** of string fields are catalogued from the transient sample
  already collected by ``standard`` (presence, types, heterogeneity), then
  optionally validated over the full scope with engine functions detected at
  run time. A sample catalogue is never presented as a complete schema.

Increasing metrics adds expressions to bounded passes; it never adds one Spark
action per column.
"""

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from tabledossier.config import DEEP_ALL_TARGETS
from tabledossier.metrics import measured, not_measured
from tabledossier.paths import (
    IdentifierError,
    field_id,
    parse_display_path,
    parse_table_identifier,
    table_lookup_key,
)
from tabledossier.planning import NUMERIC_KINDS, ORDERABLE_KINDS, TEMPORAL_KINDS, iter_nodes
from tabledossier.semantic import parse_json_text, sample_limitations

COLLECTION_KINDS = ("array", "map")
DEEP_OPERATION_KINDS = ("deep_aggregate_pass", "element_explode_pass")
JSON_TYPE_PATTERNS = {
    "object": "^OBJECT",
    "array": "^ARRAY",
    "string": "^STRING$",
    "boolean": "^BOOLEAN$",
    "number": "^(TINYINT|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DECIMAL)",
    "null": "^VOID$",
}
_DP_KEY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-. ]{0,63}$")
# A key is listed by name only when it occurs in at least this share of the sampled
# documents that contain its object (and in at least two documents).
JSON_MIN_KEY_PRESENCE = 0.1
_DP_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DP_UNQUOTABLE = re.compile(r"['\\\[\]]")

# (metric, op, tier, cost) per element kind; evaluated per row with higher-order functions.
# Deep tiers are 5-7: deep candidates are always dropped before any standard metric (tiers 0-4).
_DP_COMMON = [("null_count", "el_count_null", 5, 1)]
_DP_NUMERIC = [
    ("min", "el_min", 6, 1),
    ("max", "el_max", 6, 1),
    ("zero_count", "el_count_zero", 6, 1),
    ("negative_count", "el_count_negative", 6, 1),
    ("positive_count", "el_count_positive", 6, 1),
]
_DP_FLOAT_EXTRA = [
    ("nan_count", "el_count_nan", 6, 1),
    ("positive_infinity_count", "el_count_pos_inf", 6, 1),
    ("negative_infinity_count", "el_count_neg_inf", 6, 1),
    ("finite_count", "el_count_finite", 5, 1),
]
_DP_STRING = [
    ("empty_count", "el_count_empty", 6, 1),
    ("min_length", "el_min_length", 6, 1),
    ("max_length", "el_max_length", 6, 1),
    ("whitespace_only_count", "el_count_whitespace_only", 7, 1),
]
_DP_BOOLEAN = [("true_count", "el_count_true", 6, 1), ("false_count", "el_count_false", 6, 1)]
_DP_TEMPORAL = [("min", "el_min", 6, 1), ("max", "el_max", 6, 1)]
_DP_BINARY = [("min_length", "el_min_length", 6, 1), ("max_length", "el_max_length", 6, 1)]
DEEP_EXTREME_METRICS = ("min", "max")


def collection_unit(kind: str) -> str:
    """Denominator unit of a collection kind: ``elements`` (arrays) or ``entries`` (maps)."""
    return "entries" if kind == "map" else "elements"


# --------------------------------------------------------------------------- targets


def _dp_requested(config: Mapping[str, Any], table_lookup: str) -> list[str] | None:
    targets = config["deep"]["targets"]
    if targets == DEEP_ALL_TARGETS:
        return None
    requested = []
    for item in targets:
        try:
            if table_lookup_key(parse_table_identifier(item["table"])) == table_lookup:
                requested.append(item["column"])
        except IdentifierError:
            continue
    return requested


def _dp_collection_segments(path: Sequence[Mapping[str, Any]]) -> list[int]:
    return [index for index, segment in enumerate(path) if segment["kind"] != "field"]


def _dp_omission(node: Mapping[str, Any], reason: str, detail: str) -> dict[str, Any]:
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "reason": reason,
        "detail": detail,
        "metrics": [],
    }


def plan_deep_elements(
    tree: Mapping[str, Any],
    profiled: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    table_lookup: str,
) -> dict[str, Any]:
    """Select deep targets of one table and the element nodes to profile.

    Returns ``mode``, ``requested`` display paths (explicit mode), the
    targeted ``collections``, explicitly requested ``json_nodes`` (``None`` in
    ``all_within_budget`` mode, where probable-JSON fields are chosen after the
    sample), ``not_eligible`` targets, ``element_nodes`` with their
    ``contexts``, and the ``omissions`` that replace ``inside_collection``.
    """
    deep = config["deep"]
    requested = _dp_requested(config, table_lookup)
    nodes = list(iter_nodes(list(tree["fields"])))
    by_id = {node["field_id"]: node for node in nodes}
    profiled_ids = {node["field_id"] for node in profiled}
    collections: list[dict[str, Any]] = []
    json_nodes: list[dict[str, Any]] | None = None
    not_eligible: list[dict[str, str]] = []
    if requested is None:
        if deep["collections"]:
            collections = [dict(n) for n in profiled if n["type"]["kind"] in COLLECTION_KINDS]
    else:
        json_nodes = []
        for text in requested:
            try:
                fid = field_id(parse_display_path(text))
            except IdentifierError:
                not_eligible.append({"display_path": text, "reason": "invalid display path"})
                continue
            found = by_id.get(fid)
            kind = found["type"]["kind"] if found else None
            if found is None:
                reason = "not found in the documented schema tree"
            elif fid not in profiled_ids:
                reason = "not profiled at the standard level (not selected, inside a collection "
                reason += "or beyond limits)"
            elif kind in COLLECTION_KINDS and not deep["collections"]:
                reason = "element profiling is disabled (deep.collections = false)"
            elif kind == "string" and not deep["json_paths"]:
                reason = "JSON path profiling is disabled (deep.json_paths = false)"
            elif kind not in (*COLLECTION_KINDS, "string"):
                reason = f"deep targets must be arrays, maps or JSON strings, not {kind}"
            else:
                reason = ""
            if reason or found is None:
                not_eligible.append({"display_path": text, "reason": reason})
            elif kind in COLLECTION_KINDS:
                if all(item["field_id"] != fid for item in collections):
                    collections.append(dict(found))
            elif all(item["field_id"] != fid for item in json_nodes):
                json_nodes.append(dict(found))
    targeted = {node["field_id"] for node in collections}
    element_nodes: list[dict[str, Any]] = []
    contexts: dict[str, dict[str, Any]] = {}
    omissions: list[dict[str, Any]] = []
    for node in nodes:
        positions = _dp_collection_segments(node["path"])
        if not positions:
            continue
        owner_path = node["path"][: positions[0]]
        owner = by_id.get(field_id(owner_path))
        if owner is None or owner["field_id"] not in profiled_ids:
            continue
        direct_child = len(positions) == 1 and positions[0] == len(node["path"]) - 1
        if owner["field_id"] not in targeted:
            if direct_child:
                detail = (
                    "not listed in deep.targets"
                    if requested is not None
                    else "element profiling is disabled (deep.collections = false)"
                )
                omissions.append(_dp_omission(node, "deep_not_selected", detail))
            continue
        if len(positions) > 1:
            parent = by_id.get(node["parent_field_id"] or "")
            if parent is not None and len(_dp_collection_segments(parent["path"])) == 1:
                omissions.append(
                    _dp_omission(
                        node,
                        "nested_collection",
                        "elements of collections nested inside collections are not profiled "
                        "in this release",
                    )
                )
            continue
        segment = node["path"][positions[0]]["kind"]
        inner = [s["name"] for s in node["path"][positions[0] + 1 :]]
        contexts[node["field_id"]] = {
            "collection_field_id": owner["field_id"],
            "collection_display_path": owner["display_path"],
            "collection_kind": owner["type"]["kind"],
            "segment": segment,
            "inner": inner,
            "unit": collection_unit(owner["type"]["kind"]),
        }
        element_nodes.append(node)
    return {
        "mode": "explicit" if requested is not None else DEEP_ALL_TARGETS,
        "requested": requested,
        "collections": collections,
        "json_nodes": json_nodes,
        "not_eligible": not_eligible,
        "element_nodes": element_nodes,
        "contexts": contexts,
        "omissions": omissions,
    }


# --------------------------------------------------------------------------- element specs


def _dp_catalog(kind: str) -> list[tuple[str, str, int, int]]:
    specs = list(_DP_COMMON)
    if kind == "float":
        specs += _DP_FLOAT_EXTRA
    if kind in NUMERIC_KINDS:
        specs += _DP_NUMERIC
    elif kind == "string":
        specs += _DP_STRING
    elif kind == "boolean":
        specs += _DP_BOOLEAN
    elif kind in TEMPORAL_KINDS:
        specs += _DP_TEMPORAL
    elif kind == "binary":
        specs += _DP_BINARY
    return specs


def plan_element_specs(
    element_nodes: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    *,
    scope: str,
    redacted_field_ids: set[str] | frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(candidates, static_metrics)`` for element nodes.

    Candidates use the aggregate spec shape of ``planning.plan_aggregates``
    (deep tiers, dropped before any standard metric). A node is redacted when
    it, or its collection, is redacted by the value policy.
    """
    candidates: list[dict[str, Any]] = []
    static: list[dict[str, Any]] = []
    for node in element_nodes:
        fid = node["field_id"]
        context = contexts[fid]
        kind = node["type"]["kind"]
        base = {
            "collection_field_id": context["collection_field_id"],
            "segment": context["segment"],
            "inner": list(context["inner"]),
        }
        redacted = fid in redacted_field_ids or (
            context["collection_field_id"] in redacted_field_ids
        )
        for metric, op, tier, cost in _dp_catalog(kind):
            if metric in DEEP_EXTREME_METRICS and redacted:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            metric,
                            "redacted",
                            "value policy redacts extremes for this collection",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
                continue
            candidates.append(
                {
                    "metric": metric,
                    "op": op,
                    "field_id": fid,
                    "tier": tier,
                    "cost": cost,
                    "params": dict(base),
                    "group": "element",
                }
            )
        if context["inner"]:
            candidates.append(
                {
                    "metric": "null_count_parent_present",
                    "op": "el_count_null_parent_present",
                    "field_id": fid,
                    "tier": 5,
                    "cost": 1,
                    "params": {**base, "parent_inner": list(context["inner"][:-1])},
                    "group": "element",
                }
            )
        if kind in ("date", "timestamp"):
            candidates.append(
                {
                    "metric": "after_reference_count",
                    "op": "el_count_after_reference",
                    "field_id": fid,
                    "tier": 7,
                    "cost": 1,
                    "params": dict(base),
                    "group": "element",
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
    return candidates, static


def plan_element_distinct(
    element_nodes: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Plan the single explode pass computing distinct counts of element values."""
    deep = config["deep"]
    mode = deep["element_distinct"]
    leaves = [
        {
            "field_id": node["field_id"],
            "display_path": node["display_path"],
            **{key: contexts[node["field_id"]][key] for key in ("collection_field_id", "segment")},
            "inner": list(contexts[node["field_id"]]["inner"]),
            "unit": contexts[node["field_id"]]["unit"],
        }
        for node in element_nodes
        if node["type"]["kind"] in ORDERABLE_KINDS
    ]
    plan: dict[str, Any] = {
        "enabled": False,
        "mode": mode,
        "leaves": leaves,
        "max_rows": deep["max_explode_rows"],
        "max_elements": deep["max_elements"],
        "sampling_method": config["sampling"]["method"],
        "random_fraction": config["sampling"].get("random_fraction"),
        "seed": config["sampling"].get("seed"),
        "reason": None,
    }
    if mode == "off":
        plan["reason"] = "disabled by configuration (deep.element_distinct = off)"
    elif not leaves:
        plan["reason"] = "no atomic element fields to count"
    elif mode == "sample" and config["sampling"]["method"] == "none":
        plan["reason"] = "sampling disabled by configuration (sampling.method = none)"
    else:
        plan["enabled"] = True
    return plan


# --------------------------------------------------------------------------- JSON paths


def _dp_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def json_path_text(segments: Sequence[Mapping[str, Any]]) -> str:
    """Display form of a JSON path: ``$.a.b``, ``$["a.b"]``, ``$.items[*]``, ``$.attrs.*``."""
    out = "$"
    for segment in segments:
        kind = segment["kind"]
        if kind == "key":
            name = segment["name"]
            out += f".{name}" if _DP_IDENTIFIER.match(name) else f"[{json.dumps(name)}]"
        elif kind == "items":
            out += "[*]"
        else:
            out += ".*"
    return out


def engine_json_path(segments: Sequence[Mapping[str, Any]]) -> str | None:
    """Path for ``get_json_object``/``variant_get``, or None when it cannot be expressed.

    Only object keys are supported (no wildcards); keys that are not simple
    identifiers use ``['key']`` and must not contain quotes, brackets or
    backslashes.
    """
    out = "$"
    for segment in segments:
        if segment["kind"] != "key":
            return None
        name = segment["name"]
        if _DP_IDENTIFIER.match(name):
            out += f".{name}"
        elif _DP_UNQUOTABLE.search(name):
            return None
        else:
            out += f"['{name}']"
    return out


def _dp_key_policy(
    items: Sequence[tuple[int, Any]], max_keys: int
) -> tuple[bool, str | None, set[str]]:
    """Decide which object keys at one path may be listed by name.

    Returns ``(collapse_all, reason, rare_keys)``. All keys are collapsed into
    ``*`` when the objects behave like maps (more than ``max_keys`` distinct
    keys, or keys that do not look like structural field names). Otherwise a
    key is listed only when it occurs in at least two sampled documents and in
    at least ``JSON_MIN_KEY_PRESENCE`` of the documents that contain the
    object; rarer keys, which often are data values used as keys, are
    collapsed into ``*`` too.
    """
    key_docs: Counter[str] = Counter()
    last: dict[str, int] = {}
    documents: set[int] = set()
    for doc, value in items:
        if not isinstance(value, dict):
            continue
        documents.add(doc)
        for key in value:
            if not _DP_KEY_NAME.match(key):
                return True, "some keys do not look like structural field names", set()
            if last.get(key) != doc:
                key_docs[key] += 1
                last[key] = doc
            if len(key_docs) > max_keys:
                return True, f"more than {max_keys} distinct keys", set()
    threshold = max(2.0, JSON_MIN_KEY_PRESENCE * len(documents))
    rare = {key for key, count in key_docs.items() if count < threshold}
    if key_docs and len(rare) == len(key_docs):
        return True, "every key occurs in too few sampled documents to be listed", set()
    return False, None, rare


def json_path_catalog(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    include_names: bool,
    names_omitted_reason: str | None,
    max_depth: int,
    max_paths: int,
    max_object_keys: int,
    sample_method: str,
) -> dict[str, Any]:
    """Catalogue JSON paths of sampled string values (presence, types, heterogeneity).

    Only values that parse as JSON objects or arrays are *documents*. Keys that
    look like data (objects with too many distinct keys or non-structural
    names, and keys seen in too few documents, see ``_dp_key_policy``) are
    collapsed into ``*`` and never listed. Values are never kept; key names are
    listed only when the value policy allows them.
    """
    documents: list[Any] = []
    counts = {"structured": 0, "scalar": 0, "json_null_literal": 0, "invalid": 0}
    for value in values:
        text = value.strip()
        ok, parsed = parse_json_text(text) if text else (False, None)
        if not ok:
            counts["invalid"] += 1
        elif isinstance(parsed, (dict, list)):
            documents.append(parsed)
        elif parsed is None:
            counts["json_null_literal"] += 1
        else:
            counts["scalar"] += 1
    counts["structured"] = len(documents)
    tracked_limit = max(1000, 4 * max_paths)
    stats: dict[tuple[tuple[str, ...], ...], dict[str, Any]] = {}
    map_like: dict[tuple[tuple[str, ...], ...], str] = {}
    tracking_truncated = False
    frontier: dict[tuple[tuple[str, ...], ...], list[tuple[int, Any]]] = {
        (): list(enumerate(documents))
    }
    for _depth in range(max_depth):
        following: dict[tuple[tuple[str, ...], ...], list[tuple[int, Any]]] = {}
        for parent, items in frontier.items():
            collapse, why, rare = _dp_key_policy(items, max_object_keys)
            if collapse and why:
                map_like[parent] = why
            elif rare:
                map_like[parent] = f"{len(rare)} rare key(s) collapsed into '*'"
            for doc, value in items:
                if isinstance(value, dict):
                    children = [
                        (("any_key",) if collapse or key in rare else ("key", key), child)
                        for key, child in value.items()
                    ]
                elif isinstance(value, list):
                    children = [(("items",), child) for child in value]
                else:
                    continue
                for segment, child in children:
                    path = (*parent, segment)
                    entry = stats.get(path)
                    if entry is None:
                        if len(stats) >= tracked_limit:
                            tracking_truncated = True
                            continue
                        entry = {"docs": 0, "last": -1, "occurrences": 0, "types": Counter()}
                        stats[path] = entry
                    if entry["last"] != doc:
                        entry["docs"] += 1
                        entry["last"] = doc
                    entry["occurrences"] += 1
                    entry["types"][_dp_json_type(child)] += 1
                    if isinstance(child, (dict, list)) and child:
                        following.setdefault(path, []).append((doc, child))
        frontier = following
    depth_truncated = bool(frontier)
    total = len(documents)
    records = []
    for path, entry in stats.items():
        segments = [
            {"kind": "key", "name": part[1]} if part[0] == "key" else {"kind": part[0]}
            for part in path
        ]
        types = dict(sorted(entry["types"].items()))
        non_null = [name for name in types if name != "null"]
        dominant = max(non_null, key=lambda name: (types[name], name)) if non_null else None
        records.append(
            {
                "path": json_path_text(segments),
                "segments": segments,
                "depth": len(path),
                "present_in": entry["docs"],
                "presence_ratio": entry["docs"] / total if total else None,
                "occurrences": entry["occurrences"],
                "types": types,
                "heterogeneous": len(non_null) > 1,
                "dominant_type": dominant,
                "map_like": path in map_like,
                "full_scope": None,
            }
        )
    records.sort(key=lambda item: (item["depth"], -item["present_in"], item["path"]))
    listed = records[:max_paths]
    catalog: dict[str, Any] = {
        "scope": "sample",
        "source": "sample",
        "eligible_observations": len(values),
        "excluded": {"sql_null": sql_nulls, "truncated": truncated},
        "counts": counts,
        "documents": total,
        "max_depth": max_depth,
        "depth_truncated": depth_truncated,
        "paths_observed": len(records),
        "paths_listed": len(listed) if include_names else 0,
        "paths_omitted": len(records) - len(listed) if include_names else len(records),
        "tracking_truncated": tracking_truncated,
        "map_like_paths": len(map_like),
        "heterogeneous_paths": sum(1 for item in records if item["heterogeneous"]),
        "paths": listed if include_names else None,
        "paths_omitted_reason": None,
        "full_scope": None,
        "method": (
            "strict JSON parsing of sampled values; paths walked breadth-first up to "
            "deep.max_json_depth; map-like objects collapsed into '*'"
        ),
        "limitations": [
            *sample_limitations(sample_method, len(values)),
            "Paths inferred from the sample are not a complete or guaranteed schema: paths absent "
            "from the sample may exist in the data.",
            "Values truncated by sampling.max_value_chars are excluded.",
        ],
    }
    if not include_names:
        catalog["paths_omitted_reason"] = names_omitted_reason
    elif not total:
        catalog["paths_omitted_reason"] = "no JSON objects or arrays in the sample"
    elif len(records) > len(listed):
        catalog["paths_omitted_reason"] = (
            f"{len(records) - len(listed)} path(s) beyond deep.max_json_paths = {max_paths}"
        )
    if depth_truncated:
        catalog["limitations"].append(
            f"Nesting deeper than deep.max_json_depth = {max_depth} was not catalogued."
        )
    if tracking_truncated:
        catalog["limitations"].append(
            f"More than {tracked_limit} distinct paths: further paths were not tracked."
        )
    return catalog


def plan_json_validation(
    catalogs: Mapping[str, Mapping[str, Any]], method: str | None
) -> list[dict[str, Any]]:
    """Plan full-scope presence and type checks of listed JSON paths.

    ``method`` is ``variant`` (``try_parse_json``/``try_variant_get``/
    ``is_variant_null``/``schema_of_variant``), ``get_json_object`` or None.
    Each candidate is one aggregate expression at the lowest deep tier.
    """
    candidates: list[dict[str, Any]] = []
    if method is None:
        return candidates
    for fid, catalog in catalogs.items():
        paths = [
            (item, engine_json_path(item["segments"]))
            for item in (catalog.get("paths") or [])
            if engine_json_path(item["segments"])
        ]
        if not paths:
            continue
        candidates.append(
            {
                "metric": "json_documents",
                "op": "json_count_documents",
                "field_id": fid,
                "tier": 7,
                "cost": 1,
                "params": {"method": method, "path": "$"},
                "group": "json_path",
            }
        )
        for item, engine_path in paths:
            ops = (
                [
                    ("path_present_count", "json_path_present"),
                    ("path_json_null_count", "json_path_null"),
                    ("path_type_match_count", "json_path_type_match"),
                ]
                if method == "variant"
                else [("path_non_null_count", "json_path_non_null")]
            )
            for metric, op in ops:
                if op == "json_path_type_match" and item["dominant_type"] is None:
                    continue
                candidates.append(
                    {
                        "metric": metric,
                        "op": op,
                        "field_id": fid,
                        "tier": 7,
                        "cost": 1,
                        "params": {
                            "method": method,
                            "path": engine_path,
                            "display": item["path"],
                            "expected_type": item["dominant_type"],
                        },
                        "group": "json_path",
                    }
                )
    return candidates


_DP_JSON_METHODS = {
    "variant": (
        "try_parse_json + try_variant_get/is_variant_null/schema_of_variant over the full scope",
        [
            "Distinguishes a JSON null from an absent path.",
            "Types come from schema_of_variant (numbers: integer, decimal or double).",
        ],
    ),
    "get_json_object": (
        "get_json_object over the full scope",
        [
            "get_json_object returns NULL both for a JSON null and for an absent path, so "
            "presence counts exclude JSON nulls.",
            "It cannot tell types apart (numbers and strings both come back as text), so types "
            "are not validated.",
            "Its parser is more lenient than strict JSON.",
        ],
    ),
}


def attach_json_validation(
    catalog: dict[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    dropped: Sequence[Mapping[str, Any]],
    *,
    method: str | None,
    unsupported_reason: str | None,
    scope: str,
) -> None:
    """Record full-scope validation results in a JSON path catalogue."""
    if catalog.get("paths") is None:
        return
    if method is None:
        reason = unsupported_reason or (
            "full-scope JSON path validation disabled (deep.json_full_scope_validation = false)"
        )
        catalog["full_scope"] = {
            "method": None,
            "status": "unsupported" if unsupported_reason else "not_computed",
            "reason": reason,
            "documents": None,
            "limitations": [],
        }
        for item in catalog["paths"]:
            item["full_scope"] = {"status": "not_computed", "reason": reason, "metrics": []}
        return
    description, limitations = _DP_JSON_METHODS[method]
    by_path: dict[str, list[Mapping[str, Any]]] = {}
    documents_metric = None
    for spec in specs:
        if spec["op"] == "json_count_documents":
            documents_metric = _dp_json_metric(spec, results, failed, scope, None)
        else:
            by_path.setdefault(spec["params"]["display"], []).append(spec)
    documents = (
        documents_metric["value"]
        if documents_metric and documents_metric["status"] == "measured"
        else None
    )
    dropped_paths = {
        spec["params"].get("display") for spec in dropped if spec.get("group") == "json_path"
    }
    for item in catalog["paths"]:
        engine_path = engine_json_path(item["segments"])
        if engine_path is None:
            item["full_scope"] = {
                "status": "not_computed",
                "reason": "wildcard or unquotable path; not validated",
                "metrics": [],
            }
            continue
        path_specs = by_path.get(item["path"], [])
        if not path_specs:
            reason = (
                "omitted to respect the deep expression and pass budgets"
                if item["path"] in dropped_paths or documents_metric is None
                else "not planned"
            )
            item["full_scope"] = {"status": "not_computed", "reason": reason, "metrics": []}
            continue
        metrics = [_dp_json_metric(spec, results, failed, scope, documents) for spec in path_specs]
        item["full_scope"] = {
            "status": "measured"
            if all(m["status"] == "measured" for m in metrics)
            else "incomplete",
            "reason": None,
            "metrics": metrics,
        }
    measured_documents = documents_metric is not None and documents_metric["status"] == "measured"
    summary_reason: str | None = None
    if documents_metric is None:
        summary_reason = "omitted to respect the deep expression and pass budgets"
    elif not measured_documents:
        summary_reason = documents_metric.get("reason") or "the document count was not measured"
    catalog["full_scope"] = {
        "method": method,
        "status": "measured" if measured_documents else "not_computed",
        "reason": summary_reason,
        "documents": documents_metric,
        "method_description": description,
        "limitations": list(limitations),
    }


_DP_JSON_METRIC_METHODS = {
    "json_documents": "values whose root is a JSON object or array",
    "path_present_count": "documents where the path exists (including a JSON null value)",
    "path_json_null_count": "documents where the path holds a JSON null",
    "path_type_match_count": "documents where the path holds the type most frequent in the sample",
    "path_non_null_count": "documents where get_json_object returns a value for the path "
    "(absent paths and JSON nulls are both excluded)",
}


def _dp_json_metric(
    spec: Mapping[str, Any],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    scope: str,
    documents: int | None,
) -> dict[str, Any]:
    name = spec["metric"]
    if spec["alias"] in failed:
        return not_measured(name, "error", failed[spec["alias"]], scope=scope, source="aggregate")
    raw = results.get(spec["alias"])
    if raw is None:
        return not_measured(
            name, "error", "no value returned by the engine", scope=scope, source="aggregate"
        )
    details: dict[str, Any] = {"method": spec["params"]["method"]}
    if spec["params"].get("expected_type") and name == "path_type_match_count":
        details["expected_type"] = spec["params"]["expected_type"]
    return measured(
        name,
        int(raw),
        unit="documents",
        scope=scope,
        accuracy="exact",
        source="aggregate",
        method=_DP_JSON_METRIC_METHODS[name],
        denominator=documents if name != "json_documents" else None,
        denominator_unit="documents" if name != "json_documents" else None,
        details=details,
    )
