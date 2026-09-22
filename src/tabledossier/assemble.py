"""Engine-neutral assembly of table and run profiles.

Part of the embedded runtime (standard library only). Engine adapters return
raw Python values keyed by plan alias; this module turns them into contract
metrics (units, accuracy, method, denominators), derives ratios, attaches
findings, checks and proposals, and builds the final profile document.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from tabledossier._version import __version__
from tabledossier.config import sanitized_config, table_options_for
from tabledossier.contract import PROFILE_KIND, PROFILE_SCHEMA_VERSION
from tabledossier.findings import field_findings
from tabledossier.jsonutil import fingerprint
from tabledossier.metrics import measured, metric_value, not_measured, ratio
from tabledossier.paths import (
    field_id,
    parse_display_path,
    parse_table_identifier,
    table_lookup_key,
)
from tabledossier.planning import iter_nodes
from tabledossier.quality import evaluate_checks, suggest_rules
from tabledossier.relationships import (
    declared_relationships,
    merge_relationships,
    provided_relationships,
)
from tabledossier.semantic import candidate_roles

# metric name -> (unit, accuracy, method description)
METRIC_INFO: dict[str, tuple[str | None, str, str]] = {
    "row_count": ("rows", "exact", "count(*) over the analysed scope"),
    "null_count": (
        "rows",
        "exact",
        "rows where the field is null (includes rows whose parent struct is null)",
    ),
    "non_null_count": ("rows", "exact", "row_count - null_count"),
    "null_ratio": ("ratio", "exact", "null_count / row_count"),
    "null_count_parent_present": (
        "rows",
        "exact",
        "rows whose parent struct is not null and the field is null",
    ),
    "null_ratio_given_parent_present": (
        "ratio",
        "exact",
        "null_count_parent_present / rows with non-null parent",
    ),
    "approx_distinct_count": ("values", "approximate", "approx_count_distinct (HyperLogLog++)"),
    "all_values_equal": (None, "exact", "min(field) = max(field) over non-null values"),
    "finite_count": ("values", "exact", "non-null values that are neither NaN nor infinite"),
    "nan_count": ("values", "exact", "NaN values (distinct from SQL NULL)"),
    "positive_infinity_count": ("values", "exact", "+Infinity values"),
    "negative_infinity_count": ("values", "exact", "-Infinity values"),
    "min": (None, "exact", "minimum over non-null values (finite values only for floating point)"),
    "max": (None, "exact", "maximum over non-null values (finite values only for floating point)"),
    "mean": (None, "exact", "average over non-null values (finite values only for floating point)"),
    "stddev": (
        None,
        "exact",
        "sample standard deviation (stddev_samp), computed in double precision",
    ),
    "quantiles": (None, "approximate", "percentile_approx over non-null (finite) values"),
    "zero_count": ("values", "exact", "values equal to zero"),
    "negative_count": ("values", "exact", "values below zero"),
    "positive_count": ("values", "exact", "values above zero"),
    "empty_count": ("values", "exact", "empty strings or empty collections"),
    "whitespace_only_count": (
        "values",
        "exact",
        "non-empty strings made only of whitespace (regex ^\\s+$)",
    ),
    "min_length": ("characters", "exact", "minimum length over non-null values"),
    "max_length": ("characters", "exact", "maximum length over non-null values"),
    "mean_length": ("characters", "exact", "average length over non-null values"),
    "length_quantiles": (
        "characters",
        "approximate",
        "percentile_approx of length over non-null values",
    ),
    "true_count": ("values", "exact", "values equal to true"),
    "false_count": ("values", "exact", "values equal to false"),
    "after_reference_count": (
        "values",
        "exact",
        "values after the run reference instant (run.reference_time)",
    ),
    "min_size": ("elements", "exact", "minimum size over non-null collections"),
    "max_size": ("elements", "exact", "maximum size over non-null collections"),
    "mean_size": ("elements", "exact", "average size over non-null collections"),
    "total_element_count": ("elements", "exact", "sum of array sizes over non-null arrays"),
    "total_entry_count": ("entries", "exact", "sum of map sizes over non-null maps"),
    "null_element_count": ("elements", "exact", "null elements inside non-null arrays"),
    "null_value_count": ("entries", "exact", "null values inside non-null maps"),
    "json_invalid_count": (
        "values",
        "exact",
        "non-null strings for which try_parse_json returns NULL",
    ),
}
METRIC_ORDER = list(METRIC_INFO)
_AS_FINITE_DENOMINATOR = ("zero_count", "negative_count", "positive_count")
_AS_NON_NULL_DENOMINATOR = (
    "finite_count",
    "nan_count",
    "positive_infinity_count",
    "negative_infinity_count",
    "empty_count",
    "whitespace_only_count",
    "true_count",
    "false_count",
    "after_reference_count",
    "json_invalid_count",
)
_AS_SUM_DEFAULT_ZERO = (
    "total_element_count",
    "total_entry_count",
    "null_element_count",
    "null_value_count",
)


def _as_value_type(metric: str, kind: str, raw: Any) -> str | None:
    if metric in ("min", "max") and kind in ("timestamp", "timestamp_ntz"):
        return kind
    return None


def _as_insufficient(metric: str, kind: str, scope: str) -> dict[str, Any]:
    if metric == "stddev":
        reason = "fewer than two (finite) non-null values in scope"
    elif kind == "float" and metric in ("min", "max", "mean", "quantiles"):
        reason = "no finite non-null values in scope"
    else:
        reason = "no non-null values in scope"
    return not_measured(metric, "insufficient_data", reason, scope=scope, source="aggregate")


def aggregate_metric(
    spec: Mapping[str, Any],
    raw: Any,
    kind: str,
    *,
    scope: str,
    denominators: Mapping[str, int | None],
) -> dict[str, Any]:
    """Convert one raw aggregate result into a contract metric."""
    name = spec["metric"]
    unit, accuracy, method = METRIC_INFO[name]
    if kind == "binary" and name in ("min_length", "max_length"):
        unit = "bytes"
    if kind == "map" and unit == "elements":
        unit = "entries"
    details: dict[str, Any] = {}
    if spec["op"] == "approx_distinct":
        details["relative_standard_deviation"] = spec["params"]["rsd"]
    if name == "after_reference_count":
        details["reference"] = "run.reference_time"
        details["comparison"] = (
            "dates are compared with the UTC calendar date of run.reference_time; timestamps are "
            "compared as instants (session time zone recorded in run.environment)"
        )
    if name in _AS_SUM_DEFAULT_ZERO and raw is None:
        raw = 0
    if raw is None:
        return _as_insufficient(name, kind, scope)
    if name in ("quantiles", "length_quantiles"):
        encoded = []
        element_type = None
        for probability, item in zip(spec["params"]["probabilities"], raw, strict=False):
            probe = measured(
                "q",
                item,
                unit=None,
                scope=scope,
                accuracy="approximate",
                source="aggregate",
                method="-",
            )
            element_type = probe["value_type"]
            encoded.append({"probability": probability, "value": probe["value"]})
        details.update(
            {"element_type": element_type, "accuracy_parameter": spec["params"]["accuracy"]}
        )
        return measured(
            name,
            encoded,
            value_type="quantiles",
            unit=unit,
            scope=scope,
            accuracy=accuracy,
            source="aggregate",
            method=method,
            details=details,
        )
    denominator = None
    denominator_unit = None
    if name in _AS_FINITE_DENOMINATOR:
        key = "finite" if kind == "float" else "non_null"
        denominator, denominator_unit = denominators.get(key), "values"
    elif name in _AS_NON_NULL_DENOMINATOR:
        denominator, denominator_unit = denominators.get("non_null"), "values"
    elif name == "null_element_count":
        denominator, denominator_unit = denominators.get("total_element_count"), "elements"
    elif name == "null_value_count":
        denominator, denominator_unit = denominators.get("total_entry_count"), "entries"
    return measured(
        name,
        raw,
        value_type=_as_value_type(name, kind, raw),
        unit=unit,
        scope=scope,
        accuracy=accuracy,
        source="aggregate",
        method=method,
        denominator=denominator,
        denominator_unit=denominator_unit if denominator is not None else None,
        details=details or None,
    )


def field_metrics(
    node: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed_aliases: Mapping[str, str],
    *,
    scope: str,
    row_count: int | None,
    parent_null_count: int | None,
    static: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build the ordered metric list of one field from raw aggregate results."""
    kind = node["type"]["kind"]
    raw = {
        spec["metric"]: results.get(spec["alias"])
        for spec in specs
        if spec["alias"] not in failed_aliases
    }
    null_count = raw.get("null_count")
    non_null = (
        row_count - null_count
        if isinstance(row_count, int) and isinstance(null_count, int)
        else None
    )
    finite = raw.get("finite_count") if kind == "float" else non_null
    total_elements = raw.get("total_element_count")
    total_entries = raw.get("total_entry_count")
    denominators = {
        "non_null": non_null,
        "finite": finite if isinstance(finite, int) else None,
        "total_element_count": total_elements
        if total_elements is not None
        else (0 if "total_element_count" in raw else None),
        "total_entry_count": total_entries
        if total_entries is not None
        else (0 if "total_entry_count" in raw else None),
    }
    out: dict[str, dict[str, Any]] = {}
    for spec in specs:
        name = spec["metric"]
        if spec["alias"] in failed_aliases:
            out[name] = not_measured(
                name, "error", failed_aliases[spec["alias"]], scope=scope, source="aggregate"
            )
            continue
        metric = aggregate_metric(spec, raw.get(name), kind, scope=scope, denominators=denominators)
        if name == "null_count" and metric["status"] == "measured":
            metric["denominator"] = row_count
            metric["denominator_unit"] = "rows"
        out[name] = metric
    if isinstance(non_null, int):
        out["non_null_count"] = measured(
            "non_null_count",
            non_null,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="derived",
            method=METRIC_INFO["non_null_count"][2],
            denominator=row_count,
            denominator_unit="rows",
        )
        out["null_ratio"] = ratio(
            "null_ratio",
            null_count,
            row_count,
            scope=scope,
            source="derived",
            method=METRIC_INFO["null_ratio"][2],
            denominator_unit="rows",
        )
    if (
        "null_count_parent_present" in out
        and out["null_count_parent_present"]["status"] == "measured"
    ):
        parent_non_null = (
            row_count - parent_null_count
            if isinstance(row_count, int) and isinstance(parent_null_count, int)
            else None
        )
        metric = out["null_count_parent_present"]
        if parent_non_null is not None:
            metric["denominator"] = parent_non_null
            metric["denominator_unit"] = "rows"
        out["null_ratio_given_parent_present"] = ratio(
            "null_ratio_given_parent_present",
            metric["value"],
            parent_non_null,
            scope=scope,
            source="derived",
            method=METRIC_INFO["null_ratio_given_parent_present"][2],
            denominator_unit="rows with a non-null parent",
        )
    if "all_values_equal" in out and non_null == 0:
        out["all_values_equal"] = _as_insufficient("all_values_equal", kind, scope)
    for item in static:
        out.setdefault(item["metric"]["name"], dict(item["metric"]))
    order = {name: index for index, name in enumerate(METRIC_ORDER)}
    return [out[name] for name in sorted(out, key=lambda n: (order.get(n, len(order)), n))]


def field_profile(
    node: Mapping[str, Any], *, profiled: bool, omission_reason: str | None
) -> dict[str, Any]:
    """Return an empty field profile record for a schema node."""
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "path": node["path"],
        "type_kind": node["type"]["kind"],
        "physical_type": node["type"]["physical_type"],
        "nullable": node.get("nullable"),
        "comment": node.get("comment"),
        "profiled": profiled,
        "omission_reason": omission_reason,
        "metrics": [],
        "semantics": None,
        "json_profile": None,
        "concentration": None,
        "examples": None,
        "element_context": None,
        "json_paths": None,
    }


# metric name -> (unit or None for the element unit, method); element metrics are exact.
ELEMENT_METRIC_INFO: dict[str, tuple[str | None, str]] = {
    "element_count": (None, "size of the collection summed over rows (denominator of the others)"),
    "null_count": (
        None,
        "elements whose value is null (includes elements whose parent struct is null); "
        "higher-order functions per row, summed",
    ),
    "non_null_count": (None, "element_count - null_count"),
    "null_ratio": ("ratio", "null_count / element_count"),
    "null_count_parent_present": (
        None,
        "elements whose parent struct is not null and the field is null",
    ),
    "null_ratio_given_parent_present": (
        "ratio",
        "null_count_parent_present / elements with a non-null parent",
    ),
    "finite_count": (None, "non-null values that are neither NaN nor infinite"),
    "nan_count": (None, "NaN values (distinct from SQL NULL)"),
    "positive_infinity_count": (None, "+Infinity values"),
    "negative_infinity_count": (None, "-Infinity values"),
    "min": (None, "array_min per row, then min over rows (finite values only for floats)"),
    "max": (None, "array_max per row, then max over rows (finite values only for floats)"),
    "zero_count": (None, "values equal to zero"),
    "negative_count": (None, "values below zero"),
    "positive_count": (None, "values above zero"),
    "empty_count": (None, "empty strings"),
    "whitespace_only_count": (None, "non-empty strings made only of whitespace (regex ^\\s+$)"),
    "min_length": ("characters", "minimum length over non-null values"),
    "max_length": ("characters", "maximum length over non-null values"),
    "true_count": (None, "values equal to true"),
    "false_count": (None, "values equal to false"),
    "after_reference_count": (None, "values after the run reference instant (run.reference_time)"),
    "distinct_count": (None, "exact count of distinct non-null values among the elements examined"),
}
ELEMENT_METRIC_ORDER = list(ELEMENT_METRIC_INFO)
_AS_ELEMENT_NON_NULL_DENOMINATOR = (
    "finite_count",
    "nan_count",
    "positive_infinity_count",
    "negative_infinity_count",
    "empty_count",
    "whitespace_only_count",
    "true_count",
    "false_count",
    "after_reference_count",
)


def element_field_metrics(
    node: Mapping[str, Any],
    context: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed_aliases: Mapping[str, str],
    *,
    scope: str,
    element_total: int | None,
    parent_null_count: int | None,
    static: Sequence[Mapping[str, Any]],
    extra: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Build the metrics of an array element or map entry field.

    Denominators are the elements (arrays) or entries (maps) of the collection
    in scope, never rows. ``element_total`` is the collection's measured
    ``total_element_count``/``total_entry_count``; ``parent_null_count`` is
    the null count of the enclosing element struct (for struct leaves).
    """
    kind = node["type"]["kind"]
    unit = context["unit"]
    collection = context["collection_display_path"]
    out: dict[str, dict[str, Any]] = {}
    raw: dict[str, Any] = {}
    for spec in specs:
        name = spec["metric"]
        if spec["alias"] in failed_aliases:
            out[name] = not_measured(
                name, "error", failed_aliases[spec["alias"]], scope=scope, source="aggregate"
            )
        else:
            raw[name] = results.get(spec["alias"])
    if isinstance(element_total, int):
        out["element_count"] = measured(
            "element_count",
            element_total,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source="derived",
            method=f"{unit} of {collection} in scope ({ELEMENT_METRIC_INFO['element_count'][1]})",
        )
    null_count = raw.get("null_count")
    null_count = 0 if null_count is None and "null_count" in raw else null_count
    non_null = (
        element_total - null_count
        if isinstance(element_total, int) and isinstance(null_count, int)
        else None
    )
    finite = raw.get("finite_count") if kind == "float" else non_null
    if kind == "float" and finite is None and "finite_count" in raw:
        finite = 0
    for name, value in raw.items():
        info_unit, method = ELEMENT_METRIC_INFO[name]
        metric_unit = info_unit or unit
        if kind == "binary" and name in ("min_length", "max_length"):
            metric_unit = "bytes"
        details = None
        if name == "after_reference_count":
            details = {"reference": "run.reference_time"}
        if name in ("min", "max", "min_length", "max_length"):
            if value is None:
                out[name] = not_measured(
                    name,
                    "insufficient_data",
                    f"no non-null {unit} in scope"
                    if kind != "float"
                    else f"no finite non-null {unit} in scope",
                    scope=scope,
                    source="aggregate",
                )
                continue
            value_type = kind if name in ("min", "max") and kind.startswith("timestamp") else None
            out[name] = measured(
                name,
                value,
                value_type=value_type,
                unit=None if name in ("min", "max") else metric_unit,
                scope=scope,
                accuracy="exact",
                source="aggregate",
                method=method,
            )
            continue
        count = 0 if value is None else int(value)
        denominator: int | None = None
        if name == "null_count":
            denominator = element_total
        elif name in ("zero_count", "negative_count", "positive_count"):
            denominator = finite if isinstance(finite, int) else None
        elif name in _AS_ELEMENT_NON_NULL_DENOMINATOR:
            denominator = non_null
        elif name == "null_count_parent_present":
            denominator = (
                element_total - parent_null_count
                if isinstance(element_total, int) and isinstance(parent_null_count, int)
                else None
            )
        out[name] = measured(
            name,
            count,
            unit=metric_unit,
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method=method,
            denominator=denominator,
            denominator_unit=unit if denominator is not None else None,
            details=details,
        )
    if isinstance(non_null, int) and "null_count" in out:
        out["non_null_count"] = measured(
            "non_null_count",
            non_null,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source="derived",
            method=ELEMENT_METRIC_INFO["non_null_count"][1],
            denominator=element_total,
            denominator_unit=unit,
        )
        out["null_ratio"] = ratio(
            "null_ratio",
            null_count,
            element_total,
            scope=scope,
            source="derived",
            method=ELEMENT_METRIC_INFO["null_ratio"][1],
            denominator_unit=unit,
        )
    parent_metric = out.get("null_count_parent_present")
    if parent_metric is not None and parent_metric["status"] == "measured":
        out["null_ratio_given_parent_present"] = ratio(
            "null_ratio_given_parent_present",
            parent_metric["value"],
            parent_metric.get("denominator"),
            scope=scope,
            source="derived",
            method=ELEMENT_METRIC_INFO["null_ratio_given_parent_present"][1],
            denominator_unit=f"{unit} with a non-null parent",
        )
    for metric in extra:
        out.setdefault(metric["name"], dict(metric))
    for item in static:
        out.setdefault(item["metric"]["name"], dict(item["metric"]))
    order = {name: index for index, name in enumerate(ELEMENT_METRIC_ORDER)}
    return [out[name] for name in sorted(out, key=lambda n: (order.get(n, len(order)), n))]


def policy_field_ids(config: Mapping[str, Any], table_lookup: str, list_name: str) -> set[str]:
    """Return field ids listed for a table in ``value_policy.<list_name>``."""
    ids = set()
    for item in config["value_policy"].get(list_name, []):
        if table_lookup_key(parse_table_identifier(item["table"])) == table_lookup:
            ids.add(field_id(parse_display_path(item["column"])))
    return ids


def new_table(
    input_text: str, parts: list[str], table_key_text: str, table_id_text: str, quoted: str
) -> dict[str, Any]:
    """Return a table profile skeleton (filled in by an engine adapter)."""
    return {
        "table_id": table_id_text,
        "table_key": table_key_text,
        "identifier": {"input": input_text, "parts": list(parts), "quoted": quoted},
        "status": "succeeded",
        "errors": [],
        "purpose": None,
        "source": None,
        "consistency": None,
        "scope": {
            "population": "full",
            "scope_label": "table_metadata",
            "filters": [],
            "selected_columns": None,
            "row_semantics": (
                "Rows in the analysed scope after filters. No deduplication is applied; "
                "row counts are not counts of business entities (e.g. in historized tables)."
            ),
        },
        "sample": None,
        "summary": {},
        "table_metrics": [],
        "schema": None,
        "field_profiles": [],
        "constraints": [],
        "findings": [],
        "quality_checks": [],
        "suggested_rules": [],
        "omissions": [],
        "unsupported": [],
        "operations": {
            "planned": [],
            "observed": [],
            "physical_scans": "unknown",
            "bytes_read": "unknown",
            "monetary_cost": "unknown",
        },
        "timings_ms": {},
        "notes": [],
        "deep": None,
    }


def finalize_table(table: dict[str, Any], config: Mapping[str, Any]) -> None:
    """Attach roles, findings, checks, proposals, summary and status to a table."""
    thresholds = config["thresholds"]
    row_count = None
    for metric in table["table_metrics"]:
        if (
            metric["name"] == "row_count"
            and metric["status"] == "measured"
            and metric["source"] == "aggregate"
        ):
            row_count = metric["value"]
    findings: list[dict[str, Any]] = []
    for field in table["field_profiles"]:
        if not field["profiled"]:
            continue
        context = field.get("element_context")
        if context:
            # Element fields: counts are elements or entries of a collection, never rows.
            total = metric_value(field["metrics"], "element_count")
            findings.extend(
                field_findings(
                    field,
                    thresholds,
                    total if isinstance(total, int) else None,
                    unit=context["unit"],
                )
            )
            continue
        if field["metrics"] or field["semantics"] is not None:
            semantics = field["semantics"] or {"observed_format": None, "candidate_roles": []}
            semantics["candidate_roles"] = candidate_roles(
                field["type_kind"],
                field["metrics"],
                semantics.get("observed_format"),
                thresholds,
                field["physical_type"],
            )
            field["semantics"] = semantics
        findings.extend(field_findings(field, thresholds, row_count))
    table["findings"] = findings
    options = table_options_for(config, table_lookup_key(table["identifier"]["parts"]))
    checks = options.get("checks", [])
    if table["schema"] is None:
        table["quality_checks"] = [
            {
                "check_id": check["id"],
                "type": check["type"],
                "origin": "configured",
                "target": {"table": table["table_key"], "column": None},
                "parameters": {key: check[key] for key in ("min", "max") if key in check},
                "status": "not_evaluated",
                "observed": None,
                "message": "table could not be profiled",
                "description": check.get("description"),
            }
            for check in checks
        ]
    else:
        table["quality_checks"] = evaluate_checks(table, checks)
    table["suggested_rules"] = suggest_rules(table, thresholds)
    schema = table["schema"] or {}
    nodes = list(iter_nodes(schema.get("fields", [])))
    profiled = [field for field in table["field_profiles"] if field["profiled"]]
    table["summary"] = {
        "top_level_columns": len(schema.get("fields", [])) + schema.get("top_level_omitted", 0)
        if schema
        else None,
        "columns_selected": len(table["scope"]["selected_columns"])
        if table["scope"]["selected_columns"] is not None
        else None,
        "schema_nodes": len(nodes) if schema else None,
        "fields_profiled": len(profiled),
        "fields_omitted": len(
            {item["field_id"] for item in table["omissions"] if item["field_id"]}
        ),
        "findings": len(findings),
        "quality_checks": len(table["quality_checks"]),
        "suggested_rules": len(table["suggested_rules"]),
    }
    stages = {error["stage"] for error in table["errors"]}
    if table["schema"] is None or "resolve" in stages:
        table["status"] = "failed"
    elif stages:
        table["status"] = "partial"
    else:
        table["status"] = "succeeded"


def value_exposure(config: Mapping[str, Any]) -> dict[str, Any]:
    """Describe what kinds of values a profile can contain under the policy."""
    policy = config["value_policy"]
    examples = list(policy.get("example_columns", [])) if policy.get("persist_examples") else []
    notes = [
        "Profiles reveal schema, comments and aggregate statistics; they are not anonymized.",
        "Sampled values are inspected transiently in the execution environment for format and JSON "
        "inference and are not persisted unless a column is allow-listed.",
        "Filter values from the configuration are recorded because they define the analysed "
        "population.",
    ]
    if policy.get("aggregate_extremes") == "include":
        notes.append(
            "Numeric and temporal min, max, mean and quantiles are included; set "
            "value_policy.aggregate_extremes = redact (or list redact_columns) to omit them."
        )
    return {
        "raw_values_persisted": bool(examples),
        "example_columns": examples,
        "aggregate_extremes": policy.get("aggregate_extremes", "include"),
        "json_key_names": policy.get("json_key_names", "include"),
        "redacted_columns": list(policy.get("redact_columns", [])),
        "notes": notes,
    }


def build_profile(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    reference_time: str,
    environment: Mapping[str, Any],
    config: Mapping[str, Any],
    parameter_sources: Mapping[str, Any],
    generation: Mapping[str, Any],
    capabilities: Mapping[str, Any],
    tables: list[dict[str, Any]],
) -> dict[str, Any]:
    """Assemble the canonical profile document for a run."""
    statuses = [table["status"] for table in tables]
    if statuses and all(status == "succeeded" for status in statuses):
        status = "succeeded"
    elif not statuses or all(status == "failed" for status in statuses):
        status = "failed"
    else:
        status = "partial"
    effective = sanitized_config(config)
    relationships = merge_relationships(
        declared_relationships(tables),
        provided_relationships(config.get("relationships", []), "configuration"),
    )
    checks = [check for table in tables for check in table["quality_checks"]]
    findings = [finding for table in tables for finding in table["findings"]]
    return {
        "kind": PROFILE_KIND,
        "schema_version": PROFILE_SCHEMA_VERSION,
        "tool": {"name": "tabledossier", "version": __version__},
        "run": {
            "run_id": run_id,
            "status": status,
            "analysis_level": config["analysis_level"],
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "reference_time": reference_time,
            "environment": dict(environment),
            "effective_config": effective,
            "config_fingerprint": fingerprint(effective),
            "parameter_sources": dict(parameter_sources),
            "generation": dict(generation),
            "purpose": config.get("purpose"),
            "capabilities": dict(capabilities),
        },
        "value_exposure": value_exposure(config),
        "tables": tables,
        "relationships": relationships,
        "summary": {
            "tables_total": len(tables),
            "tables_succeeded": statuses.count("succeeded"),
            "tables_partial": statuses.count("partial"),
            "tables_failed": statuses.count("failed"),
            "fields_profiled": sum(
                table["summary"].get("fields_profiled") or 0 for table in tables
            ),
            "checks": {
                s: sum(1 for c in checks if c["status"] == s)
                for s in ("pass", "fail", "not_evaluated", "error")
            },
            "findings": {
                s: sum(1 for f in findings if f["severity"] == s) for s in ("info", "warning")
            },
            "suggested_rules": sum(len(table["suggested_rules"]) for table in tables),
        },
    }
