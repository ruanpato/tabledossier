"""Configured quality checks and proposed (never auto-applied) rules.

Part of the embedded runtime (standard library only).

Checks reuse metrics already measured by the profile; they never trigger new
reads. Their status is ``pass``, ``fail``, ``not_evaluated`` (the metric was
not measured, e.g. empty table or metadata level) or ``error`` (the check does
not fit the data, e.g. unknown column). Suggested rules are proposals derived
from observations and always require human review.
"""

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from tabledossier.jsonutil import short_hash
from tabledossier.keys import measured_keys
from tabledossier.metrics import find_metric, metric_value
from tabledossier.paths import column_reference_segments, display_path, field_id

SUGGESTED_RULES_KIND = "tabledossier.suggested_rules"
RULE_FORMATS = ("uuid", "iso_date", "iso_timestamp", "url", "email_candidate", "numeric_string")


def _q_number(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _q_result(
    check: Mapping[str, Any],
    table_key: str,
    column: str | None,
    status: str,
    message: str,
    observed: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    parameters = {key: check[key] for key in ("min", "max") if key in check}
    return {
        "check_id": check["id"],
        "type": check["type"],
        "origin": "configured",
        "target": {"table": table_key, "column": column},
        "parameters": parameters,
        "status": status,
        "observed": dict(observed) if observed else None,
        "message": message,
        "description": check.get("description"),
    }


def _q_observed(metric: Mapping[str, Any]) -> dict[str, Any]:
    return {"metric": metric["name"], "value": metric.get("value"), "scope": metric.get("scope")}


def evaluate_checks(
    table: Mapping[str, Any], checks: list[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Evaluate configured checks against an assembled table profile."""
    table_key = table["table_key"]
    results = []
    fields = {item["field_id"]: item for item in table.get("field_profiles", [])}
    schema_ids = set()
    stack = list(table.get("schema", {}).get("fields", []))
    while stack:
        node = stack.pop()
        schema_ids.add(node["field_id"])
        stack.extend(node["children"])
    for check in checks:
        check_type = check["type"]
        if check_type in ("min_row_count", "max_row_count"):
            metric = find_metric(table.get("table_metrics", []), "row_count")
            if (
                metric is None
                or metric["status"] != "measured"
                or metric.get("source") != "aggregate"
            ):
                results.append(
                    _q_result(
                        check,
                        table_key,
                        None,
                        "not_evaluated",
                        "row count in scope was not measured",
                    )
                )
                continue
            rows = metric["value"]
            ok = rows >= check["min"] if check_type == "min_row_count" else rows <= check["max"]
            results.append(
                _q_result(
                    check,
                    table_key,
                    None,
                    "pass" if ok else "fail",
                    f"{rows} row(s) in scope",
                    _q_observed(metric),
                )
            )
            continue
        segments = column_reference_segments(check["column"])
        column = display_path(segments)
        fid = field_id(segments)
        if fid not in schema_ids:
            results.append(
                _q_result(check, table_key, column, "error", "column not found in the table schema")
            )
            continue
        field = fields.get(fid)
        if field is None or not field.get("profiled"):
            results.append(
                _q_result(
                    check, table_key, column, "not_evaluated", "column was not profiled in this run"
                )
            )
            continue
        metrics = field.get("metrics", [])
        results.append(_q_evaluate_column(check, table_key, column, field, metrics))
    return results


def _q_evaluate_column(
    check: Mapping[str, Any],
    table_key: str,
    column: str,
    field: Mapping[str, Any],
    metrics: list[Mapping[str, Any]],
) -> dict[str, Any]:
    check_type = check["type"]
    if check_type in ("max_null_ratio", "max_null_count"):
        name = "null_ratio" if check_type == "max_null_ratio" else "null_count"
        metric = find_metric(metrics, name)
        if metric is None or metric["status"] != "measured":
            reason = metric.get("reason", "not measured") if metric else "not measured"
            return _q_result(check, table_key, column, "not_evaluated", f"{name}: {reason}")
        ok = metric["value"] <= check["max"]
        return _q_result(
            check,
            table_key,
            column,
            "pass" if ok else "fail",
            f"{name} = {metric['value']}",
            _q_observed(metric),
        )
    if check_type == "max_empty_string_ratio":
        if field["type_kind"] != "string":
            return _q_result(check, table_key, column, "error", "check requires a string column")
        empty = metric_value(metrics, "empty_count")
        non_null = metric_value(metrics, "non_null_count")
        if not isinstance(empty, int) or not isinstance(non_null, int):
            return _q_result(
                check, table_key, column, "not_evaluated", "empty_count was not measured"
            )
        if non_null == 0:
            return _q_result(
                check, table_key, column, "not_evaluated", "no non-null values in scope"
            )
        value = empty / non_null
        empty_metric = find_metric(metrics, "empty_count") or {}
        return _q_result(
            check,
            table_key,
            column,
            "pass" if value <= check["max"] else "fail",
            f"empty strings / non-null values = {value}",
            {
                "metric": "empty_count/non_null_count",
                "value": value,
                "scope": empty_metric.get("scope"),
            },
        )
    # value_range
    if field["type_kind"] not in ("integer", "float", "decimal"):
        return _q_result(check, table_key, column, "error", "value_range requires a numeric column")
    observed_min = find_metric(metrics, "min")
    observed_max = find_metric(metrics, "max")
    needed = [m for m, key in ((observed_min, "min"), (observed_max, "max")) if key in check]
    if any(m is None or m["status"] != "measured" for m in needed):
        return _q_result(
            check,
            table_key,
            column,
            "not_evaluated",
            "min/max not measured (empty scope or redacted)",
        )
    failures = []
    if "min" in check and observed_min is not None:
        low = _q_number(observed_min["value"])
        bound = _q_number(check["min"])
        if low is None or bound is None or low < bound:
            failures.append(f"observed min {observed_min['value']} < {check['min']}")
    if "max" in check and observed_max is not None:
        high = _q_number(observed_max["value"])
        bound = _q_number(check["max"])
        if high is None or bound is None or high > bound:
            failures.append(f"observed max {observed_max['value']} > {check['max']}")
    return _q_result(
        check,
        table_key,
        column,
        "fail" if failures else "pass",
        "; ".join(failures) if failures else "observed finite values within bounds",
        {
            "metric": "min/max",
            "value": [
                observed_min["value"] if observed_min else None,
                observed_max["value"] if observed_max else None,
            ],
            "scope": (observed_min or observed_max or {}).get("scope"),
        },
    )


def _q_rule(
    table_key: str,
    column: str | None,
    rule_type: str,
    parameters: Mapping[str, Any],
    rationale: str,
    evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    identity: list[Any] = [table_key, column, rule_type]
    if column is None and parameters:
        identity.append(dict(parameters))
    return {
        "rule_id": "sr_" + short_hash(identity),
        "table": table_key,
        "column": column,
        "rule_type": rule_type,
        "parameters": dict(parameters),
        "status": "proposed",
        "requires_review": True,
        "rationale": rationale,
        "evidence": evidence,
    }


def _q_exact_evidence(key: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = {m["name"]: m["value"] for m in key["metrics"] if m["status"] == "measured"}
    return [
        {"check": "exact_uniqueness", "key_id": key["key_id"], "scope": key["scope"]},
        *(
            {"metric": name, "value": values[name]}
            for name in (
                "rows_with_complete_key",
                "distinct_keys",
                "duplicate_key_groups",
                "rows_with_null_key",
            )
            if name in values
        ),
    ]


def _q_exact_rationale(key: Mapping[str, Any]) -> str:
    values = {m["name"]: m["value"] for m in key["metrics"] if m["status"] == "measured"}
    text = (
        f"Exact uniqueness check: no duplicate among {values.get('rows_with_complete_key')} "
        "row(s) with a complete key in the analysed scope"
    )
    if key["outcome"] == "unique_non_null":
        text += f"; {values.get('rows_with_null_key')} row(s) have NULL in the key"
    return text + ". This describes the analysed scope, not future data."


def suggest_rules(table: Mapping[str, Any], thresholds: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Propose rules from observations of one table (never applied automatically).

    ``unique`` proposals cite the exact uniqueness check of the deep level when
    one measured the column; an exact check that found duplicates suppresses
    the proposal.
    """
    if table.get("status") == "failed":
        return []
    table_key = table["table_key"]
    rules = []
    exact = {tuple(key["field_ids"]): key for key in measured_keys(table)}
    proposed_keys: set[tuple[str, ...]] = set()
    min_rows = thresholds["identifier_min_rows"]
    for field in table.get("field_profiles", []):
        if not field.get("profiled") or field.get("element_context"):
            # Element fields are described, not turned into column rules.
            continue
        metrics = field.get("metrics", [])
        column = field["display_path"]
        null_count = metric_value(metrics, "null_count")
        non_null = metric_value(metrics, "non_null_count")
        if (
            null_count == 0
            and isinstance(non_null, int)
            and non_null >= min_rows
            and field.get("nullable") is not False
        ):
            rules.append(
                _q_rule(
                    table_key,
                    column,
                    "not_null",
                    {},
                    "No nulls observed in the analysed scope although the column is declared "
                    "nullable.",
                    [
                        {"metric": "null_count", "value": 0},
                        {"metric": "non_null_count", "value": non_null},
                    ],
                )
            )
        semantics = field.get("semantics") or {}
        for role in semantics.get("candidate_roles", []):
            if role["role"] == "identifier_candidate":
                key = exact.get((field["field_id"],))
                if key is not None:
                    proposed_keys.add((field["field_id"],))
                    if key["outcome"] in ("unique", "unique_non_null"):
                        rules.append(
                            _q_rule(
                                table_key,
                                column,
                                "unique",
                                {},
                                _q_exact_rationale(key),
                                [*_q_exact_evidence(key), *role["evidence"]],
                            )
                        )
                    continue
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "unique",
                        {},
                        "Approximate distinct count close to non-null count; confirm with an exact "
                        "uniqueness check (deep.uniqueness) before adopting.",
                        role["evidence"],
                    )
                )
            elif role["role"] == "categorical_candidate":
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "accepted_values",
                        {"values": None},
                        "Few distinct values observed; the accepted set must be defined by a data "
                        "owner (observed values are not exposed by default).",
                        role["evidence"],
                    )
                )
        fmt = semantics.get("observed_format") or {}
        if fmt.get("status") == "detected":
            if fmt["format"] in ("json_object", "json_array"):
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "valid_json",
                        {
                            "expected_top_level": "object"
                            if fmt["format"] == "json_object"
                            else "array"
                        },
                        "Sampled values parse as JSON.",
                        [
                            {
                                "observed_format": fmt["format"],
                                "eligible_observations": fmt["eligible_observations"],
                            }
                        ],
                    )
                )
            elif fmt["format"] in RULE_FORMATS:
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "matches_format",
                        {"format": fmt["format"]},
                        "Sampled values consistently match this format.",
                        [
                            {
                                "observed_format": fmt["format"],
                                "eligible_observations": fmt["eligible_observations"],
                            }
                        ],
                    )
                )
    for ids, key in exact.items():
        if ids in proposed_keys or key["outcome"] not in ("unique", "unique_non_null"):
            continue
        single = len(key["columns"]) == 1
        rules.append(
            _q_rule(
                table_key,
                key["columns"][0] if single else None,
                "unique",
                {} if single else {"columns": list(key["columns"])},
                _q_exact_rationale(key),
                _q_exact_evidence(key),
            )
        )
    return rules


def suggested_rules_document(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Return the neutral ``suggested_rules.json`` document for a profile."""
    rules = []
    for table in profile.get("tables", []):
        rules.extend(table.get("suggested_rules", []))
    return {
        "kind": SUGGESTED_RULES_KIND,
        "schema_version": "1.0",
        "run_id": profile["run"]["run_id"],
        "profiled_at": profile["run"]["started_at"],
        "notice": "Proposals derived from observed data. They are not applied and require review "
        "by a data owner before being adopted as quality rules.",
        "rules": rules,
    }
