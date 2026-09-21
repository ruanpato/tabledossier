"""Heuristic findings derived from measured metrics.

Part of the embedded runtime (standard library only).

Findings are exploratory signals, never contract failures: their severity is
``info`` or ``warning`` and each one states its threshold, evidence and why the
severity was chosen. High nullity, constancy or extreme tails can all be
legitimate; no finding asserts a business error.
"""

from collections.abc import Mapping
from typing import Any

from tabledossier.jsonutil import short_hash
from tabledossier.metrics import metric_value, numeric_value


def _fd_finding(
    code: str,
    severity: str,
    field: Mapping[str, Any],
    title: str,
    message: str,
    evidence: list[dict[str, Any]],
    threshold: Mapping[str, Any] | None,
    severity_reason: str,
    limitations: str,
) -> dict[str, Any]:
    return {
        "finding_id": "fd_" + short_hash([code, field["field_id"]]),
        "code": code,
        "severity": severity,
        "field_id": field["field_id"],
        "display_path": field["display_path"],
        "title": title,
        "message": message,
        "evidence": evidence,
        "threshold": dict(threshold) if threshold else None,
        "severity_reason": severity_reason,
        "limitations": limitations,
    }


def _fd_evidence(metrics: list[Mapping[str, Any]], *names: str) -> list[dict[str, Any]]:
    out = []
    for name in names:
        value = metric_value(metrics, name)
        if value is not None:
            out.append({"metric": name, "value": value})
    return out


def field_findings(
    field: Mapping[str, Any], thresholds: Mapping[str, Any], row_count: int | None
) -> list[dict[str, Any]]:
    """Return heuristic findings for one profiled field."""
    metrics = field.get("metrics", [])
    kind = field["type_kind"]
    found: list[dict[str, Any]] = []
    null_count = metric_value(metrics, "null_count")
    non_null = metric_value(metrics, "non_null_count")
    null_ratio = numeric_value(metrics, "null_ratio")

    if isinstance(row_count, int) and row_count > 0 and null_count == row_count:
        found.append(
            _fd_finding(
                "all_null",
                "warning",
                field,
                "All values are null",
                f"All {row_count} row(s) in scope are null for this field.",
                _fd_evidence(metrics, "null_count", "null_ratio"),
                None,
                "warning: a field without any value in scope is often unused or not populated "
                "for this population, but it may be legitimate (e.g. optional or future data).",
                "Applies to the analysed scope only (filters and snapshot).",
            )
        )
    elif null_ratio is not None and null_ratio >= thresholds["high_null_ratio"]:
        found.append(
            _fd_finding(
                "high_null_ratio",
                "info",
                field,
                "High null ratio",
                f"{null_ratio:.1%} of rows in scope are null.",
                _fd_evidence(metrics, "null_count", "null_ratio"),
                {"name": "high_null_ratio", "value": thresholds["high_null_ratio"]},
                "info: sparse fields are frequently legitimate (optional attributes).",
                "Nullity of nested fields includes rows where a parent struct is null; "
                "see null_count_parent_present.",
            )
        )

    all_equal = metric_value(metrics, "all_values_equal")
    if (
        all_equal is True
        and isinstance(non_null, int)
        and non_null >= thresholds["constant_min_rows"]
    ):
        found.append(
            _fd_finding(
                "possible_constant",
                "info",
                field,
                "Possibly constant",
                f"All {non_null} non-null value(s) in scope are equal.",
                [
                    {"metric": "all_values_equal", "value": True},
                    *_fd_evidence(metrics, "non_null_count"),
                ],
                {"name": "constant_min_rows", "value": thresholds["constant_min_rows"]},
                "info: constancy may be a property of the analysed scope (filters, snapshot).",
                "Exact for the analysed scope; other rows or future data may differ.",
            )
        )

    roles = {
        role["role"]: role for role in (field.get("semantics") or {}).get("candidate_roles", [])
    }
    if "categorical_candidate" in roles:
        found.append(
            _fd_finding(
                "possible_categorical",
                "info",
                field,
                "Possibly categorical",
                "Few distinct values relative to rows in scope.",
                roles["categorical_candidate"]["evidence"],
                {
                    "name": "categorical_max_distinct",
                    "value": thresholds["categorical_max_distinct"],
                },
                "info: descriptive signal only.",
                roles["categorical_candidate"]["limitations"],
            )
        )
    if "identifier_candidate" in roles:
        found.append(
            _fd_finding(
                "identifier_candidate",
                "info",
                field,
                "Identifier candidate",
                "Approximate distinct count is close to the number of non-null values.",
                roles["identifier_candidate"]["evidence"],
                {
                    "name": "identifier_min_distinct_ratio",
                    "value": thresholds["identifier_min_distinct_ratio"],
                },
                "info: approximate cardinality does not prove uniqueness.",
                roles["identifier_candidate"]["limitations"],
            )
        )

    json_profile = field.get("json_profile")
    if json_profile and json_profile.get("eligible_observations"):
        counts = json_profile["counts"]
        eligible = json_profile["eligible_observations"]
        structured = (counts["object"] + counts["array"]) / eligible
        if structured >= thresholds["json_min_ratio"]:
            found.append(
                _fd_finding(
                    "probable_json",
                    "info",
                    field,
                    "Probable JSON document",
                    f"{structured:.1%} of eligible sampled values parse as JSON objects or arrays; "
                    f"{counts['invalid']} sampled value(s) are not valid JSON.",
                    [
                        {"sample_json_counts": dict(counts)},
                        *_fd_evidence(metrics, "json_invalid_count"),
                    ],
                    {"name": "json_min_ratio", "value": thresholds["json_min_ratio"]},
                    "info: storage format observation; not a quality defect.",
                    "Sample-based unless json_invalid_count was measured over the full scope.",
                )
            )

    if kind in ("integer", "float", "decimal"):
        quantiles = metric_value(metrics, "quantiles")
        maximum = numeric_value(metrics, "max")
        minimum = numeric_value(metrics, "min")
        if isinstance(quantiles, list) and maximum is not None and minimum is not None:
            points = {}
            for item in quantiles:
                try:
                    points[item["probability"]] = float(item["value"])
                except (TypeError, ValueError):
                    continue
            q1, q3 = points.get(0.25), points.get(0.75)
            if q1 is not None and q3 is not None and q3 > q1:
                iqr = q3 - q1
                multiplier = thresholds["tail_iqr_multiplier"]
                upper = (maximum - q3) / iqr
                lower = (q1 - minimum) / iqr
                if upper > multiplier or lower > multiplier:
                    found.append(
                        _fd_finding(
                            "extreme_numeric_tail",
                            "info",
                            field,
                            "Extreme numeric tail",
                            "The observed minimum or maximum lies far outside the interquartile "
                            "range "
                            f"(upper tail {upper:.1f}x IQR, lower tail {lower:.1f}x IQR).",
                            _fd_evidence(metrics, "min", "max", "quantiles"),
                            {"name": "tail_iqr_multiplier", "value": multiplier},
                            "info: heavy tails are common in amounts and counts; review if "
                            "unexpected.",
                            "Quantiles are approximate; min/max are exact over finite values in "
                            "scope.",
                        )
                    )

    if kind == "array":
        max_size = metric_value(metrics, "max_size")
        if isinstance(max_size, int) and max_size >= thresholds["large_array_size"]:
            found.append(
                _fd_finding(
                    "large_arrays",
                    "warning",
                    field,
                    "Very large arrays",
                    f"The largest array in scope has {max_size} element(s).",
                    _fd_evidence(metrics, "max_size", "mean_size"),
                    {"name": "large_array_size", "value": thresholds["large_array_size"]},
                    "warning: very large arrays increase memory and processing cost of "
                    "downstream explode/flatten operations.",
                    "Exact over the analysed scope.",
                )
            )
    return found
