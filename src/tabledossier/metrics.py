"""Metric records of the canonical profile contract.

Part of the embedded runtime (standard library only).

Every metric states four independent axes:

* ``source``   - where the number came from (aggregate, sample, catalog...);
* ``scope``    - which population it describes (filtered snapshot, sample...);
* ``accuracy`` - exact, approximate, as recorded by metadata, or unknown;
* ``status``   - measured, or why there is no value.

A status other than ``measured`` never carries a value, so "not computed" can
never be confused with zero.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from tabledossier.jsonutil import encode_scalar

METRIC_STATUSES = (
    "measured",
    "not_computed",
    "unsupported",
    "insufficient_data",
    "redacted",
    "error",
    "unavailable",
)
METRIC_SCOPES = (
    "table_metadata",
    "full_table",
    "filtered_table",
    "full_snapshot",
    "filtered_snapshot",
    "sample",
)
METRIC_ACCURACIES = ("exact", "approximate", "as_recorded", "unknown")
METRIC_SOURCES = (
    "catalog_metadata",
    "table_statistics",
    "table_history",
    "aggregate",
    "sample",
    "derived",
)


def measured(
    name: str,
    value: Any,
    *,
    unit: str | None,
    scope: str,
    accuracy: str,
    source: str,
    method: str,
    value_type: str | None = None,
    denominator: int | None = None,
    denominator_unit: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a measured metric, encoding ``value`` per the JSON conventions."""
    if value is None:
        raise ValueError(f"measured metric {name!r} requires a value")
    if value_type is None:
        encoded, value_type = encode_scalar(value)
    elif value_type in ("ratio", "quantiles"):
        encoded = value
    else:
        encoded, _ = encode_scalar(value)
    metric: dict[str, Any] = {
        "name": name,
        "status": "measured",
        "value": encoded,
        "value_type": value_type,
        "unit": unit,
        "scope": scope,
        "accuracy": accuracy,
        "source": source,
        "method": method,
    }
    if denominator is not None:
        metric["denominator"] = denominator
        metric["denominator_unit"] = denominator_unit
    if details:
        metric["details"] = dict(details)
    return metric


def not_measured(
    name: str,
    status: str,
    reason: str,
    *,
    scope: str,
    source: str,
    method: str | None = None,
    unit: str | None = None,
) -> dict[str, Any]:
    """Return a metric without a value, stating why (``status`` and ``reason``)."""
    if status == "measured" or status not in METRIC_STATUSES:
        raise ValueError(f"invalid non-measured status: {status!r}")
    metric: dict[str, Any] = {
        "name": name,
        "status": status,
        "value": None,
        "unit": unit,
        "scope": scope,
        "accuracy": "unknown",
        "source": source,
        "reason": reason,
    }
    if method:
        metric["method"] = method
    return metric


def ratio(
    name: str,
    numerator: int | None,
    denominator: int | None,
    *,
    scope: str,
    source: str,
    method: str,
    denominator_unit: str,
    accuracy: str = "exact",
) -> dict[str, Any]:
    """Return a ratio metric, or ``insufficient_data`` when the denominator is 0."""
    if numerator is None or denominator is None:
        return not_measured(
            name, "not_computed", "an input count is not available", scope=scope, source=source
        )
    if denominator == 0:
        return not_measured(
            name,
            "insufficient_data",
            f"undefined: no {denominator_unit} in scope (denominator is 0)",
            scope=scope,
            source=source,
            method=method,
            unit="ratio",
        )
    return measured(
        name,
        numerator / denominator,
        value_type="ratio",
        unit="ratio",
        scope=scope,
        accuracy=accuracy,
        source=source,
        method=method,
        denominator=denominator,
        denominator_unit=denominator_unit,
    )


def find_metric(metrics: Iterable[Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    """Return the first metric called ``name`` or ``None``."""
    for metric in metrics:
        if metric.get("name") == name:
            return metric
    return None


def metric_value(metrics: Iterable[Mapping[str, Any]], name: str) -> Any:
    """Return the value of a *measured* metric, else ``None``."""
    metric = find_metric(metrics, name)
    if metric is None or metric.get("status") != "measured":
        return None
    return metric.get("value")


def numeric_value(metrics: Iterable[Mapping[str, Any]], name: str) -> float | None:
    """Return a measured metric as a finite float (decimals parsed), else ``None``."""
    value = metric_value(metrics, name)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return None
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    return None
