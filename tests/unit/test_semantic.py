import pytest

from tabledossier.config import default_config
from tabledossier.metrics import measured
from tabledossier.semantic import (
    candidate_roles,
    detect_formats,
    infer_format,
    json_shape,
    value_concentration,
    wilson_interval,
)

SEM = default_config()["semantic"]
THRESHOLDS = default_config()["thresholds"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('{"a": 1}', {"json_object"}),
        (" [1, 2] ", {"json_array"}),
        ('{"a": NaN}', set()),
        ("{broken", set()),
        ("123e4567-e89b-12d3-a456-426614174000", {"uuid"}),
        ("-12.5e3", {"numeric_string"}),
        ("1", {"numeric_string", "boolean_string"}),
        ("Yes", {"boolean_string"}),
        ("2026-02-28", {"iso_date"}),
        ("2026-02-30", set()),
        ("2026-02-28T10:15:00Z", {"iso_timestamp"}),
        ("2026-02-28 10:15:00.123456789+0300", {"iso_timestamp"}),
        ("2026-02-28T25:00:00", set()),
        ("https://example.com/a?b=c", {"url"}),
        ("user@example.org", {"email_candidate"}),
        ("plain text", set()),
    ],
)
def test_detectors(value, expected):
    assert detect_formats(value) == expected


def test_wilson_interval_known_values():
    interval = wilson_interval(95, 100)
    assert interval["method"] == "wilson_score"
    assert interval["low"] == pytest.approx(0.8882, abs=1e-3)
    assert interval["high"] == pytest.approx(0.9785, abs=1e-3)
    assert wilson_interval(0, 0) is None
    full = wilson_interval(10, 10)
    assert full["high"] == 1.0 and full["low"] < 1.0


def _infer(values, **kwargs):
    return infer_format(
        values,
        sql_nulls=kwargs.get("nulls", 0),
        truncated=kwargs.get("truncated", 0),
        sample_method="prefix",
        semantic_config=SEM,
    )


def test_infer_format_statuses():
    uuids = [f"123e4567-e89b-12d3-a456-4266141740{i:02d}" for i in range(40)]
    detected = _infer(uuids)
    assert detected["status"] == "detected" and detected["format"] == "uuid"
    assert detected["candidates"][0]["interval"]["method"] == "wilson_score"
    assert any("biased" in note for note in detected["limitations"])
    flags = ["0", "1"] * 20
    ambiguous = _infer(flags)
    assert ambiguous["status"] == "ambiguous" and ambiguous["format"] is None
    mixed = _infer(uuids[:20] + ["free text"] * 20)
    assert mixed["status"] == "mixed"
    unknown = _infer(["free text"] * 40)
    assert unknown["status"] == "unknown"
    few = _infer(uuids[:5], truncated=3, nulls=2)
    assert few["status"] == "insufficient_data"
    assert few["excluded"] == {"sql_null": 2, "truncated": 3, "blank": 0}


def test_blank_values_are_excluded_from_denominator():
    result = _infer(["", "   "] + ["2026-01-01"] * 30)
    assert result["eligible_observations"] == 30
    assert result["excluded"]["blank"] == 2
    assert result["format"] == "iso_date"


def test_json_shape_denominators_and_keys():
    values = ['{"a": 1, "b": "x"}'] * 8 + ['{"a": "text"}', "null", "[1]", "42", "{broken"]
    shape = json_shape(
        values, sql_nulls=3, truncated=2, include_key_names=True, sample_method="prefix"
    )
    assert shape["counts"] == {
        "object": 9,
        "array": 1,
        "scalar": 1,
        "json_null_literal": 1,
        "invalid": 1,
    }
    assert shape["excluded"] == {"sql_null": 3, "truncated": 2}
    assert shape["eligible_observations"] == 13
    assert shape["heterogeneity"]["keys_with_conflicting_types"] == 1
    assert shape["keys"][0]["key"] == "a" and shape["keys"][0]["present_in"] == 9
    redacted = json_shape(
        values, sql_nulls=0, truncated=0, include_key_names=False, sample_method="prefix"
    )
    assert redacted["keys"] is None and "value policy" in redacted["keys_omitted_reason"]
    maplike = json_shape(
        ["{" + ",".join(f'"k{i}_{j}": 1' for j in range(3)) + "}" for i in range(30)],
        sql_nulls=0,
        truncated=0,
        include_key_names=True,
        sample_method="prefix",
    )
    assert maplike["keys"] is None and "map-like" in maplike["keys_omitted_reason"]


def test_concentration_has_no_labels():
    summary = value_concentration(["secret-a"] * 3 + ["secret-b"])
    assert summary["top_1_share"] == 0.75
    assert "secret" not in repr(summary)


def _metric(name, value):
    return measured(
        name,
        value,
        unit="rows",
        scope="full_table",
        accuracy="exact",
        source="aggregate",
        method="m",
    )


def test_candidate_roles_respect_types_and_nulls():
    metrics = [_metric("non_null_count", 1000), _metric("approx_distinct_count", 990)]
    ratio_metric = measured(
        "null_ratio",
        0.0,
        value_type="ratio",
        unit="ratio",
        scope="full_table",
        accuracy="exact",
        source="derived",
        method="m",
        denominator=1000,
        denominator_unit="rows",
    )
    roles = candidate_roles("integer", [*metrics, ratio_metric], None, THRESHOLDS, "bigint")
    assert roles[0]["role"] == "identifier_candidate"
    assert "do not prove uniqueness" in roles[0]["limitations"]
    assert (
        candidate_roles("decimal", [*metrics, ratio_metric], None, THRESHOLDS, "decimal(12,2)")
        == []
    )
    assert candidate_roles("decimal", [*metrics, ratio_metric], None, THRESHOLDS, "decimal(18,0)")
    sparse = measured(
        "null_ratio",
        0.5,
        value_type="ratio",
        unit="ratio",
        scope="full_table",
        accuracy="exact",
        source="derived",
        method="m",
        denominator=2000,
        denominator_unit="rows",
    )
    assert candidate_roles("string", [*metrics, sparse], None, THRESHOLDS) == []
    categorical = [
        _metric("non_null_count", 500),
        _metric("approx_distinct_count", 4),
        ratio_metric,
    ]
    assert (
        candidate_roles("string", categorical, None, THRESHOLDS)[0]["role"]
        == "categorical_candidate"
    )
    assert candidate_roles("boolean", categorical, None, THRESHOLDS) == []
