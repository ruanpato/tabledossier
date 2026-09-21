import datetime
import json
from decimal import Decimal

import pytest

from tabledossier.jsonutil import canonical_json, encode_scalar, fingerprint, format_utc
from tabledossier.metrics import measured, metric_value, not_measured, numeric_value, ratio


@pytest.mark.parametrize(
    ("value", "encoded", "value_type"),
    [
        (True, True, "boolean"),
        (12, 12, "integer"),
        (2**70, 2**70, "integer"),
        (1.5, 1.5, "float"),
        (float("nan"), "NaN", "float"),
        (float("inf"), "Infinity", "float"),
        (float("-inf"), "-Infinity", "float"),
        (Decimal("12.3400"), "12.3400", "decimal"),
        (datetime.date(2026, 1, 31), "2026-01-31", "date"),
        (datetime.datetime(2026, 1, 31, 10, 0), "2026-01-31T10:00:00", "timestamp_ntz"),
        (
            datetime.datetime(2026, 1, 31, 10, 0, tzinfo=datetime.timezone.utc),
            "2026-01-31T10:00:00Z",
            "timestamp",
        ),
        ("text", "text", "string"),
    ],
)
def test_encode_scalar(value, encoded, value_type):
    assert encode_scalar(value) == (encoded, value_type)


def test_encoded_values_are_valid_json():
    values = [float("nan"), Decimal("1E+2"), datetime.date(2026, 1, 1)]
    text = json.dumps([encode_scalar(v)[0] for v in values], allow_nan=False)
    assert json.loads(text) == ["NaN", "1E+2", "2026-01-01"]


def test_canonical_json_and_fingerprint_are_deterministic():
    a = {"b": 1, "a": [1, 2], "c": {"y": None, "x": "é"}}
    b = {"c": {"x": "é", "y": None}, "a": [1, 2], "b": 1}
    assert canonical_json(a) == canonical_json(b)
    assert fingerprint(a) == fingerprint(b)
    assert fingerprint(a).startswith("sha256:")
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_format_utc_requires_aware_datetime():
    moment = datetime.datetime(2026, 9, 21, 18, 30, 1, 123456, tzinfo=datetime.timezone.utc)
    assert format_utc(moment) == "2026-09-21T18:30:01.123Z"
    with pytest.raises(ValueError):
        format_utc(datetime.datetime(2026, 1, 1))


def test_not_measured_is_never_zero():
    metric = not_measured(
        "min", "insufficient_data", "no values", scope="full_table", source="aggregate"
    )
    assert metric["value"] is None
    assert metric["status"] == "insufficient_data"
    assert metric_value([metric], "min") is None
    with pytest.raises(ValueError):
        not_measured("x", "measured", "nope", scope="full_table", source="aggregate")


def test_ratio_with_zero_denominator_is_undefined():
    metric = ratio(
        "null_ratio",
        0,
        0,
        scope="full_table",
        source="derived",
        method="m",
        denominator_unit="rows",
    )
    assert metric["status"] == "insufficient_data"
    assert metric["value"] is None
    ok = ratio(
        "null_ratio",
        5,
        20,
        scope="full_table",
        source="derived",
        method="m",
        denominator_unit="rows",
    )
    assert ok["value"] == 0.25
    assert ok["denominator"] == 20


def test_measured_requires_a_value_and_preserves_types():
    with pytest.raises(ValueError):
        measured(
            "x", None, unit=None, scope="sample", accuracy="exact", source="sample", method="m"
        )
    metric = measured(
        "max",
        Decimal("99999999999999999999.99"),
        unit=None,
        scope="full_table",
        accuracy="exact",
        source="aggregate",
        method="m",
    )
    assert metric["value"] == "99999999999999999999.99"
    assert metric["value_type"] == "decimal"
    assert numeric_value([metric], "max") == pytest.approx(1e20)
