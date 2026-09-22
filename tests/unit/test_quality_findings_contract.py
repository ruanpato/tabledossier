import copy

from tabledossier.config import default_config
from tabledossier.contract import profile_invariant_errors, validate_annotations, validate_profile
from tabledossier.findings import field_findings
from tabledossier.metrics import measured, not_measured
from tabledossier.quality import evaluate_checks, suggest_rules, suggested_rules_document
from tabledossier.validation import check_profile

THRESHOLDS = default_config()["thresholds"]


def _m(name, value, **kw):
    return measured(
        name,
        value,
        unit=kw.get("unit", "rows"),
        scope="full_table",
        accuracy="exact",
        source=kw.get("source", "aggregate"),
        method="m",
        value_type=kw.get("value_type"),
        denominator=kw.get("denominator"),
        denominator_unit=kw.get("denominator_unit"),
    )


def _field(kind, metrics, path="col"):
    return {
        "field_id": "f_000000000000",
        "display_path": path,
        "type_kind": kind,
        "physical_type": kind,
        "profiled": True,
        "metrics": metrics,
        "semantics": None,
        "json_profile": None,
    }


def test_all_null_is_warning_and_high_nulls_is_info():
    all_null = _field(
        "string",
        [
            _m("null_count", 10),
            _m("non_null_count", 0),
            _m("null_ratio", 1.0, value_type="ratio", denominator=10, denominator_unit="rows"),
        ],
    )
    codes = {(f["code"], f["severity"]) for f in field_findings(all_null, THRESHOLDS, 10)}
    assert codes == {("all_null", "warning")}
    sparse = _field(
        "string",
        [
            _m("null_count", 6),
            _m("non_null_count", 4),
            _m("null_ratio", 0.6, value_type="ratio", denominator=10, denominator_unit="rows"),
        ],
    )
    findings = field_findings(sparse, THRESHOLDS, 10)
    assert [(f["code"], f["severity"]) for f in findings] == [("high_null_ratio", "info")]
    assert findings[0]["threshold"] == {"name": "high_null_ratio", "value": 0.5}
    assert findings[0]["evidence"]


def test_empty_table_produces_no_findings():
    empty = _field(
        "string",
        [
            _m("null_count", 0),
            _m("non_null_count", 0),
            not_measured(
                "null_ratio", "insufficient_data", "no rows", scope="full_table", source="derived"
            ),
        ],
    )
    assert field_findings(empty, THRESHOLDS, 0) == []


def test_extreme_tail_and_large_arrays():
    quantiles = [{"probability": p, "value": v} for p, v in ((0.25, 10), (0.5, 20), (0.75, 30))]
    numeric = _field(
        "integer",
        [_m("min", 0), _m("max", 10_000), _m("quantiles", quantiles, value_type="quantiles")],
    )
    assert [f["code"] for f in field_findings(numeric, THRESHOLDS, 100)] == ["extreme_numeric_tail"]
    arrays = _field("array", [_m("max_size", 5000, unit="elements")])
    assert [f["code"] for f in field_findings(arrays, THRESHOLDS, 100)] == ["large_arrays"]


def test_checks_statuses(demo_profile):
    orders = next(t for t in demo_profile["tables"] if t["table_key"] == "analytics.orders")
    checks = [
        {"id": "rows", "type": "min_row_count", "min": 1},
        {"id": "too_many", "type": "max_row_count", "max": 1},
        {"id": "nulls", "type": "max_null_ratio", "column": "customer_id", "max": 0.0},
        {"id": "missing", "type": "max_null_ratio", "column": "nope", "max": 0.0},
        {
            "id": "nested",
            "type": "max_null_count",
            "column": ["shipping", "address", "city"],
            "max": 10,
        },
        {"id": "inside", "type": "max_null_ratio", "column": "items", "max": 0.5},
        {"id": "range", "type": "value_range", "column": "amount", "min": 0},
        {"id": "wrongtype", "type": "value_range", "column": "status", "min": 0},
    ]
    results = {r["check_id"]: r["status"] for r in evaluate_checks(orders, checks)}
    assert results == {
        "rows": "pass",
        "too_many": "fail",
        "nulls": "pass",
        "missing": "error",
        "nested": "fail",
        "inside": "pass",
        "range": "pass",
        "wrongtype": "error",
    }
    returns = next(t for t in demo_profile["tables"] if t["table_key"] == "analytics.returns")
    empty = evaluate_checks(
        returns, [{"id": "r", "type": "max_null_ratio", "column": "reason", "max": 0.1}]
    )
    assert empty[0]["status"] == "not_evaluated"


def test_suggestions_are_proposals_only(demo_profile):
    for table in demo_profile["tables"]:
        for rule in suggest_rules(table, THRESHOLDS):
            assert rule["status"] == "proposed" and rule["requires_review"] is True
    document = suggested_rules_document(demo_profile)
    assert document["kind"] == "tabledossier.suggested_rules"
    assert "not applied" in document["notice"]
    categorical = [r for r in document["rules"] if r["rule_type"] == "accepted_values"]
    assert categorical and all(r["parameters"]["values"] is None for r in categorical)


def test_demo_profile_is_valid(demo_profile, schemas):
    assert validate_profile(demo_profile, schemas["profile"]) == []
    assert check_profile(demo_profile) == []


def test_invariants_catch_inconsistencies(demo_profile):
    broken = copy.deepcopy(demo_profile)
    broken["run"]["status"] = "partial"
    assert any("inconsistent" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(demo_profile)
    broken["tables"][0]["status"] = "failed"
    broken["tables"][0]["errors"] = []
    errors = profile_invariant_errors(broken)
    assert any("must report at least one error" in e for e in errors)
    broken = copy.deepcopy(demo_profile)
    field = next(f for f in broken["tables"][0]["field_profiles"] if f["metrics"])
    ratio_metric = next(m for m in field["metrics"] if m["name"] == "null_ratio")
    ratio_metric["value"] = 1.5
    assert any("between 0 and 1" in e for e in profile_invariant_errors(broken))


def test_incompatible_versions_are_rejected_clearly(demo_profile, schemas):
    demo_profile["schema_version"] = "2.0"
    errors = check_profile(demo_profile)
    assert len(errors) == 1 and "not supported" in errors[0] and "1.0, 1.1" in errors[0]
    assert (
        "not a tabledossier.profile" in validate_profile({"kind": "other"}, schemas["profile"])[0]
    )


def test_annotations_validation(demo_annotations, schemas):
    assert validate_annotations(demo_annotations, schemas["annotations"]) == []
    bad = copy.deepcopy(demo_annotations)
    bad["tables"]["analytics.customers"]["columns"]["bad path."] = {"description": "x"}
    assert validate_annotations(bad, schemas["annotations"])


def _element_field(profile):
    for table in profile["tables"]:
        for field in table["field_profiles"]:
            if field.get("element_context") and field["metrics"]:
                return table, field
    raise AssertionError("no element field in the deep profile")


def test_deep_profile_is_valid_1_1(deep_profile, schemas):
    assert deep_profile["schema_version"] == "1.1"
    assert validate_profile(deep_profile, schemas["profile"]) == []
    assert check_profile(deep_profile) == []


def test_1_1_invariants_keep_element_metrics_off_rows(deep_profile):
    broken = copy.deepcopy(deep_profile)
    _, field = _element_field(broken)
    metric = next(m for m in field["metrics"] if m["name"] == "null_count")
    metric["denominator_unit"] = "rows"
    assert any("never count rows" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(deep_profile)
    _, field = _element_field(broken)
    metric = next(m for m in field["metrics"] if m["name"] == "null_count")
    metric["value"] = 10**9
    assert any("exceeds element_count" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(deep_profile)
    table = next(t for t in broken["tables"] if t["table_key"] == "analytics.orders")
    top = next(f for f in table["field_profiles"] if f["display_path"] == "items")
    top["element_context"] = dict(_element_field(broken)[1]["element_context"])
    assert any("outside any collection" in e for e in profile_invariant_errors(broken))


def test_1_1_invariants_check_json_path_catalogues(deep_profile):
    broken = copy.deepcopy(deep_profile)
    events = next(t for t in broken["tables"] if t["table_key"] == "analytics.order_events")
    payload = next(f for f in events["field_profiles"] if f["display_path"] == "payload")
    payload["json_paths"]["paths"][0]["present_in"] = payload["json_paths"]["documents"] + 1
    payload["json_paths"]["paths_listed"] += 1
    errors = profile_invariant_errors(broken)
    assert any("more documents than sampled" in e for e in errors)
    assert any("paths_listed does not match" in e for e in errors)


def test_profile_1_0_is_valid_with_its_frozen_schema(profile_1_0, schemas):
    assert validate_profile(profile_1_0, schemas["profile-1.0"]) == []
    assert check_profile(profile_1_0) == []
    assert validate_profile(profile_1_0, schemas["profile"]), "the 1.1 schema requires 1.1"
