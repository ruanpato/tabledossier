"""Exact uniqueness: key selection, eligibility, budget, outcomes, proposals and the DQR section."""

import copy

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.keys import (
    plan_uniqueness,
    requested_keys,
    uniqueness_metrics,
    uniqueness_record,
)
from tabledossier.metrics import metric_value
from tabledossier.package import build_documents
from tabledossier.quality import suggest_rules

ORDERS = "analytics.orders"


def _config(**uniqueness):
    return deep_merge(
        default_config(), {"analysis_level": "deep", "deep": {"uniqueness": uniqueness}}
    )


def _orders(profile):
    return next(t for t in profile["tables"] if t["table_key"] == ORDERS)


def _constraint(name, kind, columns):
    return {
        "name": name,
        "constraint_type": kind,
        "columns": columns,
        "expression": None,
        "referenced": None,
        "enforcement": "not_enforced",
        "source": "information_schema",
    }


CONSTRAINTS = [
    _constraint("orders_uk", "unique", ["customer_id", "order_ts"]),
    _constraint("orders_pk", "primary_key", ["order_id"]),
    _constraint("orders_ck", "check", []),
]


def test_requested_keys_follow_sources_and_priority():
    candidates = [[{"kind": "field", "name": "order_id"}]]
    none = requested_keys(_config(), ORDERS, CONSTRAINTS, candidates)
    assert none == [], "every source is off by default"
    config = _config(
        keys=[
            {"table": "ANALYTICS.Orders", "columns": ["status", ["shipping", "method"]]},
            {"table": "analytics.other", "columns": ["x"]},
        ],
        declared_keys=True,
        identifier_candidates=True,
    )
    keys = requested_keys(config, ORDERS, CONSTRAINTS, candidates)
    assert [key["origin"] for key in keys] == [
        "configured",
        "declared_primary_key",
        "declared_unique",
        "identifier_candidate",
    ]
    assert keys[0]["requested"] == ["status", "shipping.method"]
    assert keys[1]["name"] == "orders_pk"


def test_plan_merges_duplicates_and_explains_ineligible_keys(deep_profile):
    orders = _orders(deep_profile)
    config = _config(
        keys=[
            {"table": ORDERS, "columns": ["order_id"], "id": "by_config"},
            {"table": ORDERS, "columns": ["items"]},
            {"table": ORDERS, "columns": ["nope"]},
            {"table": ORDERS, "columns": ["status", "status"]},
            {"table": ORDERS, "columns": [["shipping", "address", "city"], "status"]},
        ],
        declared_keys=True,
        identifier_candidates=True,
    )
    element = [
        {"kind": "field", "name": "items"},
        {"kind": "array_element"},
        {"kind": "field", "name": "sku"},
    ]
    requested = requested_keys(
        config, ORDERS, CONSTRAINTS, [[{"kind": "field", "name": "order_id"}], element]
    )
    plan = plan_uniqueness(orders["schema"], requested, config)
    by_columns = {tuple(key["columns"]): key for key in plan["keys"]}
    merged = by_columns[("order_id",)]
    assert merged["origins"] == ["configured", "declared_primary_key", "identifier_candidate"]
    assert merged["names"] == ["by_config", "orders_pk"]
    assert by_columns[("items",)]["status"] == "not_eligible"
    assert "not in the documented schema tree" in by_columns[("nope",)]["reason"]
    assert "more than once" in by_columns[("status", "status")]["reason"]
    assert "inside an array or map" in by_columns[("items[].sku",)]["reason"]
    assert by_columns[("shipping.address.city", "status")]["status"] == "planned"
    assert by_columns[("customer_id", "order_ts")]["status"] == "planned"
    assert len({key["key_id"] for key in plan["keys"]}) == len(plan["keys"])


@pytest.mark.parametrize(
    ("max_keys", "max_passes", "sizes"),
    [(5, 1, [5]), (5, 2, [3, 2]), (5, 5, [1, 1, 1, 1, 1]), (2, 3, [1, 1]), (4, 3, [2, 2])],
)
def test_budget_packs_keys_into_passes(deep_profile, max_keys, max_passes, sizes):
    orders = _orders(deep_profile)
    columns = ["order_id", "customer_id", "status", "amount", "order_ts"]
    config = _config(
        keys=[{"table": ORDERS, "columns": [column]} for column in columns],
        max_keys=max_keys,
        max_passes=max_passes,
    )
    plan = plan_uniqueness(orders["schema"], requested_keys(config, ORDERS, [], []), config)
    assert [len(members) for members in plan["passes"]] == sizes
    planned = [key for key in plan["keys"] if key["operation_id"]]
    assert len(planned) == min(max_keys, 5)
    assert len(plan["limited"]) == 5 - len(planned)
    for key in plan["keys"][len(planned) :]:
        assert key["status"] == "not_computed" and "max_keys" in key["reason"]


@pytest.mark.parametrize(
    ("raw", "outcome"),
    [
        ({"rows": 0}, "empty"),
        ({"rows": 3, "null_rows": 3}, "empty"),
        ({"rows": 5, "distinct": 5, "max_n": 1}, "unique"),
        ({"rows": 5, "null_rows": 1, "distinct": 4, "max_n": 1}, "unique_non_null"),
        ({"rows": 5, "distinct": 3, "dup_groups": 1, "dup_rows": 3, "max_n": 3}, "duplicates"),
    ],
)
def test_outcomes_follow_counts(raw, outcome):
    metrics, result = uniqueness_metrics(raw, "full_snapshot")
    assert result == outcome
    names = [metric["name"] for metric in metrics]
    assert names[:7] == [
        "rows_in_scope",
        "rows_with_null_key",
        "rows_with_complete_key",
        "distinct_keys",
        "duplicate_key_groups",
        "rows_in_duplicate_groups",
        "surplus_duplicate_rows",
    ]
    assert all(m["accuracy"] == "exact" for m in metrics if m["status"] == "measured")
    complete = metric_value(metrics, "rows_with_complete_key")
    if complete == 0:
        assert metric_value(metrics, "max_rows_per_key") is None


def test_failed_pass_leaves_keys_unmeasured(deep_profile):
    orders = _orders(deep_profile)
    config = _config(keys=[{"table": ORDERS, "columns": ["order_id"]}])
    plan = plan_uniqueness(orders["schema"], requested_keys(config, ORDERS, [], []), config)
    record = uniqueness_record(
        plan, {}, {0: "uniqueness pass failed: X"}, config=config, scope="s", requested_any=True
    )
    assert record["keys"][0]["status"] == "error"
    assert record["keys"][0]["metrics"] == [] and record["keys"][0]["outcome"] is None
    empty = uniqueness_record(
        plan_uniqueness(orders["schema"], [], config),
        {},
        {},
        config=config,
        scope="s",
        requested_any=False,
    )
    assert empty["keys"] == [] and "No key was requested" in empty["notes"][0]


def _with_keys(deep_profile, outcomes):
    """Attach measured keys (order_id, then customer_id + status) with the given outcomes."""
    profile = copy.deepcopy(deep_profile)
    orders = _orders(profile)
    config = _config(
        keys=[
            {"table": ORDERS, "columns": ["order_id"]},
            {"table": ORDERS, "columns": ["customer_id", "status"]},
        ]
    )
    plan = plan_uniqueness(orders["schema"], requested_keys(config, ORDERS, [], []), config)
    samples = {
        "unique": {"rows": 1000, "distinct": 1000, "max_n": 1},
        "duplicates": {"rows": 1000, "distinct": 998, "dup_groups": 2, "dup_rows": 4, "max_n": 2},
    }
    results = {index: samples[outcome] for index, outcome in enumerate(outcomes)}
    orders["uniqueness"] = uniqueness_record(
        plan, results, {}, config=config, scope="full_snapshot", requested_any=True
    )
    orders["operations"]["planned"].append(
        {
            "operation_id": "op_uniqueness_1",
            "kind": "uniqueness_pass",
            "description": "test",
            "reads_user_data": True,
        }
    )
    return profile, orders


def test_unique_rules_cite_exact_evidence(deep_profile):
    _, orders = _with_keys(deep_profile, ["unique", "unique"])
    rules = [r for r in suggest_rules(orders, default_config()["thresholds"]) if r["rule_type"]]
    unique = [r for r in rules if r["rule_type"] == "unique"]
    order_id = next(r for r in unique if r["column"] == "order_id")
    assert order_id["rationale"].startswith("Exact uniqueness check: no duplicate among 1000")
    assert order_id["evidence"][0] == {
        "check": "exact_uniqueness",
        "key_id": orders["uniqueness"]["keys"][0]["key_id"],
        "scope": "full_snapshot",
    }
    composite = next(r for r in unique if r["column"] is None)
    assert composite["parameters"] == {"columns": ["customer_id", "status"]}
    _, orders = _with_keys(deep_profile, ["duplicates", "duplicates"])
    rules = suggest_rules(orders, default_config()["thresholds"])
    assert not [r for r in rules if r["rule_type"] == "unique" and r["column"] == "order_id"]
    assert not [r for r in rules if r["rule_type"] == "unique" and r["column"] is None]


def test_quality_report_shows_exact_uniqueness(deep_profile):
    report = build_documents(deep_profile)["quality_report.md"]
    assert "## 7. Uniqueness (exact)" in report and "## 8. Limitations" in report
    profile, _ = _with_keys(deep_profile, ["unique", "duplicates"])
    report = build_documents(profile)["quality_report.md"]
    section = report.split("## 7. Uniqueness (exact)")[1].split("## 8.")[0]
    assert "| analytics.orders | `order_id` | configured | unique | 1,000 | 0 | 1,000 | 0 |" in (
        section
    )
    assert "**duplicates**" in section and "NULLs are not treated as equal" in section
    assert "established only for the keys of section 7" in report


def test_standard_report_keeps_uniqueness_unestablished(demo_profile):
    report = build_documents(demo_profile)["quality_report.md"]
    assert "no exact uniqueness check ran" in report
    assert "## 7. Uniqueness" not in report and "## 6. Limitations" in report
