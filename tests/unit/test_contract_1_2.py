"""Contract 1.2 additions: uniqueness, validation evidence and hypotheses (schema + invariants).

The records are built with the same functions the notebook uses, from synthetic counts, so the
schema, the embedded validator, ``jsonschema`` and the invariants are exercised together.
"""

import copy

from jsonschema import Draft202012Validator

from tabledossier.config import deep_merge, default_config
from tabledossier.contract import profile_invariant_errors, validate_profile
from tabledossier.integrity import (
    hypotheses_record,
    hypothesis_evidence,
    type_compatibility,
    validation_detail,
)
from tabledossier.keys import plan_uniqueness, requested_keys, uniqueness_record
from tabledossier.planning import iter_nodes
from tabledossier.schemacheck import schema_errors
from tabledossier.validation import check_profile

KEYS = [
    {"table": "analytics.orders", "columns": ["order_id"], "id": "orders_pk"},
    {"table": "analytics.orders", "columns": ["customer_id", "status"]},
    {"table": "analytics.orders", "columns": ["items"]},
]


def _config():
    return deep_merge(
        default_config(),
        {
            "analysis_level": "deep",
            "deep": {
                "uniqueness": {"keys": KEYS},
                "referential": {"configured": True},
                "relationship_hypotheses": {"enabled": True},
            },
        },
    )


def _table(profile, key):
    return next(t for t in profile["tables"] if t["table_key"] == key)


def _node(table, path):
    return next(n for n in iter_nodes(table["schema"]["fields"]) if n["display_path"] == path)


def _side(table, scope=None):
    return {
        "table": table["table_key"],
        "scope": scope or table["scope"]["scope_label"],
        "consistency_mode": table["consistency"]["mode"],
        "delta_version": table["consistency"]["delta_version"],
    }


def _op(op_id, kind):
    return {
        "operation_id": op_id,
        "kind": kind,
        "description": "synthetic",
        "reads_user_data": True,
    }


def integrity_profile(deep_profile):
    """A deep profile with synthetic 1.2 records: two keys, one violated FK, one hypothesis."""
    profile = copy.deepcopy(deep_profile)
    config = _config()
    orders = _table(profile, "analytics.orders")
    customers = _table(profile, "analytics.customers")
    scope = orders["scope"]["scope_label"]
    requested = requested_keys(config, "analytics.orders", [], [])
    plan = plan_uniqueness(orders["schema"], requested, config)
    results = {
        0: {"rows": 1000, "null_rows": 0, "distinct": 1000, "dup_groups": 0, "dup_rows": 0},
        1: {"rows": 1000, "null_rows": 4, "distinct": 990, "dup_groups": 6, "dup_rows": 12},
    }
    results[0]["max_n"], results[1]["max_n"] = 1, 2
    orders["uniqueness"] = uniqueness_record(
        plan, results, {}, config=config, scope=scope, requested_any=True
    )
    orders["operations"]["planned"].append(_op("op_uniqueness_1", "uniqueness_pass"))
    relationship = next(r for r in profile["relationships"] if r["name"] == "orders_customer")
    compatibility = type_compatibility(
        [_node(orders, "customer_id")], [_node(customers, "customer_id")]
    )
    raw = {"rows": 1000, "null_rows": 0, "orphans": 5, "t_rows": 500, "t_distinct": 500}
    relationship["validation"] = "violated"
    relationship["validation_detail"] = validation_detail(
        {**raw, "t_null_rows": 0, "t_dup_groups": 0},
        mode="full_scope",
        sample_rows=None,
        from_info=_side(orders),
        to_info=_side(customers, "full_snapshot"),
        compatibility=compatibility,
        operation_id="op_referential_1",
    )
    orders["operations"]["planned"].append(_op("op_referential_1", "referential_check"))
    metrics, _ = hypothesis_evidence(
        {"rows": 1000, "null_rows": 0, "orphans": 0, "t_rows": 1000, "t_distinct": 1000},
        scope="sample",
        target_scope="full_snapshot",
    )
    events = _table(profile, "analytics.order_events")
    hypothesis = {
        "hypothesis_id": "hyp_1",
        "status": "hypothesis",
        "from": {"table": "analytics.order_events", "columns": ["event_id"]},
        "to": {"table": "analytics.orders", "columns": ["order_id"]},
        "cardinality": None,
        "evidence": {
            "inclusion_scope": "sample",
            "from": _side(events, "sample"),
            "to": _side(orders, "full_snapshot"),
            "metrics": metrics,
            "target_key_id": orders["uniqueness"]["keys"][0]["key_id"],
            "target_key_unique": True,
            "type_compatibility": type_compatibility(
                [_node(events, "event_id")], [_node(orders, "order_id")]
            ),
            "range_relation": "overlapping",
            "range_basis": "lengths",
        },
        "operation_id": "op_hypothesis_1",
        "limitations": ["synthetic"],
    }
    events["operations"]["planned"].append(_op("op_hypothesis_1", "relationship_hypothesis_check"))
    profile["relationship_hypotheses"] = hypotheses_record(
        config, None, [hypothesis], evaluated=1, rejected={}, reason=None
    )
    profile["summary"]["relationship_hypotheses"] = 1
    return profile


def _agree(document, schema):
    formal = not list(Draft202012Validator(schema).iter_errors(document))
    local = not schema_errors(document, schema)
    assert formal == local, (formal, schema_errors(document, schema)[:3])
    return local


def _mutations(document):
    yield {**document, "unexpected": 1}
    for key in list(document):
        broken = copy.deepcopy(document)
        del broken[key]
        yield broken
        wrong = copy.deepcopy(document)
        wrong[key] = 12345 if not isinstance(document[key], int) else "text"
        yield wrong


def test_synthetic_1_2_records_are_valid(deep_profile, schemas):
    profile = integrity_profile(deep_profile)
    assert validate_profile(profile, schemas["profile"]) == []
    assert check_profile(profile) == []
    keys = _table(profile, "analytics.orders")["uniqueness"]["keys"]
    assert [key["status"] for key in keys] == ["measured", "measured", "not_eligible"]
    assert [key["outcome"] for key in keys[:2]] == ["unique", "duplicates"]
    assert "cannot be compared exactly" in keys[2]["reason"]


def test_validators_agree_on_1_2_additions(deep_profile, schemas):
    schema = schemas["profile"]
    profile = integrity_profile(deep_profile)
    assert _agree(profile, schema)
    record = _table(profile, "analytics.orders")["uniqueness"]
    for variant in _mutations(record):
        broken = copy.deepcopy(profile)
        _table(broken, "analytics.orders")["uniqueness"] = variant
        assert not _agree(broken, schema)
    for variant in _mutations(record["keys"][0]):
        broken = copy.deepcopy(profile)
        _table(broken, "analytics.orders")["uniqueness"]["keys"][0] = variant
        assert not _agree(broken, schema)
    index = next(i for i, r in enumerate(profile["relationships"]) if r["validation"] == "violated")
    detail = profile["relationships"][index]["validation_detail"]
    for variant in _mutations(detail):
        broken = copy.deepcopy(profile)
        broken["relationships"][index]["validation_detail"] = variant
        assert not _agree(broken, schema)
    for variant in _mutations(profile["relationship_hypotheses"]):
        _agree({**profile, "relationship_hypotheses": variant}, schema)
    hypothesis = profile["relationship_hypotheses"]["hypotheses"][0]
    for variant in _mutations(hypothesis):
        broken = copy.deepcopy(profile)
        broken["relationship_hypotheses"]["hypotheses"][0] = variant
        assert not _agree(broken, schema)
    broken = copy.deepcopy(profile)
    broken["relationship_hypotheses"]["hypotheses"][0]["cardinality"] = {
        "from": "zero_or_more",
        "to": "exactly_one",
    }
    assert not _agree(broken, schema), "a hypothesis never carries a cardinality"


def test_uniqueness_invariants(deep_profile):
    profile = integrity_profile(deep_profile)
    assert profile_invariant_errors(profile) == []
    broken = copy.deepcopy(profile)
    _table(broken, "analytics.orders")["uniqueness"]["keys"][1]["outcome"] = "unique"
    assert any("contradicts its counts" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    key = _table(broken, "analytics.orders")["uniqueness"]["keys"][0]
    next(m for m in key["metrics"] if m["name"] == "distinct_keys")["value"] = 5000
    assert any("inconsistent uniqueness counts" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    _table(broken, "analytics.orders")["uniqueness"]["keys"][0]["operation_id"] = "op_missing"
    assert any("not a planned operation" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    _table(broken, "analytics.orders")["uniqueness"]["keys"][2]["reason"] = None
    assert any("needs a reason" in e for e in profile_invariant_errors(broken))


def test_validation_status_follows_orphans(deep_profile):
    profile = integrity_profile(deep_profile)
    broken = copy.deepcopy(profile)
    relationship = next(r for r in broken["relationships"] if r["validation"] == "violated")
    relationship["validation"] = relationship["validation_detail"]["status"] = "validated"
    assert any("0 orphans in full scope" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    relationship = next(r for r in broken["relationships"] if r["validation"] == "violated")
    relationship["validation"] = "not_validated"
    assert any("status differs" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    relationship = next(r for r in broken["relationships"] if r["validation"] == "violated")
    relationship["validation_detail"]["operation_id"] = None
    assert any("not planned by the source" in e for e in profile_invariant_errors(broken))


def test_hypotheses_stay_apart_and_meet_the_threshold(deep_profile):
    profile = integrity_profile(deep_profile)
    broken = copy.deepcopy(profile)
    item = broken["relationship_hypotheses"]["hypotheses"][0]
    known = next(r for r in broken["relationships"] if r["name"] == "events_order")
    item["from"], item["to"] = copy.deepcopy(known["from"]), copy.deepcopy(known["to"])
    assert any("repeats a known relationship" in e for e in profile_invariant_errors(broken))
    broken = copy.deepcopy(profile)
    broken["relationship_hypotheses"]["budget"]["min_inclusion_ratio"] = 1.0
    ratio = next(
        m
        for m in broken["relationship_hypotheses"]["hypotheses"][0]["evidence"]["metrics"]
        if m["name"] == "inclusion_ratio"
    )
    ratio["value"] = 0.5
    assert any("below min_inclusion_ratio" in e for e in profile_invariant_errors(broken))


def test_standard_profiles_record_why_nothing_was_validated(demo_profile):
    assert demo_profile["schema_version"] == "1.2"
    assert demo_profile["referential_validation"] is None
    assert demo_profile["relationship_hypotheses"] is None
    for relationship in demo_profile["relationships"]:
        detail = relationship["validation_detail"]
        assert relationship["validation"] == detail["status"] == "not_validated"
        assert "deep level" in detail["reason"]
    assert all(table["uniqueness"] is None for table in demo_profile["tables"])
