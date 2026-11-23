"""Contract 1.3 additions: PostgreSQL engine, local CLI context, transaction snapshots.

The synthetic PostgreSQL profile (``postgres_profile`` in conftest) is the committed standard demo
profile changed the way the PostgreSQL connector will write it, so the 1.3 schema, the embedded
validator, ``jsonschema`` and the invariants are exercised together on a realistic document.
"""

import copy

from jsonschema import Draft202012Validator

from tabledossier.contract import profile_invariant_errors, validate_profile
from tabledossier.integrity import type_compatibility, validation_detail
from tabledossier.planning import iter_nodes
from tabledossier.schemacheck import schema_errors
from tabledossier.validation import check_profile


def _agree(document, schema):
    formal = not list(Draft202012Validator(schema).iter_errors(document))
    local = not schema_errors(document, schema)
    assert formal == local, (formal, schema_errors(document, schema)[:3])
    return local


def _rejected_by_both(document, schemas, schema_name="profile"):
    """Both the CLI path (jsonschema + invariants) and the embedded validator reject it."""
    return bool(check_profile(document)) and bool(validate_profile(document, schemas[schema_name]))


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


def _with_checked_relationship(profile):
    """Attach a full-scope referential check read in the transaction snapshot."""
    orders = _table(profile, "analytics.orders")
    customers = _table(profile, "analytics.customers")
    relationship = next(r for r in profile["relationships"] if r["name"] == "orders_customer")
    raw = {"rows": 1000, "null_rows": 0, "orphans": 5, "t_rows": 500, "t_distinct": 500}
    relationship["validation"] = "violated"
    relationship["validation_detail"] = validation_detail(
        {**raw, "t_null_rows": 0, "t_dup_groups": 0},
        mode="full_scope",
        sample_rows=None,
        from_info=_side(orders),
        to_info=_side(customers, "full_snapshot"),
        compatibility=type_compatibility(
            [_node(orders, "customer_id")], [_node(customers, "customer_id")]
        ),
        operation_id="op_referential_1",
    )
    orders["operations"]["planned"].append(
        {
            "operation_id": "op_referential_1",
            "kind": "referential_check",
            "description": "synthetic",
            "reads_user_data": True,
        }
    )
    return relationship


def test_synthetic_postgresql_profile_is_valid(postgres_profile, schemas):
    assert postgres_profile["schema_version"] == "1.3"
    environment = postgres_profile["run"]["environment"]
    assert environment["engine"] == "postgresql"
    assert environment["execution_context"] == "local_cli"
    assert environment["postgresql_version"] == "17.11"
    assert not any(key.startswith(("spark", "databricks", "ansi")) for key in environment)
    assert postgres_profile["run"]["generation"]["generation_id"] is None
    for table in postgres_profile["tables"]:
        assert table["consistency"]["mode"] == "transaction_snapshot"
        assert table["consistency"]["delta_version"] is None
        assert table["scope"]["scope_label"] in ("full_snapshot", "filtered_snapshot")
    constraints = [c for t in postgres_profile["tables"] for c in t["constraints"]]
    assert [(c["source"], c["enforcement"]) for c in constraints] == [
        ("system_catalog", "enforced")
    ]
    errors = [e for t in postgres_profile["tables"] for e in t["errors"]]
    assert [e["stage"] for e in errors] == ["connect"]
    assert _agree(postgres_profile, schemas["profile"])
    assert check_profile(postgres_profile) == []
    assert validate_profile(postgres_profile, schemas["profile"]) == []


def test_exact_labels_replace_the_approximate_ones(postgres_profile):
    seen = set()
    for table in postgres_profile["tables"]:
        for field in table["field_profiles"]:
            for metric in field["metrics"]:
                if metric["status"] != "measured":
                    continue
                if metric["name"] in ("approx_distinct_count", "quantiles", "length_quantiles"):
                    seen.add(metric["name"])
                    assert metric["accuracy"] == "exact", metric
                    details = metric.get("details") or {}
                    assert "relative_standard_deviation" not in details
                    assert "accuracy_parameter" not in details
    assert seen == {"approx_distinct_count", "quantiles", "length_quantiles"}


def test_transaction_snapshot_requires_the_postgresql_engine(postgres_profile, schemas):
    broken = copy.deepcopy(postgres_profile)
    broken["run"]["environment"]["engine"] = "spark"
    assert _agree(broken, schemas["profile"]), "the schema alone cannot tie the two fields"
    errors = profile_invariant_errors(broken)
    assert any("transaction_snapshot requires run.environment.engine" in e for e in errors)
    assert _rejected_by_both(broken, schemas)


def test_delta_version_only_with_a_pinned_delta_version(postgres_profile, schemas):
    broken = copy.deepcopy(postgres_profile)
    broken["tables"][0]["consistency"]["delta_version"] = 3
    errors = profile_invariant_errors(broken)
    assert errors == [
        "$.tables[0].consistency: delta_version is recorded only for pinned_delta_version"
    ]
    assert _rejected_by_both(broken, schemas)


def test_postgresql_engine_is_not_a_1_2_value(postgres_profile, schemas):
    relabelled = {**copy.deepcopy(postgres_profile), "schema_version": "1.2"}
    assert not _agree(relabelled, schemas["profile-1.2"])
    assert _rejected_by_both(relabelled, schemas, "profile-1.2")
    only_engine = copy.deepcopy(relabelled)
    for table in only_engine["tables"]:
        table["consistency"]["mode"] = "unpinned"
        table["constraints"] = []
        table["errors"] = [{**e, "stage": "aggregate"} for e in table["errors"]]
    only_engine["run"]["environment"]["execution_context"] = "spark"
    del only_engine["run"]["environment"]["postgresql_version"]
    errors = schema_errors(only_engine, schemas["profile-1.2"])
    assert errors and all("engine" in e for e in errors), errors
    assert _rejected_by_both(only_engine, schemas, "profile-1.2")


def test_validation_sides_follow_the_same_rules(postgres_profile, schemas):
    profile = copy.deepcopy(postgres_profile)
    relationship = _with_checked_relationship(profile)
    detail = relationship["validation_detail"]
    assert detail["to"]["consistency_mode"] == "transaction_snapshot"
    assert not any("Delta" in note for note in detail["limitations"]), "snapshots are pinned"
    assert "transaction snapshot" in detail["limitations"][0]
    assert check_profile(profile) == []
    assert validate_profile(profile, schemas["profile"]) == []
    broken = copy.deepcopy(profile)
    target = next(r for r in broken["relationships"] if r["name"] == "orders_customer")
    target["validation_detail"]["to"]["delta_version"] = 7
    errors = profile_invariant_errors(broken)
    assert any(".validation_detail.to: delta_version is recorded only" in e for e in errors)
    assert _rejected_by_both(broken, schemas)
    broken = copy.deepcopy(profile)
    for table in broken["tables"]:
        table["consistency"]["mode"] = "unpinned"
    broken["run"]["environment"]["engine"] = "spark"
    errors = profile_invariant_errors(broken)
    assert errors and all(".validation_detail." in e for e in errors), errors
    assert _rejected_by_both(broken, schemas)


def test_spark_profiles_satisfy_the_new_invariants(demo_profile, deep_profile, profile_1_2):
    for profile in (demo_profile, deep_profile, profile_1_2):
        assert profile_invariant_errors(profile) == []
