"""Referential validation (deep level) on real Spark, checked against independent Spark SQL.

The runtime counts orphans with one left join against the grouped target per relationship; the
ground truth below uses ``LEFT ANTI JOIN`` queries instead. Delta pinning is checked by writing new
orphans after the tables were profiled.
"""

import json

import pytest

from tabledossier.assemble import build_profile
from tabledossier.config import deep_merge, default_config
from tabledossier.contract import profile_invariant_errors
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import metric_value
from tabledossier.package import build_documents
from tabledossier.runtime.spark import profile_table, validate_relationships

DB = "integrity_r"
SECRET = "SECRET_ORPHAN_"


def _create(spark, name, select, delta):
    spark.sql(f"DROP TABLE IF EXISTS {DB}.{name}")
    using = "USING delta " if delta else ""
    spark.sql(f"CREATE TABLE {DB}.{name} {using}AS {select}")


@pytest.fixture(scope="module")
def schema(spark, delta_enabled):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    _create(spark, "customers", "SELECT id + 1 AS cust_id FROM range(100)", delta_enabled)
    _create(
        spark,
        "orders",
        "SELECT concat('o', CAST(id AS STRING)) AS order_id, "
        "CASE WHEN id % 50 = 49 THEN NULL WHEN id % 40 = 39 THEN CAST(1000 + id AS INT) "
        "ELSE CAST(id % 100 + 1 AS INT) END AS cust_id, "
        "CAST(id % 3 AS INT) AS region FROM range(400)",
        delta_enabled,
    )
    _create(
        spark,
        "stores",
        "SELECT CAST(id % 4 AS INT) AS region, concat('S', CAST(id DIV 4 AS STRING)) AS store "
        "FROM range(40)",
        delta_enabled,
    )
    _create(
        spark,
        "shipments",
        "SELECT CAST(id % 4 AS INT) AS region, "
        f"CASE WHEN id IN (7, 8) THEN concat('{SECRET}', CAST(id AS STRING)) "
        "WHEN id = 9 THEN NULL ELSE concat('S', CAST(id % 10 AS STRING)) END AS store "
        "FROM range(60)",
        delta_enabled,
    )
    _create(
        spark,
        "codes",
        "SELECT CAST(id % 30 AS INT) AS code FROM range(40)",
        delta_enabled,
    )
    return DB


def _config(relationships, **referential):
    return deep_merge(
        default_config(),
        {
            "analysis_level": "deep",
            "relationships": relationships,
            "deep": {"referential": {"configured": True, **referential}},
        },
    )


def _rel(rel_id, source, source_columns, target, target_columns):
    return {
        "id": rel_id,
        "from": {"table": f"{DB}.{source}", "columns": source_columns},
        "to": {"table": f"{DB}.{target}", "columns": target_columns},
    }


RELATIONSHIPS = [
    _rel("orders_customers", "orders", ["cust_id"], "customers", ["cust_id"]),
    _rel("shipments_stores", "shipments", ["region", "store"], "stores", ["region", "store"]),
    _rel("orders_codes", "orders", ["cust_id"], "codes", ["code"]),
    _rel("orders_stores_bad", "orders", ["order_id"], "customers", ["cust_id"]),
    _rel("orders_missing", "orders", ["cust_id"], "not_profiled", ["cust_id"]),
]
TABLES = ["customers", "orders", "stores", "shipments", "codes"]


def _profile_all(spark, capabilities, config, names=TABLES):
    return [
        profile_table(
            spark,
            f"{DB}.{name}",
            config,
            capabilities=capabilities,
            reference_time=format_utc(utc_now()),
            now=lambda: format_utc(utc_now()),
            log=lambda message: None,
        )
        for name in names
    ]


def _validated(spark, capabilities, config):
    tables = _profile_all(spark, capabilities, config)
    relationships, summary = validate_relationships(spark, tables, config, log=lambda m: None)
    return tables, {rel["name"]: rel for rel in relationships}, summary


def _metrics(rel):
    return {
        m["name"]: m["value"]
        for m in rel["validation_detail"]["metrics"]
        if m["status"] == "measured"
    }


def _anti(spark, source, source_columns, target, target_columns):
    condition = " AND ".join(
        f"s.{a} = t.{b}" for a, b in zip(source_columns, target_columns, strict=True)
    )
    complete = " AND ".join(f"s.{a} IS NOT NULL" for a in source_columns)
    return spark.sql(
        f"SELECT count(*) FROM {DB}.{source} s LEFT ANTI JOIN {DB}.{target} t ON {condition} "
        f"WHERE {complete}"
    ).collect()[0][0]


@pytest.fixture(scope="module")
def full(spark, schema, capabilities):
    return _validated(spark, capabilities, _config(RELATIONSHIPS))


def test_orphans_match_anti_join_ground_truth(spark, full):
    _, rels, _ = full
    orders = _metrics(rels["orders_customers"])
    assert orders["orphan_rows"] == _anti(spark, "orders", ["cust_id"], "customers", ["cust_id"])
    assert orders["orphan_rows"] == 8, "ids 39, 79 ... 399 minus the NULL ids 199 and 399"
    nulls = spark.sql(f"SELECT count(*) FROM {DB}.orders WHERE cust_id IS NULL").collect()[0][0]
    assert orders["source_rows_with_null_key"] == nulls == 8
    assert orders["source_rows_with_complete_key"] == 400 - nulls
    assert orders["orphan_ratio"] == pytest.approx(8 / (400 - nulls))
    assert orders["target_distinct_keys"] == 100 and orders["target_duplicate_key_groups"] == 0
    assert rels["orders_customers"]["validation"] == "violated"
    assert rels["orders_customers"]["validation_detail"]["target_key_unique"] is True


def test_composite_keys_and_null_components(spark, full):
    _, rels, _ = full
    shipments = _metrics(rels["shipments_stores"])
    truth = _anti(spark, "shipments", ["region", "store"], "stores", ["region", "store"])
    assert shipments["orphan_rows"] == truth
    assert shipments["source_rows_with_null_key"] == 1, "a NULL component is never an orphan"
    assert rels["shipments_stores"]["validation"] == "violated"


def test_validated_relationship_and_non_unique_target(spark, schema, capabilities):
    config = _config(
        [
            _rel("stores_self", "stores", ["region", "store"], "stores", ["region", "store"]),
            _rel("codes_customers", "codes", ["code"], "customers", ["cust_id"]),
            _rel("customers_codes", "customers", ["cust_id"], "codes", ["code"]),
        ]
    )
    _, rels, summary = _validated(spark, capabilities, config)
    assert rels["stores_self"]["validation"] == "validated"
    assert _metrics(rels["stores_self"])["orphan_rows"] == 0
    zero = spark.sql(f"SELECT count(*) FROM {DB}.codes WHERE code = 0").collect()[0][0]
    assert _metrics(rels["codes_customers"])["orphan_rows"] == zero
    target = rels["customers_codes"]["validation_detail"]
    assert target["target_key_unique"] is False
    assert _metrics(rels["customers_codes"])["target_duplicate_key_groups"] == 10
    assert summary["validated"] == 1 and summary["planned"] == 3


def test_reasons_for_relationships_not_validated(full):
    _, rels, summary = full
    bad = rels["orders_stores_bad"]["validation_detail"]
    assert bad["status"] == "not_validated" and "incompatible column types" in bad["reason"]
    assert bad["type_compatibility"][0]["compatible"] is False
    missing = rels["orders_missing"]["validation_detail"]
    assert "not profiled in this run" in missing["reason"] and missing["operation_id"] is None
    assert summary["planned"] == 3 and summary["not_validated"] == 2


def test_budget_counts_checks_and_declares_operations(spark, schema, capabilities):
    config = _config(RELATIONSHIPS[:3], max_relationships=2)
    tables, rels, summary = _validated(spark, capabilities, config)
    assert rels["orders_codes"]["validation"] == "not_validated"
    assert "max_relationships = 2" in rels["orders_codes"]["validation_detail"]["reason"]
    assert summary["limited"][0]["reason"] == "referential_budget"
    checks = [
        op
        for table in tables
        for op in table["operations"]["planned"]
        if op["kind"] == "referential_check"
    ]
    assert len(checks) == summary["planned"] == 2
    orders = next(t for t in tables if t["table_key"] == f"{DB}.orders")
    observed = {o["operation_id"]: o["status"] for o in orders["operations"]["observed"]}
    assert observed["op_referential_1"] == "succeeded"


def test_sample_mode_can_violate_but_never_validate(spark, schema, capabilities):
    config = _config(
        [
            RELATIONSHIPS[2],
            _rel("stores_self", "stores", ["region", "store"], "stores", ["region", "store"]),
        ],
        mode="sample",
        max_sample_rows=50,
    )
    _, rels, _ = _validated(spark, capabilities, config)
    codes = rels["orders_codes"]["validation_detail"]
    assert codes["mode"] == "sample" and codes["from"]["scope"] == "sample"
    assert _metrics(rels["orders_codes"])["source_rows"] == 50
    # Any 50 orders include ids beyond code 29: an orphan found in a sample violates the table.
    assert rels["orders_codes"]["validation"] == "violated"
    stores = rels["stores_self"]
    assert stores["validation"] == "not_validated"
    assert "cannot validate the whole scope" in stores["validation_detail"]["reason"]


def test_filters_keep_the_source_scope_and_read_the_whole_target(spark, schema, capabilities):
    config = _config([RELATIONSHIPS[0]])
    config["table_options"] = {
        f"{DB}.orders": {"filters": [{"column": "cust_id", "operator": "le", "value": 1000}]},
        f"{DB}.customers": {"filters": [{"column": "cust_id", "operator": "le", "value": 10}]},
    }
    _, rels, _ = _validated(spark, capabilities, config)
    detail = rels["orders_customers"]["validation_detail"]
    assert _metrics(rels["orders_customers"])["orphan_rows"] == 0, "1000+ ids are filtered out"
    assert _metrics(rels["orders_customers"])["target_rows"] == 100, "target filters not applied"
    assert detail["from"]["scope"] in ("filtered_table", "filtered_snapshot")
    assert detail["to"]["scope"] in ("full_table", "full_snapshot")


def test_declared_foreign_keys_are_checked_when_requested(spark, schema, capabilities, monkeypatch):
    import tabledossier.runtime.spark as runtime

    catalog = spark.catalog.currentCatalog()

    def declared(spark, parts):
        if parts[-1] != "orders":
            return [], [], None
        return (
            [
                {
                    "name": "orders_customers_fk",
                    "constraint_type": "foreign_key",
                    "columns": ["cust_id"],
                    "expression": None,
                    "referenced": {"table": f"{catalog}.{DB}.customers", "columns": ["cust_id"]},
                    "enforcement": "not_enforced",
                    "source": "information_schema",
                }
            ],
            [],
            None,
        )

    monkeypatch.setattr(runtime, "read_unity_constraints", declared)
    off = _config([], configured=False)
    _, rels, _ = _validated(spark, capabilities, off)
    fk = next(iter(rels.values()))
    assert fk["origin"] == "declared_constraint" and fk["validation"] == "not_validated"
    assert "deep.referential.declared = false" in fk["validation_detail"]["reason"]
    _, rels, _ = _validated(spark, capabilities, _config([], configured=False, declared=True))
    fk = next(iter(rels.values()))
    assert fk["validation"] == "violated" and _metrics(fk)["orphan_rows"] == 8


def test_both_tables_are_read_at_their_recorded_versions(
    spark, schema, capabilities, delta_enabled
):
    if not delta_enabled:
        pytest.skip("snapshot pinning needs Delta tables")
    _create(spark, "pin_target", "SELECT id + 1 AS k FROM range(20)", True)
    _create(spark, "pin_source", "SELECT CAST(id % 20 + 1 AS BIGINT) AS k FROM range(50)", True)
    config = _config([_rel("pinned", "pin_source", ["k"], "pin_target", ["k"])])
    tables = _profile_all(spark, capabilities, config, ["pin_source", "pin_target"])
    versions = {t["table_key"]: t["consistency"]["delta_version"] for t in tables}
    spark.sql(f"INSERT INTO {DB}.pin_source VALUES (777), (778)")
    spark.sql(f"DELETE FROM {DB}.pin_target WHERE k = 1")
    relationships, _ = validate_relationships(spark, tables, config, log=lambda m: None)
    detail = relationships[0]["validation_detail"]
    assert relationships[0]["validation"] == "validated", "later writes are not read"
    assert detail["from"]["delta_version"] == versions[f"{DB}.pin_source"]
    assert detail["to"]["delta_version"] == versions[f"{DB}.pin_target"]
    assert (
        detail["from"]["consistency_mode"]
        == detail["to"]["consistency_mode"]
        == ("pinned_delta_version")
    )
    fresh = _profile_all(spark, capabilities, config, ["pin_source", "pin_target"])
    relationships, _ = validate_relationships(spark, fresh, config, log=lambda m: None)
    assert _metrics(relationships[0])["orphan_rows"] == 2 + 50 // 20 + 1


def test_orphan_values_never_leave_the_engine(full):
    tables, rels, summary = full
    config = _config(RELATIONSHIPS)
    profile = build_profile(
        run_id="20260101T000000Z-00000000",
        started_at="2026-01-01T00:00:00.000Z",
        finished_at="2026-01-01T00:00:01.000Z",
        duration_ms=1000,
        reference_time="2026-01-01T00:00:00.000Z",
        environment={"engine": "spark", "execution_context": "spark", "python_version": "3"},
        config=config,
        parameter_sources={},
        generation={"generator_version": None, "generation_id": None},
        capabilities={},
        tables=tables,
        relationships=list(rels.values()),
        referential_validation=summary,
    )
    assert profile_invariant_errors(profile) == []
    assert SECRET not in json.dumps(profile)
    documents = build_documents(profile)
    for name, text in documents.items():
        assert SECRET not in text, name
    assert "## 8. Referential integrity" in documents["quality_report.md"]
    assert "**violated**: 8 orphan row(s)" in documents["relationships.md"]
    assert metric_value(rels["shipments_stores"]["validation_detail"]["metrics"], "orphan_rows")
