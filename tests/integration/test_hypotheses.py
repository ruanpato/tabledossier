"""Relationship hypotheses (deep level) on real Spark.

A child column that references the parent key under an unrelated name is found from the data; a
column that carries the parent key's *name* but unrelated values is not. Inclusion is compared with
an independent semi join.
"""

import pytest

from tabledossier.assemble import build_profile
from tabledossier.config import deep_merge, default_config
from tabledossier.contract import profile_invariant_errors
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.package import build_documents
from tabledossier.runtime.spark import evaluate_hypotheses, profile_table, validate_relationships

DB = "integrity_h"


@pytest.fixture(scope="module")
def tables(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    for name, select in (
        (
            "parents",
            "SELECT id + 1 AS pid, concat('P', CAST(id AS STRING)) AS label FROM range(50)",
        ),
        (
            "children",
            "SELECT id AS child_id, "
            "CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id % 40 + 1 AS INT) END AS owner_ref, "
            "CAST(id + 500 AS INT) AS pid, "
            "CAST(id % 60 + 1 AS INT) AS partial_ref FROM range(300)",
        ),
    ):
        spark.sql(f"DROP TABLE IF EXISTS {DB}.{name}")
        spark.sql(f"CREATE TABLE {DB}.{name} AS {select}")
    return [f"{DB}.parents", f"{DB}.children"]


def _config(relationships=(), **settings):
    return deep_merge(
        default_config(),
        {
            "analysis_level": "deep",
            "relationships": list(relationships),
            "deep": {
                "uniqueness": {"keys": [{"table": f"{DB}.parents", "columns": ["pid"]}]},
                "relationship_hypotheses": {"enabled": True, **settings},
            },
        },
    )


def _run(spark, capabilities, names, config):
    profiled = [
        profile_table(
            spark,
            name,
            config,
            capabilities=capabilities,
            reference_time=format_utc(utc_now()),
            now=lambda: format_utc(utc_now()),
            log=lambda message: None,
        )
        for name in names
    ]
    relationships, referential = validate_relationships(spark, profiled, config, log=lambda m: 0)
    record = evaluate_hypotheses(spark, profiled, relationships, config, log=lambda m: None)
    return profiled, relationships, referential, record


def _pairs(record):
    return {
        (item["from"]["columns"][0], item["to"]["columns"][0]): item
        for item in record["hypotheses"]
    }


def _values(item):
    return {m["name"]: m["value"] for m in item["evidence"]["metrics"] if m["status"] == "measured"}


def test_planted_reference_is_found_by_data_not_by_name(spark, tables, capabilities):
    _, _, _, record = _run(spark, capabilities, tables, _config(inclusion_scope="full_scope"))
    pairs = _pairs(record)
    assert ("owner_ref", "pid") in pairs, record
    assert ("pid", "pid") not in pairs, "a shared column name is not evidence"
    item = pairs[("owner_ref", "pid")]
    assert item["status"] == "hypothesis" and item["cardinality"] is None
    values = _values(item)
    truth = spark.sql(
        f"SELECT count(*) FROM {DB}.children c LEFT SEMI JOIN {DB}.parents p ON c.owner_ref = p.pid"
    ).collect()[0][0]
    assert values["included_rows"] == truth == values["source_rows_with_complete_key"]
    assert values["inclusion_ratio"] == 1.0
    assert item["evidence"]["target_key_unique"] is True
    assert item["evidence"]["range_relation"] == "contained"
    assert record["pairs_disjoint_excluded"] >= 1, "child pid 500+ is disjoint from parent pid"


def test_threshold_rejects_partial_inclusion(spark, tables, capabilities):
    _, _, _, record = _run(spark, capabilities, tables, _config(inclusion_scope="full_scope"))
    assert ("partial_ref", "pid") not in _pairs(record)
    assert record["pairs_rejected"].get("below_threshold", 0) >= 1
    lenient = _run(
        spark,
        capabilities,
        tables,
        _config(inclusion_scope="full_scope", min_inclusion_ratio=0.8),
    )[3]
    item = _pairs(lenient)[("partial_ref", "pid")]
    assert _values(item)["inclusion_ratio"] == pytest.approx(250 / 300)


def test_known_relationships_are_not_repeated(spark, tables, capabilities):
    known = {
        "id": "children_owner",
        "from": {"table": f"{DB}.children", "columns": ["owner_ref"]},
        "to": {"table": f"{DB}.parents", "columns": ["pid"]},
    }
    _, _, _, record = _run(spark, capabilities, tables, _config([known]))
    assert ("owner_ref", "pid") not in _pairs(record)
    assert record["pairs_known_excluded"] == 1


def test_budget_sample_and_operations(spark, tables, capabilities):
    profiled, _, _, record = _run(
        spark, capabilities, tables, _config(max_pairs=1, max_sample_rows=20)
    )
    assert record["pairs_evaluated"] == 1 and record["pairs_not_evaluated"] >= 1
    children = next(t for t in profiled if t["table_key"] == f"{DB}.children")
    checks = [
        op
        for op in children["operations"]["planned"]
        if op["kind"] == "relationship_hypothesis_check"
    ]
    assert len(checks) == 1 and checks[0]["details"]["max_sample_rows"] == 20
    item = _pairs(record)[("owner_ref", "pid")]
    assert item["evidence"]["inclusion_scope"] == "sample"
    assert _values(item)["source_rows"] == 20
    assert any("bounded sample" in note for note in item["limitations"])


def test_disabled_by_default_and_never_drawn(spark, tables, capabilities):
    config = _config()
    config["deep"]["relationship_hypotheses"]["enabled"] = False
    profiled, relationships, referential, record = _run(spark, capabilities, tables, config)
    assert record["enabled"] is False and record["hypotheses"] == []
    assert not any(
        op["kind"] == "relationship_hypothesis_check"
        for table in profiled
        for op in table["operations"]["planned"]
    )
    config = _config(inclusion_scope="full_scope")
    profiled, relationships, referential, record = _run(spark, capabilities, tables, config)
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
        tables=profiled,
        relationships=relationships,
        referential_validation=referential,
        relationship_hypotheses=record,
    )
    assert profile_invariant_errors(profile) == []
    assert profile["relationships"] == [] and profile["summary"]["relationship_hypotheses"] >= 1
    documents = build_documents(profile)
    assert "owner\\_ref" in documents["relationships.md"]
    erd = documents["erd.mmd"]
    assert "||" not in erd and "o{" not in erd, "hypotheses are never ER edges"
