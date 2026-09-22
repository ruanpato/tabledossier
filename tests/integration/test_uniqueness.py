"""Exact uniqueness (deep level) on real Spark, checked against independent Spark SQL.

The runtime packs several keys into one exploded, grouped aggregation; the ground truth below uses
plain ``GROUP BY ... HAVING`` queries per key instead.
"""

import json

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import metric_value
from tabledossier.package import build_documents
from tabledossier.runtime.spark import profile_table

SECRET = "SECRET_DUP_"
TABLE = "integrity_u.accounts"


@pytest.fixture(scope="module")
def accounts(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS integrity_u")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"""
        CREATE TABLE {TABLE} AS SELECT
          id + 1 AS account_id,
          CASE WHEN id < 6 THEN concat('{SECRET}', CAST(id % 2 AS STRING))
               WHEN id % 40 = 7 THEN NULL
               ELSE concat('C-', CAST(id AS STRING)) END AS code,
          CASE WHEN id = 199 THEN NULL ELSE CAST(id % 4 AS INT) END AS region,
          CAST(id DIV 4 AS INT) AS seq,
          named_struct('ref', concat('R', CAST(id AS STRING)), 'kind', 'x') AS meta,
          array(CAST(id AS INT)) AS tags,
          CAST(id % 7 AS DOUBLE) AS score
        FROM range(200)
        """
    )
    return TABLE


def _profile(spark, capabilities, uniqueness, **overrides):
    config = deep_merge(
        default_config(),
        {"analysis_level": "deep", "deep": {"uniqueness": uniqueness}, **overrides},
    )
    return profile_table(
        spark,
        TABLE,
        config,
        capabilities=capabilities,
        reference_time=format_utc(utc_now()),
        now=lambda: format_utc(utc_now()),
        log=lambda message: None,
    )


def _key(table, columns):
    return next(k for k in table["uniqueness"]["keys"] if k["columns"] == columns)


def _values(key):
    return {m["name"]: m["value"] for m in key["metrics"] if m["status"] == "measured"}


def _truth(spark, columns, where="TRUE"):
    cols = ", ".join(columns)
    complete = " AND ".join(f"{c} IS NOT NULL" for c in columns)
    rows = spark.sql(f"SELECT count(*) FROM {TABLE} WHERE {where}").collect()[0][0]
    nulls = spark.sql(
        f"SELECT count(*) FROM {TABLE} WHERE ({where}) AND NOT ({complete})"
    ).collect()[0][0]
    grouped = spark.sql(
        f"SELECT count(*) AS keys, sum(CASE WHEN n > 1 THEN 1 ELSE 0 END) AS groups, "
        f"sum(CASE WHEN n > 1 THEN n ELSE 0 END) AS duplicated, max(n) AS worst FROM ("
        f"SELECT {cols}, count(*) AS n FROM {TABLE} WHERE ({where}) AND {complete} "
        f"GROUP BY {cols})"
    ).collect()[0]
    return {
        "rows_in_scope": rows,
        "rows_with_null_key": nulls,
        "rows_with_complete_key": rows - nulls,
        "distinct_keys": grouped["keys"],
        "duplicate_key_groups": grouped["groups"] or 0,
        "rows_in_duplicate_groups": grouped["duplicated"] or 0,
        "surplus_duplicate_rows": rows - nulls - grouped["keys"],
        "max_rows_per_key": grouped["worst"],
    }


KEYS = [
    {"table": TABLE, "columns": ["account_id"], "id": "accounts_pk"},
    {"table": TABLE, "columns": ["code"]},
    {"table": TABLE, "columns": ["region", "seq"]},
    {"table": TABLE, "columns": [["meta", "ref"]]},
    {"table": TABLE, "columns": ["score"]},
]


@pytest.fixture(scope="module")
def checked(spark, accounts, capabilities):
    return _profile(spark, capabilities, {"keys": KEYS, "max_keys": 10})


def test_exact_counts_match_group_by_ground_truth(spark, checked):
    assert checked["status"] == "succeeded", checked["errors"]
    for columns, sql_columns in (
        (["account_id"], ["account_id"]),
        (["code"], ["code"]),
        (["region", "seq"], ["region", "seq"]),
        (["meta.ref"], ["meta.ref"]),
        (["score"], ["score"]),
    ):
        key = _key(checked, columns)
        assert key["status"] == "measured", key
        assert _values(key) == _truth(spark, sql_columns), columns
        for metric in key["metrics"]:
            if metric["status"] == "measured":
                assert metric["accuracy"] == "exact"
                assert metric["scope"] == checked["scope"]["scope_label"]


def test_outcomes_and_null_semantics(checked):
    assert _key(checked, ["account_id"])["outcome"] == "unique"
    code = _key(checked, ["code"])
    assert code["outcome"] == "duplicates"
    assert _values(code)["duplicate_key_groups"] == 2
    assert _values(code)["rows_with_null_key"] == 5, "NULL codes are not duplicates"
    assert _key(checked, ["region", "seq"])["outcome"] == "unique_non_null"
    assert _key(checked, ["score"])["outcome"] == "duplicates"
    assert any("NaN" in note for note in _key(checked, ["score"])["limitations"])
    assert "NULLs are not treated as equal" in checked["uniqueness"]["null_semantics"]


def test_all_keys_share_one_action(checked):
    passes = [op for op in checked["operations"]["planned"] if op["kind"] == "uniqueness_pass"]
    assert len(passes) == 1 and passes[0]["details"]["keys"] == len(KEYS)
    assert checked["uniqueness"]["passes"] == {"budget": 1, "planned": 1}
    observed = {o["operation_id"]: o for o in checked["operations"]["observed"]}
    assert observed["op_uniqueness_1"]["status"] == "succeeded"
    assert {k["operation_id"] for k in checked["uniqueness"]["keys"]} == {"op_uniqueness_1"}


def test_budget_limits_keys_and_passes(spark, accounts, capabilities):
    table = _profile(spark, capabilities, {"keys": KEYS, "max_keys": 3, "max_passes": 2})
    statuses = [key["status"] for key in table["uniqueness"]["keys"]]
    assert statuses == ["measured", "measured", "measured", "not_computed", "not_computed"]
    limited = table["uniqueness"]["limited"]
    assert [item["reason"] for item in limited] == ["uniqueness_budget"] * 2
    passes = [op for op in table["operations"]["planned"] if op["kind"] == "uniqueness_pass"]
    assert [op["details"]["keys"] for op in passes] == [2, 1]
    assert table["uniqueness"]["passes"] == {"budget": 2, "planned": 2}
    reads = [op for op in table["operations"]["planned"] if op["reads_user_data"]]
    config = default_config()
    budget = 1 + config["limits"]["max_aggregate_passes"] + config["deep"]["max_extra_passes"]
    assert len(reads) <= budget + 2, "uniqueness adds at most max_passes actions"


def test_filters_define_the_checked_scope(spark, accounts, capabilities):
    table = _profile(
        spark,
        capabilities,
        {"keys": [{"table": TABLE, "columns": ["code"]}]},
        table_options={
            TABLE: {"filters": [{"column": "account_id", "operator": "gt", "value": 3}]}
        },
    )
    key = _key(table, ["code"])
    assert _values(key) == _truth(spark, ["code"], "account_id > 3")
    assert key["scope"] in ("filtered_table", "filtered_snapshot")


def test_ineligible_keys_are_reported_not_guessed(spark, accounts, capabilities):
    table = _profile(
        spark,
        capabilities,
        {
            "keys": [
                {"table": TABLE, "columns": ["tags"]},
                {"table": TABLE, "columns": ["missing"]},
                {"table": TABLE, "columns": ["meta"]},
            ]
        },
    )
    reasons = [key["reason"] for key in table["uniqueness"]["keys"]]
    assert [key["status"] for key in table["uniqueness"]["keys"]] == ["not_eligible"] * 3
    assert "cannot be compared exactly" in reasons[0]
    assert "not in the documented schema tree" in reasons[1]
    assert "cannot be compared exactly" in reasons[2]
    assert not [op for op in table["operations"]["planned"] if op["kind"] == "uniqueness_pass"]


def test_declared_keys_and_identifier_candidates(spark, accounts, capabilities, monkeypatch):
    import tabledossier.runtime.spark as runtime

    def declared(spark, parts):
        return [
            {
                "name": "accounts_pk",
                "constraint_type": "primary_key",
                "columns": ["account_id"],
                "expression": None,
                "referenced": None,
                "enforcement": "not_enforced",
                "source": "information_schema",
            },
            {
                "name": "accounts_code_uk",
                "constraint_type": "unique",
                "columns": ["code"],
                "expression": None,
                "referenced": None,
                "enforcement": "not_enforced",
                "source": "information_schema",
            },
        ], None

    monkeypatch.setattr(runtime, "read_unity_constraints", declared)
    table = _profile(
        spark,
        capabilities,
        {"keys": [KEYS[0]], "declared_keys": True, "identifier_candidates": True},
    )
    keys = {tuple(k["columns"]): k for k in table["uniqueness"]["keys"]}
    assert keys[("account_id",)]["origins"] == [
        "configured",
        "declared_primary_key",
        "identifier_candidate",
    ]
    assert keys[("account_id",)]["names"] == ["accounts_pk"]
    assert keys[("code",)]["origins"] == ["declared_unique"]
    assert keys[("code",)]["outcome"] == "duplicates", "a declared UNIQUE is not trusted"
    assert keys[("meta.ref",)]["origins"] == ["identifier_candidate"]
    assert len({k["operation_id"] for k in table["uniqueness"]["keys"]}) == 1
    rules = [r for r in table["suggested_rules"] if r["rule_type"] == "unique"]
    by_column = {r["column"]: r for r in rules}
    assert by_column["account_id"]["rationale"].startswith("Exact uniqueness check")
    assert by_column["account_id"]["evidence"][0]["check"] == "exact_uniqueness"
    assert "code" not in by_column, "an exact check that found duplicates proposes nothing"


def test_unique_proposals_cite_exact_evidence(checked):
    rules = [r for r in checked["suggested_rules"] if r["rule_type"] == "unique"]
    composite = [r for r in rules if r["column"] is None]
    assert [r["parameters"] for r in composite] == [{"columns": ["region", "seq"]}]
    assert "1 row(s) have NULL in the key" in composite[0]["rationale"]
    assert {r["column"] for r in rules} >= {"account_id", "meta.ref"}
    assert all(r["evidence"][0]["check"] == "exact_uniqueness" for r in rules)
    assert "code" not in {r["column"] for r in rules}
    assert "score" not in {r["column"] for r in rules}


def _documents(table):
    from tabledossier.assemble import build_profile

    profile = build_profile(
        run_id="20260101T000000Z-00000000",
        started_at="2026-01-01T00:00:00.000Z",
        finished_at="2026-01-01T00:00:01.000Z",
        duration_ms=1000,
        reference_time="2026-01-01T00:00:00.000Z",
        environment={"engine": "spark", "execution_context": "spark", "python_version": "3"},
        config=deep_merge(default_config(), {"analysis_level": "deep"}),
        parameter_sources={},
        generation={"generator_version": None, "generation_id": None},
        capabilities={},
        tables=[table],
    )
    return json.dumps(profile), build_documents(profile)


def test_duplicated_key_values_never_leave_the_engine(checked):
    text, documents = _documents(checked)
    assert SECRET not in text
    for name, document in documents.items():
        assert SECRET not in document, name
    assert metric_value(_key(checked, ["code"])["metrics"], "duplicate_key_groups") == 2
