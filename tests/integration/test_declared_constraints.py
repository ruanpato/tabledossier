"""Declared constraints end to end, with a simulated Unity Catalog information_schema.

Unity Catalog is not available locally. Here the session catalog gets a database named
``information_schema`` whose tables have the information_schema column layout, and only the
exclusion of the local catalog is lifted: the real reader runs its parameterized queries, the pure
assembly turns the rows into constraints, and those constraints feed ``deep.uniqueness.declared_keys``
and ``deep.referential.declared`` exactly as on a workspace. The names in the simulated catalog use
another spelling than the tables (``ID`` for ``id``...), as a catalog may.
"""

import pytest

import tabledossier.runtime.spark as runtime
from tabledossier.config import deep_merge, default_config
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import metric_value
from tabledossier.runtime.spark import profile_table, validate_relationships

DB = "uc_e2e"
INFO = "information_schema"


def _values(rows, columns):
    def literal(value):
        if value is None:
            return "CAST(NULL AS INT)" if columns else "NULL"
        return str(value) if isinstance(value, int) else "'" + value + "'"

    body = ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows)
    return f"SELECT * FROM VALUES {body} AS t({', '.join(columns)})"


@pytest.fixture(scope="module")
def catalog(spark):
    name = spark.catalog.currentCatalog()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"DROP TABLE IF EXISTS {DB}.parents")
    spark.sql(f"DROP TABLE IF EXISTS {DB}.children")
    # parents.ID is unique; parents.Code has 90 distinct values in 100 rows.
    spark.sql(
        f"CREATE TABLE {DB}.parents AS SELECT id + 1 AS ID, "
        "concat('c', CAST(id % 90 AS STRING)) AS Code FROM range(100)"
    )
    # 8 children point to parents that do not exist (ids 524, 549, ...).
    spark.sql(
        f"CREATE TABLE {DB}.children AS SELECT id AS child_id, "
        "CAST(CASE WHEN id % 25 = 24 THEN 500 + id ELSE id % 100 + 1 END AS BIGINT) AS parent_id "
        "FROM range(200)"
    )
    table_constraints = [
        (name, DB, "parents_pk", DB, "parents", "PRIMARY KEY"),
        (name, DB, "parents_code_uk", DB, "parents", "UNIQUE"),
        (name, DB, "children_parent_fk", DB, "children", "FOREIGN KEY"),
        (name, DB, "children_ghost_fk", DB, "children", "FOREIGN KEY"),
        (name, DB, "children_remote_fk", DB, "children", "FOREIGN KEY"),
        (name, DB, "children_outside_fk", DB, "children", "FOREIGN KEY"),
        (name, DB, "outside_pk", DB, "outside", "PRIMARY KEY"),
    ]
    key_column_usage = [
        (name, DB, "parents_pk", name, DB, "parents", "id", 1, None),
        (name, DB, "parents_code_uk", name, DB, "parents", "CODE", 1, None),
        (name, DB, "children_parent_fk", name, DB, "children", "PARENT_ID", 1, 1),
        (name, DB, "children_ghost_fk", name, DB, "children", "child_id", 1, 1),
        (name, DB, "children_remote_fk", name, DB, "children", "child_id", 1, 1),
        (name, DB, "children_outside_fk", name, DB, "children", "child_id", 1, 1),
        (name, DB, "outside_pk", name, DB, "outside", "k", 1, None),
    ]
    referential_constraints = [
        # The referenced constraint is spelled in upper case, the schema in mixed case.
        (name, DB, "children_parent_fk", name, DB.upper(), "PARENTS_PK"),
        (name, DB, "children_ghost_fk", name, DB, "ghost_pk"),
        (name, DB, "children_remote_fk", "td_missing_catalog", "ledger", "accounts_pk"),
        (name, DB, "children_outside_fk", name, DB, "outside_pk"),
    ]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {INFO}")
    for table, rows, columns in (
        (
            "table_constraints",
            table_constraints,
            [
                "constraint_catalog",
                "constraint_schema",
                "constraint_name",
                "table_schema",
                "table_name",
                "constraint_type",
            ],
        ),
        (
            "key_column_usage",
            key_column_usage,
            [
                "constraint_catalog",
                "constraint_schema",
                "constraint_name",
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "position_in_unique_constraint",
            ],
        ),
        (
            "referential_constraints",
            referential_constraints,
            [
                "constraint_catalog",
                "constraint_schema",
                "constraint_name",
                "unique_constraint_catalog",
                "unique_constraint_schema",
                "unique_constraint_name",
            ],
        ),
    ):
        spark.sql(f"DROP TABLE IF EXISTS {INFO}.{table}")
        spark.sql(f"CREATE TABLE {INFO}.{table} AS {_values(rows, columns)}")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS {INFO} CASCADE")


def _config():
    return deep_merge(
        default_config(),
        {
            "analysis_level": "deep",
            "deep": {
                "uniqueness": {"declared_keys": True},
                "referential": {"declared": True},
            },
        },
    )


def _profile(spark, capabilities, config, name):
    return profile_table(
        spark,
        f"{DB}.{name}",
        config,
        capabilities=capabilities,
        reference_time=format_utc(utc_now()),
        now=lambda: format_utc(utc_now()),
        log=lambda message: None,
    )


@pytest.fixture(scope="module")
def run(spark, catalog, capabilities):
    patch = pytest.MonkeyPatch()
    # Only the exclusion of the local session catalog is lifted: everything else is the real code.
    patch.setattr(runtime, "_SP_UNITY_EXCLUDED", ())
    try:
        config = _config()
        tables = [_profile(spark, capabilities, config, name) for name in ("parents", "children")]
        relationships, summary = validate_relationships(spark, tables, config, log=lambda m: None)
    finally:
        patch.undo()
    return {t["table_key"]: t for t in tables}, relationships, summary


def test_constraints_are_read_through_the_information_schema_queries(run, catalog):
    tables, _, _ = run
    parents, children = tables[f"{DB}.parents"], tables[f"{DB}.children"]
    for table in (parents, children):
        observed = {op["operation_id"]: op for op in table["operations"]["observed"]}
        assert observed["op_constraints"]["status"] == "succeeded"
    assert {c["name"]: c["constraint_type"] for c in parents["constraints"]} == {
        "parents_code_uk": "unique",
        "parents_pk": "primary_key",
    }
    fks = {c["name"]: c for c in children["constraints"]}
    assert fks["children_parent_fk"]["referenced"] == {
        "table": f"{catalog}.{DB}.parents",
        "columns": ["id"],
    }
    assert fks["children_outside_fk"]["referenced"]["table"] == f"{catalog}.{DB}.outside"
    assert fks["children_ghost_fk"]["referenced"] is None
    assert fks["children_remote_fk"]["referenced"] is None, "an unreadable catalog is not guessed"
    notes = " ".join(children["notes"])
    assert "children_ghost_fk" in notes and "children_remote_fk" in notes
    assert "td_missing_catalog" in notes


def test_declared_keys_are_checked_with_the_schema_spelling(run):
    tables, _, _ = run
    keys = {tuple(k["columns"]): k for k in tables[f"{DB}.parents"]["uniqueness"]["keys"]}
    assert set(keys) == {("ID",), ("Code",)}
    assert keys[("ID",)]["origins"] == ["declared_primary_key"]
    assert keys[("ID",)]["names"] == ["parents_pk"]
    assert keys[("ID",)]["outcome"] == "unique"
    code = keys[("Code",)]
    assert code["origins"] == ["declared_unique"] and code["outcome"] == "duplicates"
    assert metric_value(code["metrics"], "duplicate_key_groups") == 10, "a UNIQUE is not trusted"


def test_declared_foreign_keys_are_validated_against_the_data(spark, run):
    _, relationships, summary = run
    by_name = {r["name"]: r for r in relationships}
    assert set(by_name) == {"children_parent_fk", "children_outside_fk"}, (
        "unresolved FKs are skipped"
    )
    fk = by_name["children_parent_fk"]
    assert fk["origin"] == "declared_constraint" and fk["validation"] == "violated"
    orphans = spark.sql(
        f"SELECT count(*) AS n FROM {DB}.children c LEFT ANTI JOIN {DB}.parents p "
        "ON c.parent_id = p.ID"
    ).collect()[0]["n"]
    assert orphans == 8
    assert metric_value(fk["validation_detail"]["metrics"], "orphan_rows") == orphans
    assert [pair["to_column"] for pair in fk["validation_detail"]["type_compatibility"]] == ["ID"]
    outside = by_name["children_outside_fk"]
    assert outside["validation"] == "not_validated"
    assert "not profiled in this run" in outside["validation_detail"]["reason"]
    assert summary["planned"] == 1
