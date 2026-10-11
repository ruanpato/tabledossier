"""PostgreSQL connector robustness: read-only, isolation, hostile input, credentials, consistency."""

import json

import pytest

from conftest import PASSWORD_MARKER, scalar
from tabledossier.connectors import postgres as connector
from tabledossier.connectors.sqlcompose import ident, sql

HOSTILE_TABLE = '`Hostile Schema`.`x"; DROP TABLE td_types.coverage; --`'


def test_the_session_cannot_write(pg_env, psycopg):
    with (
        psycopg.connect(pg_env.reader_dsn, autocommit=False) as holder,
        psycopg.connect(pg_env.reader_dsn, autocommit=False) as worker,
    ):
        for conn in (holder, worker):
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            conn.read_only = True
        snapshot = holder.execute("SELECT pg_export_snapshot()").fetchone()[0]
        session = connector.PgSession(
            psycopg, worker, snapshot, connector.ConnectionSettings(), secrets=[]
        )
        with session.table_transaction():
            with session.operation():
                assert (
                    session.rows(sql("SHOW transaction_read_only"))[0]["transaction_read_only"]
                    == "on"
                )
            with pytest.raises(connector.EngineError) as caught, session.operation():
                session.rows(sql("CREATE TEMP TABLE td_write_attempt (i integer)"))
            assert caught.value.sqlstate == "25006"
            with pytest.raises(connector.EngineError) as caught, session.operation():
                session.rows(
                    sql(
                        "INSERT INTO ", ident("analytics", "returns"), " VALUES (1, 'x', 'y', NULL)"
                    )
                )
            assert caught.value.sqlstate in ("25006", "42501")
            with session.operation():  # the transaction survives both failures
                assert session.rows(sql("SELECT 1 AS one"))[0]["one"] == 1
        holder.rollback()


def test_a_statement_timeout_costs_only_its_table(profile_run):
    run = profile_run(
        {"tables": ["td_types.slow", "analytics.customers"]}, "--statement-timeout", "1"
    )
    assert run.code == 5, run.stderr
    slow = run.table("td_types.slow")
    assert slow["status"] == "partial"
    assert [error["condition"] for error in slow["errors"]] == ["57014"]
    assert run.table("analytics.customers")["status"] == "succeeded"


def test_a_missing_table_is_isolated(profile_run):
    run = profile_run({"tables": ["analytics.nope", "analytics.customers"]})
    assert run.code == 5, run.stderr
    missing = run.table("analytics.nope")
    assert missing["status"] == "failed"
    assert missing["errors"][0]["condition"] == "TABLE_NOT_FOUND"
    assert run.table("analytics.customers")["status"] == "succeeded"


def test_hostile_identifiers_are_quoted(profile_run, admin):
    run = profile_run({"tables": [HOSTILE_TABLE]})
    assert run.code == 0, run.stderr
    assert scalar(admin, "SELECT count(*) FROM td_types.coverage") == 4
    table = run.profile["tables"][0]
    names = {field["path"][0]["name"] for field in table["field_profiles"]}
    assert names == {'a"b', "c%d", "%s", "%(x)s", "sp ace", "semi;colon", "naïve ✓"}
    by_name = {field["path"][0]["name"]: field for field in table["field_profiles"]}
    nulls = {m["name"]: m["value"] for m in by_name["c%d"]["metrics"]}
    assert nulls["null_count"] == 0 and nulls["max"] == 2
    assert {m["name"]: m["value"] for m in by_name["%s"]["metrics"]}["null_count"] == 1


def test_hostile_filter_values_are_bound(profile_run, admin):
    hostile = "'; DROP TABLE td_types.coverage; --"
    run = profile_run(
        {
            "tables": ["td_types.coverage"],
            "table_options": {
                "td_types.coverage": {
                    "filters": [
                        {"column": "tx", "operator": "in", "value": [hostile, "abc"]},
                        {"column": "u", "operator": "is_not_null"},
                        {"column": "m", "operator": "ne", "value": "%s"},
                    ]
                }
            },
        }
    )
    assert run.code == 0, run.stderr
    assert scalar(admin, "SELECT count(*) FROM td_types.coverage") == 4
    assert run.table("td_types.coverage")["table_metrics"][0]["value"] == scalar(
        admin,
        "SELECT count(*) FROM td_types.coverage WHERE tx IN ('abc') AND u IS NOT NULL "
        "AND m::text <> '%%s'",
    )


def test_filters_on_uuid_enum_and_text_use_byte_order(profile_run, admin):
    run = profile_run(
        {
            "tables": ["td_types.coverage"],
            "table_options": {
                "td_types.coverage": {
                    "filters": [
                        {
                            "column": "u",
                            "operator": "in",
                            "value": ["00000000-0000-4000-8000-000000000001"],
                        },
                    ]
                }
            },
        }
    )
    assert run.table("td_types.coverage")["table_metrics"][0]["value"] == 1
    mismatch = profile_run(
        {
            "tables": ["td_types.coverage"],
            "table_options": {
                "td_types.coverage": {
                    "filters": [
                        {"column": "i", "operator": "eq", "value": "x", "value_type": "string"}
                    ]
                }
            },
        }
    )
    table = mismatch.table("td_types.coverage")
    assert table["status"] == "failed"
    assert table["errors"][0]["condition"] == "FILTER_TYPE_MISMATCH"


def test_functions_in_other_schemas_cannot_shadow_builtins(profile_run, admin):
    run = profile_run({"tables": ["td_types.coverage"]})
    assert run.metric("td_types.coverage", "vc", "max_length") == scalar(
        admin, "SELECT max(pg_catalog.char_length(vc)) FROM td_types.coverage"
    )
    assert "HIJACKED" not in run.package_text()


def test_every_table_reads_the_same_snapshot(profile_run, admin, monkeypatch):
    original = connector.profile_table
    calls = {"n": 0}

    def profile_then_write(db, input_text, config, run):
        table = original(db, input_text, config, run)
        calls["n"] += 1
        if calls["n"] == 1:
            admin.execute("INSERT INTO td_types.mixed VALUES (99, 99)")
        return table

    monkeypatch.setattr(connector, "profile_table", profile_then_write)
    try:
        run = profile_run({"tables": ["analytics.returns", "td_types.mixed"]})
    finally:
        admin.execute("DELETE FROM td_types.mixed WHERE v = 99")
    assert run.table("td_types.mixed")["table_metrics"][0]["value"] == 2


def test_row_level_security_is_reported(profile_run):
    run = profile_run({"tables": ["td_types.tenants"]})
    table = run.table("td_types.tenants")
    assert table["table_metrics"][0]["value"] == 2
    assert any("Row-level security" in note for note in table["notes"])


def test_unreadable_columns_are_omitted(profile_run):
    run = profile_run({"tables": ["td_types.partly_readable"]})
    assert run.code == 0, run.stderr
    secret = run.field("td_types.partly_readable", "secret")
    assert secret["profiled"] is False and secret["omission_reason"] == "no_select_privilege"
    assert "SECRET-COLUMN-MARKER" not in run.package_text()


def test_constraint_mapping_edge_cases(profile_run):
    run = profile_run(
        {"tables": ["td_types.child", "td_types.linked"], "analysis_level": "metadata"}
    )
    child = run.table("td_types.child")
    assert [(c["name"], c["referenced"]) for c in child["constraints"]] == [
        ("child_pt_fk", {"table": "td_types.pt", "columns": ["id"]})
    ]
    linked = {c["name"]: c for c in run.table("td_types.linked")["constraints"]}
    assert linked["linked_idx_fk"]["referenced"] == {
        "table": "td_types.refs",
        "columns": ["code", "id"],
    }
    assert linked["linked_permuted_fk"]["columns"] == ["y", "x"]
    assert linked["linked_permuted_fk"]["referenced"]["columns"] == ["b", "a"]
    check = linked["linked_positive"]
    assert check["constraint_type"] == "check" and "NOT VALID" not in check["expression"]
    assert any("linked_positive" in note for note in run.table("td_types.linked")["notes"])


def test_names_that_differ_only_by_case(profile_run):
    exact = profile_run({"tables": ["td_types.`Mixed`"], "analysis_level": "metadata"})
    assert [f["path"][0]["name"] for f in exact.profile["tables"][0]["field_profiles"]] == ["v"]
    ambiguous = profile_run({"tables": ["td_types.MIXED"], "analysis_level": "metadata"})
    table = ambiguous.profile["tables"][0]
    assert table["status"] == "failed"
    assert table["errors"][0]["condition"] == "AMBIGUOUS_TABLE_NAME"


@pytest.mark.parametrize("view", ["td_types.casted", "td_types.raising"])
def test_values_in_engine_errors_are_not_recorded(profile_run, view):
    run = profile_run({"tables": [view]})
    assert run.code in (5, 6), run.stderr
    table = run.profile["tables"][0]
    assert table["errors"]
    text = run.package_text() + run.stdout + run.stderr
    assert "SECRET-ROW-MARKER" not in text and "999-88-7777" not in text


def test_a_float_overflow_fails_only_the_float_aggregates(profile_run):
    run = profile_run({"tables": ["td_types.huge_floats"]})
    table = "td_types.huge_floats"
    assert run.metric(table, "f", "mean") == "error"
    assert run.metric(table, "f", "max") == 1e300
    assert run.metric(table, "label", "null_count") == 0


def test_json_validation_follows_the_server_version(profile_run, pg_env):
    run = profile_run({"tables": ["analytics.order_events"]})
    payload = {m["name"]: m for m in run.field("analytics.order_events", "payload")["metrics"]}
    if pg_env.server_version_num >= 160000:
        assert payload["json_invalid_count"]["status"] == "measured"
        assert "pg_input_is_valid" in payload["json_invalid_count"]["method"]
    else:
        assert payload["json_invalid_count"]["status"] == "unsupported"
        unsupported = run.table("analytics.order_events")["unsupported"]
        assert {"json_validation"} <= {item["capability"] for item in unsupported}


def test_connection_failures_print_no_credentials(profile_run, pg_env, psycopg):
    wrong = psycopg.conninfo.make_conninfo(pg_env.reader_dsn, password=f"bad-{PASSWORD_MARKER}")
    run = profile_run({"tables": ["analytics.returns"]}, dsn=wrong)
    assert run.code == 9
    assert run.run_dir is None
    for secret in (PASSWORD_MARKER, pg_env.role, pg_env.database):
        assert secret not in run.stderr + run.stdout
    empty = profile_run({"tables": ["analytics.returns"]}, dsn="  ")
    assert empty.code == 9 and "is not set or is empty" in empty.stderr


def test_deep_level_reports_what_postgresql_does_not_support(profile_run):
    config = json.loads(json.dumps({"tables": ["analytics.orders"], "analysis_level": "deep"}))
    run = profile_run(config)
    table = run.table("analytics.orders")
    assert table["deep"] is None
    assert "deep_elements" in {item["capability"] for item in table["unsupported"]}
    assert run.profile["relationship_hypotheses"]["reason"].startswith("disabled by configuration")


def test_planned_reads_stay_within_the_budgets(profile_run):
    run = profile_run({"tables": ["analytics.customers", "analytics.order_events"]})
    for table in run.profile["tables"]:
        reading = [op for op in table["operations"]["planned"] if op["reads_user_data"]]
        assert len(reading) <= 1 + 2  # one sample plus at most max_aggregate_passes
        observed = {op["operation_id"] for op in table["operations"]["observed"]}
        assert {op["operation_id"] for op in reading} <= observed
