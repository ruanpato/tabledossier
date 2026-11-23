"""The PostgreSQL connector's table pipeline with a fake session (no driver, no server)."""

import re
from contextlib import contextmanager

import pytest

from tabledossier.assemble import build_profile
from tabledossier.config import default_config
from tabledossier.connectors import postgres as connector
from tabledossier.connectors.sqlcompose import Ident, Sql, debug_render
from tabledossier.validation import check_profile

TYPES = [
    {
        "oid": 23,
        "typname": "int4",
        "typtype": "b",
        "typcategory": "N",
        "typbasetype": 0,
        "typtypmod": -1,
        "typelem": 0,
        "builtin": True,
    },
    {
        "oid": 25,
        "typname": "text",
        "typtype": "b",
        "typcategory": "S",
        "typbasetype": 0,
        "typtypmod": -1,
        "typelem": 0,
        "builtin": True,
    },
    {
        "oid": 16,
        "typname": "bool",
        "typtype": "b",
        "typcategory": "B",
        "typbasetype": 0,
        "typtypmod": -1,
        "typelem": 0,
        "builtin": True,
    },
]
COLUMNS = [
    {
        "attnum": 1,
        "name": "id",
        "not_null": True,
        "type_oid": 23,
        "typmod": -1,
        "physical_type": "integer",
        "comment": "key",
        "readable": True,
        "element_physical_type": None,
    },
    {
        "attnum": 2,
        "name": "name",
        "not_null": False,
        "type_oid": 25,
        "typmod": -1,
        "physical_type": "text",
        "comment": None,
        "readable": True,
        "element_physical_type": None,
    },
    {
        "attnum": 3,
        "name": "flag",
        "not_null": False,
        "type_oid": 16,
        "typmod": -1,
        "physical_type": "boolean",
        "comment": None,
        "readable": True,
        "element_physical_type": None,
    },
]


def _expressions(statement):
    """Split ``SELECT <expr> AS <alias>, ...`` into ``(alias, expression text)`` pairs."""
    out, current = [], []
    for part in statement.parts:
        alias = isinstance(part, Ident) and re.fullmatch(r"a\d{4}", part.names[0])
        if alias and current and isinstance(current[-1], str) and current[-1].endswith(" AS "):
            current[-1] = current[-1][: -len(" AS ")]
            text = debug_render(Sql(tuple(current)))[0]
            out.append((part.names[0], text.split("SELECT ", 1)[-1].removeprefix(", ")))
            current = []
        else:
            current.append(part)
    return out


class FakeSession(connector.Session):
    """Answers the connector's statements from canned catalog rows and simple aggregates."""

    def __init__(self, *, relations=None, fail=None, lost=False):
        self.relations = (
            relations
            if relations is not None
            else [{"oid": 7, "schema_name": "s", "table_name": "t", "relkind": "r"}]
        )
        self.fail = fail
        self.lost = lost
        self.statements = []

    @contextmanager
    def operation(self):
        yield

    @contextmanager
    def table_transaction(self):
        yield

    def stream(self, statement, consume):
        self.statements.append(debug_render(statement)[0])
        return consume(iter([("a", False), ("b", False), (None, None)]))

    def _aggregate(self, text, aliases):
        if self.fail:
            raise connector.EngineError(self.fail, "canceling statement", lost=self.lost)
        row = {}
        for alias, expression in aliases:
            if "percentile_disc" in expression:
                value = [1, 1, 2, 3, 3]
            elif "bool_and" in expression or "= pg_catalog.max(" in expression:
                value = False
            elif "FILTER" in expression and expression.startswith("pg_catalog.count(*)"):
                value = 0
            elif expression.startswith(("pg_catalog.count(", "pg_catalog.count(DISTINCT")):
                value = 3
            elif "stddev_samp" in expression:
                value = 1.0
            elif "pg_catalog.avg(" in expression:
                value = 2.0
            else:
                value = 1
            row[alias] = value
        return [row]

    def rows(self, statement):
        text, _ = debug_render(statement)
        self.statements.append(text)
        if "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace" in text:
            return list(self.relations)
        if "pg_catalog.obj_description" in text:
            return [
                {
                    "relkind": "r",
                    "reltuples": 3.0,
                    "comment": "synthetic",
                    "can_select": True,
                    "rls_enabled": False,
                    "rls_forced": False,
                    "rls_active": False,
                    "size_bytes": 8192,
                }
            ]
        if "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type" in text:
            return list(COLUMNS)
        if "FROM pg_catalog.pg_type t WHERE" in text:
            return list(TYPES)
        if "FROM pg_catalog.pg_constraint con" in text:
            return [
                {
                    "constraint_oid": 9,
                    "name": "t_pk",
                    "contype": "p",
                    "validated": True,
                    "enforced": True,
                    "ord": 1,
                    "column_name": "id",
                    "ref_schema": None,
                    "ref_table": None,
                    "ref_column": None,
                    "definition": "PRIMARY KEY (id)",
                }
            ]
        if text.startswith("EXPLAIN"):
            return []
        if text.startswith("SELECT ") and " AS " in text:
            return self._aggregate(text, _expressions(statement))
        raise AssertionError(f"unexpected statement: {text}")


def _run():
    return connector.RunContext(
        server_version_num=170011,
        search_schemas=["public"],
        database_name="db",
        json_validation=True,
        text_decoding=True,
        snapshot_started_at="2026-10-11T10:00:00.000Z",
        reference_time="2026-10-11T10:00:00.000Z",
        now=lambda: "2026-10-11T10:00:01.000Z",
        log=lambda text: None,
    )


def _config(level="standard"):
    config = default_config()
    config["tables"] = ["s.t"]
    config["analysis_level"] = level
    return config


def _profile(tables, config):
    return build_profile(
        run_id="20261011T100000Z-0a1b2c3d",
        started_at="2026-10-11T10:00:00.000Z",
        finished_at="2026-10-11T10:00:02.000Z",
        duration_ms=2000,
        reference_time="2026-10-11T10:00:00.000Z",
        environment={
            "engine": "postgresql",
            "execution_context": "local_cli",
            "python_version": "3.12.0",
            "postgresql_version": "17.11",
            "session_timezone": "UTC",
        },
        config=config,
        parameter_sources={},
        generation={"generator_version": "0.5.0", "generation_id": None},
        capabilities={},
        tables=tables,
    )


@pytest.mark.parametrize("level", ["metadata", "standard"])
def test_a_table_profile_is_a_valid_postgresql_profile(level):
    config = _config(level)
    table = connector.profile_table(FakeSession(), "s.t", config, _run())
    assert table["status"] == "succeeded", table["errors"]
    profile = _profile([table], config)
    assert check_profile(profile) == []
    assert table["identifier"]["quoted"] == '"s"."t"'
    assert table["constraints"][0]["source"] == "system_catalog"
    if level == "standard":
        assert table["consistency"]["mode"] == "transaction_snapshot"
        assert table["scope"]["scope_label"] == "full_snapshot"
        distinct = next(
            m
            for f in table["field_profiles"]
            for m in f["metrics"]
            if m["name"] == "approx_distinct_count"
        )
        assert distinct["accuracy"] == "exact"
        assert table["sample"]["rows_collected"] == 3
    else:
        assert table["consistency"]["mode"] == "metadata_only"
        assert all(not op["reads_user_data"] for op in table["operations"]["planned"])
        assert table["table_metrics"][0]["accuracy"] == "as_recorded"


def test_a_failed_pass_makes_the_table_partial():
    table = connector.profile_table(FakeSession(fail="57014"), "s.t", _config(), _run())
    assert table["status"] == "partial"
    assert [(e["stage"], e["condition"]) for e in table["errors"]] == [("aggregate", "57014")]
    assert check_profile(_profile([table], _config())) == []


def test_a_lost_connection_is_a_connect_error():
    session = FakeSession(fail=None)
    session.fail, session.lost = "57P01", True
    table = connector.profile_table(session, "s.t", _config(), _run())
    assert table["errors"][0]["stage"] == "connect"


def test_unresolved_and_ambiguous_tables_fail_with_a_resolve_error():
    missing = connector.profile_table(FakeSession(relations=[]), "s.t", _config(), _run())
    assert missing["status"] == "failed"
    assert missing["errors"][0]["condition"] == "TABLE_NOT_FOUND"
    twins = [
        {"oid": 1, "schema_name": "s", "table_name": "Mixed", "relkind": "r"},
        {"oid": 2, "schema_name": "s", "table_name": "mixed", "relkind": "r"},
    ]
    exact = connector.profile_table(FakeSession(relations=twins), "s.`Mixed`", _config(), _run())
    assert exact["status"] == "succeeded", exact["errors"]
    ambiguous = connector.profile_table(FakeSession(relations=twins), "s.MIXED", _config(), _run())
    assert ambiguous["errors"][0]["condition"] == "AMBIGUOUS_TABLE_NAME"
    cross = connector.profile_table(FakeSession(), "other.s.t", _config(), _run())
    assert cross["errors"][0]["condition"] == "CROSS_DATABASE_NOT_SUPPORTED"


def test_safe_messages_drop_values_hosts_and_passwords():
    class DataError(Exception):
        sqlstate = "22P02"

    assert connector.safe_message(DataError('invalid input: "7" SECRET 999')) == "DataError"

    class Refused(Exception):
        sqlstate = None

    message = connector.safe_message(
        Refused(
            'connection to server at "db.internal" (10.1.2.3), port 5432 failed: FATAL: '
            "password authentication failed for user reader"
        ),
        secrets=["reader"],
    )
    for secret in ("db.internal", "10.1.2.3", "5432", "reader"):
        assert secret not in message


def test_dsn_env_names_and_empty_variables():
    with pytest.raises(ValueError):
        connector.check_dsn_env_name("postgresql://u:p@h/d")
    assert connector.check_dsn_env_name("TD_DSN") == "TD_DSN"
    settings = connector.ConnectionSettings(dsn_env="TD_DSN")
    with pytest.raises(connector.ConnectFailed):
        connector.connection_string(settings, {"TD_DSN": "  "})
    assert connector.connection_string(settings, {"TD_DSN": "dbname=x"}) == "dbname=x"
    assert connector.connection_string(connector.ConnectionSettings(), {}) == ""
