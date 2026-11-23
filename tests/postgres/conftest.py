"""PostgreSQL integration fixtures.

Skipped unless ``TD_TEST_POSTGRES_DSN`` holds the administrator connection string of a
DISPOSABLE server (CI: a service container; locally: Docker, or ``initdb`` and ``pg_ctl``; see
CONTRIBUTING.md). Each test session creates its own database and a SELECT-only login role, loads
the synthetic demo plus the test tables, and drops both at the end. The connector under test
always connects as that role, through ``--dsn-env``.
"""

from __future__ import annotations

import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
ADMIN_ENV = "TD_TEST_POSTGRES_DSN"
READER_ENV = "TD_TEST_POSTGRES_READER_DSN"
PASSWORD_MARKER = "SECRET-PASSWORD-MARKER"


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/postgres" in str(item.fspath).replace(os.sep, "/"):
            item.add_marker(pytest.mark.postgres)


@dataclass
class PgEnv:
    """The disposable test database."""

    admin_dsn: str
    reader_dsn: str
    database: str
    role: str
    password: str
    server_version_num: int


@pytest.fixture(scope="session")
def psycopg():
    return pytest.importorskip("psycopg")


@pytest.fixture(scope="session")
def pg_env(psycopg):
    admin = os.environ.get(ADMIN_ENV)
    if not admin:
        pytest.skip(
            f"set {ADMIN_ENV} to the administrator connection string of a disposable PostgreSQL "
            "server to run the PostgreSQL integration tests"
        )
    from psycopg import sql

    suffix = secrets.token_hex(4)
    database = f"td_test_{suffix}"
    role = f"td_reader_{suffix}"
    password = f"pw-{PASSWORD_MARKER}-{secrets.token_hex(8)}"
    with psycopg.connect(admin, autocommit=True) as conn:
        version = int(conn.execute("SHOW server_version_num").fetchone()[0])
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)
            )
        )
    admin_db = psycopg.conninfo.make_conninfo(admin, dbname=database)
    try:
        with psycopg.connect(admin_db, autocommit=True) as conn:
            conn.execute((ROOT / "examples" / "postgres" / "create_demo_tables.sql").read_text())
            conn.execute((Path(__file__).parent / "fixtures.sql").read_text())
            for schema in ("analytics", "td_types", "Hostile Schema"):
                conn.execute(
                    sql.SQL(
                        "GRANT USAGE ON SCHEMA {s} TO {r}; GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO {r}"
                    ).format(s=sql.Identifier(schema), r=sql.Identifier(role))
                )
            conn.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database), sql.Identifier(role)
                )
            )
            # Column privileges: the role may read only the first column of this table.
            conn.execute(
                sql.SQL(
                    "REVOKE SELECT ON td_types.partly_readable FROM {r}; GRANT SELECT (id) ON td_types.partly_readable TO {r}"
                ).format(r=sql.Identifier(role))
            )
        reader = psycopg.conninfo.make_conninfo(
            admin, dbname=database, user=role, password=password
        )
        yield PgEnv(admin_db, reader, database, role, password, version)
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database))
            )
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


@pytest.fixture
def admin(pg_env, psycopg):
    """Administrator connection to the test database (setup and ground truth only)."""
    with psycopg.connect(pg_env.admin_dsn, autocommit=True) as conn:
        yield conn


def scalar(conn, query: str, *params: Any) -> Any:
    """Run an independent ground-truth query."""
    return conn.execute(query, params).fetchone()[0]


@dataclass
class Run:
    code: int
    stdout: str
    stderr: str
    run_dir: Path | None
    profile: dict[str, Any] | None

    def table(self, key: str) -> dict[str, Any]:
        return next(t for t in self.profile["tables"] if t["table_key"] == key)

    def field(self, table: str, name: str) -> dict[str, Any]:
        return next(f for f in self.table(table)["field_profiles"] if f["path"][0]["name"] == name)

    def metric(self, table: str, name: str, metric: str) -> Any:
        found = next(m for m in self.field(table, name)["metrics"] if m["name"] == metric)
        return found["value"] if found["status"] == "measured" else found["status"]

    def package_text(self) -> str:
        return "\n".join(p.read_text(encoding="utf-8") for p in sorted(self.run_dir.iterdir()))


@pytest.fixture
def profile_run(pg_env, tmp_path, monkeypatch, capsys):
    """Run ``tabledossier profile --engine postgres`` as the reader role."""
    from tabledossier.cli import main

    counter = {"n": 0}

    def run(config: dict[str, Any], *extra: str, dsn: str | None = None) -> Run:
        counter["n"] += 1
        work = tmp_path / f"run{counter['n']}"
        work.mkdir()
        full = {"kind": "tabledossier.config", "config_version": "1.0", **config}
        (work / "config.json").write_text(json.dumps(full), encoding="utf-8")
        monkeypatch.setenv(READER_ENV, dsn if dsn is not None else pg_env.reader_dsn)
        monkeypatch.chdir(work)
        capsys.readouterr()
        code = main(
            [
                "profile",
                "--engine",
                "postgres",
                "--config",
                "config.json",
                "--output",
                "results",
                "--dsn-env",
                READER_ENV,
                *extra,
            ]
        )
        out = capsys.readouterr()
        results = work / "results"
        runs = sorted(results.iterdir()) if results.exists() else []
        run_dir = runs[-1] if runs else None
        profile = None
        if run_dir is not None and (run_dir / "profile.json").exists():
            profile = json.loads((run_dir / "profile.json").read_text(encoding="utf-8"))
        return Run(code, out.out, out.err, run_dir, profile)

    return run
