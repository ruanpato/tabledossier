"""Rebuild the PostgreSQL demo outputs committed under ``examples/postgres/output``.

Requires psycopg (``pip install "tabledossier[postgres]"``) and a disposable PostgreSQL server
whose administrator connection string is in ``TD_TEST_POSTGRES_DSN``::

    TD_TEST_POSTGRES_DSN="host=localhost port=5432 user=postgres password=..." \\
        python examples/postgres/build_demo_outputs.py

Steps (synthetic data only):

1. create the database ``tabledossier_demo`` (dropped first if it exists) and load
   ``create_demo_tables.sql`` as the administrator;
2. create a login role that may only read the demo schema;
3. run ``tabledossier profile --engine postgres`` AS THAT ROLE, at the standard and deep levels,
   into a temporary directory (the output directory is recorded as given: ``results``);
4. copy each result package to ``output/run`` and ``output/deep``;
5. drop the database and the role.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import sys
import tempfile
from pathlib import Path

import psycopg
from psycopg import sql

from tabledossier.cli import main as cli

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "output"
DATABASE = "tabledossier_demo"
ROLE = "tabledossier_demo_reader"


def _admin(dsn: str, **overrides: str) -> psycopg.Connection:
    return psycopg.connect(psycopg.conninfo.make_conninfo(dsn, **overrides), autocommit=True)


def build() -> None:
    """Rebuild every committed PostgreSQL demo artefact."""
    admin = os.environ.get("TD_TEST_POSTGRES_DSN")
    if not admin:
        raise SystemExit("set TD_TEST_POSTGRES_DSN to the administrator DSN of a disposable server")
    password = "demo-" + secrets.token_hex(16)
    with _admin(admin) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(DATABASE))
        )
        conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(ROLE)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE)))
        conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(ROLE), sql.Literal(password)
            )
        )
    try:
        with _admin(admin, dbname=DATABASE) as conn:
            conn.execute((HERE / "create_demo_tables.sql").read_text(encoding="utf-8"))
            conn.execute(
                sql.SQL(
                    "GRANT CONNECT ON DATABASE {db} TO {role}; GRANT USAGE ON SCHEMA analytics TO "
                    "{role}; GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO {role}"
                ).format(db=sql.Identifier(DATABASE), role=sql.Identifier(ROLE))
            )
        reader = psycopg.conninfo.make_conninfo(
            admin, dbname=DATABASE, user=ROLE, password=password
        )
        if OUTPUT.exists():
            shutil.rmtree(OUTPUT)
        config = json.loads((HERE / "demo.config.json").read_text(encoding="utf-8"))
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            try:
                for level, target in (("standard", "run"), ("deep", "deep")):
                    Path("config.json").write_text(
                        json.dumps({**config, "analysis_level": level}), encoding="utf-8"
                    )
                    os.environ["TD_DEMO_READER_DSN"] = reader
                    code = cli(
                        [
                            "profile",
                            "--engine",
                            "postgres",
                            "--config",
                            "config.json",
                            "--output",
                            "results",
                            "--dsn-env",
                            "TD_DEMO_READER_DSN",
                        ]
                    )
                    if code not in (0, 5):
                        raise SystemExit(f"{level} run exited with {code}")
                    runs = sorted(Path("results").iterdir())
                    shutil.copytree(runs[-1], OUTPUT / target)
                    shutil.rmtree("results")
            finally:
                os.environ.pop("TD_DEMO_READER_DSN", None)
                os.chdir(cwd)
    finally:
        with _admin(admin) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(DATABASE))
            )
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(ROLE)))
    print(f"PostgreSQL demo outputs rebuilt under {OUTPUT}", file=sys.stderr)


if __name__ == "__main__":
    build()
