"""PostgreSQL connector: profile tables from the local CLI with psycopg 3, read-only.

One run reads ONE snapshot. A holder connection opens a ``READ ONLY REPEATABLE READ``
transaction and exports its snapshot; every table is then profiled by a worker connection in
its own read-only transaction that imports that snapshot (``SET TRANSACTION SNAPSHOT``), so
locks are released after each table while all tables, samples, aggregation passes and checks
see the same data. Inside a table transaction every operation runs in a savepoint: a failed
statement (a timeout, a rejected expression) costs only that operation.

The driver is imported only by :func:`load_driver`; importing this module never loads it.
Statements are composed with :mod:`tabledossier.connectors.sqlcompose` (names as
``psycopg.sql.Identifier``, values as bound parameters) and compiled by
:mod:`tabledossier.connectors.postgres_plan`.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from tabledossier import __version__
from tabledossier.assemble import (
    assemble_field_results,
    build_profile,
    finalize_table,
    known_relationships,
    new_table,
    policy_field_ids,
    table_field_profiles,
)
from tabledossier.config import table_options_for
from tabledossier.connectors import postgres_plan as plan_sql
from tabledossier.connectors.postgres_plan import ColumnInfo
from tabledossier.connectors.sqlcompose import Ident, Param, Sql, quote_ident
from tabledossier.errors import sanitize_message
from tabledossier.integrity import hypotheses_record, not_validated_detail, referential_summary
from tabledossier.jsonutil import format_utc, pretty_json, utc_now
from tabledossier.metrics import measured, not_measured
from tabledossier.package import (
    build_documents,
    new_run_id,
    run_manifest,
    running_manifest,
    write_files,
)
from tabledossier.paths import (
    IdentifierError,
    column_reference_segments,
    field_id,
    parse_table_identifier,
    table_id,
    table_key,
    table_lookup_key,
)
from tabledossier.planning import (
    aggregate_operations,
    build_schema_tree,
    collect_sample_rows,
    elapsed_ms,
    iter_nodes,
    new_sample_record,
    observed_operation,
    operation,
    plan_aggregates,
    plan_sample,
    sample_notes,
    scope_label,
    select_profile_fields,
)
from tabledossier.relationships import key_constraints_from_rows
from tabledossier.semantic import sample_semantics

INSTALL_HINT = 'pip install "tabledossier[postgres]"'
_DSN_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}(?![\w:])")
_PORT = re.compile(r"\bport \d+\b")
_DEEP_PART_I_DETAIL = (
    "deep level, part I (array and map elements, JSON paths) is not supported by the "
    "PostgreSQL connector in this release; the standard metrics of every field were computed"
)
_HYPOTHESES_REASON = "relationship hypotheses are not supported by the PostgreSQL connector"
_JSON_UNSUPPORTED = (
    "pg_input_is_valid is not available before PostgreSQL 16; JSON validity is reported for "
    "the sample only"
)
_FLOAT_OVERFLOW_OPS = ("mean_value", "stddev_value", "quantiles")


class DriverMissingError(RuntimeError):
    """psycopg is not installed."""


class ConnectFailed(RuntimeError):
    """The connection could not be opened (the message is safe to print)."""


class EngineError(Exception):
    """A statement failed; ``message`` is sanitized and safe to record."""

    def __init__(self, sqlstate: str | None, message: str, *, lost: bool = False) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.message = message
        self.lost = lost

    @property
    def cause(self) -> str:
        """Return the SQLSTATE, or the error kind when there is none."""
        return self.sqlstate or ("ConnectionLost" if self.lost else "ClientError")


@dataclass(frozen=True)
class ConnectionSettings:
    """How to connect and the server-side limits of every statement."""

    dsn_env: str | None = None
    statement_timeout_s: int = 300
    lock_timeout_s: int = 10
    connect_timeout_s: int = 10
    work_mem: str = "16MB"


@dataclass
class RunContext:
    """Facts shared by every table of one run."""

    server_version_num: int
    search_schemas: list[str]
    database_name: str
    json_validation: bool
    text_decoding: bool
    snapshot_started_at: str
    reference_time: str
    now: Callable[[], str]
    log: Callable[[str], None]
    resolved: dict[tuple[str, str], str] = field(default_factory=dict)


def check_dsn_env_name(name: str) -> str:
    """Validate the name given to ``--dsn-env`` (never echo the value: it may be a DSN)."""
    if not _DSN_ENV_NAME.match(name):
        raise ValueError(
            "--dsn-env takes the NAME of an environment variable (letters, digits and "
            "underscores), not a connection string"
        )
    return name


def load_driver() -> Any:
    """Import psycopg, or raise :class:`DriverMissingError` with the install hint."""
    try:
        import psycopg  # the driver is optional and loaded on demand
    except ImportError as exc:
        raise DriverMissingError(
            f"the PostgreSQL driver (psycopg 3) is not installed: {INSTALL_HINT}"
        ) from exc
    logger = logging.getLogger("psycopg")
    # psycopg logs connection details (host, user, database) on some failures.
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return psycopg


def connection_string(settings: ConnectionSettings, environ: Mapping[str, str]) -> str:
    """Return the conninfo: an environment variable named on the command line, or libpq's own."""
    if settings.dsn_env is None:
        return ""
    value = environ.get(settings.dsn_env, "")
    if not value.strip():
        raise ConnectFailed(f"environment variable {settings.dsn_env} is not set or is empty")
    return value


def safe_message(exc: BaseException, secrets: Sequence[str] = ()) -> str:
    """Return a driver message with no data value, host, port, user, database or password.

    Data exceptions (SQLSTATE class 22), PL/pgSQL errors (P0) and internal errors carry
    values in unquoted or badly quoted text, so they are replaced by the error name.
    """
    sqlstate = getattr(exc, "sqlstate", None)
    name = type(exc).__name__
    if sqlstate and (sqlstate[:2] in ("22", "P0", "XX", "58") or not sqlstate[:2].isalnum()):
        return name
    diag = getattr(exc, "diag", None)
    text = getattr(diag, "message_primary", None) or str(exc) or name
    first = (text.strip().splitlines() or [name])[0]
    for value in secrets:
        if value and len(value) > 1:
            first = first.replace(value, "<redacted>")
    quote = min((i for i in (first.find('"'), first.find("'")) if i >= 0), default=-1)
    if quote >= 0:
        first = first[:quote] + "<redacted>"
    first = _PORT.sub("port <redacted>", first)
    first = _IPV4.sub(_redact_ip, first)
    first = _IPV6.sub(_redact_ip, first)
    return sanitize_message(first) or name


def _redact_ip(match: re.Match[str]) -> str:
    try:
        ipaddress.ip_address(match.group(0))
    except ValueError:
        return match.group(0)
    return "<host>"


def conninfo_secrets(psycopg: Any, conninfo: str) -> list[str]:
    """Values of the connection string that must never be printed."""
    if not conninfo:
        return [value for key, value in os.environ.items() if key.startswith("PG") and value]
    try:
        parsed = psycopg.conninfo.conninfo_to_dict(conninfo)
    except Exception:  # noqa: BLE001 - an unparsable string is reported without its content
        return [conninfo]
    return [str(value) for value in parsed.values() if value]


def to_driver(psycopg: Any, query: Sql) -> tuple[Any, list[Any] | None]:
    """Convert a composed query to ``psycopg.sql`` with placeholders and its parameters.

    When a statement has parameters psycopg scans the whole text (quoted identifiers
    included) for placeholders, so ``%`` is doubled everywhere; without parameters the text
    is sent as written.
    """
    pgsql = psycopg.sql
    has_params = any(isinstance(part, Param) for part in query.parts)
    pieces: list[Any] = []
    values: list[Any] = []
    for part in query.parts:
        if isinstance(part, str):
            pieces.append(pgsql.SQL(part.replace("%", "%%") if has_params else part))
        elif isinstance(part, Ident):
            names = [name.replace("%", "%%") if has_params else name for name in part.names]
            pieces.append(pgsql.Identifier(*names))
        else:
            pieces.append(pgsql.Placeholder())
            values.append(part.value)
    return pgsql.Composed(pieces), (values if has_params else None)


# --------------------------------------------------------------------------- sessions


class Session:
    """The executor seam: statements in, rows out, ``EngineError`` on failure."""

    def rows(self, statement: Sql) -> list[dict[str, Any]]:
        """Run a statement inside the current operation and return its rows."""
        raise NotImplementedError

    def operation(self) -> AbstractContextManager[None]:
        """Run the enclosed statements in a savepoint (a failure costs only them)."""
        raise NotImplementedError

    def stream(self, statement: Sql, consume: Callable[[Iterator[tuple[Any, ...]]], Any]) -> Any:
        """Run a statement through a server-side cursor inside one operation."""
        raise NotImplementedError

    def table_transaction(self) -> AbstractContextManager[None]:
        """Open a read-only transaction on the run snapshot; roll it back afterwards."""
        raise NotImplementedError


class PgSession(Session):
    """Worker connection: one transaction per table, importing the run's snapshot."""

    def __init__(
        self,
        psycopg: Any,
        connection: Any,
        snapshot_id: str,
        settings: ConnectionSettings,
        secrets: Sequence[str],
    ) -> None:
        self._pg = psycopg
        self._conn = connection
        self._snapshot = plan_sql.snapshot_literal(snapshot_id)
        self._settings = plan_sql.worker_settings_statement(
            statement_timeout=f"{settings.statement_timeout_s}s",
            lock_timeout=f"{settings.lock_timeout_s}s",
            work_mem=settings.work_mem,
        )
        self._secrets = list(secrets)
        self.lost = False

    def _error(self, exc: BaseException) -> EngineError:
        lost = bool(getattr(self._conn, "closed", False) or getattr(self._conn, "broken", False))
        self.lost = self.lost or lost
        return EngineError(
            getattr(exc, "sqlstate", None), safe_message(exc, self._secrets), lost=lost
        )

    def _check_open(self) -> None:
        status = self._conn.info.transaction_status
        if self.lost or status != self._pg.pq.TransactionStatus.INTRANS:
            self.lost = self.lost or bool(self._conn.closed or self._conn.broken)
            raise EngineError(None, "the table transaction is not open", lost=self.lost)

    def rows(self, statement: Sql) -> list[dict[str, Any]]:
        """Run a statement inside the current operation and return its rows."""
        query, params = to_driver(self._pg, statement)
        try:
            cursor = self._conn.cursor(row_factory=self._pg.rows.dict_row)
            cursor.execute(query, params)
            return list(cursor.fetchall()) if cursor.description else []
        except self._pg.Error as exc:
            raise self._error(exc) from None

    @contextmanager
    def operation(self) -> Iterator[None]:
        """Run the enclosed statements in a savepoint (a failure costs only them)."""
        self._check_open()
        try:
            with self._conn.transaction():
                yield
        except self._pg.Error as exc:
            raise self._error(exc) from None

    def stream(self, statement: Sql, consume: Callable[[Iterator[tuple[Any, ...]]], Any]) -> Any:
        """Run a statement through a server-side cursor inside one operation.

        The cursor is opened and closed inside the savepoint: closing a cursor whose
        savepoint was already rolled back would abort the whole table transaction.
        """
        query, params = to_driver(self._pg, statement)
        with self.operation(), self._conn.cursor(name="td_sample") as cursor:
            cursor.itersize = 500
            try:
                cursor.execute(query, params)
                return consume(iter(cursor))
            except self._pg.Error as exc:
                raise self._error(exc) from None

    @contextmanager
    def table_transaction(self) -> Iterator[None]:
        """Open a read-only transaction on the run snapshot; roll it back afterwards."""
        if self.lost or self._conn.closed:
            raise EngineError(None, "the connection to the server was lost", lost=True)
        try:
            with self._conn.transaction() as transaction:
                cursor = self._conn.cursor()
                cursor.execute(self._pg.sql.SQL("SET TRANSACTION SNAPSHOT " + self._snapshot))
                query, params = to_driver(self._pg, self._settings)
                cursor.execute(query, params)
                yield
                raise self._pg.Rollback(transaction)
        except self._pg.Error as exc:
            raise self._error(exc) from None


# --------------------------------------------------------------------------- helpers


def postgres_quoted(parts: Sequence[str]) -> str:
    """Display form of a table name in PostgreSQL quoting (never executed)."""
    return ".".join(quote_ident(part) for part in parts)


def _error_record(exc: EngineError, stage: str) -> dict[str, Any]:
    return {
        "stage": "connect" if exc.lost else stage,
        "error_class": "EngineError" if exc.sqlstate else type(exc).__name__,
        "condition": exc.sqlstate,
        "message": exc.message[:500],
    }


def _resolve_error(condition: str, message: str) -> dict[str, Any]:
    return {
        "stage": "resolve",
        "error_class": "ConfigurationError",
        "condition": condition,
        "message": message,
    }


def _finish(table: dict[str, Any], config: Mapping[str, Any], start: float) -> dict[str, Any]:
    table["timings_ms"]["total"] = elapsed_ms(start)
    finalize_table(table, config)
    return table


def _version_text(server_version_num: int) -> str:
    return f"{server_version_num // 10000}.{server_version_num % 10000}"


# --------------------------------------------------------------------------- catalog


def _resolve_relation(
    db: Session, parts: list[str], run: RunContext
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return ``(relation row, None)`` or ``(None, resolve error)``."""
    if any(len(part.encode("utf-8")) > 63 for part in parts):
        return None, _resolve_error(
            "IDENTIFIER_TOO_LONG",
            "a name is longer than 63 bytes, which PostgreSQL would silently truncate",
        )
    if len(parts) == 3:
        if parts[0] != run.database_name:
            return None, _resolve_error(
                "CROSS_DATABASE_NOT_SUPPORTED",
                "a three-part name must name the database of the connection",
            )
        schemas, name = [parts[1]], parts[2]
    elif len(parts) == 2:
        schemas, name = [parts[0]], parts[1]
    else:
        schemas, name = list(run.search_schemas), parts[0]
    with db.operation():
        candidates = db.rows(plan_sql.relation_lookup_statement(name, schemas))
    order = {schema.lower(): index for index, schema in enumerate(schemas)}
    candidates.sort(key=lambda row: order.get(str(row["schema_name"]).lower(), len(order)))
    exact = [
        row
        for row in candidates
        if row["table_name"] == name and (len(parts) == 1 or row["schema_name"] == schemas[0])
    ]
    if len(parts) == 1 and candidates:
        first_schema = candidates[0]["schema_name"]
        candidates = [row for row in candidates if row["schema_name"] == first_schema]
        exact = [row for row in exact if row["schema_name"] == first_schema]
    if exact:
        return exact[0], None
    if len(candidates) == 1:
        return candidates[0], None
    if len(candidates) > 1:
        return None, _resolve_error(
            "AMBIGUOUS_TABLE_NAME",
            "the name matches several relations that differ only by case; quote it exactly",
        )
    return None, _resolve_error("TABLE_NOT_FOUND", "the relation does not exist or is not visible")


def _columns(db: Session, oid: int) -> tuple[list[dict[str, Any]], dict[str, ColumnInfo]]:
    with db.operation():
        rows = db.rows(plan_sql.columns_statement(oid))
        types: dict[int, dict[str, Any]] = {}
        pending = {int(row["type_oid"]) for row in rows}
        while pending:
            for row in db.rows(plan_sql.types_statement(sorted(pending))):
                types[int(row["oid"])] = row
            nested = {
                int(row[key])
                for row in types.values()
                for key in ("typbasetype", "typelem")
                if row.get(key)
            }
            pending = nested - set(types)
    fields: list[dict[str, Any]] = []
    infos: dict[str, ColumnInfo] = {}
    for row in rows:
        descriptor, info = plan_sql.map_column(row, types)
        fields.append(descriptor)
        infos[info.name] = info
    return fields, infos


def _constraints(db: Session, oid: int, run: RunContext) -> tuple[list[dict[str, Any]], list[str]]:
    """PK, UNIQUE, FK and CHECK constraints, assembled by ``key_constraints_from_rows``."""
    with db.operation():
        rows = db.rows(
            plan_sql.constraints_statement(oid, server_version_num=run.server_version_num)
        )
    key_rows: list[dict[str, Any]] = []
    referenced_rows: list[dict[str, Any]] = []
    checks: dict[int, dict[str, Any]] = {}
    notes: list[str] = []
    kinds = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY"}
    for row in rows:
        enforcement = "enforced" if row["enforced"] else "not_enforced"
        if row["contype"] == "c":
            definition = str(row["definition"] or "")
            if definition.endswith(" NOT VALID"):
                definition = definition[: -len(" NOT VALID")]
            checks.setdefault(
                int(row["constraint_oid"]),
                {
                    "name": row["name"],
                    "constraint_type": "check",
                    "columns": [],
                    "expression": definition,
                    "referenced": None,
                    "enforcement": enforcement,
                    "source": "system_catalog",
                },
            )
            continue
        synthetic = f"fk:{row['constraint_oid']}"
        key_rows.append(
            {
                "constraint_catalog": "",
                "constraint_schema": "",
                "constraint_name": row["name"],
                "constraint_type": kinds[row["contype"]],
                "column_name": row["column_name"],
                "ordinal_position": row["ord"],
                "position_in_unique_constraint": row["ord"] if row["contype"] == "f" else None,
                "unique_constraint_catalog": "" if row["contype"] == "f" else None,
                "unique_constraint_schema": "" if row["contype"] == "f" else None,
                "unique_constraint_name": synthetic if row["contype"] == "f" else None,
                "enforcement": enforcement,
            }
        )
        if row["contype"] == "f":
            referenced_rows.append(
                {
                    "constraint_catalog": "",
                    "constraint_schema": "",
                    "constraint_name": synthetic,
                    "table_catalog": "",
                    "table_schema": row["ref_schema"],
                    "table_name": row["ref_table"],
                    "column_name": row["ref_column"],
                    "ordinal_position": row["ord"],
                }
            )
        if not row["validated"] and row["ord"] in (1, None):
            notes.append(
                f"Constraint {row['name']} was added NOT VALID: rows that existed before it was "
                "added were never checked."
            )
    for check in checks.values():
        name = check["name"]
        if any(r["name"] == name and not r["validated"] for r in rows):
            notes.append(
                f"Constraint {name} was added NOT VALID: rows that existed before it was added "
                "were never checked."
            )
    constraints, built_notes = key_constraints_from_rows(
        key_rows, referenced_rows, source="system_catalog", case_sensitive=True
    )
    targets = {
        row["name"]: (str(row["ref_schema"]), str(row["ref_table"]))
        for row in rows
        if row["contype"] == "f"
    }
    for constraint in constraints:
        reference = constraint.get("referenced")
        target = targets.get(constraint["name"])
        if reference is not None and target is not None:
            # The database is never recorded: the target is named schema.table (foreign keys
            # cannot cross databases); targets profiled in this run are renamed to their own
            # table key once every table is resolved (see run_profile).
            reference["table"] = table_key(list(target))
    return [*constraints, *checks.values()], [*notes, *built_notes]


def _source(
    detail: Mapping[str, Any], relkind: str, partition_columns: list[str], captured_at: str
) -> dict[str, Any]:
    table_types = {
        "r": "BASE TABLE",
        "p": "PARTITIONED TABLE",
        "v": "VIEW",
        "m": "MATERIALIZED VIEW",
        "f": "FOREIGN TABLE",
    }
    size = detail.get("size_bytes")
    size_metric = (
        measured(
            "size_in_bytes",
            int(size),
            unit="bytes",
            scope="table_metadata",
            accuracy="as_recorded",
            source="catalog_metadata",
            method="pg_total_relation_size (table, indexes and TOAST; partitions summed)",
        )
        if size is not None
        else not_measured(
            "size_in_bytes",
            "unavailable",
            "no storage size for this kind of relation",
            scope="table_metadata",
            source="catalog_metadata",
        )
    )
    return {
        "source_type": "view" if relkind in ("v", "m") else "table",
        "table_type": table_types.get(relkind),
        "provider": "postgresql",
        "comment": detail.get("comment"),
        "created_at": None,
        "last_modified": None,
        "partition_columns": partition_columns,
        "clustering_columns": [],
        "size_in_bytes": size_metric,
        "file_count": not_measured(
            "file_count",
            "unsupported",
            "PostgreSQL does not expose a file count",
            scope="table_metadata",
            source="catalog_metadata",
        ),
        "metadata_state": "current_at_capture",
        "captured_at": captured_at,
        "notes": [],
    }


def _statistics_row_count(reltuples: Any) -> dict[str, Any]:
    if reltuples is not None and float(reltuples) >= 0:
        return measured(
            "row_count",
            int(float(reltuples)),
            unit="rows",
            scope="table_metadata",
            accuracy="as_recorded",
            source="table_statistics",
            method="pg_class.reltuples (estimate maintained by VACUUM and ANALYZE)",
            details={
                "freshness": "unknown",
                "limitations": "An estimate recorded by the server; it may be stale and describes "
                "the whole table, never a filtered scope.",
            },
        )
    return not_measured(
        "row_count",
        "unavailable",
        "the table was never analysed (reltuples = -1); the metadata level does not scan rows "
        "and never runs ANALYZE",
        scope="table_metadata",
        source="table_statistics",
    )


def _consistency(level: str, relkind: str, run: RunContext) -> tuple[dict[str, Any], bool]:
    """Return ``(consistency record, pinned)`` for a table."""
    if level == "metadata":
        return {
            "mode": "metadata_only",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                "Metadata only: no table rows were read. The schema reflects the current "
                "table definition and catalog metadata reflects the state at capture time."
            ),
            "notes": [],
        }, False
    if relkind == "f":
        return {
            "mode": "unpinned",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                "Foreign table: its rows are read through a foreign data wrapper, in a separate "
                "remote transaction or without any snapshot, so the run's snapshot does not cover "
                "them and each statement may observe different data."
            ),
            "notes": [],
        }, False
    notes = [
        "TRUNCATE and table-rewriting ALTER TABLE are not MVCC-safe: if they run concurrently, "
        "the snapshot can see the table as empty.",
        "The size (pg_total_relation_size) describes the physical size at capture, not the "
        "snapshot.",
    ]
    if relkind == "v":
        notes.append(
            "A view is evaluated again by every statement: volatile functions or foreign tables "
            "it uses may return different rows to the sample and to each aggregation pass."
        )
    return {
        "mode": "transaction_snapshot",
        "delta_version": None,
        "version_timestamp": run.snapshot_started_at,
        "guarantee": (
            "All reads of this run (catalog metadata, sample, aggregation passes and deep "
            "checks) used one READ ONLY REPEATABLE READ snapshot, exported at the start of the "
            "run and imported by each table's own transaction; changes committed after it are "
            "not visible."
        ),
        "notes": notes,
    }, True


# --------------------------------------------------------------------------- passes


def _run_passes(
    db: Session,
    rel: Sql,
    where: Sql,
    agg_plan: Mapping[str, Any],
    infos_by_id: Mapping[str, ColumnInfo],
    reference_time: str,
    observed: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]:
    """Execute the planned passes; same contract as Spark's ``run_aggregates``.

    An expression the server rejects while planning (SQLSTATE class 42 or 0A; found with
    EXPLAIN, which reads no data) fails only its alias. A floating-point overflow (22003)
    fails only the float averages, deviations and quantiles of the pass. Any other failure
    fails every alias of the pass and records one ``aggregate`` error.
    """
    compiled: dict[str, Sql] = {}
    decoders: dict[str, str | None] = {}
    failed: dict[str, str] = {}
    errors: list[dict[str, Any]] = []
    specs = {spec["alias"]: spec for spec in agg_plan["specs"]}
    for alias, spec in specs.items():
        info = infos_by_id.get(spec["field_id"]) if spec["field_id"] else None
        try:
            compiled[alias] = plan_sql.compile_metric(spec, info, reference_time)
            decoders[alias] = plan_sql.decode_kind(spec, info)
        except (plan_sql.UnsupportedOperation, KeyError, ValueError) as exc:
            failed[alias] = "expression could not be built: " + sanitize_message(str(exc), 200)
    results: dict[str, Any] = {}
    for index, aliases in enumerate(agg_plan["passes"]):
        op_id = f"op_aggregate_{index + 1}"
        active = [alias for alias in aliases if alias in compiled]
        if not active:
            observed.append(
                observed_operation(op_id, "skipped", None, detail="no valid expressions")
            )
            continue
        start = time.perf_counter()
        attempt = 0
        while True:
            attempt += 1
            statement = plan_sql.aggregate_statement(
                rel, [(alias, compiled[alias]) for alias in active], where
            )
            try:
                with db.operation():
                    rows = db.rows(statement)
                row = rows[0] if rows else {}
                for alias in active:
                    try:
                        results[alias] = plan_sql.decode_value(decoders[alias], row.get(alias))
                    except (ValueError, TypeError, ArithmeticError):
                        failed[alias] = "value outside the representable range (not decoded)"
                observed.append(observed_operation(op_id, "succeeded", start, rows=1))
                break
            except EngineError as exc:
                if exc.lost or attempt > 2:
                    _fail_pass(op_id, active, exc, failed, errors, observed, start)
                    break
                if (exc.sqlstate or "")[:2] in ("42", "0A") or exc.sqlstate is None:
                    rejected = _probe(db, rel, where, active, compiled)
                    for alias, cause in rejected.items():
                        failed[alias] = "expression rejected by the engine: " + cause
                    remaining = [alias for alias in active if alias not in rejected]
                    if not rejected or not remaining:
                        if not remaining:
                            errors.append(_error_record(exc, "aggregate"))
                            observed.append(
                                observed_operation(
                                    op_id, "skipped", start, detail="all expressions rejected"
                                )
                            )
                        else:
                            _fail_pass(op_id, active, exc, failed, errors, observed, start)
                        break
                    active = remaining
                    continue
                if exc.sqlstate == "22003":
                    suspects = [
                        alias
                        for alias in active
                        if specs[alias]["op"] in _FLOAT_OVERFLOW_OPS
                        and (infos_by_id.get(specs[alias]["field_id"] or "") or _NO_INFO).kind
                        == "float"
                    ]
                    if suspects:
                        for alias in suspects:
                            failed[alias] = "floating-point overflow (22003)"
                        active = [alias for alias in active if alias not in suspects]
                        if active:
                            continue
                _fail_pass(op_id, active, exc, failed, errors, observed, start)
                break
    return results, failed, errors


_NO_INFO = ColumnInfo(name="", kind="other", physical_type="")


def _fail_pass(
    op_id: str,
    active: Sequence[str],
    exc: EngineError,
    failed: dict[str, str],
    errors: list[dict[str, Any]],
    observed: list[dict[str, Any]],
    start: float,
) -> None:
    errors.append(_error_record(exc, "aggregate"))
    for alias in active:
        failed.setdefault(alias, "aggregation pass failed: " + exc.cause)
    observed.append(observed_operation(op_id, "failed", start, detail=exc.cause))


def _probe(
    db: Session, rel: Sql, where: Sql, aliases: Sequence[str], compiled: Mapping[str, Sql]
) -> dict[str, str]:
    rejected: dict[str, str] = {}
    for alias in aliases:
        try:
            with db.operation():
                db.rows(plan_sql.probe_statement(rel, compiled[alias], where))
        except EngineError as exc:
            if exc.lost:
                raise
            rejected[alias] = exc.cause
    return rejected


# --------------------------------------------------------------------------- one table


def profile_table(
    db: Session,
    input_text: str,
    config: Mapping[str, Any],
    run: RunContext,
) -> dict[str, Any]:
    """Profile one table and return its contract record (never raises for data errors)."""
    total_start = time.perf_counter()
    parts = parse_table_identifier(input_text)
    key = table_key(parts)
    table = new_table(input_text, parts, key, table_id(parts), postgres_quoted(parts))
    options = table_options_for(config, table_lookup_key(parts))
    level = config["analysis_level"]
    filters = list(options.get("filters", []))
    selected = options.get("columns")
    table["purpose"] = options.get("purpose")
    table["scope"]["filters"] = filters
    table["scope"]["selected_columns"] = list(selected) if selected is not None else None
    table["scope"]["population"] = "filtered" if filters else "full"
    planned = table["operations"]["planned"]
    observed = table["operations"]["observed"]
    timings = table["timings_ms"]

    # 1. catalog -----------------------------------------------------------------------
    metadata_start = time.perf_counter()
    planned.append(
        operation(
            "op_catalog",
            "catalog_metadata",
            "pg_catalog lookup of the relation, its columns, comment, statistics and size",
            reads_user_data=False,
        )
    )
    try:
        relation_row, problem = _resolve_relation(db, parts, run)
        if problem is not None or relation_row is None:
            observed.append(
                observed_operation("op_catalog", "failed", metadata_start, detail="not resolved")
            )
            table["errors"].append(problem or _resolve_error("TABLE_NOT_FOUND", "not found"))
            run.log(f"[tabledossier] {key}: could not be resolved")
            return _finish(table, config, total_start)
        oid = int(relation_row["oid"])
        schema_name = str(relation_row["schema_name"])
        relation_name = str(relation_row["table_name"])
        run.resolved[(schema_name, relation_name)] = key
        relkind = str(relation_row["relkind"])
        with db.operation():
            detail = db.rows(plan_sql.relation_detail_statement(oid))[0]
            partition_columns: list[str] = []
            if relkind == "p":
                for row in db.rows(plan_sql.partition_key_statement(oid)):
                    if row["name"] is None:
                        table["notes"].append("The partition key includes expressions.")
                    else:
                        partition_columns.append(str(row["name"]))
        fields, infos = _columns(db, oid)
        observed.append(
            observed_operation("op_catalog", "succeeded", metadata_start, rows=len(fields))
        )
    except EngineError as exc:
        observed.append(
            observed_operation("op_catalog", "failed", metadata_start, detail=exc.cause)
        )
        table["errors"].append(_error_record(exc, "resolve"))
        run.log(f"[tabledossier] {key}: could not be resolved ({exc.cause})")
        return _finish(table, config, total_start)
    table["source"] = _source(detail, relkind, partition_columns, run.now())
    if detail.get("rls_enabled"):
        table["notes"].append(
            "Row-level security filtered the rows this role can read: the profile describes "
            "only those rows."
            if detail.get("rls_active")
            else "Row-level security is enabled but does not apply to this role (owner, "
            "superuser or BYPASSRLS): the profile describes every row."
        )

    planned.append(
        operation(
            "op_constraints",
            "catalog_metadata",
            "pg_catalog primary key, unique, foreign key and check constraints",
            reads_user_data=False,
        )
    )
    start = time.perf_counter()
    try:
        constraints, notes = _constraints(db, oid, run)
        table["constraints"] = constraints
        table["notes"].extend(notes)
        observed.append(
            observed_operation("op_constraints", "succeeded", start, rows=len(constraints))
        )
    except EngineError as exc:
        observed.append(observed_operation("op_constraints", "failed", start, detail=exc.cause))
        table["notes"].append("Declared constraints could not be read from pg_catalog.")

    limits = config["limits"]
    tree = build_schema_tree(fields, max_depth=limits["max_depth"], max_fields=limits["max_fields"])
    consistency, pinned = _consistency(level, relkind, run)
    tree["captured_from"] = "pinned_snapshot" if pinned else "current_table"
    table["schema"] = tree
    table["consistency"] = consistency
    timings["metadata"] = elapsed_ms(metadata_start)
    if level == "metadata":
        table["scope"]["scope_label"] = "table_metadata"

    # 2. configuration checks against the schema --------------------------------------------
    top_names = [str(item["name"]) for item in fields]
    if selected is not None:
        unknown = [name for name in selected if name not in top_names]
        if unknown:
            table["errors"].append(
                _resolve_error(
                    "SELECTED_COLUMN_NOT_FOUND",
                    f"{len(unknown)} selected column(s) not found in the table schema",
                )
            )
    schema_ids = {node["field_id"] for node in iter_nodes(tree["fields"])}
    for item in filters:
        if field_id(column_reference_segments(item["column"])) not in schema_ids:
            table["errors"].append(
                _resolve_error(
                    "FILTER_COLUMN_NOT_FOUND",
                    "a filter references a column that is not in the table schema",
                )
            )
    profiled, omissions = select_profile_fields(tree, selected)
    unreadable = {
        node["field_id"]
        for node in profiled
        if not infos.get(node["path"][0]["name"], _NO_INFO).readable
    }
    if unreadable and level != "metadata":
        for node in profiled:
            if node["field_id"] in unreadable:
                omissions.append(
                    {
                        "field_id": node["field_id"],
                        "display_path": node["display_path"],
                        "reason": "no_select_privilege",
                        "detail": "the role may not read this column (column privileges)",
                        "metrics": [],
                    }
                )
        profiled = [node for node in profiled if node["field_id"] not in unreadable]
    table["omissions"].extend(omissions)
    table["field_profiles"] = table_field_profiles(
        tree, {node["field_id"] for node in profiled}, omissions, selected
    )
    if level == "metadata":
        for profile in table["field_profiles"]:
            profile["profiled"] = False
            profile["omission_reason"] = "metadata_level"
        table["table_metrics"].append(_statistics_row_count(detail.get("reltuples")))
        return _finish(table, config, total_start)
    if not detail.get("can_select"):
        table["errors"].append(
            _resolve_error("INSUFFICIENT_PRIVILEGE", "the role may not read this relation")
        )
    rel = plan_sql.relation(schema_name, relation_name)
    where = Sql(())
    if not any(error["stage"] == "resolve" for error in table["errors"]):
        try:
            where = plan_sql.where_clause(filters, infos)
            with db.operation():
                db.rows(plan_sql.filter_probe_statement(rel, where))
        except plan_sql.FilterValueError as exc:
            table["errors"].append(_resolve_error("FILTER_VALUE_INVALID", str(exc)))
        except EngineError as exc:
            if exc.lost:
                table["errors"].append(_error_record(exc, "resolve"))
            else:
                condition = {
                    "42501": "INSUFFICIENT_PRIVILEGE",
                }.get(exc.sqlstate or "", "FILTER_VALUE_INVALID")
                if (exc.sqlstate or "")[:2] == "42" and condition != "INSUFFICIENT_PRIVILEGE":
                    condition = "FILTER_TYPE_MISMATCH"
                table["errors"].append(
                    _resolve_error(condition, f"the filters cannot be applied ({exc.cause})")
                )
    if any(error["stage"] == "resolve" for error in table["errors"]):
        return _finish(table, config, total_start)

    scope = scope_label(pinned, bool(filters))
    table["scope"]["scope_label"] = scope
    nodes_by_id = {node["field_id"]: node for node in iter_nodes(tree["fields"])}
    infos_by_id = {
        node["field_id"]: infos[node["path"][0]["name"]]
        for node in profiled
        if node["path"][0]["name"] in infos and len(node["path"]) == 1
    }
    lookup = table_lookup_key(parts)

    # 3. transient sample ------------------------------------------------------------------
    sample_plan = plan_sample(profiled, config)
    if sample_plan["enabled"] and not run.text_decoding:
        sample_plan["enabled"] = False
        sample_plan["reason"] = (
            "the server encoding is SQL_ASCII: text values cannot be decoded reliably"
        )
    sample_nodes = [nodes_by_id[fid] for fid in sample_plan["field_ids"]]
    sample_record = new_sample_record(sample_plan, config)
    table["sample"] = sample_record
    json_ids: set[str] = set()
    semantic_by_id: dict[str, dict[str, Any]] = {}
    redacted_ids = policy_field_ids(config, lookup, "redact_columns")
    sample_start = time.perf_counter()
    if sample_plan["enabled"]:
        tablesample = relkind in ("r", "p", "m")
        method = sample_plan["method"]
        notes = sample_notes(method)
        if method == "prefix":
            notes[1] = (
                "prefix: the first rows the server returns without ORDER BY "
                "(synchronize_seqscans is off for the run); potentially biased, not random."
            )
        elif not tablesample:
            sample_record["seed"] = None
            notes[1] = (
                "random: rows kept with random() < fraction (views and foreign tables cannot use "
                "TABLESAMPLE); not reproducible, so no seed applies. Every row in scope is read."
            )
        else:
            notes[1] = (
                "random: TABLESAMPLE BERNOULLI with REPEATABLE (seed); every block is read, so "
                "the fraction does not reduce I/O."
            )
        sample_record["notes"] = notes
        planned.append(
            operation(
                "op_sample",
                "sample_collect",
                f"projected {method} sample of string fields, values cut to "
                f"{sample_plan['max_value_chars']} characters (server-side cursor)",
                reads_user_data=True,
                max_rows=sample_plan["max_rows"],
                max_bytes=sample_plan["max_bytes"],
                columns=len(sample_nodes),
            )
        )
        statement = plan_sql.sample_statement(
            rel,
            [infos_by_id[node["field_id"]] for node in sample_nodes],
            where,
            max_value_chars=sample_plan["max_value_chars"],
            max_rows=sample_plan["max_rows"],
            method=method,
            tablesample=tablesample,
            random_fraction=sample_plan["random_fraction"],
            seed=sample_plan["seed"],
        )
        try:
            sample = db.stream(
                statement,
                lambda rows: collect_sample_rows(rows, sample_plan["field_ids"], sample_plan),
            )
            observed.append(
                observed_operation("op_sample", "succeeded", sample_start, rows=sample["rows"])
            )
            sample_record.update(
                {
                    "rows_collected": sample["rows"],
                    "bytes_retained": sample["bytes"],
                    "values_truncated": sum(
                        item["truncated"] for item in sample["fields"].values()
                    ),
                    "stopped_reason": sample["stopped_reason"],
                }
            )
            example_ids = (
                policy_field_ids(config, lookup, "example_columns")
                if config["value_policy"]["persist_examples"]
                else set()
            )
            sample_semantics(
                sample,
                sample_nodes,
                method,
                config,
                redacted_ids=redacted_ids,
                example_ids=example_ids,
                semantic_by_id=semantic_by_id,
                json_ids=json_ids,
            )
            del sample
        except EngineError as exc:
            table["errors"].append(_error_record(exc, "sample"))
            sample_record["stopped_reason"] = "error"
            observed.append(
                observed_operation("op_sample", "failed", sample_start, detail=exc.cause)
            )
    timings["sample"] = elapsed_ms(sample_start)

    # 4. shared aggregation passes ---------------------------------------------------------------
    redacted = set(redacted_ids)
    if config["value_policy"]["aggregate_extremes"] == "redact":
        redacted = {node["field_id"] for node in profiled}
    plan_config = dict(config)
    if limits["max_expressions_per_pass"] > plan_sql.MAX_EXPRESSIONS_PER_STATEMENT:
        plan_config["limits"] = {
            **limits,
            "max_expressions_per_pass": plan_sql.MAX_EXPRESSIONS_PER_STATEMENT,
        }
        table["notes"].append(
            f"limits.max_expressions_per_pass was lowered to "
            f"{plan_sql.MAX_EXPRESSIONS_PER_STATEMENT} for PostgreSQL (one SELECT holds at most "
            "1664 expressions)."
        )
    agg_plan = plan_aggregates(
        profiled,
        config=plan_config,
        capabilities={"try_parse_json": run.json_validation},
        scope=scope,
        json_field_ids=json_ids,
        redacted_field_ids=redacted,
        json_unsupported_reason=_JSON_UNSUPPORTED,
    )
    table["omissions"].extend(agg_plan["omissions"])
    planned.extend(
        aggregate_operations(
            agg_plan,
            deep=False,
            standard_description="one shared SELECT over the analysed scope (one statement; "
            "count(DISTINCT) and percentile_disc each sort their values)",
            extra_description="extra SELECT for expressions that did not fit the standard passes",
            extra_details=lambda specs: {
                "sort_aggregates": sum(
                    1
                    for spec in specs
                    if spec["op"] in ("approx_distinct", "quantiles", "length_quantiles")
                )
            },
        )
    )
    for spec in agg_plan["specs"]:
        info = infos_by_id.get(spec["field_id"]) if spec["field_id"] else None
        spec_labels = plan_sql.labels(spec, info)
        if spec_labels:
            spec["labels"] = spec_labels
    aggregate_start = time.perf_counter()
    results, failed, agg_errors = _run_passes(
        db, rel, where, agg_plan, infos_by_id, run.reference_time, observed
    )
    table["errors"].extend(agg_errors)
    timings["aggregate"] = elapsed_ms(aggregate_start)
    assemble_field_results(
        table,
        agg_plan,
        results,
        failed,
        scope=scope,
        nodes_by_id=nodes_by_id,
        semantic_by_id=semantic_by_id,
    )
    if json_ids and not run.json_validation:
        table["unsupported"].append({"capability": "json_validation", "detail": _JSON_UNSUPPORTED})
    if level == "deep":
        table["unsupported"].append({"capability": "deep_elements", "detail": _DEEP_PART_I_DETAIL})
        uniqueness = config["deep"]["uniqueness"]
        if uniqueness["keys"] or uniqueness["declared_keys"] or uniqueness["identifier_candidates"]:
            table["unsupported"].append(
                {
                    "capability": "exact_uniqueness",
                    "detail": "exact uniqueness of keys (deep level, part II) is not yet "
                    "supported by the PostgreSQL connector",
                }
            )
    _finish(table, config, total_start)
    run.log(
        f"[tabledossier] {key}: {table['status']} — {len(profiled)} field(s) profiled in "
        f"{len(agg_plan['passes'])} aggregation pass(es)"
    )
    return table


# --------------------------------------------------------------------------- run


@dataclass
class RunResult:
    """What the CLI needs after a run."""

    run_dir: str
    profile: dict[str, Any]
    validation_errors: list[str]
    files: list[str]


def _failed_table(
    input_text: str, error: dict[str, Any], config: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        parts = parse_table_identifier(input_text)
    except IdentifierError:
        parts = [input_text]
    table = new_table(input_text, parts, table_key(parts), table_id(parts), postgres_quoted(parts))
    table["errors"].append(error)
    finalize_table(table, config)
    return table


def _relationships(
    tables: list[dict[str, Any]], config: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
    relationships = known_relationships(tables, config)
    if config["analysis_level"] != "deep":
        return relationships, None, None
    for relationship in relationships:
        if relationship.get("validation_detail") is None:
            relationship["validation_detail"] = not_validated_detail(
                "referential validation is not yet supported by the PostgreSQL connector"
            )
    summary = referential_summary(
        relationships, config, planned=0, limited=[], target_mode="transaction_snapshot"
    )
    settings = config["deep"]["relationship_hypotheses"]
    hypotheses = hypotheses_record(
        config,
        None,
        [],
        evaluated=0,
        rejected={},
        reason=_HYPOTHESES_REASON
        if settings["enabled"]
        else "disabled by configuration (deep.relationship_hypotheses.enabled = false)",
    )
    return relationships, summary, hypotheses


def run_profile(
    config: Mapping[str, Any],
    *,
    output_dir: str,
    settings: ConnectionSettings,
    parameter_sources: Mapping[str, Any],
    check: Callable[[Mapping[str, Any]], list[str]],
    log: Callable[[str], None] = lambda text: print(text, file=sys.stderr),
    environ: Mapping[str, str] | None = None,
) -> RunResult:
    """Profile the configured tables and write ``<output_dir>/<run_id>/``.

    Raises :class:`DriverMissingError`, :class:`ConnectFailed` (before anything is
    written) or ``OSError`` (when the run directory cannot be created).
    """
    psycopg = load_driver()
    conninfo = connection_string(settings, environ if environ is not None else os.environ)
    secrets = conninfo_secrets(psycopg, conninfo)
    with ExitStack() as stack:
        connections = []
        for _ in range(2):
            try:
                connection = psycopg.connect(
                    conninfo,
                    autocommit=False,
                    connect_timeout=settings.connect_timeout_s,
                    application_name="tabledossier",
                )
            except psycopg.Error as exc:
                raise ConnectFailed(
                    "could not connect to PostgreSQL: " + safe_message(exc, secrets)
                ) from None
            stack.enter_context(connection)
            connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            connection.read_only = True
            connections.append(connection)
        holder, worker = connections
        try:
            cursor = holder.cursor(row_factory=psycopg.rows.dict_row)
            query, params = to_driver(psycopg, plan_sql.holder_statement())
            cursor.execute(query, params)
            facts = cursor.fetchone()
            server_version_num = int(facts["server_version_num"])
            if server_version_num >= 170000:
                query, params = to_driver(psycopg, plan_sql.holder_timeout_statement())
                cursor.execute(query, params)
        except psycopg.Error as exc:
            raise ConnectFailed(
                "could not open the read-only snapshot: " + safe_message(exc, secrets)
            ) from None
        started = utc_now()
        started_at = format_utc(started)
        run_id = new_run_id(started)
        run_dir = os.path.join(output_dir, run_id)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(run_dir, exist_ok=False)
        write_files(
            run_dir,
            {"manifest.json": pretty_json(running_manifest(run_id, started_at, None))},
        )
        encoding = str(facts["server_encoding"])
        snapshot_started = facts["started_at"]
        run = RunContext(
            server_version_num=server_version_num,
            search_schemas=[str(schema) for schema in facts["search_schemas"] or []],
            database_name=str(facts["database_name"]),
            json_validation=server_version_num >= 160000,
            text_decoding=encoding.upper() != "SQL_ASCII",
            snapshot_started_at=format_utc(
                snapshot_started.astimezone(timezone.utc)
                if isinstance(snapshot_started, datetime)
                else started
            ),
            reference_time=started_at,
            now=lambda: format_utc(utc_now()),
            log=log,
        )
        session = PgSession(psycopg, worker, str(facts["snapshot_id"]), settings, secrets)
        tables: list[dict[str, Any]] = []
        for name in config["tables"]:
            if session.lost:
                tables.append(
                    _failed_table(
                        name,
                        {
                            "stage": "connect",
                            "error_class": "EngineError",
                            "condition": None,
                            "message": "the connection to the server was lost",
                        },
                        config,
                    )
                )
                continue
            try:
                with session.table_transaction():
                    tables.append(profile_table(session, name, config, run))
            except EngineError as exc:
                tables.append(_failed_table(name, _error_record(exc, "snapshot"), config))
            except Exception as exc:  # noqa: BLE001 - one table never costs the run
                tables.append(
                    _failed_table(
                        name,
                        {
                            "stage": "assemble",
                            "error_class": type(exc).__name__,
                            "condition": None,
                            "message": sanitize_message(str(exc)) or type(exc).__name__,
                        },
                        config,
                    )
                )
        with suppress(psycopg.Error):
            holder.rollback()
    finished = utc_now()
    _rename_run_targets(tables, run.resolved)
    relationships, referential, hypotheses = _relationships(tables, config)
    profile = build_profile(
        run_id=run_id,
        started_at=started_at,
        finished_at=format_utc(finished),
        duration_ms=max(0, int((finished - started).total_seconds() * 1000)),
        reference_time=started_at,
        environment={
            "engine": "postgresql",
            "execution_context": "local_cli",
            "python_version": ".".join(str(part) for part in sys.version_info[:3]),
            "postgresql_version": _version_text(server_version_num),
            "session_timezone": "UTC",
        },
        config=config,
        parameter_sources=parameter_sources,
        generation={"generator_version": __version__, "generation_id": None},
        capabilities={
            "json_validation": {
                "available": run.json_validation,
                "detail": "pg_input_is_valid(text, 'json') (PostgreSQL 16 or later)",
            },
            "tablesample": {
                "available": True,
                "detail": "TABLESAMPLE BERNOULLI ... REPEATABLE (base tables, partitioned "
                "tables and materialized views)",
            },
            "text_decoding": {
                "available": run.text_decoding,
                "detail": "server encoding " + encoding,
            },
        },
        tables=tables,
        relationships=relationships,
        referential_validation=referential,
        relationship_hypotheses=hypotheses,
    )
    validation_errors = check(profile)
    documents = build_documents(profile)
    files = {"profile.json": pretty_json(profile), **documents}
    files["manifest.json"] = pretty_json(
        run_manifest(
            profile,
            files,
            status=profile["run"]["status"],
            validation_errors=validation_errors,
            generation_id=None,
        )
    )
    written = write_files(run_dir, files, replaceable=("manifest.json",))
    return RunResult(
        run_dir=run_dir, profile=profile, validation_errors=validation_errors, files=written
    )


def _rename_run_targets(
    tables: Sequence[dict[str, Any]], resolved: Mapping[tuple[str, str], str]
) -> None:
    """Point declared foreign keys at the run's own table keys when the target was profiled."""
    by_key = {table_key(list(name)): key for name, key in resolved.items()}
    for table in tables:
        for constraint in table.get("constraints", []):
            reference = constraint.get("referenced")
            if reference is not None and reference["table"] in by_key:
                reference["table"] = by_key[reference["table"]]
