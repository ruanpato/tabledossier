"""Command-line interface: offline commands, plus ``profile``, which connects to a database.

Commands: ``init``, ``validate``, ``generate``, ``render``, ``schema`` and ``profile``.

Every command except ``profile`` works offline and without Spark, database drivers or cloud
SDKs. ``profile`` connects to a database (PostgreSQL, optional ``[postgres]`` extra); its driver
is imported only when it runs.

Exit codes are stable (see ``docs/cli.md``)::

    0  success
    1  unexpected internal error
    2  usage error (invalid arguments)
    3  invalid input (configuration, profile or annotations; incompatible version)
    4  I/O problem (unreadable input, or output exists without --overwrite)
    5  valid profile of a *partial* run            (validate --profile)
    6  valid profile of a *failed* run             (validate --profile)
    7  configured quality checks failed            (validate/profile --fail-on-check-failures)
    8  the database driver is not installed        (profile)
    9  could not connect to the database           (profile)
"""

import argparse
import json
import os
import re
import sys
from collections.abc import Sequence
from typing import Any

from tabledossier._version import __version__
from tabledossier.config import ANALYSIS_LEVELS, DEFAULT_CONFIG, default_config
from tabledossier.errors import sanitize_message
from tabledossier.jsonutil import pretty_json
from tabledossier.notebook import generate_notebook
from tabledossier.package import DOCUMENT_FILES, OutputExistsError, build_documents, write_files
from tabledossier.resources import SCHEMA_FILES, schema_text
from tabledossier.validation import check_annotations, check_config, check_profile

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USAGE = 2
EXIT_INVALID = 3
EXIT_IO = 4
EXIT_PARTIAL = 5
EXIT_FAILED_RUN = 6
EXIT_CHECKS = 7
EXIT_DRIVER = 8
EXIT_CONNECT = 9
_DSN_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WORK_MEM = re.compile(r"^[1-9][0-9]{0,6}(kB|MB|GB)$")
_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


class CliError(Exception):
    """An error with a stable exit code and an actionable message."""

    def __init__(self, code: int, message: str, details: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.code = code
        self.details = list(details)


def _err(message: str) -> None:
    print(message, file=sys.stderr)


def load_json(path: str, what: str) -> Any:
    """Read a JSON file or raise :class:`CliError` with an actionable message."""
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        raise CliError(EXIT_IO, f"cannot read {what} {path!r}: {exc.strerror or exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise CliError(
            EXIT_INVALID,
            f"{what} {path!r} is not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})",
        ) from exc


def _write_new(path: str, text: str, overwrite: bool) -> None:
    if os.path.exists(path) and not overwrite:
        raise CliError(
            EXIT_IO,
            f"{path!r} already exists; pass --overwrite to replace it or choose another path",
        )
    directory = os.path.dirname(os.path.abspath(path))
    try:
        os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
    except OSError as exc:
        raise CliError(EXIT_IO, f"cannot write {path!r}: {exc.strerror or exc}") from exc


# --------------------------------------------------------------------------- commands


def cmd_init(args: argparse.Namespace) -> int:
    """Write a complete, valid configuration file with every default made explicit."""
    config = default_config()
    config["tables"] = list(args.tables or [])
    config["analysis_level"] = args.level
    config["output_dir"] = args.output_dir or ""
    _, errors = check_config(config)
    if errors:
        raise CliError(EXIT_INVALID, "the requested configuration is invalid", errors)
    _write_new(args.output, pretty_json(config), args.force)
    print(f"Wrote {args.output}.")
    if not config["tables"]:
        print(
            "No tables yet: the notebook can be generated now, and you can fill the tables_json "
            "widget in Databricks."
        )
    print(
        "Every option is documented in docs/configuration.md. Never put credentials in this file."
    )
    print(f"Next: tabledossier validate --config {args.output}")
    return EXIT_OK


def _validate_config(path: str) -> dict[str, Any]:
    document = load_json(path, "configuration")
    normalized, errors = check_config(document)
    if errors or normalized is None:
        raise CliError(EXIT_INVALID, f"configuration {path!r} is invalid", errors)
    return normalized


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a configuration, a profile or an annotations file."""
    if args.config:
        config = _validate_config(args.config)
        print(
            f"Configuration {args.config!r} is valid ({len(config['tables'])} table(s), level "
            f"{config['analysis_level']})."
        )
        if not config["tables"]:
            print("Note: no tables configured; set tables_json in the notebook before running it.")
        return EXIT_OK
    if args.annotations:
        document = load_json(args.annotations, "annotations")
        errors = check_annotations(document)
        if errors:
            raise CliError(EXIT_INVALID, f"annotations {args.annotations!r} are invalid", errors)
        print(
            f"Annotations {args.annotations!r} are valid ({len(document.get('tables', {}))} "
            "table(s))."
        )
        return EXIT_OK
    profile = load_json(args.profile, "profile")
    errors = check_profile(profile)
    if errors:
        raise CliError(EXIT_INVALID, f"profile {args.profile!r} is invalid", errors)
    run = profile["run"]
    summary = profile["summary"]
    print(
        f"Profile {args.profile!r} is valid (schema {profile['schema_version']}). Run "
        f"{run['run_id']} "
        f"measured at {run['started_at']}: status {run['status']}, {summary['tables_total']} "
        "table(s)."
    )
    checks = summary["checks"]
    print(
        f"Checks: {checks['pass']} pass, {checks['fail']} fail, {checks['not_evaluated']} not "
        "evaluated, "
        f"{checks['error']} error."
    )
    if run["status"] == "failed":
        _err("The recorded run failed for every table; see errors in the profile.")
        return EXIT_FAILED_RUN
    if run["status"] == "partial":
        _err("The recorded run is partial: at least one table failed or is incomplete.")
        return EXIT_PARTIAL
    if args.fail_on_check_failures and checks["fail"]:
        _err(f"{checks['fail']} configured check(s) failed.")
        return EXIT_CHECKS
    return EXIT_OK


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate the Databricks source notebook (and its generation manifest)."""
    config = _validate_config(args.config)
    notebook, manifest = generate_notebook(config)
    manifest_path = args.manifest or (os.path.splitext(args.output)[0] + ".generation.json")
    manifest["notebook"]["file_name"] = os.path.basename(args.output)
    for path in (args.output, manifest_path):
        if os.path.exists(path) and not args.overwrite:
            raise CliError(
                EXIT_IO,
                f"{path!r} already exists; pass --overwrite to replace it or choose another path",
            )
    _write_new(args.output, notebook, True)
    _write_new(manifest_path, pretty_json(manifest), True)
    print(f"Wrote {args.output} ({manifest['notebook']['cells']} cells) and {manifest_path}.")
    print(f"Generation id: {manifest['generation_id']}")
    if not config["tables"]:
        print(
            "No tables configured: set the tables_json widget in Databricks before running the "
            "notebook."
        )
    if not config["output_dir"]:
        print(
            "No output_dir configured: set the output_dir widget (e.g. "
            "/Volumes/<catalog>/<schema>/<volume>/tabledossier)."
        )
    print(
        "Next: import the file into your Databricks workspace (Workspace > Import), attach "
        "compute and Run all."
    )
    return EXIT_OK


def cmd_render(args: argparse.Namespace) -> int:
    """Regenerate documentation from a profile, offline, without recomputing metrics."""
    profile = load_json(args.input, "profile")
    errors = check_profile(profile)
    if errors:
        raise CliError(
            EXIT_INVALID, f"profile {args.input!r} is invalid; nothing was rendered", errors
        )
    annotations = None
    if args.annotations:
        annotations = load_json(args.annotations, "annotations")
        errors = check_annotations(annotations)
        if errors:
            raise CliError(
                EXIT_INVALID,
                f"annotations {args.annotations!r} are invalid; nothing was rendered",
                errors,
            )
    documents = build_documents(profile, annotations)
    try:
        written = write_files(args.output, documents, overwrite=args.overwrite)
    except OutputExistsError as exc:
        raise CliError(EXIT_IO, f"{exc} (pass --overwrite to replace the documents)") from exc
    except OSError as exc:
        raise CliError(EXIT_IO, f"cannot write to {args.output!r}: {exc.strerror or exc}") from exc
    print(
        f"Rendered {len(written)} file(s) into {args.output} from run {profile['run']['run_id']} "
        f"(measured at {profile['run']['started_at']}; no metrics were recomputed)."
    )
    for name in DOCUMENT_FILES:
        print(f"  - {name}")
    if profile["run"]["status"] != "succeeded":
        _err(
            f"Note: the profiled run is {profile['run']['status']}; the documents describe its "
            "errors."
        )
    return EXIT_OK


def cmd_schema(args: argparse.Namespace) -> int:
    """Print or write a packaged JSON Schema."""
    text = schema_text(args.name)
    if args.output:
        _write_new(args.output, text, args.overwrite)
        print(f"Wrote {args.output}.")
    else:
        sys.stdout.write(text)
    return EXIT_OK


# --------------------------------------------------------------------------- parser


def _dsn_env_name(text: str) -> str:
    # Never repeat the value: a connection string passed by mistake would end up in logs.
    if not _DSN_ENV_NAME.match(text):
        raise argparse.ArgumentTypeError(
            "takes the NAME of an environment variable (letters, digits, underscores), not a "
            "connection string"
        )
    return text


def _seconds(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a whole number of seconds") from None
    if not 1 <= value <= 86400:
        raise argparse.ArgumentTypeError("expected between 1 and 86400 seconds")
    return value


def _work_mem(text: str) -> str:
    if not _WORK_MEM.match(text):
        raise argparse.ArgumentTypeError("expected a size such as 4MB, 64MB or 1GB")
    return text


# Configuration keys that do nothing with --engine postgres: a notice is printed when they were
# changed from their default (an init-generated configuration always lists them).
_NOT_APPLICABLE = (
    ("consistency", "pin_delta_version"),
    ("metrics", "approx_distinct_rsd"),
    ("metrics", "quantile_accuracy"),
    ("jobs", "exit_summary"),
)


def cmd_profile(args: argparse.Namespace) -> int:
    """Profile the configured tables in a database and write a new result package."""
    config = _validate_config(args.config)
    if not config["tables"]:
        raise CliError(EXIT_INVALID, f"configuration {args.config!r} lists no tables")
    output = args.output or config["output_dir"]
    if not output:
        raise CliError(
            EXIT_INVALID,
            "no output directory: pass --output or set output_dir in the configuration",
        )
    if _URI.match(output) and not re.match(r"^[A-Za-z]:[\\/]", output):
        raise CliError(EXIT_INVALID, f"--output must be a local directory, not a URI ({output!r})")
    if output.replace("\\", "/").startswith("/Volumes/"):
        raise CliError(EXIT_INVALID, "--output must be a local directory, not a Databricks volume")
    not_applicable = [
        f"{section}.{key}"
        for section, key in _NOT_APPLICABLE
        if config[section][key] != DEFAULT_CONFIG[section][key]
    ]
    for item in not_applicable:
        _err(f"note: {item} has no effect with --engine postgres")
    recorded_output = output if not os.path.isabs(output) else "<absolute path>"
    config = {**config, "output_dir": recorded_output}
    parameter_sources = {
        "precedence": ["built-in defaults", "configuration file", "command-line options"],
        "command_line": {
            "engine": args.engine,
            "output": recorded_output if args.output else None,
            "statement_timeout_s": args.statement_timeout,
            "lock_timeout_s": args.lock_timeout,
            "connect_timeout_s": args.connect_timeout,
            "work_mem": args.work_mem,
        },
        "connection": {
            "source": "environment variable named by --dsn-env"
            if args.dsn_env
            else "libpq environment (PG* variables, service file, .pgpass)"
        },
        "not_applicable": not_applicable,
    }
    # Imported here: only this command connects, and the offline commands must stay light.
    from tabledossier.connectors import postgres

    settings = postgres.ConnectionSettings(
        dsn_env=args.dsn_env,
        statement_timeout_s=args.statement_timeout,
        lock_timeout_s=args.lock_timeout,
        connect_timeout_s=args.connect_timeout,
        work_mem=args.work_mem,
    )
    try:
        result = postgres.run_profile(
            config,
            output_dir=output,
            settings=settings,
            parameter_sources=parameter_sources,
            check=check_profile,
        )
    except postgres.DriverMissingError as exc:
        raise CliError(EXIT_DRIVER, str(exc)) from None
    except postgres.ConnectFailed as exc:
        raise CliError(EXIT_CONNECT, str(exc)) from None
    except OutputExistsError as exc:
        raise CliError(EXIT_IO, str(exc)) from None
    except OSError as exc:
        raise CliError(
            EXIT_IO, f"cannot create the run directory under {output!r}: {exc.strerror or exc}"
        ) from None
    profile = result.profile
    run = profile["run"]
    summary = profile["summary"]
    checks = summary["checks"]
    print(f"Run {run['run_id']}: status {run['status']}, {summary['tables_total']} table(s).")
    print(
        f"Tables: {summary['tables_succeeded']} succeeded, {summary['tables_partial']} partial, "
        f"{summary['tables_failed']} failed. Checks: {checks['pass']} pass, {checks['fail']} "
        f"fail, {checks['not_evaluated']} not evaluated, {checks['error']} error."
    )
    print(f"Result package: {result.run_dir}")
    if result.validation_errors:
        _err("The profile failed its own validation; the package was written with the errors:")
        for error in result.validation_errors[:20]:
            _err(f"  - {error}")
        return EXIT_INTERNAL
    if run["status"] == "failed":
        _err("Every table failed; see the errors in the profile.")
        return EXIT_FAILED_RUN
    if run["status"] == "partial":
        _err("The run is partial: at least one table failed or is incomplete.")
        return EXIT_PARTIAL
    if args.fail_on_check_failures and checks["fail"]:
        _err(f"{checks['fail']} configured check(s) failed.")
        return EXIT_CHECKS
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="tabledossier",
        description=(
            "Portable data profiling, notebooks, and documentation. Every command works "
            "offline except profile, which connects to a database."
        ),
    )
    parser.add_argument("--version", action="version", version=f"tabledossier {__version__}")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    init = commands.add_parser(
        "init", help="write a configuration file with every default made explicit"
    )
    init.add_argument(
        "--output", required=True, help="configuration file to create (e.g. profile.config.json)"
    )
    init.add_argument(
        "--tables", nargs="*", metavar="TABLE", help="default tables (catalog.schema.table)"
    )
    init.add_argument("--output-dir", help="default output directory in the execution environment")
    init.add_argument(
        "--level",
        choices=ANALYSIS_LEVELS,
        default="standard",
        help="default analysis level",
    )
    init.add_argument("--force", action="store_true", help="replace an existing file")
    init.set_defaults(handler=cmd_init)

    validate = commands.add_parser(
        "validate", help="validate a configuration, profile or annotations file"
    )
    target = validate.add_mutually_exclusive_group(required=True)
    target.add_argument("--config", help="configuration file")
    target.add_argument("--profile", help="profile.json exported by a notebook run")
    target.add_argument("--annotations", help="human annotations file")
    validate.add_argument(
        "--fail-on-check-failures",
        action="store_true",
        help="exit with code 7 when configured quality checks failed (profiles only)",
    )
    validate.set_defaults(handler=cmd_validate)

    generate = commands.add_parser("generate", help="generate the Databricks notebook (offline)")
    generate.add_argument("--config", required=True, help="configuration file")
    generate.add_argument(
        "--output", required=True, help="notebook to create (e.g. dist/profile_databricks.py)"
    )
    generate.add_argument(
        "--manifest", help="generation manifest path (default: <output>.generation.json)"
    )
    generate.add_argument("--overwrite", action="store_true", help="replace existing files")
    generate.set_defaults(handler=cmd_generate)

    render = commands.add_parser("render", help="render documentation from a profile (offline)")
    render.add_argument("--input", required=True, help="profile.json")
    render.add_argument("--output", required=True, help="output directory")
    render.add_argument(
        "--annotations", help="human annotations file (descriptions, owners, relationships)"
    )
    render.add_argument("--overwrite", action="store_true", help="replace existing documents")
    render.set_defaults(handler=cmd_render)

    schema = commands.add_parser("schema", help="print a packaged JSON Schema")
    schema.add_argument("name", choices=sorted(SCHEMA_FILES), help="schema name")
    schema.add_argument("--output", help="write to a file instead of stdout")
    schema.add_argument("--overwrite", action="store_true", help="replace an existing file")
    schema.set_defaults(handler=cmd_schema)

    profile = commands.add_parser(
        "profile",
        help="profile the configured tables directly in a database (connects; read-only)",
    )
    profile.add_argument(
        "--engine", required=True, choices=("postgres",), help="database engine (postgres)"
    )
    profile.add_argument("--config", required=True, help="configuration file")
    profile.add_argument(
        "--output",
        help="directory that receives <run_id>/ (default: the configuration's output_dir)",
    )
    profile.add_argument(
        "--dsn-env",
        type=_dsn_env_name,
        metavar="NAME",
        help="name of an environment variable holding the connection string (default: libpq "
        "environment variables, service file and .pgpass)",
    )
    profile.add_argument(
        "--statement-timeout",
        type=_seconds,
        default=300,
        metavar="SECONDS",
        help="server-side limit of each statement (default: 300)",
    )
    profile.add_argument(
        "--lock-timeout",
        type=_seconds,
        default=10,
        metavar="SECONDS",
        help="how long a statement may wait for a lock (default: 10)",
    )
    profile.add_argument(
        "--connect-timeout",
        type=_seconds,
        default=10,
        metavar="SECONDS",
        help="how long to wait for the connection (default: 10)",
    )
    profile.add_argument(
        "--work-mem",
        type=_work_mem,
        default="16MB",
        metavar="SIZE",
        help="work_mem of the run's transactions, e.g. 16MB (each exact distinct count or "
        "quantile in a pass may sort up to this much in memory; default: 16MB)",
    )
    profile.add_argument(
        "--fail-on-check-failures",
        action="store_true",
        help="exit with code 7 when configured quality checks failed",
    )
    profile.set_defaults(handler=cmd_profile)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "validate" and args.fail_on_check_failures and not args.profile:
        parser.error("--fail-on-check-failures only applies to --profile")
    try:
        code: int = args.handler(args)
        return code
    except CliError as exc:
        _err(f"error: {exc}")
        for detail in exc.details[:50]:
            _err(f"  - {detail}")
        if len(exc.details) > 50:
            _err(f"  ... and {len(exc.details) - 50} more")
        return exc.code
    except KeyboardInterrupt:
        _err("interrupted")
        return EXIT_INTERNAL
    except Exception as exc:  # noqa: BLE001 - last-resort guard with a stable exit code
        _err(f"internal error: {type(exc).__name__}: {sanitize_message(str(exc))}")
        _err("Please report this with the command you ran (never include credentials or data).")
        return EXIT_INTERNAL


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
