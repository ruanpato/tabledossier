"""Command-line interface: ``init``, ``validate``, ``generate``, ``render`` and ``schema``.

Every command works offline and without Spark, database drivers or cloud SDKs.

Exit codes are stable (see ``docs/cli.md``)::

    0  success
    1  unexpected internal error
    2  usage error (invalid arguments)
    3  invalid input (configuration, profile or annotations; incompatible version)
    4  I/O problem (unreadable input, or output exists without --overwrite)
    5  valid profile of a *partial* run            (validate --profile)
    6  valid profile of a *failed* run             (validate --profile)
    7  configured quality checks failed            (validate --profile --fail-on-check-failures)
"""

import argparse
import json
import os
import sys
from collections.abc import Sequence
from typing import Any

from tabledossier._version import __version__
from tabledossier.config import default_config
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


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="tabledossier",
        description=(
            "Portable data profiling, notebooks, and documentation. All commands work offline."
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
        choices=("metadata", "standard"),
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
        _err(f"internal error: {type(exc).__name__}: {exc}")
        _err("Please report this with the command you ran (never include credentials or data).")
        return EXIT_INTERNAL


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
