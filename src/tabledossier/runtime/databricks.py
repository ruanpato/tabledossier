"""Notebook orchestration: parameters, destination probe, batch execution and export.

Part of the embedded runtime. Functions receive ``spark`` and widget values
explicitly, so the same code runs inside a Databricks notebook and in local
integration tests with a plain SparkSession.

The notebook only *reads* sources. It writes exclusively to one new directory
per run under the configured ``output_dir``; existing files are never
overwritten.
"""

import datetime
import os
import secrets
import time
from collections.abc import Callable, Mapping
from typing import Any

from tabledossier.assemble import build_profile, finalize_table, new_table
from tabledossier.config import ConfigError, execution_errors, resolve_parameters
from tabledossier.contract import validate_profile
from tabledossier.errors import error_record
from tabledossier.jsonutil import format_utc, pretty_json, utc_now
from tabledossier.package import build_documents, run_manifest, running_manifest, write_files
from tabledossier.paths import parse_table_identifier, quote_table_identifier, table_id, table_key
from tabledossier.runtime.spark import (
    detect_capabilities,
    evaluate_hypotheses,
    profile_table,
    spark_environment,
    validate_relationships,
)


class DestinationError(RuntimeError):
    """Raised when the output directory cannot be used."""


def new_run_id(moment: datetime.datetime) -> str:
    """Return a unique run identifier such as ``20260921T183000Z-1a2b3c4d``."""
    utc = moment.astimezone(datetime.timezone.utc)
    return utc.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)


def prepare_run(
    *,
    spark: Any,
    widget_values: Mapping[str, str],
    generated_config: Mapping[str, Any],
    schemas: Mapping[str, Mapping[str, Any]],
    generation: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate parameters and the destination before any table is read.

    Raises :class:`ConfigError` for invalid parameters and
    :class:`DestinationError` when the run directory cannot be created.
    """
    config, sources = resolve_parameters(generated_config, widget_values, schemas["config"])
    problems = execution_errors(config)
    if problems:
        raise ConfigError("the notebook cannot run yet", problems)
    started = utc_now()
    run_id = new_run_id(started)
    run_dir = os.path.join(config["output_dir"], run_id)
    try:
        os.makedirs(config["output_dir"], exist_ok=True)
        os.makedirs(run_dir, exist_ok=False)
        write_files(
            run_dir,
            {
                "manifest.json": pretty_json(
                    running_manifest(run_id, format_utc(started), generation.get("generation_id"))
                )
            },
        )
    except OSError as exc:
        raise DestinationError(
            f"cannot write to output_dir {config['output_dir']!r} ({type(exc).__name__}: "
            f"{exc.strerror or exc}). "
            "Use a path you can write to, for example a Unity Catalog volume "
            "/Volumes/<catalog>/<schema>/<volume>/tabledossier (requires READ VOLUME and WRITE "
            "VOLUME), "
            "or a workspace folder such as /Workspace/Users/<you>/tabledossier."
        ) from exc
    return {
        "run_id": run_id,
        "started": started,
        "started_at": format_utc(started),
        "reference_time": format_utc(started),
        "config": config,
        "parameter_sources": sources,
        "run_dir": run_dir,
        "environment": spark_environment(spark),
        "capabilities": detect_capabilities(spark),
        "generation": dict(generation),
    }


def describe_plan(ctx: Mapping[str, Any]) -> str:
    """Return a human-readable plan of the run (nothing has been read yet)."""
    config = ctx["config"]
    sampling = config["sampling"]
    limits = config["limits"]
    lines = [
        f"Run {ctx['run_id']} — analysis level: {config['analysis_level']}",
        f"Results directory: {ctx['run_dir']}",
        f"Tables ({len(config['tables'])}, processed sequentially):",
    ]
    lines += [f"  - {name}" for name in config["tables"]]
    lines.append("Per table, the engine will be asked for:")
    lines.append(
        "  - catalog metadata: DESCRIBE TABLE EXTENDED / DETAIL, key constraints (no row scan)"
    )
    if config["analysis_level"] in ("standard", "deep"):
        pinning = (
            "Delta tables are pinned to one version (VERSION AS OF)"
            if config["consistency"]["pin_delta_version"]
            else "snapshot pinning disabled"
        )
        lines.append(f"  - consistency: {pinning}")
        if sampling["method"] == "none":
            lines.append("  - sample: disabled (format and JSON inference not computed)")
        else:
            lines.append(
                f"  - one {sampling['method']} sample of up to {sampling['max_rows']} rows, "
                f"{sampling['max_bytes']} bytes retained, values cut at "
                f"{sampling['max_value_chars']} characters"
            )
        lines.append(
            f"  - up to {limits['max_aggregate_passes']} shared aggregation pass(es) of at most "
            f"{limits['max_expressions_per_pass']} expressions over at most "
            f"{limits['max_fields']} fields"
        )
        if config["analysis_level"] == "deep":
            deep = config["deep"]
            targets = (
                "every array, map and probable-JSON field within the budgets"
                if not isinstance(deep["targets"], list)
                else f"{len(deep['targets'])} explicitly listed field(s)"
            )
            lines += [
                f"  - deep level ({targets}):",
                "      element metrics of arrays and maps inside the shared passes (higher-order "
                "functions, no explode; lowest priority)",
                f"      at most {deep['max_extra_passes']} extra pass(es) per table: aggregation "
                "overflow and one element explode pass "
                + (
                    f"over a sample of up to {deep['max_explode_rows']} rows and "
                    f"{deep['max_elements']} elements"
                    if deep["element_distinct"] == "sample"
                    else (
                        f"over the full scope when it has at most {deep['max_elements']} elements"
                        if deep["element_distinct"] == "full_scope"
                        else "(disabled)"
                    )
                ),
                f"      JSON paths from the sample (up to {deep['max_json_paths']} paths, depth "
                f"{deep['max_json_depth']})"
                + (
                    ", validated over the full scope when the runtime supports it"
                    if deep["json_full_scope_validation"]
                    else ""
                ),
            ]
            uniqueness = deep["uniqueness"]
            sources = [
                name
                for name, enabled in (
                    (f"{len(uniqueness['keys'])} listed key(s)", bool(uniqueness["keys"])),
                    ("declared keys", uniqueness["declared_keys"]),
                    ("identifier candidates", uniqueness["identifier_candidates"]),
                )
                if enabled
            ]
            lines.append(
                f"      exact uniqueness ({', '.join(sources)}): up to {uniqueness['max_keys']} "
                f"key(s) per table in at most {uniqueness['max_passes']} grouped pass(es); counts "
                "only"
                if sources
                else "      exact uniqueness: no key requested (deep.uniqueness)"
            )
            referential = deep["referential"]
            origins = [
                name
                for name, enabled in (
                    ("configured", referential["configured"]),
                    ("declared", referential["declared"]),
                )
                if enabled
            ]
            lines.append(
                f"  - referential validation of {' and '.join(origins)} relationships between "
                f"tables of this run: up to {referential['max_relationships']} check(s) per run, "
                "one anti join each, "
                + (
                    "over the full source scope"
                    if referential["mode"] == "full_scope"
                    else f"over a sample of at most {referential['max_sample_rows']} source rows"
                )
                + " (both tables at their recorded versions; counts only)"
                if origins
                else "  - referential validation: not requested (deep.referential)"
            )
            hypotheses = deep["relationship_hypotheses"]
            lines.append(
                f"  - relationship hypotheses: up to {hypotheses['max_pairs']} pair(s) per run "
                "chosen by type and measured ranges (never names), one inclusion check each "
                + (
                    f"over a sample of at most {hypotheses['max_sample_rows']} source rows"
                    if hypotheses["inclusion_scope"] == "sample"
                    else "over the full source scope"
                )
                + f"; listed from {hypotheses['min_inclusion_ratio']:.0%} inclusion"
                if hypotheses["enabled"]
                else "  - relationship hypotheses: off (deep.relationship_hypotheses.enabled)"
            )
    else:
        lines.append("  - no table rows are read at the metadata level")
    capabilities = ctx["capabilities"]
    lines.append(
        "Detected capabilities: "
        + ", ".join(
            f"{name}={'yes' if item['available'] else 'no'}"
            for name, item in sorted(capabilities.items())
        )
    )
    return "\n".join(lines)


def execute_run(
    spark: Any, ctx: Mapping[str, Any], log: Callable[[str], None] = print
) -> dict[str, Any]:
    """Profile every table sequentially; one failing table never stops the others."""
    config = ctx["config"]
    tables = []
    for position, name in enumerate(config["tables"], start=1):
        log(f"[tabledossier] ({position}/{len(config['tables'])}) profiling {name}")
        try:
            table = profile_table(
                spark,
                name,
                config,
                capabilities=ctx["capabilities"],
                reference_time=ctx["reference_time"],
                now=lambda: format_utc(utc_now()),
                log=log,
            )
        except Exception as exc:  # noqa: BLE001 - isolate unexpected failures per table
            parts = parse_table_identifier(name)
            table = new_table(
                name, parts, table_key(parts), table_id(parts), quote_table_identifier(parts)
            )
            table["errors"].append(error_record(exc, "assemble"))
            finalize_table(table, config)
            log(f"[tabledossier] {name}: failed unexpectedly ({type(exc).__name__})")
        tables.append(table)
    relationships, referential = validate_relationships(spark, tables, config, log=log)
    hypotheses = evaluate_hypotheses(spark, tables, relationships, config, log=log)
    finished = utc_now()
    return build_profile(
        run_id=ctx["run_id"],
        started_at=ctx["started_at"],
        finished_at=format_utc(finished),
        duration_ms=max(0, int((finished - ctx["started"]).total_seconds() * 1000)),
        reference_time=ctx["reference_time"],
        environment=ctx["environment"],
        config=config,
        parameter_sources=ctx["parameter_sources"],
        generation=ctx["generation"],
        capabilities=ctx["capabilities"],
        tables=tables,
        relationships=relationships,
        referential_validation=referential,
        relationship_hypotheses=hypotheses,
    )


def summary_text(profile: Mapping[str, Any]) -> str:
    """Return a short plain-text summary of a profile."""
    run = profile["run"]
    summary = profile["summary"]
    lines = [
        f"Run {run['run_id']}: {run['status']} ({summary['tables_succeeded']} succeeded, "
        f"{summary['tables_partial']} partial, {summary['tables_failed']} failed)",
    ]
    for table in profile["tables"]:
        rows = next((m for m in table["table_metrics"] if m["name"] == "row_count"), None)
        rows_text = (
            f"{rows['value']:,} rows in scope"
            if rows and rows["status"] == "measured"
            else "rows not measured"
        )
        lines.append(
            f"  - {table['table_key']}: {table['status']}, {rows_text}, "
            f"{table['summary'].get('fields_profiled', 0)} field(s) profiled, "
            f"{len(table['findings'])} finding(s)"
        )
        for error in table["errors"]:
            lines.append(
                f"      error [{error['stage']}] {error['condition'] or error['error_class']}: "
                f"{error['message']}"
            )
    checks = summary["checks"]
    lines.append(
        f"Checks: {checks['pass']} pass, {checks['fail']} fail, {checks['not_evaluated']} not "
        "evaluated, "
        f"{checks['error']} error. Findings: {summary['findings']['warning']} warning, "
        f"{summary['findings']['info']} info."
    )
    return "\n".join(lines)


def export_run(
    ctx: Mapping[str, Any],
    profile: Mapping[str, Any],
    documents: Mapping[str, str],
    schemas: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate the profile and write the complete result package."""
    start = time.perf_counter()
    errors = validate_profile(profile, schemas["profile"])
    files = {"profile.json": pretty_json(profile), **documents}
    manifest = run_manifest(
        profile,
        files,
        status=profile["run"]["status"],
        validation_errors=errors,
        generation_id=ctx["generation"].get("generation_id"),
    )
    files["manifest.json"] = pretty_json(manifest)
    written = write_files(ctx["run_dir"], files, replaceable=("manifest.json",))
    return {
        "run_dir": ctx["run_dir"],
        "files": written,
        "validation_errors": errors,
        "duration_ms": max(0, int((time.perf_counter() - start) * 1000)),
    }


def transfer_instructions(run_dir: str, run_id: str) -> str:
    """Return concrete instructions to copy the results to a local machine."""
    lines = [f"Results written to: {run_dir}", "", "Copy them to your computer with one of:"]
    if run_dir.startswith("/Volumes/"):
        lines += [
            f"  databricks fs cp -r dbfs:{run_dir} ./downloaded/{run_id}",
            "  or Catalog Explorer > the volume > select the files > Download.",
        ]
    elif run_dir.startswith("/Workspace/"):
        lines += [
            f"  databricks workspace export-dir {run_dir} ./downloaded/{run_id}",
            "  or the workspace file browser > the folder > Download.",
        ]
    else:
        lines += ["  the file transfer mechanism approved for this location in your environment."]
    lines += [
        "",
        "Then, offline:",
        f"  tabledossier validate --profile downloaded/{run_id}/profile.json",
        f"  tabledossier render --input downloaded/{run_id}/profile.json --output "
        f"docs/generated/{run_id}",
    ]
    return "\n".join(lines)


def prepare_documents(profile: Mapping[str, Any]) -> dict[str, str]:
    """Render the documentation package for a profile (same code as the CLI)."""
    return build_documents(profile)
