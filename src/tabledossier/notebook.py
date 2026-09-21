"""Generate a self-contained Databricks source notebook (``.py``).

CLI-only module. The notebook embeds the *runtime*: tested modules of this
package, copied verbatim into cells, in dependency order. The only change is
the removal of intra-package ``from tabledossier... import ...`` statements,
because the names they import are already defined by earlier cells. This keeps
one source of truth (the modules under ``src/tabledossier``) and readable,
reviewable cells: no encoded payloads, no ``exec`` of strings, no downloads.

Composition rules enforced here (and by tests):

* embedded modules import only the standard library, plus ``pyspark`` in
  ``tabledossier.runtime.*``;
* intra-package imports are top-level ``from tabledossier.x import a, b``
  statements without aliases, importing names defined by earlier modules;
* top-level names are unique across embedded modules and import bindings are
  consistent, so concatenation cannot silently shadow anything;
* no ``from __future__`` imports (the whole notebook must parse as one module)
  and no line that Databricks would read as a cell separator or magic.

Generation is deterministic: the same configuration and tool version always
produce the same bytes (no timestamps).
"""

import ast
import hashlib
import pprint
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tabledossier._version import __version__
from tabledossier.config import widget_defaults
from tabledossier.jsonutil import fingerprint
from tabledossier.resources import SCHEMA_FILES, module_source, schema_text

NOTEBOOK_HEADER = "# Databricks notebook source"
CELL_SEPARATOR = "\n\n# COMMAND ----------\n\n"
TARGET_RUNTIMES = (
    "Databricks Runtime 16.4 LTS (Spark 3.5.2, Python 3.12) - primary target",
    "Databricks Runtime 15.4 LTS (Spark 3.5.0, Python 3.11) - secondary target",
    "Databricks Runtime 17.3 LTS (Spark 4.0.0, Python 3.12) - secondary target",
)
PARAMETER_MODULES = (("tabledossier.widgets", "widgets.py"),)
RUNTIME_MODULES = (
    ("tabledossier._version", "_version.py"),
    ("tabledossier.jsonutil", "jsonutil.py"),
    ("tabledossier.schemacheck", "schemacheck.py"),
    ("tabledossier.errors", "errors.py"),
    ("tabledossier.paths", "paths.py"),
    ("tabledossier.config", "config.py"),
    ("tabledossier.metrics", "metrics.py"),
    ("tabledossier.planning", "planning.py"),
    ("tabledossier.semantic", "semantic.py"),
    ("tabledossier.findings", "findings.py"),
    ("tabledossier.quality", "quality.py"),
    ("tabledossier.relationships", "relationships.py"),
    ("tabledossier.contract", "contract.py"),
    ("tabledossier.render", "render.py"),
    ("tabledossier.package", "package.py"),
    ("tabledossier.assemble", "assemble.py"),
    ("tabledossier.runtime.spark", "runtime/spark.py"),
    ("tabledossier.runtime.databricks", "runtime/databricks.py"),
)
EMBEDDED_SCHEMAS = ("config", "profile")
_NB_FORBIDDEN_PREFIXES = ("# COMMAND", "# MAGIC", "# DBTITLE", NOTEBOOK_HEADER)
_NB_THIRD_PARTY = {"pyspark"}
LICENSE_LINES = (
    "Copyright 2026 ruanpato and TableDossier contributors.",
    "Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0",
)


class CompositionError(RuntimeError):
    """Raised when an embedded module breaks the composition rules."""


@dataclass(frozen=True)
class EmbeddedModule:
    """A runtime module prepared for embedding."""

    module: str
    source_path: str
    sha256: str
    body: str
    defined: frozenset[str]


def _nb_sha(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _nb_binding(node: ast.Import | ast.ImportFrom, alias: ast.alias) -> tuple[str, str]:
    if isinstance(node, ast.Import):
        name = alias.asname or alias.name.split(".")[0]
        target = alias.name if alias.asname else alias.name.split(".")[0]
        return name, f"module:{target}"
    return alias.asname or alias.name, f"from:{node.module}.{alias.name}"


def prepare_module(
    module: str,
    relative_path: str,
    defined_before: set[str],
    bindings: dict[str, str],
) -> EmbeddedModule:
    """Check one module against the composition rules and strip its package imports."""
    source = module_source(relative_path)
    tree = ast.parse(source, filename=relative_path)
    allow_pyspark = module.startswith("tabledossier.runtime.")
    strip: list[tuple[int, int]] = []
    defined: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node not in tree.body:
            names = [alias.name for alias in node.names]
            target = node.module if isinstance(node, ast.ImportFrom) else names[0]
            if isinstance(node, ast.ImportFrom) and (
                node.level or (target or "").startswith("tabledossier")
            ):
                raise CompositionError(f"{module}: nested package imports are not allowed")
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            if node.level:
                raise CompositionError(f"{module}: relative imports are not allowed")
            target = node.module or ""
            if target == "__future__":
                raise CompositionError(
                    f"{module}: __future__ imports are not allowed in embedded modules"
                )
            if target.split(".")[0] == "tabledossier":
                for alias in node.names:
                    if alias.asname and alias.asname != alias.name:
                        raise CompositionError(
                            f"{module}: aliased package import {alias.name} as {alias.asname}"
                        )
                    if alias.name not in defined_before:
                        raise CompositionError(
                            f"{module}: imports {alias.name!r} from {target}, which is not "
                            "defined by an earlier module"
                        )
                strip.append((node.lineno, node.end_lineno or node.lineno))
                continue
            _nb_check_external(module, target, allow_pyspark)
            for alias in node.names:
                name, value = _nb_binding(node, alias)
                _nb_bind(module, name, value, bindings, defined_before)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                _nb_check_external(module, alias.name, allow_pyspark)
                name, value = _nb_binding(node, alias)
                _nb_bind(module, name, value, bindings, defined_before)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for target_node in node.targets:
                if not isinstance(target_node, ast.Name):
                    raise CompositionError(
                        f"{module}: only simple top-level assignments are allowed"
                    )
                defined.add(target_node.id)
        elif isinstance(node, ast.AnnAssign):
            if not isinstance(node.target, ast.Name):
                raise CompositionError(f"{module}: only simple top-level assignments are allowed")
            defined.add(node.target.id)
        elif isinstance(node, ast.If):
            raise CompositionError(f"{module}: top-level if statements are not allowed")
    duplicated = sorted(defined & (defined_before | set(bindings)))
    if duplicated:
        raise CompositionError(
            f"{module}: top-level names already defined elsewhere: {', '.join(duplicated)}"
        )
    lines = source.splitlines()
    removed = {line for start, end in strip for line in range(start, end + 1)}
    body = "\n".join(
        line for number, line in enumerate(lines, start=1) if number not in removed
    ).strip("\n")
    for line in body.splitlines():
        if line.lstrip().startswith(_NB_FORBIDDEN_PREFIXES):
            raise CompositionError(
                f"{module}: line would be read as notebook markup: {line.strip()[:40]!r}"
            )
    return EmbeddedModule(
        module, f"src/tabledossier/{relative_path}", _nb_sha(source), body, frozenset(defined)
    )


def _nb_check_external(module: str, target: str, allow_pyspark: bool) -> None:
    root = target.split(".")[0]
    if root in sys.stdlib_module_names:
        return
    if root in _NB_THIRD_PARTY and allow_pyspark:
        return
    raise CompositionError(f"{module}: import of {target!r} is not allowed in the embedded runtime")


def _nb_bind(
    module: str, name: str, value: str, bindings: dict[str, str], defined_before: set[str]
) -> None:
    if name in defined_before:
        raise CompositionError(
            f"{module}: import binding {name!r} shadows a name defined by an earlier module"
        )
    if name in bindings and bindings[name] != value:
        raise CompositionError(
            f"{module}: import binding {name!r} conflicts ({bindings[name]} vs {value})"
        )
    bindings[name] = value


def compose_modules() -> tuple[list[EmbeddedModule], list[EmbeddedModule]]:
    """Prepare the parameter-section and runtime-section modules, in order."""
    defined: set[str] = set()
    bindings: dict[str, str] = {}
    prepared: list[list[EmbeddedModule]] = [[], []]
    for index, group in enumerate((PARAMETER_MODULES, RUNTIME_MODULES)):
        for module, path in group:
            item = prepare_module(module, path, defined, bindings)
            defined |= item.defined
            prepared[index].append(item)
    return prepared[0], prepared[1]


def _nb_markdown(lines: list[str]) -> str:
    out = ["# MAGIC %md"]
    for line in lines:
        out.append(f"# MAGIC {line}".rstrip())
    return "\n".join(out)


def _nb_module_cell(item: EmbeddedModule) -> str:
    header = [
        f"# DBTITLE 1,Runtime: {item.module}",
        f"# TableDossier {__version__} embedded runtime: module {item.module}",
        f"# Source: {item.source_path} ({item.sha256})",
        *[f"# {line}" for line in LICENSE_LINES],
        "# Intra-package imports were removed at generation time; the names they",
        "# provided are defined by earlier cells. Do not edit: regenerate instead.",
    ]
    return "\n".join(header) + "\n\n" + item.body


def _nb_literal(value: Any) -> str:
    return pprint.pformat(value, width=100, sort_dicts=False)


def generation_id(config: Mapping[str, Any], modules: list[EmbeddedModule]) -> str:
    """Return the deterministic identifier of a generated notebook."""
    return fingerprint(
        {
            "tool_version": __version__,
            "config": config,
            "modules": {item.module: item.sha256 for item in modules},
            "schemas": {name: _nb_sha(schema_text(name)) for name in EMBEDDED_SCHEMAS},
        }
    )


def generate_notebook(config: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return ``(notebook_source, generation_manifest)`` for a validated config."""
    parameter_modules, runtime_modules = compose_modules()
    modules = parameter_modules + runtime_modules
    gen_id = generation_id(config, modules)
    generation = {"generator_version": __version__, "generation_id": gen_id}
    defaults = widget_defaults(config)
    table_count = len(config.get("tables", []))
    cells: list[str] = []
    cells.append(
        NOTEBOOK_HEADER
        + "\n"
        + _nb_markdown(
            [
                "# TableDossier profiling notebook",
                "",
                f"Generated offline by TableDossier {__version__} (generation id `{gen_id}`).",
                "",
                "**This notebook contains no results yet.** It was generated without access to "
                "your data; "
                "metrics exist only after you run it here.",
                "",
                "**What it does**",
                "1. Reads the parameters (widgets) at the top of the notebook.",
                "2. Validates them and creates a new results directory under `output_dir` before "
                "reading any table.",
                "3. Profiles each table sequentially: catalog metadata and, at the `standard` "
                "level, one bounded "
                "sample and a bounded number of shared aggregation passes.",
                "4. Writes `profile.json`, `manifest.json` and the derived documentation to the "
                "results directory.",
                "",
                "**Safety.** Sources are only read. The notebook never alters schemas or "
                "constraints, never runs "
                "OPTIMIZE or ANALYZE TABLE and never modifies data. It writes only to its own "
                "results directory "
                "and never overwrites existing files. It installs nothing and downloads nothing.",
                "",
                "**Requirements.** A Databricks Runtime with Python and PySpark (targets: 16.4 "
                "LTS, 15.4 LTS, "
                "17.3 LTS; see docs/compatibility.md for what has been validated).",
                "",
                f"**How to use.** {table_count} default table(s) were configured at generation. "
                "Set `tables_json` "
                '(for example `["demo.analytics.orders"]`) and `output_dir` (for example '
                "`/Volumes/<catalog>/<schema>/<volume>/tabledossier`), then choose *Run all*.",
                "",
                "**License.** The embedded runtime cells are TableDossier source code, licensed "
                "under the "
                "Apache License, Version 2.0. Your data, parameters and results are yours and are "
                "not subject "
                "to that license.",
            ]
        )
    )
    cells.append(
        _nb_markdown(
            [
                "## 1. Parameters",
                "",
                "| Widget | Meaning |",
                "| --- | --- |",
                "| `tables_json` | JSON list of `catalog.schema.table` identifiers (quote unusual "
                "names with backticks). |",
                "| `analysis_level` | `metadata` (no row reads) or `standard` (sample + "
                "aggregations). |",
                "| `output_dir` | Directory where a new `<run_id>/` folder is created. |",
                "| `config_json` | Optional JSON object merged over the generated configuration "
                "(no secrets). |",
                "",
                "Precedence: built-in defaults < generated configuration < `config_json` < the "
                "three dedicated "
                "widgets. Existing widget values (typed by you or passed by a Job) are never "
                "reset.",
            ]
        )
    )
    cells.extend(_nb_module_cell(item) for item in parameter_modules)
    cells.append(
        "\n".join(
            [
                "# DBTITLE 1,Parameters",
                "# Configuration captured at generation time (defaults for the widgets below).",
                f"TD_GENERATED_CONFIG = {_nb_literal(dict(config))}",
                "",
                f"TD_GENERATION = {_nb_literal(generation)}",
                "",
                f"TD_WIDGET_DEFAULTS = {_nb_literal(defaults)}",
                "",
                "ensure_widgets(dbutils, TD_WIDGET_DEFAULTS)",
            ]
        )
    )
    cells.append(
        _nb_markdown(
            [
                "## 2. Runtime definitions",
                "",
                "The following cells define the TableDossier runtime. Each cell is a module of "
                "the TableDossier "
                "package copied verbatim from the source file named in its header (with its "
                "SHA-256), except that "
                "`from tabledossier... import ...` lines were removed because earlier cells "
                "define those names. "
                "They use only the Python standard library and the PySpark provided by the "
                "runtime.",
                "",
                "Run them as they are; to change behaviour, change the configuration or "
                "regenerate the notebook.",
            ]
        )
    )
    schema_lines = [
        "# DBTITLE 1,Runtime: JSON Schemas",
        f"# JSON Schemas shipped with TableDossier {__version__}: the exact text of the files in",
        "# src/tabledossier/schemas/ (SHA-256 below), parsed with json.loads.",
        "import json",
        "",
        "TD_SCHEMAS = {}",
    ]
    for name in EMBEDDED_SCHEMAS:
        text = schema_text(name)
        if "'" * 3 in text or text.rstrip().endswith("\\"):
            raise CompositionError(f"schema {name} cannot be embedded as a raw string literal")
        schema_lines.append(f"# {SCHEMA_FILES[name]}: {_nb_sha(text)}")
        quote = "'" * 3
        schema_lines.append(f"TD_SCHEMAS[{name!r}] = json.loads(r{quote}{text.rstrip()}{quote})")
    cells.append("\n".join(schema_lines))
    cells.extend(_nb_module_cell(item) for item in runtime_modules)
    steps = [
        (
            "## 3. Validate parameters and environment",
            "Checks the widgets and the configuration, then creates the run directory and writes "
            "a `running` "
            "manifest so that permission problems appear before any table is read.",
            [
                "# DBTITLE 1,Validate",
                "td_ctx = prepare_run(",
                "    spark=spark,",
                "    widget_values=read_widgets(dbutils),",
                "    generated_config=TD_GENERATED_CONFIG,",
                "    schemas=TD_SCHEMAS,",
                "    generation=TD_GENERATION,",
                ")",
                'print("Parameters are valid. Results directory:", td_ctx["run_dir"])',
            ],
        ),
        (
            "## 4. Analysis plan",
            "What will be asked of the engine for each table. Nothing has been read yet.",
            ["# DBTITLE 1,Plan", "print(describe_plan(td_ctx))"],
        ),
        (
            "## 5. Execution",
            "Tables are profiled one after another. A failure in one table is recorded with a "
            "sanitized message "
            "and does not stop the others (the run status becomes `partial`).",
            ["# DBTITLE 1,Execute", "td_profile = execute_run(spark, td_ctx)"],
        ),
        (
            "## 6. Summary",
            "Status per table and an overview rendered from the profile.",
            [
                "# DBTITLE 1,Summary",
                "td_documents = prepare_documents(td_profile)",
                "print(summary_text(td_profile))",
                "print()",
                'print(td_documents["overview.md"])',
            ],
        ),
        (
            "## 7. Data dictionary",
            "Descriptions come only from source comments (and, locally, from your annotations). "
            "Unknown "
            "meanings stay unknown.",
            ["# DBTITLE 1,Data dictionary", 'print(td_documents["data_dictionary.md"])'],
        ),
        (
            "## 8. Quality and limitations",
            "Observed quality report: executed checks, completeness, heuristic alerts, proposals "
            "and limits.",
            ["# DBTITLE 1,Quality report", 'print(td_documents["quality_report.md"])'],
        ),
        (
            "## 9. Export",
            "Validates the profile and writes the result package. Existing files are never "
            "overwritten.",
            [
                "# DBTITLE 1,Export",
                "td_export = export_run(td_ctx, td_profile, td_documents, TD_SCHEMAS)",
                'if td_export["validation_errors"]:',
                '    print("WARNING: the profile did not pass validation; see manifest.json:")',
                '    for td_error in td_export["validation_errors"][:20]:',
                '        print("  -", td_error)',
                'print(transfer_instructions(td_export["run_dir"], td_ctx["run_id"]))',
            ],
        ),
    ]
    for title, text, code in steps:
        cells.append(_nb_markdown([title, "", text]))
        cells.append("\n".join(code))
    notebook = CELL_SEPARATOR.join(cells) + "\n"
    _nb_check_structure(notebook, len(cells))
    manifest = {
        "kind": "tabledossier.generation_manifest",
        "manifest_version": "1.0",
        "tool_version": __version__,
        "generation_id": gen_id,
        "contains_results": False,
        "notebook": {
            "file_name": "",
            "format": "databricks_source_python",
            "sha256": _nb_sha(notebook),
            "cells": len(cells),
        },
        "config_fingerprint": fingerprint(dict(config)),
        "embedded_modules": [
            {"module": item.module, "source_path": item.source_path, "sha256": item.sha256}
            for item in modules
        ],
        "embedded_schemas": [
            {"name": name, "sha256": _nb_sha(schema_text(name))} for name in EMBEDDED_SCHEMAS
        ],
        "target_runtimes": list(TARGET_RUNTIMES),
        "notes": [
            "This manifest describes a generated notebook. It contains no metrics: results exist "
            "only after "
            "the notebook runs, in the run manifest written next to profile.json.",
        ],
    }
    return notebook, manifest


def _nb_check_structure(notebook: str, expected_cells: int) -> None:
    lines = notebook.splitlines()
    if lines[0] != NOTEBOOK_HEADER:
        raise CompositionError("notebook must start with the Databricks source header")
    separators = sum(1 for line in lines if line == "# COMMAND ----------")
    if separators != expected_cells - 1:
        raise CompositionError(
            f"unexpected cell separators: {separators} for {expected_cells} cells"
        )
    ast.parse(notebook)
