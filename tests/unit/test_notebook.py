import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

import tabledossier.notebook as notebook_module
from tabledossier.config import default_config, normalize_config
from tabledossier.notebook import (
    RUNTIME_MODULES,
    CompositionError,
    compose_modules,
    generate_notebook,
)
from tabledossier.resources import schema_text

SRC = Path(__file__).resolve().parents[2] / "src" / "tabledossier"


def _cells(text):
    return text.split("\n\n# COMMAND ----------\n\n")


def test_generation_is_deterministic():
    config = normalize_config(
        {"kind": "tabledossier.config", "config_version": "1.0", "tables": ["a.b.c"]}
    )
    first, manifest_a = generate_notebook(config)
    second, manifest_b = generate_notebook(config)
    assert first == second
    assert manifest_a == manifest_b
    assert manifest_a["contains_results"] is False
    assert (
        manifest_a["notebook"]["sha256"] == "sha256:" + hashlib.sha256(first.encode()).hexdigest()
    )


def test_notebook_structure_and_sections():
    text, manifest = generate_notebook(default_config())
    assert text.splitlines()[0] == "# Databricks notebook source"
    cells = _cells(text)
    assert len(cells) == manifest["notebook"]["cells"]
    ast.parse(text)
    for cell in cells:
        if not cell.startswith("# MAGIC") and not cell.startswith("# Databricks notebook source"):
            ast.parse(cell)
    headings = [line for line in text.splitlines() if line.startswith("# MAGIC ## ")]
    assert headings == [
        "# MAGIC ## 1. Parameters",
        "# MAGIC ## 2. Runtime definitions",
        "# MAGIC ## 3. Validate parameters and environment",
        "# MAGIC ## 4. Analysis plan",
        "# MAGIC ## 5. Execution",
        "# MAGIC ## 6. Summary",
        "# MAGIC ## 7. Data dictionary",
        "# MAGIC ## 8. Quality and limitations",
        "# MAGIC ## 9. Export",
    ]
    assert "contains no results yet" in text


def test_notebook_is_self_contained():
    text, _ = generate_notebook(default_config())
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("tabledossier")
            assert node.module.split(".")[0] in sys.stdlib_module_names | {"pyspark"}
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] in sys.stdlib_module_names | {"pyspark"}
    code_lines = [line for line in text.splitlines() if not line.lstrip().startswith("#")]
    lowered = "\n".join(code_lines).lower()
    for forbidden in (
        "%pip",
        "pip install",
        "%run",
        "urllib.request",
        "requests.get",
        "b64decode",
        "exec(",
        "eval(",
    ):
        assert forbidden not in lowered, forbidden


def _sql_literals(tree):
    """Return the literal text of every first argument passed to ``<x>.sql(...)``."""
    found = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
        ):
            arg = node.args[0]
            if isinstance(arg, ast.Constant):
                found.append(arg.value)
            elif isinstance(arg, ast.JoinedStr):
                found.append(
                    "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in arg.values)
                )
            elif isinstance(arg, ast.Name):
                found.append(f"<variable {arg.id}>")
    return found


def test_engine_statements_are_read_only():
    text, _ = generate_notebook(default_config())
    statements = _sql_literals(ast.parse(text))
    assert statements, "expected SQL statements"
    for statement in statements:
        if statement.startswith("<variable"):
            continue
        head = statement.strip().split()[0].upper()
        assert head in ("DESCRIBE", "SELECT"), statement
        upper = statement.upper()
        for keyword in (
            "ALTER",
            "OPTIMIZE",
            "ANALYZE",
            "INSERT",
            "DELETE",
            "UPDATE",
            "MERGE",
            "DROP",
            "VACUUM",
            "CREATE",
        ):
            assert keyword not in upper.split(), (keyword, statement)


def test_embedded_modules_match_sources():
    text, manifest = generate_notebook(default_config())
    for entry in manifest["embedded_modules"]:
        path = SRC.parent.parent / entry["source_path"]
        source = path.read_text(encoding="utf-8")
        assert entry["sha256"] == "sha256:" + hashlib.sha256(source.encode()).hexdigest()
        assert f"# Source: {entry['source_path']} ({entry['sha256']})" in text
    for name in ("config", "profile"):
        body = schema_text(name).rstrip()
        assert body in text


def test_every_runtime_module_is_embedded_exactly_once():
    _, runtime = compose_modules()
    assert [item.module for item in runtime] == [name for name, _ in RUNTIME_MODULES]


def test_config_values_cannot_inject_cells_or_code():
    hostile = "x\n\n# COMMAND ----------\n\nimport os; os.system('rm -rf /')\n'''\"\"\""
    config = normalize_config(
        {
            "kind": "tabledossier.config",
            "config_version": "1.0",
            "purpose": hostile,
            "table_options": {
                "a.b.c": {
                    "filters": [{"column": "c", "operator": "eq", "value": hostile}],
                    "purpose": hostile,
                }
            },
        }
    )
    text, manifest = generate_notebook(config)
    assert len(_cells(text)) == manifest["notebook"]["cells"]
    assert "\nimport os; os.system" not in text
    parameters = next(cell for cell in _cells(text) if cell.startswith("# DBTITLE 1,Parameters"))
    namespace: dict = {"dbutils": None, "ensure_widgets": lambda dbutils, defaults: None}
    exec(compile(parameters, "parameters", "exec"), namespace)
    assert namespace["TD_GENERATED_CONFIG"] == config
    assert json.loads(namespace["TD_WIDGET_DEFAULTS"]["tables_json"]) == []


def test_generation_does_not_import_spark_or_network_modules():
    code = (
        "import sys\n"
        "from tabledossier.config import default_config\n"
        "from tabledossier.notebook import generate_notebook\n"
        "from tabledossier.package import build_documents\n"
        "generate_notebook(default_config())\n"
        "bad = [m for m in sys.modules if m.split('.')[0] in ('pyspark', 'py4j', 'databricks', 'psycopg', 'psycopg2', 'requests', 'urllib3')]\n"
        "print(bad)\n"
        "sys.exit(1 if bad else 0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_composition_rejects_forbidden_imports(monkeypatch):
    original = notebook_module.module_source

    def fake(path):
        source = original(path)
        if path == "semantic.py":
            return "import requests\n" + source
        return source

    monkeypatch.setattr(notebook_module, "module_source", fake)
    with pytest.raises(CompositionError, match="requests"):
        compose_modules()


def test_composition_rejects_name_collisions(monkeypatch):
    original = notebook_module.module_source

    def fake(path):
        source = original(path)
        if path == "render.py":
            return source + "\n\ndef measured():\n    return None\n"
        return source

    monkeypatch.setattr(notebook_module, "module_source", fake)
    with pytest.raises(CompositionError, match="measured"):
        compose_modules()


def test_composition_rejects_cell_markup(monkeypatch):
    original = notebook_module.module_source

    def fake(path):
        source = original(path)
        if path == "paths.py":
            return source + "\n# COMMAND ----------\n"
        return source

    monkeypatch.setattr(notebook_module, "module_source", fake)
    with pytest.raises(CompositionError, match="notebook markup"):
        compose_modules()
