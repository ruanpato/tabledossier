"""Access to files shipped inside the package (schemas and runtime sources).

CLI-only module: it is not embedded in notebooks.
"""

import json
from importlib import resources
from typing import Any

SCHEMA_FILES = {
    "config": "config.schema.json",
    "profile": "profile.schema.json",
    # Frozen schemas of earlier profile contracts, still accepted by readers:
    # 1.0 (TableDossier 0.1.x) and 1.1 (TableDossier 0.2.x).
    "profile-1.0": "profile-1.0.schema.json",
    "profile-1.1": "profile-1.1.schema.json",
    "annotations": "annotations.schema.json",
    "suggested_rules": "suggested_rules.schema.json",
    "manifest": "manifest.schema.json",
    "job_summary": "job_summary.schema.json",
}


def schema_text(name: str) -> str:
    """Return the raw text of a packaged JSON Schema."""
    return (
        resources.files("tabledossier")
        .joinpath("schemas")
        .joinpath(SCHEMA_FILES[name])
        .read_text(encoding="utf-8")
    )


def load_schema(name: str) -> dict[str, Any]:
    """Load a packaged JSON Schema by short name (config, profile, annotations...)."""
    data = json.loads(schema_text(name))
    if not isinstance(data, dict):
        raise ValueError(f"schema {name} is not a JSON object")
    return data


def module_source(relative_path: str) -> str:
    """Return the source text of a module in the package (e.g. ``runtime/spark.py``)."""
    target = resources.files("tabledossier")
    for part in relative_path.split("/"):
        target = target.joinpath(part)
    return target.read_text(encoding="utf-8")
