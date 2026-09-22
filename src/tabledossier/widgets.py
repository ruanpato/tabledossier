"""Databricks notebook widgets (the notebook's parameters).

Part of the embedded runtime (standard library only). It is placed in the
*Parameters* section of the generated notebook, before the other runtime
modules, so it must not depend on them.

Widgets are created only when missing: values typed by a user or passed by a
Job are never reset by re-running the notebook.
"""

from collections.abc import Mapping
from typing import Any

NOTEBOOK_WIDGETS = (
    ("tables_json", "1. Tables (JSON list of catalog.schema.table)"),
    ("analysis_level", "2. Analysis level"),
    ("output_dir", "3. Output directory (e.g. /Volumes/<catalog>/<schema>/<volume>/tabledossier)"),
    ("config_json", "4. Extra configuration (JSON object, no secrets)"),
)
ANALYSIS_LEVEL_CHOICES = ("metadata", "standard", "deep")


def ensure_widgets(dbutils: Any, defaults: Mapping[str, str]) -> list[str]:
    """Create missing widgets with the generated defaults; return the names created."""
    created = []
    for name, label in NOTEBOOK_WIDGETS:
        try:
            dbutils.widgets.get(name)
            continue
        except Exception:  # noqa: BLE001 - Databricks raises when a widget is not defined
            pass
        if name == "analysis_level":
            dbutils.widgets.dropdown(name, defaults[name], list(ANALYSIS_LEVEL_CHOICES), label)
        else:
            dbutils.widgets.text(name, defaults[name], label)
        created.append(name)
    return created


def read_widgets(dbutils: Any) -> dict[str, str]:
    """Return the current string value of every TableDossier widget."""
    return {name: str(dbutils.widgets.get(name)) for name, _ in NOTEBOOK_WIDGETS}
