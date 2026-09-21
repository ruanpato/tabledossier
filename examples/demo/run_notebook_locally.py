"""Development harness: execute a generated notebook with a *local* SparkSession.

This is tooling for contributors and for building the synthetic demo profile.
It is not part of the TableDossier package and it is not a supported way to
profile production data. It needs PySpark and a Java runtime.

The generated ``.py`` notebook is valid Python as a whole (markdown cells are
comments), so it is executed as one module with two injected globals:

* ``spark``   - a local SparkSession;
* ``dbutils`` - a minimal stand-in that implements only ``dbutils.widgets``.

Only widget handling is simulated: every Spark call in the notebook is real.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
DEMO_SQL = HERE / "create_demo_tables.sql"


class LocalWidgets:
    """Implements the subset of ``dbutils.widgets`` used by TableDossier notebooks."""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})
        self.created: list[str] = []

    def get(self, name: str) -> str:
        if name not in self.values:
            raise ValueError(f"InputWidgetNotDefined: {name}")
        return self.values[name]

    def text(self, name: str, default: str, label: str | None = None) -> None:
        self.values.setdefault(name, default)
        self.created.append(name)

    def dropdown(
        self, name: str, default: str, choices: list[str], label: str | None = None
    ) -> None:
        if default not in choices:
            raise ValueError(f"default {default!r} is not one of {choices!r}")
        self.values.setdefault(name, default)
        self.created.append(name)


class LocalDbutils:
    """Minimal ``dbutils`` stand-in exposing ``widgets`` only."""

    def __init__(self, widget_values: dict[str, str] | None = None) -> None:
        self.widgets = LocalWidgets(widget_values)


def split_sql(text: str) -> list[str]:
    """Split a simple SQL script (no semicolons inside literals) into statements."""
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


def load_demo_tables(spark: Any, database: str = "analytics") -> list[str]:
    """Create the synthetic demo tables in a local database; return their names."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    for statement in split_sql(DEMO_SQL.read_text(encoding="utf-8")):
        spark.sql(statement)
    return [f"{database}.{name}" for name in ("customers", "orders", "order_events", "returns")]


def delta_available() -> bool:
    """Return True when the optional ``delta-spark`` package is installed."""
    try:
        import delta  # noqa: F401
    except ImportError:
        return False
    return True


def local_spark(
    warehouse: str,
    app_name: str = "tabledossier-local",
    delta: bool = False,
    delta_by_default: bool = False,
) -> Any:
    """Start a small local SparkSession with deterministic settings (UTC).

    With ``delta=True`` the session is configured with ``delta-spark`` (its JARs
    are resolved from Maven Central on first use). ``delta_by_default`` makes
    ``CREATE TABLE`` without ``USING`` create Delta tables, as on Databricks.
    """
    import sys

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse)
        # Without a USING clause, create data source tables (Parquet) as Databricks creates Delta.
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        if delta_by_default:
            builder = builder.config("spark.sql.sources.default", "delta")
        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


def run_notebook(path: str | Path, spark: Any, widget_values: dict[str, str]) -> dict[str, Any]:
    """Execute a generated notebook file and return its global namespace."""
    source = Path(path).read_text(encoding="utf-8")
    if not source.startswith("# Databricks notebook source"):
        raise ValueError("not a Databricks source notebook")
    namespace: dict[str, Any] = {
        "__name__": "__tabledossier_notebook__",
        "spark": spark,
        "dbutils": LocalDbutils(widget_values),
    }
    exec(compile(source, str(path), "exec"), namespace)
    return namespace


def cell_count(path: str | Path) -> int:
    """Return the number of cells in a Databricks source notebook."""
    text = Path(path).read_text(encoding="utf-8")
    return len(re.findall(r"^# COMMAND ----------$", text, flags=re.MULTILINE)) + 1
