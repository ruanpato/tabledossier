"""Spark integration fixtures (skipped when PySpark or Java is unavailable)."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples" / "demo"))


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / "java") if java_home else shutil.which("java")
    if not java:
        return False
    try:
        result = subprocess.run([java, "-version"], capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.spark)


def requested_spark_mode() -> str:
    """Spark session flavour under test: ``classic`` (default) or ``connect``.

    Set ``TD_TEST_SPARK_MODE=connect`` to run the whole integration suite through
    a local Spark Connect server (as Databricks shared access mode and serverless
    compute do). Connect mode never falls back to classic: missing client
    dependencies fail the session fixture instead of skipping silently.
    """
    mode = os.environ.get("TD_TEST_SPARK_MODE", "classic").strip().lower()
    if mode not in ("classic", "connect"):
        raise ValueError(f"TD_TEST_SPARK_MODE must be 'classic' or 'connect', not {mode!r}")
    return mode


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not _java_available():
        pytest.skip("a Java runtime is required for Spark integration tests (set JAVA_HOME)")
    from run_notebook_locally import connect_available, delta_available, local_spark

    connect = requested_spark_mode() == "connect"
    if connect and not connect_available():
        raise RuntimeError(
            "TD_TEST_SPARK_MODE=connect requires the Spark Connect client dependencies "
            "(pip install 'pyspark[connect]', plus setuptools on Python 3.12 with PySpark 3.5)"
        )
    use_delta = delta_enabled_by_environment() and delta_available()
    session = local_spark(
        str(tmp_path_factory.mktemp("warehouse")),
        "tabledossier-tests",
        delta=use_delta,
        connect=connect,
    )
    yield session
    session.stop()


def delta_enabled_by_environment() -> bool:
    """Delta is used when delta-spark is installed, unless ``TD_TEST_DELTA=0``."""
    return os.environ.get("TD_TEST_DELTA", "1") != "0"


@pytest.fixture(scope="session")
def spark_mode() -> str:
    return requested_spark_mode()


@pytest.fixture(scope="session")
def delta_enabled(spark) -> bool:
    from run_notebook_locally import delta_available

    return delta_enabled_by_environment() and delta_available()


@pytest.fixture(scope="session")
def demo_tables(spark):
    from run_notebook_locally import load_demo_tables

    return load_demo_tables(spark)


@pytest.fixture(scope="session")
def capabilities(spark):
    from tabledossier.runtime.spark import detect_capabilities

    return detect_capabilities(spark)
