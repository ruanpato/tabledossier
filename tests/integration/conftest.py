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


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not _java_available():
        pytest.skip("a Java runtime is required for Spark integration tests (set JAVA_HOME)")
    from run_notebook_locally import delta_available, local_spark

    use_delta = delta_available() and os.environ.get("TD_TEST_DELTA", "1") != "0"
    session = local_spark(
        str(tmp_path_factory.mktemp("warehouse")), "tabledossier-tests", delta=use_delta
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def demo_tables(spark):
    from run_notebook_locally import load_demo_tables

    return load_demo_tables(spark)


@pytest.fixture(scope="session")
def capabilities(spark):
    from tabledossier.runtime.spark import detect_capabilities

    return detect_capabilities(spark)
