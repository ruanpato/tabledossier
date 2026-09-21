"""Delta-specific paths: snapshot pinning, DESCRIBE DETAIL and CHECK constraints.

Runs only when ``delta-spark`` is installed (the Spark session is then created
with the Delta extensions). Unity Catalog information_schema constraints cannot
be exercised locally; see docs/compatibility.md.
"""

import pytest
from run_notebook_locally import delta_available

from tabledossier.config import default_config
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import find_metric, metric_value
from tabledossier.runtime.spark import profile_table

pytestmark = pytest.mark.skipif(not delta_available(), reason="delta-spark is not installed")


@pytest.fixture(scope="module")
def delta_table(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS lake")
    spark.sql("DROP TABLE IF EXISTS lake.accounts")
    spark.sql(
        "CREATE TABLE lake.accounts (account_id BIGINT NOT NULL, balance DECIMAL(10,2), status STRING) "
        "USING DELTA COMMENT 'Synthetic Delta accounts'"
    )
    spark.sql(
        "INSERT INTO lake.accounts SELECT id, CAST(id AS DECIMAL(10,2)), 'open' FROM range(10)"
    )
    spark.sql("ALTER TABLE lake.accounts ADD CONSTRAINT balance_non_negative CHECK (balance >= 0)")
    return "lake.accounts"


def _profile(spark, name, capabilities, config=None):
    return profile_table(
        spark,
        name,
        config or default_config(),
        capabilities=capabilities,
        reference_time=format_utc(utc_now()),
        now=lambda: format_utc(utc_now()),
        log=lambda message: None,
    )


def test_delta_version_is_pinned_for_every_read(spark, delta_table, capabilities, monkeypatch):
    import tabledossier.runtime.spark as runtime

    original = runtime.read_latest_version

    def resolve_then_write(session, quoted):
        version, timestamp = original(session, quoted)
        # A concurrent writer commits after the version was resolved.
        session.sql("INSERT INTO lake.accounts SELECT id + 100, 1.00, 'late' FROM range(5)")
        return version, timestamp

    monkeypatch.setattr(runtime, "read_latest_version", resolve_then_write)
    table = _profile(spark, delta_table, capabilities)
    assert table["status"] == "succeeded", table["errors"]
    consistency = table["consistency"]
    assert consistency["mode"] == "pinned_delta_version"
    assert metric_value(table["table_metrics"], "row_count") == 10, "later commits must not be read"
    assert table["scope"]["scope_label"] == "full_snapshot"
    assert table["schema"]["captured_from"] == "pinned_snapshot"
    assert spark.table(delta_table).count() == 15


def test_delta_metadata_and_constraints(spark, delta_table, capabilities):
    table = _profile(spark, delta_table, capabilities)
    source = table["source"]
    assert source["provider"] == "delta"
    assert source["comment"] == "Synthetic Delta accounts"
    assert source["size_in_bytes"]["status"] == "measured"
    assert source["size_in_bytes"]["accuracy"] == "as_recorded"
    assert source["file_count"]["value"] >= 1
    checks = [c for c in table["constraints"] if c["constraint_type"] == "check"]
    assert checks and checks[0]["name"] == "balance_non_negative"
    assert checks[0]["enforcement"] == "enforced" and checks[0]["source"] == "delta_table_property"
    account = next(f for f in table["field_profiles"] if f["display_path"] == "account_id")
    assert account["nullable"] is False
    assert "location" not in str(source).lower()


def test_pinning_can_be_disabled(spark, delta_table, capabilities):
    config = default_config()
    config["consistency"]["pin_delta_version"] = False
    table = _profile(spark, delta_table, capabilities, config)
    assert table["consistency"]["mode"] == "unpinned"
    assert find_metric(table["table_metrics"], "row_count")["scope"] == "full_table"
