"""PostgreSQL connector end to end: result package, ground truth, types, privacy, metadata level."""

import json
from decimal import Decimal

import pytest

from conftest import PASSWORD_MARKER, ROOT, scalar
from tabledossier.cli import main
from tabledossier.package import DOCUMENT_FILES

DEMO_CONFIG = json.loads((ROOT / "examples" / "postgres" / "demo.config.json").read_text())


def demo(**overrides):
    config = {
        key: value for key, value in DEMO_CONFIG.items() if key not in ("kind", "config_version")
    }
    config.update(overrides)
    return config


def test_demo_run_writes_a_valid_package_equal_to_render(profile_run, pg_env, tmp_path, capsys):
    run = profile_run(demo())
    assert run.code == 0, run.stderr
    assert run.profile["schema_version"] == "1.3"
    names = {path.name for path in run.run_dir.iterdir()}
    assert names == {"profile.json", "manifest.json", *DOCUMENT_FILES}
    manifest = json.loads((run.run_dir / "manifest.json").read_text())
    assert manifest["status"] == "succeeded"
    assert manifest["profile_validation"] == {"checked": True, "valid": True, "errors": []}
    # The offline CLI accepts the profile and renders exactly the same documents.
    assert main(["validate", "--profile", str(run.run_dir / "profile.json")]) == 0
    rendered = tmp_path / "rendered"
    assert (
        main(["render", "--input", str(run.run_dir / "profile.json"), "--output", str(rendered)])
        == 0
    )
    for name in DOCUMENT_FILES:
        assert (rendered / name).read_text() == (run.run_dir / name).read_text(), name
    env = run.profile["run"]["environment"]
    assert env["engine"] == "postgresql" and env["execution_context"] == "local_cli"
    major, minor = divmod(pg_env.server_version_num, 10000)
    assert env["postgresql_version"] == f"{major}.{minor}"
    assert run.profile["run"]["generation"]["generation_id"] is None


def test_one_snapshot_for_every_table(profile_run):
    run = profile_run(demo())
    modes = {table["consistency"]["mode"] for table in run.profile["tables"]}
    stamps = {table["consistency"]["version_timestamp"] for table in run.profile["tables"]}
    assert modes == {"transaction_snapshot"} and len(stamps) == 1
    assert run.table("analytics.customers")["scope"]["scope_label"] == "full_snapshot"
    assert run.table("analytics.orders")["scope"]["scope_label"] == "filtered_snapshot"
    assert run.table("analytics.customers")["schema"]["captured_from"] == "pinned_snapshot"


def test_no_credential_host_user_or_database_in_the_package(profile_run, pg_env):
    run = profile_run(demo())
    text = run.package_text() + run.stdout + run.stderr
    for secret in (PASSWORD_MARKER, pg_env.password, pg_env.role, pg_env.database):
        assert secret not in text
    assert run.profile["run"]["effective_config"]["output_dir"] == "results"
    sources = run.profile["run"]["parameter_sources"]
    assert sources["connection"]["source"] == "environment variable named by --dsn-env"
    assert "TD_TEST_POSTGRES_READER_DSN" not in json.dumps(sources)


def test_metrics_equal_independent_sql(profile_run, admin):
    run = profile_run(demo())
    customers = "analytics.customers"
    assert run.table(customers)["table_metrics"][0]["value"] == 500
    for column in ("email", "segment", "score", "referrer_id", "middle_name", "display name"):
        nulls = scalar(admin, f'SELECT count(*) - count("{column}") FROM analytics.customers')
        assert run.metric(customers, column, "null_count") == nulls, column
    distinct = scalar(admin, "SELECT count(DISTINCT segment) FROM analytics.customers")
    assert run.metric(customers, "segment", "approx_distinct_count") == distinct
    field = run.field(customers, "segment")
    approx = next(m for m in field["metrics"] if m["name"] == "approx_distinct_count")
    assert approx["accuracy"] == "exact" and "relative_standard_deviation" not in approx.get(
        "details", {}
    )
    finite = "score NOT IN ('NaN', 'Infinity', '-Infinity')"
    assert run.metric(customers, "score", "nan_count") == scalar(
        admin, "SELECT count(*) FROM analytics.customers WHERE score = 'NaN'"
    )
    assert run.metric(customers, "score", "positive_infinity_count") == scalar(
        admin, "SELECT count(*) FROM analytics.customers WHERE score = 'Infinity'"
    )
    assert run.metric(customers, "score", "max") == scalar(
        admin, f"SELECT max(score) FROM analytics.customers WHERE {finite}"
    )
    assert run.metric(customers, "score", "mean") == pytest.approx(
        scalar(admin, f"SELECT avg(score) FROM analytics.customers WHERE {finite}")
    )
    quantiles = run.metric(customers, "score", "quantiles")
    expected = scalar(
        admin,
        "SELECT percentile_disc(ARRAY[0.05, 0.25, 0.5, 0.75, 0.95]) WITHIN GROUP (ORDER BY score) "
        f"FROM analytics.customers WHERE {finite}",
    )
    assert [item["value"] for item in quantiles] == expected
    assert run.metric(customers, "lifetime_value", "max") == str(
        scalar(admin, "SELECT max(lifetime_value) FROM analytics.customers")
    )
    mean = scalar(admin, "SELECT round(avg(lifetime_value), 6) FROM analytics.customers")
    assert Decimal(run.metric(customers, "lifetime_value", "mean")) == mean
    assert run.metric(customers, "is_active", "true_count") == scalar(
        admin, "SELECT count(*) FROM analytics.customers WHERE is_active"
    )
    assert run.metric(customers, "currency", "all_values_equal") is True
    assert run.metric(customers, "display name", "max_length") == scalar(
        admin, 'SELECT max(char_length("display name")) FROM analytics.customers'
    )
    signup_min = scalar(admin, "SELECT min(signup_date)::text FROM analytics.customers")
    assert run.metric(customers, "signup_date", "min") == signup_min

    orders = "analytics.orders"
    assert run.table(orders)["table_metrics"][0]["value"] == scalar(
        admin, "SELECT count(*) FROM analytics.orders WHERE order_ts >= '2025-01-01T00:00:00Z'"
    )
    assert run.metric(orders, "order_ts", "min") == "2025-01-01T00:00:00.000000Z"
    assert run.metric(orders, "tags", "empty_count") == scalar(
        admin, "SELECT count(*) FROM analytics.orders WHERE cardinality(tags) = 0"
    )
    assert run.metric(orders, "tags", "null_element_count") == scalar(
        admin, "SELECT count(*) FROM analytics.orders, unnest(tags) AS t(v) WHERE v IS NULL"
    )
    assert run.metric(orders, "shipping", "null_count") == 40
    checks = {check["check_id"]: check["status"] for check in run.table(orders)["quality_checks"]}
    assert checks["orders_amount_range"] == "fail"

    events = "analytics.order_events"
    sample = run.table(events)["sample"]
    assert sample["values_truncated"] == 5 and sample["biased"] is True
    assert run.metric(events, "created_at_local", "max") == scalar(
        admin,
        "SELECT to_char(max(created_at_local), 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM analytics.order_events",
    )


def test_type_coverage(profile_run, admin):
    run = profile_run({"tables": ["td_types.coverage"], "analysis_level": "standard"})
    assert run.code == 0, run.stderr
    kinds = {
        f["path"][0]["name"]: f["type_kind"]
        for f in run.table("td_types.coverage")["field_profiles"]
        if len(f["path"]) == 1
    }
    assert kinds == {
        "id": "integer",
        "i": "integer",
        "f": "float",
        "r": "float",
        "n": "decimal",
        "nu": "decimal",
        "p": "decimal",
        "tx": "string",
        "vc": "string",
        "ch": "string",
        "u": "string",
        "m": "string",
        "bo": "boolean",
        "d": "date",
        "ts": "timestamp",
        "tn": "timestamp_ntz",
        "iv": "interval",
        "by": "binary",
        "j": "variant",
        "js": "variant",
        "ar": "array",
        "co": "other",
        "ip": "other",
    }
    table = "td_types.coverage"
    assert run.metric(table, "f", "nan_count") == 1
    assert run.metric(table, "f", "negative_infinity_count") == 1
    assert run.metric(table, "f", "min") == -1e10
    assert run.metric(table, "nu", "max") == "NaN"
    assert run.metric(table, "p", "max") == "3.30"
    assert run.metric(table, "u", "min_length") == 36
    assert run.metric(table, "m", "approx_distinct_count") == 3
    assert run.metric(table, "by", "max_length") == 2
    assert run.metric(table, "ar", "null_element_count") == 2
    assert run.metric(table, "ar", "max_size") == 4
    assert run.metric(table, "co", "null_count") == 1  # ROW(NULL, NULL) is a value, not NULL
    assert run.metric(table, "d", "min") == "error"  # 44 BC is outside Python's date range
    assert run.metric(table, "d", "max") == "2026-12-31"
    assert run.metric(table, "ts", "max") == "2027-01-01T00:00:00.000000Z"
    assert run.metric(table, "ch", "empty_count") == 1
    assert run.metric(table, "tx", "whitespace_only_count") == 1
    assert run.table(table)["status"] == "succeeded"


def test_sampled_values_never_leave_the_process(profile_run):
    run = profile_run({"tables": ["td_types.markers"]})
    assert run.code == 0, run.stderr
    assert run.table("td_types.markers")["sample"]["rows_collected"] == 50
    text = run.package_text() + run.stdout + run.stderr
    assert "SECRET-MARKER" not in text and "SECRET-JSON-MARKER" not in text


def test_metadata_level_reads_no_rows(profile_run):
    run = profile_run(demo(analysis_level="metadata"))
    assert run.code == 0, run.stderr
    for table in run.profile["tables"]:
        assert all(not op["reads_user_data"] for op in table["operations"]["planned"])
        assert table["consistency"]["mode"] == "metadata_only"
    count = run.table("analytics.customers")["table_metrics"][0]
    assert count["value"] == 500 and count["accuracy"] == "as_recorded"
    assert count["source"] == "table_statistics"


def test_declared_constraints_from_pg_catalog(profile_run):
    run = profile_run(demo(tables=["analytics.orders", "analytics.customers"]))
    orders = run.table("analytics.orders")
    fk = next(c for c in orders["constraints"] if c["name"] == "orders_customer_fk")
    assert fk["source"] == "system_catalog" and fk["enforcement"] == "enforced"
    assert fk["referenced"] == {"table": "analytics.customers", "columns": ["customer_id"]}
    assert any("NOT VALID" in note for note in orders["notes"])
    pk = next(c for c in orders["constraints"] if c["constraint_type"] == "primary_key")
    assert pk["columns"] == ["order_id"]
    erd = (run.run_dir / "erd.mmd").read_text()
    assert "analytics_customers is referenced but was not profiled" not in erd
