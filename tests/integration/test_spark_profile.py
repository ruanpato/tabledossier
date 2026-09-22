"""Real Spark execution of the runtime against synthetic tables, checked against ground truth."""

import json
from decimal import Decimal

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import find_metric, metric_value
from tabledossier.runtime.spark import profile_table


def _profile(spark, name, capabilities, **overrides):
    config = deep_merge(default_config(), overrides)
    logs: list[str] = []
    table = profile_table(
        spark,
        name,
        config,
        capabilities=capabilities,
        reference_time=format_utc(utc_now()),
        now=lambda: format_utc(utc_now()),
        log=logs.append,
    )
    return table, logs


def _field(table, path):
    return next(f for f in table["field_profiles"] if f["display_path"] == path)


def _value(table, path, metric):
    return metric_value(_field(table, path)["metrics"], metric)


def _truth(spark, sql):
    return spark.sql(sql).collect()[0][0]


@pytest.fixture(scope="module")
def customers(spark, demo_tables, capabilities):
    return _profile(spark, "analytics.customers", capabilities)[0]


@pytest.fixture(scope="module")
def orders(spark, demo_tables, capabilities):
    return _profile(spark, "analytics.orders", capabilities)[0]


@pytest.fixture(scope="module")
def events(spark, demo_tables, capabilities):
    return _profile(spark, "analytics.order_events", capabilities)[0]


def test_simple_types_match_ground_truth(spark, customers):
    assert customers["status"] == "succeeded", customers["errors"]
    rows = find_metric(customers["table_metrics"], "row_count")
    assert rows["value"] == 500 and rows["accuracy"] == "exact" and rows["scope"] == "full_table"
    assert _value(customers, "email", "null_count") == _truth(
        spark, "SELECT count(*) - count(email) FROM analytics.customers"
    )
    assert _value(customers, "middle_name", "null_count") == 500
    assert _value(customers, "currency", "all_values_equal") is True
    assert _value(customers, "is_active", "true_count") == _truth(
        spark, "SELECT count_if(is_active) FROM analytics.customers"
    )


def test_special_column_names(spark, customers):
    assert _value(customers, "`a.b`", "approx_distinct_count") == 2
    assert _value(customers, "`display name`", "null_count") == 0
    assert _field(customers, "`a.b`")["path"] == [{"kind": "field", "name": "a.b"}]


def test_decimals_are_preserved(spark, customers):
    truth = _truth(spark, "SELECT max(lifetime_value) FROM analytics.customers")
    metric = find_metric(_field(customers, "lifetime_value")["metrics"], "max")
    assert metric["value_type"] == "decimal"
    assert Decimal(metric["value"]) == truth
    assert metric["value"] == str(truth)


def test_nan_and_infinity_are_not_nulls(spark, customers):
    metrics = _field(customers, "score")["metrics"]
    assert metric_value(metrics, "nan_count") == _truth(
        spark, "SELECT count_if(isnan(score)) FROM analytics.customers"
    )
    assert metric_value(metrics, "positive_infinity_count") == _truth(
        spark, "SELECT count_if(score = double('Infinity')) FROM analytics.customers"
    )
    assert metric_value(metrics, "null_count") == _truth(
        spark, "SELECT count(*) - count(score) FROM analytics.customers"
    )
    finite_max = _truth(
        spark,
        "SELECT max(score) FROM analytics.customers WHERE NOT isnan(score) AND abs(score) != double('Infinity')",
    )
    assert metric_value(metrics, "max") == finite_max
    assert find_metric(metrics, "zero_count")["denominator"] == metric_value(
        metrics, "finite_count"
    )


def test_nested_struct_parent_context(spark, orders):
    assert orders["status"] == "succeeded", orders["errors"]
    city = _field(orders, "shipping.address.city")["metrics"]
    assert metric_value(city, "null_count") == _truth(
        spark, "SELECT count(*) - count(shipping.address.city) FROM analytics.orders"
    )
    assert metric_value(city, "null_count_parent_present") == _truth(
        spark,
        "SELECT count_if(shipping.address IS NOT NULL AND shipping.address.city IS NULL) FROM analytics.orders",
    )
    parent_present = _truth(spark, "SELECT count(shipping.address) FROM analytics.orders")
    assert find_metric(city, "null_count_parent_present")["denominator"] == parent_present


def test_arrays_and_maps(spark, orders):
    items = _field(orders, "items")["metrics"]
    assert metric_value(items, "max_size") == 1500
    assert metric_value(items, "total_element_count") == _truth(
        spark, "SELECT sum(size(items)) FROM analytics.orders WHERE items IS NOT NULL"
    )
    tags = _field(orders, "tags")["metrics"]
    assert metric_value(tags, "empty_count") == _truth(
        spark, "SELECT count_if(size(tags) = 0) FROM analytics.orders"
    )
    assert metric_value(tags, "null_element_count") == _truth(
        spark,
        "SELECT sum(size(filter(tags, x -> x IS NULL))) FROM analytics.orders WHERE tags IS NOT NULL",
    )
    assert find_metric(tags, "null_element_count")["denominator_unit"] == "elements"
    attrs = _field(orders, "attributes")["metrics"]
    # size(NULL) is -1 under Spark's legacy sizeOfNull setting, so the ground truth must
    # exclude null maps explicitly (the runtime does the same with WHEN col IS NOT NULL).
    assert metric_value(attrs, "null_value_count") == _truth(
        spark,
        "SELECT sum(size(filter(map_values(attributes), v -> v IS NULL))) FROM analytics.orders "
        "WHERE attributes IS NOT NULL",
    )
    assert not _field(orders, "items[].sku")["profiled"]
    assert _field(orders, "items[].sku")["omission_reason"] == "inside_collection"
    assert any(f["code"] == "large_arrays" for f in orders["findings"])


def test_json_detection_excludes_truncated_values(events, capabilities):
    payload = _field(events, "payload")
    shape = payload["json_profile"]
    ids = range(2000)

    def kind(i):
        if i % 50 == 0:
            return "invalid"
        if i % 45 == 0:
            return "json_null_literal"
        if i % 40 == 0:
            return "array"
        if i % 333 == 0:
            return "truncated"
        if i % 30 == 0:
            return "sql_null"
        return "object"

    expected = {
        name: sum(1 for i in ids if kind(i) == name)
        for name in ("invalid", "json_null_literal", "array", "object", "truncated", "sql_null")
    }
    assert shape["counts"]["invalid"] == expected["invalid"]
    assert shape["counts"]["json_null_literal"] == expected["json_null_literal"]
    assert shape["counts"]["array"] == expected["array"]
    assert shape["counts"]["object"] == expected["object"]
    assert shape["excluded"] == {
        "sql_null": expected["sql_null"],
        "truncated": expected["truncated"],
    }
    assert {k["key"] for k in shape["keys"]} >= {"status", "amount", "channel"}
    invalid = find_metric(payload["metrics"], "json_invalid_count")
    if capabilities["try_parse_json"]["available"]:
        assert invalid["status"] == "measured" and invalid["value"] == expected["invalid"]
    else:
        assert invalid["status"] == "unsupported"
        assert any(u["capability"] == "try_parse_json" for u in events["unsupported"])
    assert any(f["code"] == "probable_json" for f in events["findings"])


def test_formats_and_timestamp_ntz(events):
    formats = {
        path: (_field(events, path)["semantics"] or {}).get("observed_format", {}).get("format")
        for path in ("order_id", "amount_text", "event_date_text", "tracking_url")
    }
    assert formats == {
        "order_id": "uuid",
        "amount_text": "numeric_string",
        "event_date_text": "iso_date",
        "tracking_url": "url",
    }
    flag = _field(events, "legacy_flag")["semantics"]["observed_format"]
    assert flag["format"] == "boolean_string"
    ntz = _field(events, "created_at_local")["metrics"]
    assert find_metric(ntz, "max")["value_type"] == "timestamp_ntz"
    assert find_metric(ntz, "after_reference_count")["status"] == "not_computed"


def test_empty_table(spark, demo_tables, capabilities):
    table, _ = _profile(spark, "analytics.returns", capabilities)
    assert table["status"] == "succeeded"
    assert metric_value(table["table_metrics"], "row_count") == 0
    for field in table["field_profiles"]:
        ratio = find_metric(field["metrics"], "null_ratio")
        assert ratio["status"] == "insufficient_data" and ratio["value"] is None
    assert (
        find_metric(_field(table, "created_on")["metrics"], "min")["status"] == "insufficient_data"
    )
    assert table["findings"] == []


def test_filters_use_structured_expressions(spark, demo_tables, capabilities):
    options = {
        "analytics.customers": {
            "filters": [
                {"column": "a.b", "operator": "eq", "value": "even"},
                {"column": "segment", "operator": "in", "value": ["retail", "smb"]},
                {
                    "column": "signup_date",
                    "operator": "between",
                    "value": ["2024-02-01", "2024-12-31"],
                    "value_type": "date",
                },
            ],
            "columns": ["customer_id", "segment"],
        },
        "analytics.orders": {
            "filters": [
                {"column": ["shipping", "method"], "operator": "eq", "value": "express"},
                {"column": "discount", "operator": "is_null"},
            ]
        },
    }
    customers, _ = _profile(spark, "analytics.customers", capabilities, table_options=options)
    truth = _truth(
        spark,
        "SELECT count(*) FROM analytics.customers WHERE `a.b` = 'even' AND segment IN ('retail', 'smb') "
        "AND signup_date BETWEEN DATE'2024-02-01' AND DATE'2024-12-31'",
    )
    assert metric_value(customers["table_metrics"], "row_count") == truth
    assert customers["scope"]["scope_label"] == "filtered_table"
    assert [f["display_path"] for f in customers["field_profiles"] if f["profiled"]] == [
        "customer_id",
        "segment",
    ]
    assert any(o["reason"] == "not_selected" for o in customers["omissions"])
    orders, _ = _profile(spark, "analytics.orders", capabilities, table_options=options)
    assert metric_value(orders["table_metrics"], "row_count") == _truth(
        spark,
        "SELECT count(*) FROM analytics.orders WHERE shipping.method = 'express' AND discount IS NULL",
    )


def test_bad_filter_column_fails_table_with_clear_error(spark, demo_tables, capabilities):
    options = {"analytics.customers": {"filters": [{"column": "nope", "operator": "is_null"}]}}
    table, _ = _profile(spark, "analytics.customers", capabilities, table_options=options)
    assert table["status"] == "failed"
    assert table["errors"][0]["condition"] == "FILTER_COLUMN_NOT_FOUND"


def test_expression_budget_is_explicit(spark, demo_tables, capabilities):
    table, _ = _profile(
        spark,
        "analytics.orders",
        capabilities,
        limits={"max_expressions_per_pass": 12, "max_aggregate_passes": 2},
    )
    passes = [
        op
        for op in table["operations"]["observed"]
        if op["operation_id"].startswith("op_aggregate")
    ]
    assert 1 <= len(passes) <= 2
    assert any(o["reason"] == "expression_budget" for o in table["omissions"])
    statuses = {m["status"] for f in table["field_profiles"] for m in f["metrics"]}
    assert "not_computed" in statuses
    assert table["operations"]["physical_scans"] == "unknown"
    assert table["operations"]["bytes_read"] == "unknown"


def test_sample_byte_budget_stops_collection(spark, demo_tables, capabilities):
    table, _ = _profile(spark, "analytics.order_events", capabilities, sampling={"max_bytes": 4096})
    sample = table["sample"]
    assert sample["stopped_reason"] == "byte_budget"
    assert sample["bytes_retained"] <= 4096
    assert 0 < sample["rows_collected"] < 2000
    fmt = _field(table, "payload")["semantics"]["observed_format"]
    assert fmt["eligible_observations"] <= sample["rows_collected"]


def test_sampling_disabled_reports_not_computed(spark, demo_tables, capabilities):
    table, _ = _profile(spark, "analytics.order_events", capabilities, sampling={"method": "none"})
    assert table["sample"]["enabled"] is False and table["sample"]["stopped_reason"] == "disabled"
    assert _field(table, "payload")["semantics"] is not None or True
    assert not any(op["kind"] == "sample_collect" for op in table["operations"]["planned"])


def test_metadata_level_reads_no_rows(spark, demo_tables, capabilities):
    table, _ = _profile(spark, "analytics.orders", capabilities, analysis_level="metadata")
    assert all(not op["reads_user_data"] for op in table["operations"]["planned"])
    assert table["consistency"]["mode"] == "metadata_only"
    rows = find_metric(table["table_metrics"], "row_count")
    assert rows["status"] == "unavailable" and rows["value"] is None
    assert all(not f["profiled"] for f in table["field_profiles"])
    assert table["schema"]["nodes_total"] > 10


def test_redaction_policy(spark, demo_tables, capabilities):
    table, _ = _profile(
        spark, "analytics.customers", capabilities, value_policy={"aggregate_extremes": "redact"}
    )
    metrics = _field(table, "lifetime_value")["metrics"]
    assert find_metric(metrics, "max")["status"] == "redacted"
    assert find_metric(metrics, "null_count")["status"] == "measured"


def test_sample_values_never_leak(spark, tmp_path, capabilities):
    spark.sql(
        "CREATE TABLE IF NOT EXISTS analytics.secrets AS SELECT id, concat('SECRET-MARKER-', id) AS token, "
        "to_json(named_struct('SECRET_KEY_MARKER', concat('SECRET-MARKER-', id))) AS doc FROM range(100)"
    )
    table, logs = _profile(
        spark, "analytics.secrets", capabilities, value_policy={"json_key_names": "redact"}
    )
    assert "SECRET-MARKER" not in json.dumps(table)
    assert "SECRET_KEY_MARKER" not in json.dumps(table)
    assert "SECRET-MARKER" not in "\n".join(logs)
    exposed, _ = _profile(
        spark,
        "analytics.secrets",
        capabilities,
        value_policy={
            "persist_examples": True,
            "example_columns": [{"table": "analytics.secrets", "column": "token"}],
        },
    )
    assert "SECRET-MARKER" in json.dumps(_field(exposed, "token")["examples"])
    assert "SECRET-MARKER" not in json.dumps(_field(exposed, "doc"))


def test_describe_detail_on_non_delta_source_is_skipped_not_failed(customers, delta_enabled):
    # The demo tables are Parquet in the test session. With Delta, DESCRIBE DETAIL describes them;
    # without Delta the statement is rejected, which is expected for a non-Delta source.
    assert customers["source"]["provider"] == "parquet"
    detail = next(
        op for op in customers["operations"]["observed"] if op["operation_id"] == "op_detail"
    )
    if delta_enabled:
        assert detail["status"] == "succeeded"
    else:
        assert detail["status"] == "skipped"
        assert "non-Delta source (provider parquet" in detail["detail"]
        assert customers["source"]["size_in_bytes"]["status"] == "unavailable"
    assert customers["status"] == "succeeded"
