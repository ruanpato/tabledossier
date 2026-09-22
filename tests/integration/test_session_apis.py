"""Session APIs the runtime relies on, exercised for real in classic and Spark Connect modes.

Databricks shared access mode and serverless compute use Spark Connect. Run this module (and the
whole integration suite) with ``TD_TEST_SPARK_MODE=connect`` to execute every call through a local
Spark Connect server; nothing here is mocked.
"""

import pytest
from run_notebook_locally import is_connect_session

from tabledossier.config import default_config
from tabledossier.planning import build_schema_tree, plan_sample
from tabledossier.runtime.spark import (
    collect_sample,
    detect_capabilities,
    neutral_fields,
    query_key_constraints,
    read_describe_detail,
    read_describe_extended,
    spark_environment,
)


def test_session_flavour_is_the_requested_one(spark, spark_mode):
    assert is_connect_session(spark) is (spark_mode == "connect")


def test_environment_records_spark_connect_and_configuration(spark, spark_mode):
    env = spark_environment(spark)
    # The recorded value is observed from the session object, not assumed.
    assert env["spark_connect"] is (spark_mode == "connect")
    assert env["session_timezone"] == "UTC"  # spark.conf.get through the session in use
    assert isinstance(env["ansi_mode"], bool)
    assert env["spark_version"] == spark.version


def test_capability_detection_uses_the_catalog(spark):
    capabilities = detect_capabilities(spark)
    detail = capabilities["try_parse_json"]["detail"]
    assert "functionExists('try_parse_json') = unknown" not in detail, detail
    major = int(spark.version.split(".")[0])
    assert capabilities["try_parse_json"]["available"] is (major >= 4)
    assert capabilities["parameterized_sql"]["available"] is True


def test_describe_statements_and_row_conversion(spark, demo_tables, delta_enabled):
    extended = read_describe_extended(spark, "`analytics`.`orders`")
    assert extended["Type"] in ("MANAGED", "EXTERNAL")
    assert extended.get("Comment") == "Synthetic orders with nested structs, arrays and maps"
    provider = (extended.get("Provider") or "").lower()
    if delta_enabled:
        # Delta's DESCRIBE DETAIL also describes other formats; rows arrive through Row.asDict.
        detail = read_describe_detail(spark, "`analytics`.`orders`")
        assert detail["format"] == provider
        assert "numFiles" in detail and "sizeInBytes" in detail  # None for non-Delta formats
        assert detail["check_constraints"] == {}
    else:
        with pytest.raises(Exception):  # noqa: B017 - DESCRIBE DETAIL is Delta syntax
            read_describe_detail(spark, "`analytics`.`orders`")


def test_sample_iterates_rows_with_the_session_in_use(spark, demo_tables):
    config = default_config()
    frame = spark.table("analytics.order_events")
    tree = build_schema_tree(neutral_fields(frame.schema), max_depth=3, max_fields=200)
    strings = [node for node in tree["fields"] if node["type"]["kind"] == "string"]
    plan = plan_sample(strings, config)
    plan["max_rows"] = 50
    sample = collect_sample(frame, strings, plan)
    assert sample["rows"] == 50 and sample["stopped_reason"] == "row_limit"
    payload = sample["fields"][next(n["field_id"] for n in strings if n["name"] == "payload")]
    assert payload["truncated"] == 0
    assert len(payload["values"]) + payload["nulls"] == 50


def test_parameterized_constraint_query(spark):
    """The information_schema query and its bound parameters run for real on synthetic views.

    Unity Catalog is not available locally, so the same query runs against tables with the
    information_schema column layout in a local database.
    """
    rows = {
        "table_constraints": (
            "('demo', 'analytics', 'orders_pk', 'analytics', 'orders', 'PRIMARY KEY'), "
            "('demo', 'analytics', 'orders_customer_fk', 'analytics', 'orders', 'FOREIGN KEY'), "
            "('demo', 'analytics', 'customers_pk', 'analytics', 'customers', 'PRIMARY KEY'), "
            "('demo', 'analytics', 'orders_amount_ck', 'analytics', 'orders', 'CHECK') "
            "AS t(constraint_catalog, constraint_schema, constraint_name, table_schema, "
            "table_name, constraint_type)"
        ),
        "key_column_usage": (
            "('demo', 'analytics', 'orders_pk', 'demo', 'analytics', 'orders', 'order_id', 1, "
            "CAST(NULL AS INT)), "
            "('demo', 'analytics', 'orders_customer_fk', 'demo', 'analytics', 'orders', "
            "'customer_id', 1, 1), "
            "('demo', 'analytics', 'customers_pk', 'demo', 'analytics', 'customers', "
            "'customer_id', 1, CAST(NULL AS INT)) "
            "AS t(constraint_catalog, constraint_schema, constraint_name, table_catalog, "
            "table_schema, table_name, column_name, ordinal_position, "
            "position_in_unique_constraint)"
        ),
        "referential_constraints": (
            "('demo', 'analytics', 'orders_customer_fk', 'demo', 'analytics', 'customers_pk') "
            "AS t(constraint_catalog, constraint_schema, constraint_name, "
            "unique_constraint_catalog, unique_constraint_schema, unique_constraint_name)"
        ),
    }
    spark.sql("CREATE DATABASE IF NOT EXISTS td_info")
    for name, values in rows.items():
        spark.sql(f"DROP TABLE IF EXISTS td_info.{name}")
        spark.sql(f"CREATE TABLE td_info.{name} AS SELECT * FROM VALUES {values}")

    constraints = query_key_constraints(
        spark, "demo", "ANALYTICS", "Orders", lambda catalog: "`td_info`"
    )
    by_name = {item["name"]: item for item in constraints}
    assert set(by_name) == {"orders_pk", "orders_customer_fk"}  # CHECK rows are ignored
    assert by_name["orders_pk"]["constraint_type"] == "primary_key"
    assert by_name["orders_pk"]["columns"] == ["order_id"]
    fk = by_name["orders_customer_fk"]
    assert fk["constraint_type"] == "foreign_key"
    assert fk["referenced"] == {"table": "demo.analytics.customers", "columns": ["customer_id"]}
    assert all(item["enforcement"] == "not_enforced" for item in constraints)

    # A hostile table name is a bound value: it matches nothing and changes no statement.
    assert (
        query_key_constraints(
            spark, "demo", "analytics", "orders' OR '1'='1", lambda catalog: "`td_info`"
        )
        == []
    )
