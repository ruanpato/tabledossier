"""Deep level on real Spark (classic or Connect), checked against independent Spark SQL.

The runtime computes element metrics with higher-order functions and no explode; the ground truth
below explodes the collections (or parses JSON with ``from_json``) in plain Spark SQL instead.
"""

import json
from decimal import Decimal

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.jsonutil import format_utc, utc_now
from tabledossier.metrics import find_metric, metric_value
from tabledossier.package import build_documents
from tabledossier.runtime.spark import profile_table


def _profile(spark, name, capabilities, **overrides):
    config = deep_merge(default_config(), {"analysis_level": "deep", **overrides})
    return profile_table(
        spark,
        name,
        config,
        capabilities=capabilities,
        reference_time=format_utc(utc_now()),
        now=lambda: format_utc(utc_now()),
        log=lambda message: None,
    )


def _field(table, path):
    return next(f for f in table["field_profiles"] if f["display_path"] == path)


def _value(table, path, metric):
    return metric_value(_field(table, path)["metrics"], metric)


def _truth(spark, sql):
    return spark.sql(sql).collect()[0][0]


def _row(spark, sql):
    return spark.sql(sql).collect()[0]


ITEMS = "(SELECT explode(items) AS it FROM analytics.orders)"
ATTRS = "(SELECT explode(attributes) AS (k, v) FROM analytics.orders)"
TAGS = "(SELECT explode(tags) AS tag FROM analytics.orders)"


def _reads(table):
    return [op for op in table["operations"]["planned"] if op["reads_user_data"]]


@pytest.fixture(scope="module")
def orders(spark, demo_tables, capabilities):
    return _profile(spark, "analytics.orders", capabilities)


@pytest.fixture(scope="module")
def events(spark, demo_tables, capabilities):
    return _profile(spark, "analytics.order_events", capabilities)


def test_array_element_metrics_match_exploded_ground_truth(spark, orders):
    assert orders["status"] == "succeeded", orders["errors"]
    total = _truth(spark, f"SELECT count(*) FROM {ITEMS}")
    for path in ("items[]", "items[].sku", "items[].qty", "items[].price"):
        assert _value(orders, path, "element_count") == total, path
    assert _value(orders, "items[]", "null_count") == _truth(
        spark, f"SELECT count_if(it IS NULL) FROM {ITEMS}"
    )
    truth = _row(
        spark,
        f"SELECT count_if(it.qty IS NULL), min(it.qty), max(it.qty), count_if(it.qty > 0), "
        f"count_if(it IS NOT NULL AND it.qty IS NULL) FROM {ITEMS}",
    )
    assert _value(orders, "items[].qty", "null_count") == truth[0]
    assert _value(orders, "items[].qty", "min") == truth[1]
    assert _value(orders, "items[].qty", "max") == truth[2]
    assert _value(orders, "items[].qty", "positive_count") == truth[3]
    assert _value(orders, "items[].qty", "null_count_parent_present") == truth[4]
    price = _row(spark, f"SELECT min(it.price), max(it.price), count_if(it.price = 0) FROM {ITEMS}")
    assert Decimal(_value(orders, "items[].price", "min")) == price[0]
    assert Decimal(_value(orders, "items[].price", "max")) == price[1]
    assert _value(orders, "items[].price", "zero_count") == price[2]
    sku = _row(
        spark,
        f"SELECT min(length(it.sku)), max(length(it.sku)), count_if(it.sku = '') FROM {ITEMS}",
    )
    assert _value(orders, "items[].sku", "min_length") == sku[0]
    assert _value(orders, "items[].sku", "max_length") == sku[1]
    assert _value(orders, "items[].sku", "empty_count") == sku[2]
    tags = _row(spark, f"SELECT count(*), count_if(tag IS NULL) FROM {TAGS}")
    assert _value(orders, "tags[]", "element_count") == tags[0]
    assert _value(orders, "tags[]", "null_count") == tags[1]


def test_map_entry_metrics_match_exploded_ground_truth(spark, orders):
    truth = _row(
        spark,
        f"SELECT count(*), count_if(v IS NULL), max(length(k)), min(length(v)) FROM {ATTRS}",
    )
    assert _value(orders, "attributes{key}", "element_count") == truth[0]
    assert _value(orders, "attributes{value}", "null_count") == truth[1]
    assert _value(orders, "attributes{key}", "max_length") == truth[2]
    assert _value(orders, "attributes{value}", "min_length") == truth[3]
    assert _value(orders, "attributes", "total_entry_count") == truth[0]


def test_element_denominators_are_elements_or_entries(orders):
    elements = [f for f in orders["field_profiles"] if f.get("element_context")]
    assert {f["display_path"] for f in elements} == {
        "items[]",
        "items[].sku",
        "items[].qty",
        "items[].price",
        "attributes{key}",
        "attributes{value}",
        "tags[]",
    }
    for field in elements:
        unit = field["element_context"]["unit"]
        assert field["profiled"] and field["omission_reason"] is None
        assert unit == ("entries" if field["display_path"].startswith("attributes") else "elements")
        for metric in field["metrics"]:
            assert "rows" not in (metric.get("unit"), metric.get("denominator_unit")), (
                field["display_path"],
                metric["name"],
            )
            if metric.get("denominator_unit") in ("elements", "entries"):
                assert metric["denominator_unit"] == unit
        null_count = find_metric(field["metrics"], "null_count")
        assert null_count["denominator"] == metric_value(field["metrics"], "element_count")
    assert not any(o["reason"] == "inside_collection" for o in orders["omissions"])


def test_element_distinct_counts_are_labelled_as_sample(spark, orders):
    # 1,000 rows fit max_explode_rows = 1,000, so the bounded sample reads every element here;
    # the metric is still labelled as a sample because that is how it was planned.
    metric = find_metric(_field(orders, "items[].sku")["metrics"], "distinct_count")
    assert metric["scope"] == "sample" and metric["source"] == "sample"
    assert metric["accuracy"] == "exact"
    assert metric["denominator_unit"] == "elements"
    assert metric["value"] == _truth(spark, f"SELECT count(DISTINCT it.sku) FROM {ITEMS}")
    assert metric["details"]["stopped_by_element_limit"] is False
    assert _value(orders, "attributes{key}", "distinct_count") == _truth(
        spark, f"SELECT count(DISTINCT k) FROM {ATTRS}"
    )
    op = next(o for o in orders["operations"]["planned"] if o["operation_id"] == "op_deep_elements")
    assert op["kind"] == "element_explode_pass" and op["reads_user_data"] is True
    assert orders["deep"]["element_distinct"]["status"] == "measured"


def test_full_scope_element_distinct_within_the_element_budget(spark, demo_tables, capabilities):
    table = _profile(
        spark, "analytics.orders", capabilities, deep={"element_distinct": "full_scope"}
    )
    metric = find_metric(_field(table, "items[].price")["metrics"], "distinct_count")
    assert metric["scope"] == "full_table" and metric["source"] == "aggregate"
    assert metric["value"] == _truth(spark, f"SELECT count(DISTINCT it.price) FROM {ITEMS}")
    too_small = _profile(
        spark,
        "analytics.orders",
        capabilities,
        deep={"element_distinct": "full_scope", "max_elements": 100},
    )
    skipped = find_metric(_field(too_small, "items[].price")["metrics"], "distinct_count")
    assert skipped["status"] == "not_computed" and "deep.max_elements" in skipped["reason"]
    observed = {o["operation_id"]: o["status"] for o in too_small["operations"]["observed"]}
    assert observed["op_deep_elements"] == "skipped"
    assert any(item["reason"] == "element_budget" for item in too_small["deep"]["limited"])


def test_explode_sample_stops_at_the_element_budget(spark, demo_tables, capabilities):
    table = _profile(
        spark, "analytics.orders", capabilities, deep={"max_explode_rows": 20, "max_elements": 7}
    )
    record = table["deep"]["element_distinct"]
    assert record["elements_examined"] == 7 and record["stopped_reason"] == "element_limit"
    metric = find_metric(_field(table, "items[].sku")["metrics"], "distinct_count")
    assert metric["details"]["stopped_by_element_limit"] is True
    assert metric["details"]["elements_examined"] <= 7
    assert any(item["reason"] == "element_budget" for item in table["deep"]["limited"])


@pytest.mark.parametrize(
    ("limits", "extra"),
    [
        ({"max_expressions_per_pass": 800, "max_aggregate_passes": 2}, 2),
        ({"max_expressions_per_pass": 40, "max_aggregate_passes": 1}, 0),
        ({"max_expressions_per_pass": 40, "max_aggregate_passes": 1}, 1),
        ({"max_expressions_per_pass": 20, "max_aggregate_passes": 2}, 3),
    ],
)
def test_planned_reads_respect_the_budgets(spark, demo_tables, capabilities, limits, extra):
    table = _profile(
        spark, "analytics.orders", capabilities, limits=limits, deep={"max_extra_passes": extra}
    )
    assert table["status"] == "succeeded", table["errors"]
    reads = _reads(table)
    assert len(reads) <= 1 + limits["max_aggregate_passes"] + extra
    deep_ops = [op for op in reads if op["kind"] in ("deep_aggregate_pass", "element_explode_pass")]
    assert len(deep_ops) == table["deep"]["extra_passes"]["planned"] <= extra
    standard = [op for op in reads if op["kind"] == "aggregate_pass"]
    assert len(standard) <= limits["max_aggregate_passes"]
    for op in reads:
        if op["kind"] != "sample_collect" and op["kind"] != "element_explode_pass":
            assert op["details"]["expressions"] <= limits["max_expressions_per_pass"]
    observed = {o["operation_id"] for o in table["operations"]["observed"]}
    assert {op["operation_id"] for op in reads} <= observed
    if extra == 0:
        assert any(o["reason"] == "deep_budget" for o in table["omissions"])
        assert table["deep"]["expressions"]["omitted"] > 0
        statuses = {m["status"] for f in table["field_profiles"] for m in f["metrics"]}
        assert "not_computed" in statuses
        assert table["deep"]["element_distinct"]["status"] == "not_computed"
        assert any(item["reason"] == "deep_budget" for item in table["deep"]["limited"])


def test_more_collections_do_not_add_spark_actions(spark, capabilities):
    spark.sql("DROP TABLE IF EXISTS analytics.wide_arrays")
    spark.sql(
        "CREATE TABLE analytics.wide_arrays AS SELECT id, "
        + ", ".join(
            f"transform(sequence(0, CAST(id % 4 AS INT)), i -> i * {n}) AS a{n}" for n in range(8)
        )
        + ", map('k', id) AS m FROM range(200)"
    )
    narrow = _profile(
        spark,
        "analytics.wide_arrays",
        capabilities,
        table_options={"analytics.wide_arrays": {"columns": ["id", "a0"]}},
    )
    wide = _profile(spark, "analytics.wide_arrays", capabilities)
    assert len(_reads(wide)) == len(_reads(narrow)), "one aggregation and one explode pass"
    assert _value(wide, "a7[]", "max") == _truth(
        spark, "SELECT max(x) FROM (SELECT explode(a7) AS x FROM analytics.wide_arrays)"
    )
    assert _value(wide, "m{value}", "element_count") == 200


def test_explicit_targets_limit_the_work(spark, demo_tables, capabilities):
    table = _profile(
        spark,
        "analytics.orders",
        capabilities,
        deep={
            "targets": [
                {"table": "analytics.orders", "column": "tags"},
                {"table": "analytics.orders", "column": "amount"},
            ]
        },
    )
    profiled = {f["display_path"] for f in table["field_profiles"] if f.get("element_context")}
    assert profiled == {"tags[]"}
    reasons = {o["display_path"]: o["reason"] for o in table["omissions"]}
    assert reasons["items[]"] == "deep_not_selected"
    assert table["deep"]["not_eligible"][0]["display_path"] == "amount"


def _payload_kinds():
    def kind(i):
        if i % 50 == 0:
            return "invalid"
        if i % 45 == 0:
            return "json_null_literal"
        if i % 40 == 0:
            return "array"
        if i % 333 == 0:
            return "bulk"
        if i % 30 == 0:
            return "sql_null"
        return "object"

    counts: dict[str, int] = {}
    for i in range(2000):
        counts[kind(i)] = counts.get(kind(i), 0) + 1
    return counts


def test_json_path_catalogue_and_full_scope_validation(spark, events, capabilities):
    payload = _field(events, "payload")
    catalog = payload["json_paths"]
    kinds = _payload_kinds()
    sample = events["sample"]
    assert (
        catalog["scope"] == "sample"
        and catalog["eligible_observations"] <= sample["rows_collected"]
    )
    # Truncated (bulk) values are excluded from the sample catalogue.
    assert catalog["documents"] == kinds["object"] + kinds["array"]
    paths = {item["path"]: item for item in catalog["paths"]}
    assert set(paths) == {"$.status", "$.amount", "$.channel", "$[*]"}
    assert paths["$.status"]["present_in"] == kinds["object"]
    assert paths["$.amount"]["types"] == {"number": kinds["object"]}
    assert paths["$[*]"]["full_scope"]["status"] == "not_computed"
    full = catalog["full_scope"]
    truth_documents = kinds["object"] + kinds["array"] + kinds["bulk"]
    assert full["documents"]["value"] == truth_documents
    status_truth = _truth(
        spark,
        "SELECT count_if(from_json(payload, 'status STRING').status IS NOT NULL) "
        "FROM analytics.order_events",
    )
    metrics = {m["name"]: m for m in paths["$.status"]["full_scope"]["metrics"]}
    if capabilities["variant_functions"]["available"]:
        assert full["method"] == "variant"
        assert metrics["path_present_count"]["value"] == status_truth
        assert metrics["path_json_null_count"]["value"] == 0
        assert metrics["path_type_match_count"]["value"] == status_truth
        amount = {m["name"]: m for m in paths["$.amount"]["full_scope"]["metrics"]}
        assert amount["path_type_match_count"]["details"]["expected_type"] == "number"
        assert amount["path_type_match_count"]["value"] == kinds["object"]
    else:
        assert full["method"] == "get_json_object"
        assert metrics["path_non_null_count"]["value"] == status_truth
        assert "path_json_null_count" not in metrics
        assert any("JSON null" in note for note in full["limitations"])
    for metric in metrics.values():
        assert metric["scope"] == "full_table" and metric["denominator"] == truth_documents
    field_names = {m["name"] for m in payload["metrics"]}
    assert not field_names & {"path_present_count", "path_non_null_count", "json_documents"}


def test_json_validation_can_be_disabled(spark, demo_tables, capabilities):
    table = _profile(
        spark, "analytics.order_events", capabilities, deep={"json_full_scope_validation": False}
    )
    catalog = _field(table, "payload")["json_paths"]
    assert catalog["paths"] and catalog["full_scope"]["status"] == "not_computed"
    assert all(item["full_scope"]["status"] == "not_computed" for item in catalog["paths"])
    assert table["deep"]["expressions"]["planned"] == 0


def test_deep_never_leaks_sampled_values(spark, demo_tables, capabilities):
    spark.sql("DROP TABLE IF EXISTS analytics.deep_secrets")
    spark.sql(
        "CREATE TABLE analytics.deep_secrets AS SELECT id, "
        "array(concat('SECRET-MARKER-', id), 'x') AS words, "
        "map(concat('SECRET-KEY-', id), concat('SECRET-VALUE-', id)) AS attrs, "
        "array(named_struct('code', concat('SECRET-CODE-', id))) AS records, "
        "to_json(named_struct('kind', 'event', 'token', concat('SECRET-MARKER-', id), "
        "'by_user', map(concat('SECRET-JSONKEY-', id), 1), "
        "'SECRET_CONSTANT_KEY', 1)) AS doc "
        "FROM range(100)"
    )
    table = _profile(spark, "analytics.deep_secrets", capabilities)
    text = json.dumps(table) + "".join(build_documents_for(table))
    for marker in ("SECRET-MARKER", "SECRET-KEY", "SECRET-VALUE", "SECRET-CODE", "SECRET-JSONKEY"):
        assert marker not in text, marker
    catalog = _field(table, "doc")["json_paths"]
    assert "$.by_user.*" in {item["path"] for item in catalog["paths"]}
    assert _value(table, "words[]", "max_length") == len("SECRET-MARKER-99")
    redacted = _profile(
        spark, "analytics.deep_secrets", capabilities, value_policy={"json_key_names": "redact"}
    )
    assert "SECRET_CONSTANT_KEY" not in json.dumps(redacted)
    assert _field(redacted, "doc")["json_paths"]["paths"] is None


def build_documents_for(table):
    """Render the documents of a one-table profile (the renderer must not leak values either)."""
    from tabledossier.assemble import build_profile

    config = deep_merge(default_config(), {"analysis_level": "deep"})
    profile = build_profile(
        run_id="20260101T000000Z-00000000",
        started_at="2026-01-01T00:00:00.000Z",
        finished_at="2026-01-01T00:00:01.000Z",
        duration_ms=1000,
        reference_time="2026-01-01T00:00:00.000Z",
        environment={"engine": "spark", "execution_context": "spark", "python_version": "3"},
        config=config,
        parameter_sources={},
        generation={"generator_version": None, "generation_id": None},
        capabilities={},
        tables=[table],
    )
    return build_documents(profile).values()


def test_standard_metrics_are_unchanged_at_the_deep_level(spark, demo_tables, capabilities):
    config = deep_merge(default_config(), {"analysis_level": "standard"})
    standard = profile_table(
        spark,
        "analytics.orders",
        config,
        capabilities=capabilities,
        reference_time="2026-01-01T00:00:00.000Z",
        now=lambda: format_utc(utc_now()),
        log=lambda message: None,
    )
    deep = _profile(spark, "analytics.orders", capabilities)
    assert standard["deep"] is None
    assert not any(f.get("element_context") for f in standard["field_profiles"])
    assert not any(
        op["kind"].startswith(("deep", "element")) for op in standard["operations"]["planned"]
    )

    def values(table):
        return {
            (f["display_path"], m["name"]): (m["status"], m["value"])
            for f in table["field_profiles"]
            if not f.get("element_context")
            for m in f["metrics"]
            if m["name"] != "after_reference_count"
        }

    assert values(standard) == values(deep)
    assert metric_value(standard["table_metrics"], "row_count") == metric_value(
        deep["table_metrics"], "row_count"
    )
