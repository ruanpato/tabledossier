"""Engine-neutral runtime helpers shared by the Spark adapter and the PostgreSQL connector.

Everything here runs without Spark: the helpers take plain Python rows, plans and results.
"""

import re
from datetime import datetime, timedelta, timezone

import pytest

from tabledossier import semantic
from tabledossier.assemble import (
    assemble_field_results,
    new_table,
    omission_reason,
    table_field_profiles,
)
from tabledossier.config import default_config
from tabledossier.metrics import not_measured
from tabledossier.package import new_run_id
from tabledossier.planning import (
    aggregate_operations,
    build_schema_tree,
    collect_sample_rows,
    elapsed_ms,
    iter_nodes,
    new_sample_record,
    observed_operation,
    plan_aggregates,
    plan_sample,
    sample_notes,
    select_profile_fields,
)
from tabledossier.semantic import sample_semantics


def t(kind, **extra):
    return {"kind": kind, "physical_type": extra.pop("physical_type", kind), **extra}


def f(name, ntype, nullable=True, comment=None):
    return {"name": name, "type": ntype, "nullable": nullable, "comment": comment}


FIELDS = [
    f("id", t("integer", physical_type="bigint"), nullable=False),
    f("name", t("string")),
    f(
        "customer",
        t(
            "struct",
            fields=[
                f("email", t("string")),
                f("geo", t("struct", fields=[f("lat", t("float"))])),
            ],
        ),
    ),
    f(
        "items",
        t(
            "array",
            element_type=t("struct", fields=[f("sku", t("string")), f("qty", t("integer"))]),
            contains_null=True,
        ),
    ),
]


def tree_and_ids():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    ids = {node["display_path"]: node["field_id"] for node in iter_nodes(tree["fields"])}
    return tree, ids


# --------------------------------------------------------------------------- timing records


def test_observed_operation_records_duration_only_when_started():
    record = observed_operation("op_sample", "failed", None, detail="boom")
    assert record == {
        "operation_id": "op_sample",
        "status": "failed",
        "duration_ms": None,
        "rows_returned": None,
        "detail": "boom",
    }
    started = observed_operation("op_aggregate_1", "succeeded", 0.0, rows=1)
    assert list(started) == list(record)
    assert isinstance(started["duration_ms"], int) and started["duration_ms"] >= 0
    assert started["rows_returned"] == 1


def test_elapsed_ms_is_never_negative():
    import time

    assert elapsed_ms(time.perf_counter() + 60) == 0
    assert elapsed_ms(time.perf_counter() - 1.5) >= 1500


# --------------------------------------------------------------------------- sample loop


def plan_of(max_rows, max_bytes):
    return {"max_rows": max_rows, "max_bytes": max_bytes}


def test_collect_sample_rows_reaches_the_row_limit():
    rows = [
        ("ab", False, None, False),
        ("c", False, "xyz", True),
        (None, False, "d", False),
    ]
    sample = collect_sample_rows(rows, ["f1", "f2"], plan_of(3, 1000))
    assert sample == {
        "rows": 3,
        "bytes": 4,  # "ab" + "c" + "d"; the truncated "xyz" is counted, not charged
        "stopped_reason": "row_limit",
        "fields": {
            "f1": {"values": ["ab", "c"], "nulls": 1, "truncated": 0},
            "f2": {"values": ["d"], "nulls": 1, "truncated": 1},
        },
    }


def test_collect_sample_rows_exhausts_a_short_source():
    sample = collect_sample_rows([("a", False)], ["f1"], plan_of(10, 1000))
    assert sample == {
        "rows": 1,
        "bytes": 1,
        "stopped_reason": "exhausted",
        "fields": {"f1": {"values": ["a"], "nulls": 0, "truncated": 0}},
    }
    empty = collect_sample_rows(iter(()), ["f1"], plan_of(10, 1000))
    assert empty["rows"] == 0 and empty["stopped_reason"] == "exhausted"
    assert empty["fields"] == {"f1": {"values": [], "nulls": 0, "truncated": 0}}


def test_collect_sample_rows_stops_at_the_byte_budget_without_keeping_the_row():
    rows = iter(
        [
            ("ab", False, None, False),
            ("é", False, "x" * 50, True),  # 2 bytes; the truncated value is not charged
            ("zz", False, None, False),  # 2 more bytes: 6 > 5, dropped
            ("never", False, "read", False),
        ]
    )
    sample = collect_sample_rows(rows, ["f1", "f2"], plan_of(100, 5))
    assert sample == {
        "rows": 2,
        "bytes": 4,
        "stopped_reason": "byte_budget",
        "fields": {
            "f1": {"values": ["ab", "é"], "nulls": 0, "truncated": 0},
            # the dropped row's null is not counted either
            "f2": {"values": [], "nulls": 1, "truncated": 1},
        },
    }
    assert next(rows) == ("never", False, "read", False)  # iteration stopped


def test_collect_sample_rows_budget_is_inclusive_and_wins_over_the_row_limit():
    exact = collect_sample_rows([("abc", False), ("de", False)], ["f"], plan_of(2, 5))
    assert exact["rows"] == 2 and exact["bytes"] == 5 and exact["stopped_reason"] == "row_limit"
    over = collect_sample_rows([("abc", False), ("def", False)], ["f"], plan_of(2, 5))
    assert over["rows"] == 1 and over["stopped_reason"] == "byte_budget"


# --------------------------------------------------------------------------- sample record

RECORD_KEYS = [
    "enabled",
    "method",
    "biased",
    "reads_full_source",
    "max_rows",
    "max_bytes",
    "max_value_chars",
    "random_fraction",
    "seed",
    "rows_collected",
    "bytes_retained",
    "values_truncated",
    "stopped_reason",
    "fields_sampled",
    "fields_omitted",
    "notes",
]


def profiled_nodes(config):
    tree, _ = tree_and_ids()
    profiled, _ = select_profile_fields(tree, None)
    return profiled


def test_sample_record_when_sampling_is_disabled():
    config = default_config()
    config["sampling"]["method"] = "none"
    plan = plan_sample(profiled_nodes(config), config)
    record = new_sample_record(plan, config)
    assert list(record) == RECORD_KEYS
    assert record["enabled"] is False
    assert record["method"] == "none"
    assert record["biased"] is None and record["reads_full_source"] is None
    assert record["stopped_reason"] == "disabled"
    assert record["fields_sampled"] == 0 and record["fields_omitted"] == 0
    assert record["notes"] == ["sampling disabled by configuration (sampling.method = none)"]
    assert record["rows_collected"] is None and record["values_truncated"] is None


def test_sample_record_at_the_metadata_level():
    config = default_config()
    config["analysis_level"] = "metadata"
    plan = plan_sample(profiled_nodes(config), config)
    record = new_sample_record(plan, config)
    assert record["enabled"] is False
    assert record["method"] == "prefix"
    assert record["biased"] is True and record["reads_full_source"] is None
    assert record["stopped_reason"] == "not_applicable"
    assert record["notes"] == ["the metadata level does not read table rows"]


def test_sample_record_for_an_enabled_random_sample():
    config = default_config()
    config["sampling"].update({"method": "random", "random_fraction": 0.25, "max_columns": 2})
    plan = plan_sample(profiled_nodes(config), config)
    record = new_sample_record(plan, config)
    assert list(record) == RECORD_KEYS
    assert record == {
        "enabled": True,
        "method": "random",
        "biased": False,
        "reads_full_source": True,
        "max_rows": 2000,
        "max_bytes": 16777216,
        "max_value_chars": 8192,
        "random_fraction": 0.25,
        "seed": 42,
        "rows_collected": None,
        "bytes_retained": None,
        "values_truncated": None,
        "stopped_reason": "not_applicable",
        "fields_sampled": 2,  # name, customer.email
        "fields_omitted": 0,
        "notes": [],
    }


def test_sample_notes_describe_the_method():
    prefix = sample_notes("prefix")
    random = sample_notes("random")
    transient = "Values are inspected transiently for format and JSON inference; they are not "
    assert prefix[0] == random[0] == transient + "persisted."
    assert prefix[1] == (
        "prefix: the first rows produced by the engine; potentially biased, not random."
    )
    assert random[1] == (
        "random: Bernoulli sampling may evaluate every row in scope; the fraction does not "
        "reduce I/O proportionally."
    )
    assert sample_notes("prefix") is not prefix  # a fresh list the caller may edit


# --------------------------------------------------------------------------- sample semantics


def fake_sample(fields):
    return {
        "rows": 5,
        "bytes": 0,
        "stopped_reason": "exhausted",
        "fields": {
            fid: {"values": values, "nulls": nulls, "truncated": 0}
            for fid, (values, nulls) in fields.items()
        },
    }


def node(fid):
    return {"field_id": fid}


JSON_80 = ['{"a": 1}', '{"b": 2}', "[1]", "[2]", "oops"]  # 4/5 structured = 0.8
JSON_75 = ['{"a": 1}', '{"b": 2}', "[1]", "oops"]  # 3/4 structured = 0.75
PLAIN = ["red", "red", "blue"]


def test_sample_semantics_json_threshold_examples_and_redaction():
    config = default_config()
    config["value_policy"]["max_examples_per_column"] = 1
    sample = fake_sample(
        {"j80": (JSON_80, 1), "j75": (JSON_75, 0), "plain": (PLAIN, 0), "secret": (JSON_80, 0)}
    )
    semantic_by_id = {}
    json_ids = set()
    result = sample_semantics(
        sample,
        [node("j80"), node("j75"), node("plain"), node("secret")],
        "prefix",
        config,
        redacted_ids={"secret"},
        example_ids={"plain", "secret"},
        semantic_by_id=semantic_by_id,
        json_ids=json_ids,
    )
    assert result is None
    assert list(semantic_by_id) == ["j80", "j75", "plain", "secret"]
    assert json_ids == {"j80", "secret"}  # 0.8 reaches json_min_ratio; 0.75 does not
    for fid in ("j80", "j75", "plain", "secret"):
        assert list(semantic_by_id[fid]) == [
            "observed_format",
            "json_profile",
            "concentration",
            "examples",
        ]
    j80 = semantic_by_id["j80"]
    assert j80["json_profile"]["excluded"]["sql_null"] == 1
    assert [item["key"] for item in j80["json_profile"]["keys"]] == ["a", "b"]
    assert semantic_by_id["j75"]["json_profile"] is not None  # profiled, but not JSON enough
    assert semantic_by_id["plain"]["json_profile"] is None
    assert semantic_by_id["plain"]["concentration"]["observations"] == 3
    # Examples only for allow-listed, non-redacted fields.
    assert semantic_by_id["plain"]["examples"]["values"] == [
        {"value": "red", "sample_count": 2, "truncated": False}
    ]
    assert semantic_by_id["j80"]["examples"] is None
    assert semantic_by_id["secret"]["examples"] is None
    # Redacted fields never list JSON key names.
    assert semantic_by_id["secret"]["json_profile"]["keys"] is None


def test_sample_semantics_key_names_follow_the_value_policy():
    config = default_config()
    config["value_policy"]["json_key_names"] = "redact"
    semantic_by_id = {}
    sample_semantics(
        fake_sample({"j80": (JSON_80, 0)}),
        [node("j80")],
        "random",
        config,
        redacted_ids=set(),
        example_ids=set(),
        semantic_by_id=semantic_by_id,
        json_ids=set(),
    )
    assert semantic_by_id["j80"]["json_profile"]["keys"] is None


def test_sample_semantics_keeps_what_was_inferred_before_a_failure(monkeypatch):
    real = semantic.infer_format
    calls = []

    def flaky(values, **kwargs):
        calls.append(values)
        if len(calls) == 2:
            raise RuntimeError("detector failed")
        return real(values, **kwargs)

    monkeypatch.setattr(semantic, "infer_format", flaky)
    semantic_by_id = {}
    json_ids = set()
    with pytest.raises(RuntimeError):
        sample_semantics(
            fake_sample({"j80": (JSON_80, 0), "plain": (PLAIN, 0), "late": (PLAIN, 0)}),
            [node("j80"), node("plain"), node("late")],
            "prefix",
            default_config(),
            redacted_ids=set(),
            example_ids=set(),
            semantic_by_id=semantic_by_id,
            json_ids=json_ids,
        )
    assert list(semantic_by_id) == ["j80"]
    assert json_ids == {"j80"}


# --------------------------------------------------------------------------- aggregate operations


def fake_agg_plan():
    specs = [
        {"alias": "a0000", "cost": 1, "field_id": None},
        {"alias": "a0001", "cost": 1, "field_id": "f1"},
        {"alias": "a0002", "cost": 2, "field_id": "f1"},
        {"alias": "a0003", "cost": 1, "field_id": "f2", "group": "element"},
        {"alias": "a0004", "cost": 3, "field_id": "f3", "group": "json_path"},
    ]
    return {
        "specs": specs,
        "passes": [["a0000", "a0001"], ["a0002", "a0003"], ["a0004"]],
        "standard_passes": 2,
    }


def test_aggregate_operations_standard_and_deep_extra_passes():
    seen = []

    def extra(specs):
        seen.append([spec["alias"] for spec in specs])
        return {"sorts": len(specs)}

    ops = aggregate_operations(
        fake_agg_plan(),
        deep=True,
        standard_description="standard",
        extra_description="extra",
        extra_details=extra,
    )
    assert seen == [["a0000", "a0001"], ["a0002", "a0003"], ["a0004"]]
    assert [op["operation_id"] for op in ops] == [
        "op_aggregate_1",
        "op_aggregate_2",
        "op_aggregate_3",
    ]
    assert [op["kind"] for op in ops] == ["aggregate_pass", "aggregate_pass", "deep_aggregate_pass"]
    assert [op["description"] for op in ops] == ["standard", "standard", "extra"]
    assert all(op["reads_user_data"] is True for op in ops)
    assert [list(op["details"]) for op in ops] == [
        ["expressions", "fields", "deep_expressions", "sorts"]
    ] * 3
    assert [op["details"] for op in ops] == [
        {"expressions": 2, "fields": 1, "deep_expressions": 0, "sorts": 2},
        {"expressions": 3, "fields": 2, "deep_expressions": 1, "sorts": 2},
        {"expressions": 3, "fields": 1, "deep_expressions": 3, "sorts": 1},
    ]


def test_aggregate_operations_without_deep_details():
    ops = aggregate_operations(
        fake_agg_plan(), deep=False, standard_description="s", extra_description="e"
    )
    assert [op["details"] for op in ops] == [
        {"expressions": 2, "fields": 1},
        {"expressions": 3, "fields": 2},
        {"expressions": 3, "fields": 1},
    ]
    assert list(ops[0]) == ["operation_id", "kind", "description", "reads_user_data", "details"]
    assert (
        aggregate_operations(
            {"specs": [], "passes": [], "standard_passes": 0},
            deep=False,
            standard_description="s",
            extra_description="e",
        )
        == []
    )


# --------------------------------------------------------------------------- field profiles


def test_omission_reason_walks_up_to_the_nearest_recorded_reason():
    parents = {"child": {"field_id": "parent"}, "parent": {"field_id": "top"}}
    reasons = {"parent": "max_depth", "top": "not_selected"}
    assert omission_reason("child", "top", reasons, parents, None) == "max_depth"
    assert omission_reason("child", "top", {}, parents, {"top"}) == "inside_collection"
    assert omission_reason("child", "top", {}, parents, {"other"}) == "not_selected"
    assert omission_reason("child", "top", {"child": "own"}, parents, None) == "own"


def test_table_field_profiles_with_a_selection():
    tree, ids = tree_and_ids()
    selected = ["id", "customer", "items"]
    profiled, omissions = select_profile_fields(tree, selected)
    profiles = table_field_profiles(
        tree, {item["field_id"] for item in profiled}, omissions, selected
    )
    assert [p["field_id"] for p in profiles] == [n["field_id"] for n in iter_nodes(tree["fields"])]
    by_path = {p["display_path"]: p for p in profiles}
    assert by_path["id"]["profiled"] is True and by_path["id"]["omission_reason"] is None
    assert by_path["customer.geo.lat"]["profiled"] is True
    assert by_path["name"]["omission_reason"] == "not_selected"
    assert by_path["items"]["profiled"] is True
    assert by_path["items[]"]["omission_reason"] == "inside_collection"
    assert by_path["items[].sku"]["omission_reason"] == "inside_collection"  # inherited
    assert all(p["element_context"] is None for p in profiles)
    assert by_path["items[].qty"]["field_id"] == ids["items[].qty"]


def test_table_field_profiles_fall_back_without_recorded_omissions():
    tree, _ = tree_and_ids()
    profiles = table_field_profiles(tree, set(), [], ["id"])
    by_path = {p["display_path"]: p for p in profiles}
    assert by_path["name"]["omission_reason"] == "not_selected"
    assert by_path["customer.email"]["omission_reason"] == "not_selected"
    assert by_path["id"]["omission_reason"] == "inside_collection"  # selected, no reason given
    unselected = table_field_profiles(tree, set(), [], None)
    assert {p["omission_reason"] for p in unselected} == {"inside_collection"}


def test_table_field_profiles_with_budget_omissions_and_element_contexts():
    tree, ids = tree_and_ids()
    profiled, _ = select_profile_fields(tree, None)
    context = {"collection_field_id": ids["items"], "element_kind": "array_element"}
    omissions = [
        {"field_id": ids["items[]"], "reason": "deep_budget"},
        {"field_id": None, "reason": "max_fields"},  # table-wide, never a field's reason
    ]
    profiles = table_field_profiles(
        tree,
        [item["field_id"] for item in profiled],
        omissions,
        None,
        {ids["items[].qty"]: context},
    )
    by_path = {p["display_path"]: p for p in profiles}
    assert by_path["items[]"]["omission_reason"] == "deep_budget"
    assert by_path["items[].sku"]["omission_reason"] == "deep_budget"
    qty = by_path["items[].qty"]
    assert qty["profiled"] is True and qty["omission_reason"] is None
    assert qty["element_context"] == context and qty["element_context"] is not context
    assert by_path["name"]["profiled"] is True


# --------------------------------------------------------------------------- field results


def assembly_case():
    fields = [
        f("id", t("integer")),
        f("name", t("string")),
        f("customer", t("struct", fields=[f("email", t("string"))])),
    ]
    tree = build_schema_tree(fields, max_depth=3, max_fields=200)
    ids = {n["display_path"]: n["field_id"] for n in iter_nodes(tree["fields"])}
    profiled, omissions = select_profile_fields(tree, None)
    agg_plan = plan_aggregates(
        profiled,
        config=default_config(),
        capabilities={"try_parse_json": False},
        scope="full_table",
        json_field_ids={ids["name"]},
    )
    agg_plan["specs"].append(
        {
            "metric": "path_count",
            "op": "json_path_presence",
            "field_id": ids["name"],
            "group": "json_path",
            "tier": 6,
            "cost": 1,
            "params": {},
            "alias": "a9999",
        }
    )
    table = new_table("db.t", ["db", "t"], "db.t", "t_1", "`db`.`t`")
    table["field_profiles"] = table_field_profiles(
        tree, {n["field_id"] for n in profiled}, omissions, None
    )
    nodes_by_id = {n["field_id"]: n for n in iter_nodes(tree["fields"])}
    aliases = {(spec["field_id"], spec["metric"]): spec["alias"] for spec in agg_plan["specs"]}
    return table, agg_plan, ids, nodes_by_id, aliases


def metric(field, name):
    return next(item for item in field["metrics"] if item["name"] == name)


def test_assemble_field_results_measures_rows_and_fields():
    table, agg_plan, ids, nodes_by_id, aliases = assembly_case()
    results = {
        aliases[(None, "row_count")]: 10,
        aliases[(ids["id"], "null_count")]: 0,
        aliases[(ids["name"], "null_count")]: 2,
        aliases[(ids["name"], "max_length")]: 7,
        aliases[(ids["customer"], "null_count")]: 1,
        aliases[(ids["customer.email"], "null_count")]: 3,
    }
    failed = {aliases[(ids["name"], "min_length")]: "expression rejected by the engine: X"}
    inferred = {
        "observed_format": {"format": "json_object"},
        "json_profile": {"scope": "sample"},
        "concentration": {"observations": 8},
        "examples": {"marker": "examples are attached"},
    }
    extra = [
        {
            "field_id": ids["id"],
            "metric": not_measured(
                "distinct_count",
                "not_computed",
                "planned elsewhere",
                scope="full_table",
                source="aggregate",
            ),
        }
    ]
    row_count, specs_by_field, static_by_field, null_counts = assemble_field_results(
        table,
        agg_plan,
        results,
        failed,
        scope="full_table",
        nodes_by_id=nodes_by_id,
        semantic_by_id={ids["name"]: inferred},
        extra_static=extra,
        skip_field_ids={ids["customer.email"]},
    )
    assert row_count == 10
    assert table["table_metrics"] == [
        {
            **table["table_metrics"][0],
            "name": "row_count",
            "status": "measured",
            "value": 10,
            "unit": "rows",
            "scope": "full_table",
            "accuracy": "exact",
            "source": "aggregate",
            "method": "count(*) over the analysed scope",
        }
    ]
    assert "a9999" not in {s["alias"] for specs in specs_by_field.values() for s in specs}
    assert specs_by_field[None][0]["metric"] == "row_count"
    assert [item["metric"]["name"] for item in static_by_field[ids["name"]]] == [
        "json_invalid_count"
    ]
    assert static_by_field[ids["id"]] == extra
    assert null_counts == {
        ids["id"]: 0,
        ids["name"]: 2,
        ids["customer"]: 1,
        ids["customer.email"]: 3,
    }
    by_path = {p["display_path"]: p for p in table["field_profiles"]}
    name = by_path["name"]
    assert metric(name, "null_ratio")["value"] == 0.2
    assert metric(name, "min_length")["status"] == "error"
    assert metric(name, "min_length")["reason"] == "expression rejected by the engine: X"
    assert metric(name, "max_length")["value"] == 7
    assert metric(name, "json_invalid_count")["status"] == "unsupported"
    assert name["semantics"] == {
        "observed_format": {"format": "json_object"},
        "candidate_roles": [],
    }
    assert name["json_profile"] == {"scope": "sample"}
    assert name["concentration"] == {"observations": 8}
    assert name["examples"] == {"marker": "examples are attached"}
    assert metric(by_path["id"], "distinct_count")["status"] == "not_computed"
    assert by_path["id"]["semantics"] is None
    assert metric(by_path["customer"], "null_count")["value"] == 1
    # Skipped fields are left to the caller.
    assert by_path["customer.email"]["metrics"] == []


def test_assemble_field_results_without_a_row_count():
    table, agg_plan, ids, nodes_by_id, aliases = assembly_case()
    row_alias = aliases[(None, "row_count")]
    name_null = aliases[(ids["name"], "null_count")]
    results = {name_null: 2}
    failed = {
        row_alias: "aggregation pass failed: TIMEOUT",
        aliases[(ids["id"], "null_count")]: "x",
    }
    row_count, _, _, null_counts = assemble_field_results(
        table,
        agg_plan,
        results,
        failed,
        scope="full_table",
        nodes_by_id=nodes_by_id,
        semantic_by_id={},
    )
    assert row_count is None
    assert table["table_metrics"][-1]["status"] == "error"
    assert table["table_metrics"][-1]["reason"] == "aggregation pass failed: TIMEOUT"
    assert ids["id"] not in null_counts  # a failed null count is not a measured one
    assert null_counts[ids["name"]] == 2
    name = next(p for p in table["field_profiles"] if p["display_path"] == "name")
    assert metric(name, "null_count")["status"] == "measured"
    assert not any(item["name"] == "null_ratio" for item in name["metrics"])
    email = next(p for p in table["field_profiles"] if p["display_path"] == "customer.email")
    assert email["metrics"]  # not skipped this time

    empty = assembly_case()
    assemble_field_results(
        empty[0],
        empty[1],
        {},
        {},
        scope="full_table",
        nodes_by_id=empty[3],
        semantic_by_id={},
    )
    assert empty[0]["table_metrics"][-1]["reason"] == "row count not returned"


def test_assemble_field_results_uses_the_parent_null_count():
    table, agg_plan, ids, nodes_by_id, aliases = assembly_case()
    results = {
        aliases[(None, "row_count")]: 10,
        aliases[(ids["customer"], "null_count")]: 1,
        aliases[(ids["customer.email"], "null_count")]: 3,
        aliases[(ids["customer.email"], "null_count_parent_present")]: 2,
    }
    assemble_field_results(
        table,
        agg_plan,
        results,
        {},
        scope="full_table",
        nodes_by_id=nodes_by_id,
        semantic_by_id={},
    )
    email = next(p for p in table["field_profiles"] if p["display_path"] == "customer.email")
    given = metric(email, "null_ratio_given_parent_present")
    assert given["denominator"] == 9  # rows whose parent struct is not null: 10 - 1
    assert given["value"] == 2 / 9


# --------------------------------------------------------------------------- run id


def test_new_run_id_is_utc_and_unique():
    moment = datetime(2026, 9, 21, 20, 30, 0, tzinfo=timezone(timedelta(hours=2)))
    first = new_run_id(moment)
    assert re.fullmatch(r"20260921T183000Z-[0-9a-f]{8}", first)
    assert new_run_id(moment) != first
