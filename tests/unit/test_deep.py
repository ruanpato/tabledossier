"""Deep level planning, element metric assembly and JSON path catalogues (no Spark)."""

import json

import pytest

from tabledossier.assemble import element_field_metrics
from tabledossier.config import deep_merge, default_config
from tabledossier.deep import (
    engine_json_path,
    json_path_catalog,
    json_path_text,
    plan_deep_elements,
    plan_element_distinct,
    plan_element_specs,
    plan_json_validation,
)
from tabledossier.planning import build_schema_tree, plan_aggregates, select_profile_fields


def t(kind, **extra):
    return {"kind": kind, "physical_type": extra.pop("physical_type", kind), **extra}


def f(name, ntype, nullable=True):
    return {"name": name, "type": ntype, "nullable": nullable, "comment": None}


FIELDS = [
    f("id", t("integer", physical_type="bigint"), nullable=False),
    f(
        "items",
        t(
            "array",
            element_type=t(
                "struct",
                fields=[
                    f("sku", t("string")),
                    f("qty", t("integer", physical_type="int")),
                    f("price", t("decimal", physical_type="decimal(10,2)", precision=10, scale=2)),
                ],
            ),
            contains_null=True,
        ),
    ),
    f("attrs", t("map", key_type=t("string"), value_type=t("string"), value_contains_null=True)),
    f("matrix", t("array", element_type=t("array", element_type=t("integer")))),
    f("scores", t("array", element_type=t("float", physical_type="double"))),
    f("payload", t("string")),
]


def _config(**overrides):
    return deep_merge(default_config(), {"analysis_level": "deep", **overrides})


def _tree():
    return build_schema_tree(FIELDS, max_depth=3, max_fields=200)


def _plan(config=None, selected=None):
    tree = _tree()
    profiled, omissions = select_profile_fields(tree, selected)
    return tree, profiled, omissions, plan_deep_elements(tree, profiled, config or _config(), "t")


def test_all_within_budget_profiles_one_collection_level():
    _, _, omissions, plan = _plan()
    paths = sorted(node["display_path"] for node in plan["element_nodes"])
    assert paths == sorted(
        [
            "items[]",
            "items[].sku",
            "items[].qty",
            "items[].price",
            "attrs{key}",
            "attrs{value}",
            "matrix[]",
            "scores[]",
        ]
    )
    contexts = plan["contexts"]
    by_path = {node["display_path"]: node["field_id"] for node in plan["element_nodes"]}
    assert contexts[by_path["items[].sku"]]["unit"] == "elements"
    assert contexts[by_path["items[].sku"]]["inner"] == ["sku"]
    assert contexts[by_path["attrs{value}"]]["unit"] == "entries"
    assert contexts[by_path["attrs{key}"]]["segment"] == "map_key"
    reasons = {o["display_path"]: o["reason"] for o in plan["omissions"]}
    assert reasons == {"matrix[][]": "nested_collection"}
    assert {o["display_path"] for o in omissions if o["reason"] == "inside_collection"} >= {
        "items[]",
        "attrs{key}",
    }, "standard selection still reports them; the runtime replaces those records"
    assert plan["json_nodes"] is None


def test_explicit_targets_and_ineligible_fields():
    config = _config(
        deep={
            "targets": [
                {"table": "t", "column": "items"},
                {"table": "t", "column": "payload"},
                {"table": "t", "column": "id"},
                {"table": "t", "column": "missing"},
                {"table": "other", "column": "attrs"},
            ]
        }
    )
    _, _, _, plan = _plan(config)
    assert plan["mode"] == "explicit"
    assert [node["display_path"] for node in plan["collections"]] == ["items"]
    assert [node["display_path"] for node in plan["json_nodes"]] == ["payload"]
    reasons = {item["display_path"]: item["reason"] for item in plan["not_eligible"]}
    assert "not integer" in reasons["id"].replace("arrays, maps or JSON strings, ", "")
    assert "not found" in reasons["missing"]
    omitted = {o["display_path"]: o["reason"] for o in plan["omissions"]}
    assert omitted["attrs{key}"] == "deep_not_selected"
    assert all(n["display_path"].startswith("items") for n in plan["element_nodes"])


def test_unselected_columns_are_not_deep_targets():
    _, _, _, plan = _plan(selected=["id", "attrs"])
    assert {n["display_path"] for n in plan["collections"]} == {"attrs"}


def test_element_specs_tiers_and_redaction():
    _, _, _, plan = _plan()
    items = next(n for n in plan["collections"] if n["display_path"] == "items")
    specs, static = plan_element_specs(
        plan["element_nodes"],
        plan["contexts"],
        scope="full_table",
        redacted_field_ids={items["field_id"]},
    )
    assert specs and all(spec["tier"] >= 5 for spec in specs)
    assert all(spec["op"].startswith("el_") for spec in specs)
    by_path = {n["field_id"]: n["display_path"] for n in plan["element_nodes"]}
    metrics = {(by_path[s["field_id"]], s["metric"]) for s in specs}
    assert ("items[].price", "max") not in metrics, "extremes of a redacted collection"
    assert ("scores[]", "max") in metrics and ("scores[]", "nan_count") in metrics
    assert ("items[].sku", "null_count_parent_present") in metrics
    assert ("items[]", "null_count_parent_present") not in metrics
    assert ("attrs{value}", "max_length") in metrics
    redacted = {(by_path[s["field_id"]], s["metric"]["name"]) for s in static}
    assert ("items[].price", "max") in redacted


def test_standard_plan_is_unchanged_and_deep_respects_pass_budget():
    _, profiled, _, plan = _plan()
    specs, _ = plan_element_specs(plan["element_nodes"], plan["contexts"], scope="full_table")
    base = _config(limits={"max_expressions_per_pass": 30, "max_aggregate_passes": 2})
    standard = plan_aggregates(profiled, config=base, capabilities={}, scope="full_table")
    for extra in (0, 1, 3):
        deep = plan_aggregates(
            profiled,
            config=base,
            capabilities={},
            scope="full_table",
            deep_candidates=specs,
            max_extra_passes=extra,
        )
        kept_standard = [s for s in deep["specs"] if not s.get("group")]
        assert [(s["field_id"], s["metric"]) for s in kept_standard] == [
            (s["field_id"], s["metric"]) for s in standard["specs"]
        ], "deep candidates never displace standard metrics"
        assert deep["standard_passes"] == len(standard["passes"])
        assert deep["extra_passes"] <= extra
        assert len(deep["passes"]) <= base["limits"]["max_aggregate_passes"] + extra
        assert all(len(p) <= 30 for p in deep["passes"])
        dropped = deep["deep_dropped"]
        if extra == 0:
            assert dropped, "no room: deep expressions are dropped and reported"
            reasons = {o["reason"] for o in deep["omissions"]}
            assert "deep_budget" in reasons
            statuses = {s["metric"]["status"] for s in deep["static_metrics"]}
            assert "not_computed" in statuses
        tiers = [s["tier"] for s in dropped]
        kept_tiers = [s["tier"] for s in deep["specs"] if s.get("group")]
        if dropped and kept_tiers:
            assert min(tiers) >= max(kept_tiers), "highest tiers are dropped first"


def test_element_distinct_plan():
    _, _, _, plan = _plan()
    distinct = plan_element_distinct(plan["element_nodes"], plan["contexts"], _config())
    leaves = {leaf["display_path"] for leaf in distinct["leaves"]}
    assert leaves == {
        "items[].sku",
        "items[].qty",
        "items[].price",
        "attrs{key}",
        "attrs{value}",
        "scores[]",
    }, "structs and nested collections are not counted"
    assert distinct["enabled"] and distinct["mode"] == "sample"
    off = plan_element_distinct(
        plan["element_nodes"], plan["contexts"], _config(deep={"element_distinct": "off"})
    )
    assert not off["enabled"] and "off" in off["reason"]
    no_sample = plan_element_distinct(
        plan["element_nodes"], plan["contexts"], _config(sampling={"method": "none"})
    )
    assert not no_sample["enabled"] and "sampling" in no_sample["reason"]
    full = plan_element_distinct(
        plan["element_nodes"],
        plan["contexts"],
        _config(sampling={"method": "none"}, deep={"element_distinct": "full_scope"}),
    )
    assert full["enabled"], "full-scope mode does not depend on sampling"


def _node(path, kind):
    return {"field_id": "f_" + "1" * 12, "display_path": path, "type": {"kind": kind}}


def _spec(metric, alias, op="el_x"):
    return {"metric": metric, "alias": alias, "op": op, "params": {}}


def test_element_metrics_use_element_denominators():
    context = {
        "collection_field_id": "f_" + "2" * 12,
        "collection_display_path": "items",
        "collection_kind": "array",
        "segment": "array_element",
        "inner": ["qty"],
        "unit": "elements",
    }
    specs = [
        _spec("null_count", "a1"),
        _spec("null_count_parent_present", "a2"),
        _spec("min", "a3"),
        _spec("max", "a4"),
        _spec("zero_count", "a5"),
        _spec("positive_count", "a6"),
    ]
    results = {"a1": 50, "a2": 40, "a3": 1, "a4": 5, "a5": 0, "a6": 150}
    metrics = element_field_metrics(
        _node("items[].qty", "integer"),
        context,
        specs,
        results,
        {},
        scope="full_snapshot",
        element_total=200,
        parent_null_count=10,
        static=[],
    )
    by_name = {m["name"]: m for m in metrics}
    assert metrics[0]["name"] == "element_count" and metrics[0]["value"] == 200
    assert by_name["null_count"]["denominator"] == 200
    assert by_name["null_ratio"]["value"] == 0.25
    assert by_name["non_null_count"]["value"] == 150
    assert by_name["null_count_parent_present"]["denominator"] == 190
    assert by_name["positive_count"]["denominator"] == 150
    for metric in metrics:
        assert "rows" not in (metric.get("unit"), metric.get("denominator_unit")), metric["name"]
        if metric.get("denominator_unit"):
            assert metric["denominator_unit"].startswith("elements")
    empty = element_field_metrics(
        _node("items[].qty", "integer"),
        context,
        specs,
        {"a1": None, "a2": None, "a3": None, "a4": None, "a5": None, "a6": None},
        {},
        scope="full_table",
        element_total=0,
        parent_null_count=0,
        static=[],
    )
    by_name = {m["name"]: m for m in empty}
    assert by_name["null_ratio"]["status"] == "insufficient_data"
    assert by_name["min"]["status"] == "insufficient_data" and by_name["min"]["value"] is None


# --------------------------------------------------------------------------- JSON paths


def _catalog(values, **overrides):
    options = {
        "sql_nulls": 0,
        "truncated": 0,
        "include_names": True,
        "names_omitted_reason": None,
        "max_depth": 3,
        "max_paths": 50,
        "max_object_keys": 50,
        "sample_method": "prefix",
    }
    options.update(overrides)
    return json_path_catalog(values, **options)


DOCS = [
    json.dumps({"status": "ok", "amount": 1.5, "customer": {"id": 7, "tier": "gold"}}),
    json.dumps({"status": "retry", "amount": "2.00", "customer": {"id": 8}}),
    json.dumps({"status": None, "items": [{"sku": "a"}, {"sku": "b"}]}),
    json.dumps({"status": "ok", "items": [{"sku": "c", "note": "only here"}]}),
    "[1, 2]",
    "null",
    '"scalar"',
    '{"broken',
]


def test_json_path_catalog_presence_types_and_heterogeneity():
    catalog = _catalog(DOCS)
    assert catalog["documents"] == 5 and catalog["scope"] == "sample"
    assert catalog["counts"] == {
        "structured": 5,
        "scalar": 1,
        "json_null_literal": 1,
        "invalid": 1,
    }
    paths = {item["path"]: item for item in catalog["paths"]}
    assert paths["$.status"]["present_in"] == 4
    assert paths["$.status"]["types"] == {"null": 1, "string": 3}
    assert paths["$.status"]["heterogeneous"] is False
    assert paths["$.amount"]["heterogeneous"] is True
    assert paths["$.customer.id"]["depth"] == 2 and paths["$.customer.id"]["present_in"] == 2
    assert paths["$.items[*].sku"]["occurrences"] == 3
    assert paths["$.items[*].sku"]["present_in"] == 2
    # Keys seen in a single document are collapsed into '*' (they may be data used as keys).
    assert "$.customer.tier" not in paths and "$.customer.*" in paths
    assert "$.items[*].note" not in paths and paths["$.items[*]"]["map_like"] is True
    assert paths["$[*]"]["types"] == {"number": 2}
    assert catalog["paths_listed"] == len(catalog["paths"])
    assert any("not a complete or guaranteed schema" in item for item in catalog["limitations"])


def test_json_path_limits_are_reported():
    deep_doc = json.dumps({"a": {"b": {"c": {"d": 1}}}})
    catalog = _catalog([deep_doc] * 3, max_depth=2)
    assert {item["path"] for item in catalog["paths"]} == {"$.a", "$.a.b"}
    assert catalog["depth_truncated"] is True
    limited = _catalog(DOCS, max_paths=2)
    assert limited["paths_listed"] == 2 and limited["paths_omitted"] > 0
    assert "deep.max_json_paths" in limited["paths_omitted_reason"]


def test_secret_markers_never_leak_from_json_values_or_map_like_keys():
    values = [
        json.dumps(
            {
                "kind": "event",
                "token": f"SECRET-MARKER-{i}",
                "by_user": {f"SECRET-MARKER-{i}": 1},
                "tags": {f"SECRET_KEY_{i}": True, f"SECRET_KEY_{i + 1}": False},
                "emails": {f"secret{i}@example.com": 1},
            }
        )
        for i in range(40)
    ]
    catalog = _catalog(values)
    text = json.dumps(catalog)
    assert "SECRET-MARKER" not in text and "SECRET_KEY" not in text and "@example" not in text
    paths = {item["path"]: item for item in catalog["paths"]}
    assert paths["$.by_user"]["map_like"] is True and "$.by_user.*" in paths
    assert paths["$.tags"]["map_like"] is True
    assert paths["$.emails"]["map_like"] is True
    assert "$.kind" in paths and "$.token" in paths
    many = _catalog([json.dumps({f"k{j}_{i}": 1 for j in range(3)}) for i in range(30)])
    assert all("k0_" not in item["path"] for item in many["paths"])


def test_json_key_names_policy_hides_every_path():
    catalog = _catalog(
        DOCS, include_names=False, names_omitted_reason="value policy (json_key_names = redact)"
    )
    assert catalog["paths"] is None and catalog["paths_listed"] == 0
    assert catalog["paths_observed"] > 0
    assert "status" not in json.dumps(catalog)
    assert plan_json_validation({"f_1": catalog}, "variant") == []


@pytest.mark.parametrize(
    ("segments", "text", "engine"),
    [
        ([{"kind": "key", "name": "status"}], "$.status", "$.status"),
        ([{"kind": "key", "name": "a.b"}], '$["a.b"]', "$['a.b']"),
        ([{"kind": "key", "name": "it's"}], '$["it\'s"]', None),
        ([{"kind": "key", "name": "items"}, {"kind": "items"}], "$.items[*]", None),
        ([{"kind": "key", "name": "attrs"}, {"kind": "any_key"}], "$.attrs.*", None),
    ],
)
def test_json_path_quoting(segments, text, engine):
    assert json_path_text(segments) == text
    assert engine_json_path(segments) == engine


def test_json_validation_plan_by_method():
    catalog = _catalog(DOCS)
    listed = [item for item in catalog["paths"] if engine_json_path(item["segments"])]
    variant = plan_json_validation({"f_1": catalog}, "variant")
    gjo = plan_json_validation({"f_1": catalog}, "get_json_object")
    assert all(spec["tier"] == 7 and spec["group"] == "json_path" for spec in variant + gjo)
    assert sum(1 for spec in gjo if spec["op"] == "json_path_non_null") == len(listed)
    assert sum(1 for spec in variant if spec["op"] == "json_path_present") == len(listed)
    assert all(
        spec["params"]["expected_type"] for spec in variant if spec["op"] == "json_path_type_match"
    )
    assert plan_json_validation({"f_1": catalog}, None) == []
