from tabledossier.config import deep_merge, default_config
from tabledossier.planning import (
    build_schema_tree,
    iter_nodes,
    plan_aggregates,
    plan_sample,
    select_profile_fields,
)


def t(kind, **extra):
    return {"kind": kind, "physical_type": extra.pop("physical_type", kind), **extra}


def f(name, ntype, nullable=True, comment=None):
    return {"name": name, "type": ntype, "nullable": nullable, "comment": comment}


FIELDS = [
    f("id", t("integer", physical_type="bigint"), nullable=False, comment="key"),
    f("a.b", t("string")),
    f(
        "customer",
        t(
            "struct",
            fields=[
                f("name", t("string")),
                f(
                    "address",
                    t(
                        "struct",
                        fields=[
                            f("city", t("string")),
                            f("geo", t("struct", fields=[f("lat", t("float"))])),
                        ],
                    ),
                ),
            ],
        ),
    ),
    f(
        "items",
        t("array", element_type=t("struct", fields=[f("sku", t("string"))]), contains_null=True),
    ),
    f("attrs", t("map", key_type=t("string"), value_type=t("string"), value_contains_null=True)),
    f("ts", t("timestamp_ntz")),
    f("amount", t("decimal", physical_type="decimal(12,2)", precision=12, scale=2)),
]


def _config(**overrides):
    return deep_merge(default_config(), overrides)


def test_tree_depth_limit_and_paths():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    paths = [node["display_path"] for node in iter_nodes(tree["fields"])]
    assert "`a.b`" in paths
    assert "customer.address.city" in paths
    assert "customer.address.geo.lat" not in paths
    geo = next(n for n in iter_nodes(tree["fields"]) if n["display_path"] == "customer.address.geo")
    assert geo["children_omitted"] == 1 and geo["children_omitted_reason"] == "max_depth"
    assert "items[]" in paths and "items[].sku" in paths
    assert "attrs{key}" in paths and "attrs{value}" in paths
    key = next(n for n in iter_nodes(tree["fields"]) if n["display_path"] == "attrs{key}")
    assert key["nullable"] is False


def test_tree_field_limit_is_breadth_first():
    tree = build_schema_tree(FIELDS, max_depth=5, max_fields=8)
    top = [node["display_path"] for node in tree["fields"]]
    assert len(top) == 7, "all top-level columns come before nested fields"
    assert tree["nodes_total"] == 8
    assert tree["nodes_omitted"] > 0


def test_selection_and_collection_omissions():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    profiled, omissions = select_profile_fields(tree, ["id", "customer", "items"])
    names = [node["display_path"] for node in profiled]
    assert "customer.address.city" in names
    assert "items" in names and "items[]" not in names
    reasons = {o["display_path"]: o["reason"] for o in omissions}
    assert reasons["`a.b`"] == "not_selected"
    assert reasons["items[]"] == "inside_collection"
    assert reasons["customer.address.geo"] == "max_depth"


def test_aggregate_plan_shares_passes_instead_of_actions_per_column():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    plan = plan_aggregates(
        profiled, config=_config(), capabilities={"try_parse_json": False}, scope="full_snapshot"
    )
    assert len(plan["passes"]) == 1
    assert plan["expression_count"] <= plan["expression_capacity"]
    ops = {(spec["field_id"], spec["metric"]) for spec in plan["specs"]}
    assert (None, "row_count") in ops
    city = next(n for n in profiled if n["display_path"] == "customer.address.city")
    assert (city["field_id"], "null_count_parent_present") in ops
    ts = next(n for n in profiled if n["display_path"] == "ts")
    static = [s for s in plan["static_metrics"] if s["field_id"] == ts["field_id"]]
    assert static[0]["metric"]["name"] == "after_reference_count"
    assert static[0]["metric"]["status"] == "not_computed"


def test_budget_trims_high_tiers_first_and_records_omissions():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    config = _config(limits={"max_expressions_per_pass": 10, "max_aggregate_passes": 2})
    plan = plan_aggregates(profiled, config=config, capabilities={}, scope="full_table")
    assert plan["expression_count"] <= 20
    assert all(sum(1 for _ in p) <= 10 for p in plan["passes"])
    assert len(plan["passes"]) <= 2
    kept_metrics = {spec["metric"] for spec in plan["specs"]}
    assert "row_count" in kept_metrics and "null_count" in kept_metrics
    assert "quantiles" not in kept_metrics
    assert plan["omissions"] and all(o["reason"] == "expression_budget" for o in plan["omissions"])
    statuses = {s["metric"]["status"] for s in plan["static_metrics"]}
    assert "not_computed" in statuses


def test_redaction_and_json_capability():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    amount = next(n for n in profiled if n["display_path"] == "amount")
    text = next(n for n in profiled if n["display_path"] == "`a.b`")
    plan = plan_aggregates(
        profiled,
        config=_config(),
        capabilities={"try_parse_json": False},
        scope="full_table",
        json_field_ids={text["field_id"]},
        redacted_field_ids={amount["field_id"]},
    )
    kept = {(s["field_id"], s["metric"]) for s in plan["specs"]}
    assert (amount["field_id"], "max") not in kept
    assert (amount["field_id"], "null_count") in kept
    static = {
        (s["field_id"], s["metric"]["name"]): s["metric"]["status"] for s in plan["static_metrics"]
    }
    assert static[(amount["field_id"], "max")] == "redacted"
    assert static[(text["field_id"], "json_invalid_count")] == "unsupported"
    plan2 = plan_aggregates(
        profiled,
        config=_config(),
        capabilities={"try_parse_json": True},
        scope="full_table",
        json_field_ids={text["field_id"]},
    )
    assert (text["field_id"], "json_invalid_count") in {
        (s["field_id"], s["metric"]) for s in plan2["specs"]
    }


def test_sample_plan_only_strings_and_respects_policy():
    tree = build_schema_tree(FIELDS, max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    plan = plan_sample(profiled, _config())
    kinds = {n["type"]["kind"] for n in profiled if n["field_id"] in plan["field_ids"]}
    assert plan["enabled"] and kinds == {"string"}
    assert not plan_sample(profiled, _config(sampling={"method": "none"}))["enabled"]
    assert not plan_sample(profiled, _config(analysis_level="metadata"))["enabled"]
    limited = plan_sample(profiled, _config(sampling={"max_columns": 1}))
    assert len(limited["field_ids"]) == 1 and limited["omitted_field_ids"]


def _string_fields(count):
    return [f(f"s{index}", t("string")) for index in range(count)]


def _pass_costs(plan):
    costs = {spec["alias"]: spec["cost"] for spec in plan["specs"]}
    return [sum(costs[alias] for alias in aliases) for aliases in plan["passes"]]


def test_pass_budget_holds_when_a_cost_two_expression_does_not_fit_the_last_slot():
    # Three string fields at 13 expressions per pass used to pack into passes of 12, 13 and 1
    # expressions: all_values_equal costs 2 and is never split across passes.
    tree = build_schema_tree(_string_fields(3), max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    config = _config(limits={"max_expressions_per_pass": 13, "max_aggregate_passes": 2})
    plan = plan_aggregates(profiled, config=config, capabilities={}, scope="full_table")
    assert plan["standard_passes"] == len(plan["passes"]) <= 2
    assert all(cost <= 13 for cost in _pass_costs(plan))
    dropped = [o for o in plan["omissions"] if o["reason"] == "expression_budget"]
    assert dropped and "never split across passes" in dropped[0]["detail"]
    kept = {(spec["field_id"], spec["metric"]) for spec in plan["specs"]}
    for item in plan["static_metrics"]:
        assert (item["field_id"], item["metric"]["name"]) not in kept


def test_pass_budget_property_over_random_plans():
    import random

    kinds = ["integer", "float", "decimal", "string", "boolean", "date", "timestamp", "binary"]
    rng = random.Random(20261010)
    for _ in range(600):
        fields = [f(f"c{index}", t(rng.choice(kinds))) for index in range(rng.randint(1, 10))]
        tree = build_schema_tree(fields, max_depth=3, max_fields=200)
        profiled, _ = select_profile_fields(tree, None)
        per_pass = rng.randint(10, 20)
        passes = rng.randint(1, 4)
        config = _config(
            limits={"max_expressions_per_pass": per_pass, "max_aggregate_passes": passes}
        )
        plan = plan_aggregates(profiled, config=config, capabilities={}, scope="full_table")
        assert plan["standard_passes"] <= passes
        assert all(cost <= per_pass for cost in _pass_costs(plan))
        assert plan["expression_count"] <= plan["expression_capacity"]


def test_deep_expressions_never_open_more_than_max_extra_passes():
    tree = build_schema_tree(_string_fields(1), max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    field = profiled[0]["field_id"]
    deep = [
        {
            "metric": f"deep_{index}",
            "op": "count_all",
            "field_id": field,
            "tier": 5,
            # ten 1-cost expressions, then a 2-cost one that does not fit the 11th slot
            "cost": 2 if index == 10 else 1,
            "params": {},
            "group": "element",
        }
        for index in range(40)
    ]
    for extra in (0, 1, 2, 3):
        config = _config(limits={"max_expressions_per_pass": 11, "max_aggregate_passes": 1})
        plan = plan_aggregates(
            profiled,
            config=config,
            capabilities={},
            scope="full_table",
            deep_candidates=deep,
            max_extra_passes=extra,
        )
        assert plan["extra_passes"] <= extra
        assert all(cost <= 11 for cost in _pass_costs(plan))
    # When the fragmented packing still fits deep.max_extra_passes, nothing is dropped.
    config = _config(limits={"max_expressions_per_pass": 11, "max_aggregate_passes": 1})
    plan = plan_aggregates(
        profiled,
        config=config,
        capabilities={},
        scope="full_table",
        deep_candidates=deep[:21],
        max_extra_passes=5,
    )
    assert plan["deep_dropped"] == []
    assert plan["extra_passes"] == 3
