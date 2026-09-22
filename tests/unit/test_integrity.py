"""Referential validation: selection, type compatibility, status rules and rendering."""

import copy

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.integrity import (
    compatible_kinds,
    end_segments,
    not_validated_detail,
    referential_requested,
    referential_summary,
    type_compatibility,
    validation_detail,
)
from tabledossier.metrics import metric_value
from tabledossier.package import build_documents


def _node(kind, physical=None, path="c", **extra):
    return {
        "display_path": path,
        "type": {"kind": kind, "physical_type": physical or kind, **extra},
    }


@pytest.mark.parametrize(
    ("left", "right", "ok"),
    [
        (_node("integer", "int"), _node("integer", "bigint"), True),
        (_node("integer"), _node("decimal", "decimal(12,0)", scale=0), True),
        (_node("string"), _node("string", "varchar(10)"), True),
        (_node("date"), _node("date"), True),
        (_node("float", "double"), _node("float", "float"), True),
        (_node("float", "double"), _node("integer"), False),
        (_node("string"), _node("integer"), False),
        (_node("timestamp"), _node("timestamp_ntz"), False),
        (_node("struct"), _node("struct"), False),
        (_node("array", "array<int>"), _node("array", "array<int>"), False),
    ],
)
def test_type_compatibility_is_conservative(left, right, ok):
    assert compatible_kinds(left, right)[0] is ok
    record = type_compatibility([left], [right])[0]
    assert record["compatible"] is ok and record["rule"]
    assert record["from_type"] == left["type"]["physical_type"]


def _relationship(origin, columns=("a.b",)):
    return {
        "origin": origin,
        "from": {"table": "s.t", "columns": list(columns)},
        "to": {"table": "s.u", "columns": list(columns)},
    }


def test_end_segments_keep_literal_declared_names():
    declared = end_segments(_relationship("declared_constraint"), "from")
    configured = end_segments(_relationship("configuration", ["`a.b`", "x.y"]), "from")
    assert declared == [[{"kind": "field", "name": "a.b"}]]
    assert configured == [
        [{"kind": "field", "name": "a.b"}],
        [{"kind": "field", "name": "x"}, {"kind": "field", "name": "y"}],
    ]


def test_relationships_are_validated_only_when_requested():
    standard = default_config()
    deep = deep_merge(default_config(), {"analysis_level": "deep"})
    configured = _relationship("configuration")
    declared = _relationship("declared_constraint")
    assert "deep level" in referential_requested(configured, standard)
    assert "configured = false" in referential_requested(configured, deep)
    assert "declared = false" in referential_requested(declared, deep)
    both = deep_merge(deep, {"deep": {"referential": {"configured": True, "declared": True}}})
    assert referential_requested(configured, both) == ""
    assert referential_requested(declared, both) == ""
    assert referential_requested(_relationship("annotation"), both)


SIDE = {"table": "s.t", "scope": "full_snapshot", "consistency_mode": "pinned_delta_version"}


def _detail(raw, mode="full_scope"):
    return validation_detail(
        {"t_rows": 10, "t_distinct": 10, **raw},
        mode=mode,
        sample_rows=50 if mode == "sample" else None,
        from_info={**SIDE, "delta_version": 3},
        to_info={**SIDE, "table": "s.u", "delta_version": 7},
        compatibility=[],
        operation_id="op_referential_1",
    )


@pytest.mark.parametrize(
    ("raw", "mode", "status"),
    [
        ({"rows": 100, "null_rows": 4, "orphans": 0}, "full_scope", "validated"),
        ({"rows": 100, "orphans": 2}, "full_scope", "violated"),
        ({"rows": 50, "orphans": 1}, "sample", "violated"),
        ({"rows": 50, "orphans": 0}, "sample", "not_validated"),
        ({"rows": 5, "null_rows": 5}, "full_scope", "not_validated"),
        ({"rows": 0}, "full_scope", "not_validated"),
    ],
)
def test_status_follows_orphans_and_mode(raw, mode, status):
    detail = _detail(raw, mode)
    assert detail["status"] == status
    assert (detail["reason"] is None) is (status != "not_validated")
    metrics = detail["metrics"]
    complete = metric_value(metrics, "source_rows_with_complete_key")
    assert complete == raw["rows"] - raw.get("null_rows", 0)
    orphans = next(m for m in metrics if m["name"] == "orphan_rows")
    assert orphans["value"] == raw.get("orphans", 0)
    assert orphans.get("denominator") == (complete or None)
    ratio = next(m for m in metrics if m["name"] == "orphan_ratio")
    assert ratio["status"] == ("measured" if complete else "insufficient_data")
    scopes = {m["scope"] for m in metrics if m["name"].startswith("source")}
    assert scopes == {"sample" if mode == "sample" else "full_snapshot"}


def test_target_uniqueness_is_evidence_not_cardinality():
    unique = _detail({"rows": 10, "t_dup_groups": 0})
    assert unique["target_key_unique"] is True
    duplicated = _detail({"rows": 10, "t_dup_groups": 2})
    assert duplicated["target_key_unique"] is False
    assert "cardinality" not in duplicated
    unpinned = validation_detail(
        {"rows": 1},
        mode="full_scope",
        sample_rows=None,
        from_info={**SIDE, "consistency_mode": "unpinned", "delta_version": None},
        to_info={**SIDE, "delta_version": 1},
        compatibility=[],
        operation_id="op_referential_1",
    )
    assert any("not pinned" in note for note in unpinned["limitations"])
    assert unpinned["target_key_unique"] is None, "no target key was measured"


def test_referential_summary_counts_statuses():
    config = deep_merge(default_config(), {"analysis_level": "deep"})
    relationships = [{"validation": s} for s in ("validated", "violated", "violated", "x")]
    summary = referential_summary(relationships, config, planned=3, limited=[])
    assert (summary["validated"], summary["violated"], summary["planned"]) == (1, 2, 3)
    assert summary["mode"] == "full_scope"


def _violate(profile):
    """Mark the demo's first configured relationship as violated with synthetic counts."""
    profile = copy.deepcopy(profile)
    profile["run"]["analysis_level"] = "deep"
    relationship = profile["relationships"][0]
    detail = _detail({"rows": 1000, "orphans": 5})
    relationship["validation"], relationship["validation_detail"] = "violated", detail
    source = next(t for t in profile["tables"] if t["table_key"] == relationship["from"]["table"])
    source["operations"]["planned"].append(
        {
            "operation_id": "op_referential_1",
            "kind": "referential_check",
            "description": "test",
            "reads_user_data": True,
        }
    )
    return profile, relationship


def test_documents_show_validation_evidence(demo_profile):
    documents = build_documents(demo_profile)
    assert "did not validate any relationship" in documents["relationships.md"]
    assert "Referential integrity** is not verified" in documents["quality_report.md"]
    profile, relationship = _violate(demo_profile)
    documents = build_documents(profile)
    relationships = documents["relationships.md"]
    assert "1 relationship(s) were checked against the data" in relationships
    assert "**violated**: 5 orphan row(s) (0.50%)" in relationships
    report = documents["quality_report.md"]
    section = report.split("## 8. Referential integrity")[1].split("## 9. Limitations")[0]
    assert f"`{relationship['name']}`" in section and "from v3, to v7" in section
    assert "established only for the 1 relationship(s)" in report


def test_not_validated_detail_always_states_a_reason():
    detail = not_validated_detail("because")
    assert detail["status"] == "not_validated" and detail["reason"] == "because"
    assert detail["metrics"] == [] and detail["operation_id"] is None


# --------------------------------------------------------------------------- hypotheses


def _field(name, kind, metrics, physical=None, **extra):
    path = [{"kind": "field", "name": name}]
    node = {
        "field_id": f"f_{abs(hash(name)) % 10**12:012d}",
        "display_path": name,
        "path": path,
        "type": {"kind": kind, "physical_type": physical or kind, **extra},
        "children": [],
    }
    profile = {
        "field_id": node["field_id"],
        "display_path": name,
        "profiled": True,
        "element_context": None,
        "metrics": [
            {"name": key, "status": "measured", "value": value} for key, value in metrics.items()
        ],
    }
    return node, profile


def _table(name, fields, unique=()):
    nodes = [node for node, _ in fields]
    return {
        "table_key": name,
        "identifier": {"parts": name.split(".")},
        "status": "succeeded",
        "schema": {"fields": nodes},
        "field_profiles": [profile for _, profile in fields],
        "uniqueness": {
            "keys": [
                {
                    "key_id": "k_" + node["field_id"][2:],
                    "field_ids": [node["field_id"]],
                    "status": "measured",
                    "outcome": "unique",
                }
                for node in nodes
                if node["display_path"] in unique
            ]
        },
    }


def _hypothesis_tables(names):
    """Parents with a unique id; children with a contained, an overlapping and a disjoint column."""
    pid, ref, other, far, text, flag = names
    parents = _table(
        "s.parents",
        [
            _field(pid, "integer", {"non_null_count": 50, "min": 1, "max": 50}),
            _field(text, "string", {"non_null_count": 50, "min_length": 3, "max_length": 5}),
        ],
        unique=(pid, text),
    )
    children = _table(
        "s.children",
        [
            _field(ref, "integer", {"non_null_count": 90, "min": 2, "max": 40}),
            _field(other, "integer", {"non_null_count": 90, "min": 30, "max": 90}),
            _field(far, "integer", {"non_null_count": 90, "min": 100, "max": 200}),
            _field(flag, "float", {"non_null_count": 90, "min": 1, "max": 5}, "double"),
            _field("empty", "integer", {"non_null_count": 0}),
        ],
    )
    return [parents, children]


def _hconfig(**settings):
    return deep_merge(
        default_config(),
        {
            "analysis_level": "deep",
            "deep": {"relationship_hypotheses": {"enabled": True, **settings}},
        },
    )


def _pairs(plan):
    return [(p["from_node"]["display_path"], p["to_node"]["display_path"]) for p in plan["pairs"]]


def test_hypothesis_pairs_come_from_types_and_ranges():
    from tabledossier.integrity import plan_hypotheses

    tables = _hypothesis_tables(["pid", "parent_ref", "misc", "far", "code", "ratio"])
    plan = plan_hypotheses(tables, set(), _hconfig())
    assert _pairs(plan) == [("parent_ref", "pid"), ("misc", "pid")], "contained range first"
    assert [p["range_relation"] for p in plan["pairs"]] == ["contained", "overlapping"]
    assert plan["targets"] == 2 and plan["disjoint_excluded"] == 1
    assert plan["considered"] == 2 and plan["not_evaluated"] == 0


def test_hypotheses_never_use_column_names():
    from tabledossier.integrity import plan_hypotheses

    plain = plan_hypotheses(
        _hypothesis_tables(["pid", "parent_ref", "misc", "far", "code", "ratio"]), set(), _hconfig()
    )
    # The same data with misleading names: "pid" everywhere would suggest a join by name.
    renamed = plan_hypotheses(
        _hypothesis_tables(["id", "x1", "pid", "parent_id", "name", "pid_ratio"]), set(), _hconfig()
    )
    shape = [
        [(p["range_relation"], p["from_table_index"]) for p in plan["pairs"]]
        for plan in (plain, renamed)
    ]
    assert shape[0] == shape[1]
    assert _pairs(renamed) == [("x1", "id"), ("pid", "id")]


def test_known_relationships_and_budget_are_respected():
    from tabledossier.integrity import plan_hypotheses
    from tabledossier.paths import table_lookup_key

    tables = _hypothesis_tables(["pid", "parent_ref", "misc", "far", "code", "ratio"])
    ref = tables[1]["schema"]["fields"][0]["field_id"]
    pid = tables[0]["schema"]["fields"][0]["field_id"]
    known = {
        (table_lookup_key(["s", "children"]), (ref,), table_lookup_key(["s", "parents"]), (pid,))
    }
    plan = plan_hypotheses(tables, known, _hconfig(max_pairs=0))
    assert plan["known_excluded"] == 1 and plan["pairs"] == [] and plan["not_evaluated"] == 1


def test_documents_list_hypotheses_apart_and_never_draw_them(deep_profile):
    from tabledossier.integrity import hypotheses_record, hypothesis_evidence, hypothesis_item

    profile = copy.deepcopy(deep_profile)
    customers = next(t for t in profile["tables"] if t["table_key"] == "analytics.customers")
    node = next(n for n in customers["schema"]["fields"] if n["display_path"] == "customer_id")
    metrics, _ = hypothesis_evidence(
        {"rows": 60, "null_rows": 10, "orphans": 0, "t_rows": 500, "t_distinct": 500},
        scope="full_snapshot",
        target_scope="full_snapshot",
    )
    side = {
        "table": "analytics.customers",
        "scope": "full_snapshot",
        "consistency_mode": "pinned_delta_version",
        "delta_version": 1,
    }
    item = hypothesis_item(
        1,
        {"from_node": node, "to_node": node},
        source_table=customers,
        target_table=customers,
        evidence={
            "inclusion_scope": "full_scope",
            "from": side,
            "to": side,
            "metrics": metrics,
            "target_key_id": "k_" + "0" * 12,
            "target_key_unique": True,
            "type_compatibility": type_compatibility([node], [node]),
            "range_relation": "contained",
            "range_basis": "values",
        },
        operation_id="op_hypothesis_1",
        sample_rows=None,
    )
    config = _hconfig(inclusion_scope="full_scope")
    profile["relationship_hypotheses"] = hypotheses_record(
        config, None, [item], evaluated=1, rejected={"below_threshold": 2}, reason=None
    )
    documents = build_documents(profile)
    text = documents["relationships.md"]
    section = text.split("## Hypotheses (data-driven, not relationships)")[1]
    assert "`hyp_1`" in section and "100.00% (full scope)" in section and "50 of 50" in section
    assert "2 below threshold" in section
    assert (
        "hyp_1" not in documents["erd.mmd"] and "analytics_customers ||" not in documents["erd.mmd"]
    )
    disabled = copy.deepcopy(deep_profile)
    disabled["relationship_hypotheses"]["enabled"] = False
    disabled["relationship_hypotheses"]["reason"] = "disabled by configuration"
    assert (
        "Not evaluated: disabled by configuration." in build_documents(disabled)["relationships.md"]
    )
