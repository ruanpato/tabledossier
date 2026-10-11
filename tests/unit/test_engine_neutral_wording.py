"""Shared wording follows the labels of the records it describes (contract 1.3).

Spark records keep their labels, so Spark profiles and documents keep their exact text; an
exact-labelled PostgreSQL record must never be described as approximate, HyperLogLog-based, read
at a Delta version or declared in Unity Catalog.
"""

import copy
import re

import pytest

from tabledossier.assemble import aggregate_metric
from tabledossier.integrity import type_compatibility, validation_detail
from tabledossier.jsonutil import pretty_json
from tabledossier.package import build_documents
from tabledossier.planning import iter_nodes
from tabledossier.render import render_overview, render_quality_report, render_relationships

FORBIDDEN = {
    "Unity": re.compile(r"Unity"),
    "HyperLogLog": re.compile(r"HyperLogLog"),
    "percentile_approx": re.compile(r"percentile_approx"),
    "try_parse_json": re.compile(r"try_parse_json"),
    "Delta version": re.compile(r"Delta version"),
    # A word, not a configuration key: ``deep.max_explode_rows`` is part of the effective config.
    "explode": re.compile(r"\bexplode\b"),
    "≈": re.compile("≈"),
}
# The large-array alert advises on downstream processing of the data (any consumer), not on how
# the profile was computed; it is engine-neutral and its Spark text must not change.
DOWNSTREAM_ADVICE = "downstream explode/flatten operations"


def _texts(profile):
    return {"profile.json": pretty_json(profile), **build_documents(profile)}


def _found(texts):
    found = []
    for name, text in texts.items():
        text = text.replace(DOWNSTREAM_ADVICE, "")
        for word, pattern in FORBIDDEN.items():
            if pattern.search(text):
                found.append((name, word))
    return found


def _wording(profile):
    """Every finding, rule and candidate-role text of a profile, with its field's metrics."""
    for table in profile["tables"]:
        fields = {f["field_id"]: f for f in table["field_profiles"]}
        for finding in table["findings"]:
            text = " ".join(
                finding[k] for k in ("title", "message", "severity_reason", "limitations")
            )
            yield text, fields[finding["field_id"]]["metrics"]
        for rule in table["suggested_rules"]:
            yield rule["rationale"], []
        for field in table["field_profiles"]:
            for role in (field.get("semantics") or {}).get("candidate_roles", []):
                yield role["limitations"], field["metrics"]


def test_postgresql_profile_and_documents_carry_no_spark_wording(postgres_profile):
    assert _found(_texts(postgres_profile)) == []


def test_no_approximate_wording_for_exact_metrics(postgres_profile):
    texts = list(_wording(postgres_profile))
    assert any("Exact distinct count" in text for text, _ in texts)
    for text, metrics in texts:
        assert "pproximat" not in text, text
        assert all(m.get("accuracy") != "approximate" for m in metrics)
    docs = build_documents(postgres_profile)
    assert "≈" not in docs["data_dictionary.md"]
    assert "exact distinct counts show whether a value repeats" in docs["quality_report.md"]


def test_spark_documents_keep_their_wording(demo_profile, deep_profile):
    assert _found(_texts(demo_profile)), "sanity: the Spark demo uses these words"
    docs = build_documents(demo_profile)
    assert "| ≈ " in docs["data_dictionary.md"]
    assert "approximated (HyperLogLog-based distinct counts)" in docs["quality_report.md"]
    assert "Approximate distinct count is close" in docs["quality_report.md"]
    assert (
        "Declared PRIMARY KEY/FOREIGN KEY constraints in Unity Catalog"
        in (docs["relationships.md"])
    )
    assert "Delta version" in docs["overview.md"]
    deep = build_documents(deep_profile)["quality_report.md"]
    assert "Element distinct counts come from one explode pass" in deep
    assert "the target is read in full at its recorded version" in deep
    assert "approximate (HyperLogLog-based) distinct counts" in deep


def test_overview_describes_a_local_cli_run(postgres_profile):
    overview = render_overview(postgres_profile)
    assert "local\\_cli, PostgreSQL 17.11, Python " in overview
    assert "| Generated notebook | not applicable (local CLI run) |" in overview
    assert "transaction snapshot" in overview


# --------------------------------------------------------------------------- metric labels

_SPEC = {"alias": "a1", "metric": "approx_distinct_count", "op": "approx_distinct"}


def _quantile_spec(**extra):
    return {
        "alias": "a2",
        "metric": "quantiles",
        "op": "quantiles",
        "params": {"probabilities": [0.5], "accuracy": 10000},
        **extra,
    }


def test_spark_specs_without_labels_are_unchanged():
    metric = aggregate_metric(
        {**_SPEC, "params": {"rsd": 0.02}}, 42, "string", scope="full_table", denominators={}
    )
    assert metric == {
        "name": "approx_distinct_count",
        "status": "measured",
        "value": 42,
        "value_type": "integer",
        "unit": "values",
        "scope": "full_table",
        "accuracy": "approximate",
        "source": "aggregate",
        "method": "approx_count_distinct (HyperLogLog++)",
        "details": {"relative_standard_deviation": 0.02},
    }
    quantiles = aggregate_metric(_quantile_spec(), [7], "integer", scope="s", denominators={})
    assert quantiles["accuracy"] == "approximate"
    assert quantiles["method"] == "percentile_approx over non-null (finite) values"
    assert quantiles["details"] == {"element_type": "integer", "accuracy_parameter": 10000}


def test_labels_override_accuracy_method_unit_and_details():
    labels = {
        "accuracy": "exact",
        "method": "count(DISTINCT field) over non-null values (exact)",
        "details": {"relative_standard_deviation": None},
    }
    metric = aggregate_metric(
        {**_SPEC, "params": {"rsd": 0.02}, "labels": labels},
        42,
        "string",
        scope="full_table",
        denominators={},
    )
    assert metric["accuracy"] == "exact" and metric["method"] == labels["method"]
    assert "details" not in metric, "a detail mapped to None is removed"
    quantiles = aggregate_metric(
        _quantile_spec(
            labels={
                "accuracy": "exact",
                "method": "percentile_disc over non-null (finite) values",
                "details": {"accuracy_parameter": None, "function": "percentile_disc"},
            }
        ),
        [7],
        "integer",
        scope="s",
        denominators={},
    )
    assert quantiles["accuracy"] == "exact"
    assert quantiles["details"] == {"element_type": "integer", "function": "percentile_disc"}
    length = aggregate_metric(
        {
            "alias": "a3",
            "metric": "max_length",
            "op": "max_length",
            "params": {},
            "labels": {"unit": "bytes"},
        },
        12,
        "string",
        scope="s",
        denominators={},
    )
    assert length["unit"] == "bytes" and length["accuracy"] == "exact"


def test_labels_do_not_apply_to_metrics_without_a_value():
    metric = aggregate_metric(
        {**_SPEC, "params": {"rsd": 0.02}, "labels": {"accuracy": "exact"}},
        None,
        "string",
        scope="s",
        denominators={},
    )
    assert metric["status"] == "insufficient_data" and metric["accuracy"] == "unknown"


# --------------------------------------------------------------------------- render


def _table(profile, key):
    return next(t for t in profile["tables"] if t["table_key"] == key)


def _side(table):
    return {
        "table": table["table_key"],
        "scope": table["scope"]["scope_label"],
        "consistency_mode": table["consistency"]["mode"],
        "delta_version": table["consistency"]["delta_version"],
    }


def _checked(profile):
    orders = _table(profile, "analytics.orders")
    customers = _table(profile, "analytics.customers")
    relationship = next(r for r in profile["relationships"] if r["name"] == "orders_customer")
    node = next(
        n for n in iter_nodes(orders["schema"]["fields"]) if n["display_path"] == "customer_id"
    )
    target = next(
        n for n in iter_nodes(customers["schema"]["fields"]) if n["display_path"] == "customer_id"
    )
    relationship["validation"] = "violated"
    relationship["validation_detail"] = validation_detail(
        {"rows": 10, "null_rows": 0, "orphans": 2, "t_rows": 5, "t_distinct": 5},
        mode="full_scope",
        sample_rows=None,
        from_info=_side(orders),
        to_info=_side(customers),
        compatibility=type_compatibility([node], [target]),
        operation_id="op_referential_1",
    )
    return profile


def test_referential_text_follows_the_target_consistency(postgres_profile):
    text = render_relationships(_checked(copy.deepcopy(postgres_profile)))
    assert "the target is read in full in the run's transaction snapshot" in text
    assert "from snapshot, to snapshot" in text
    assert "recorded version" not in text


@pytest.mark.parametrize(
    ("unsupported", "expected"),
    [
        (
            True,
            "Deep level part I (element and JSON path profiling) is not supported by this engine.",
        ),
        (False, "No table reached the deep analysis (see errors)."),
    ],
)
def test_deep_coverage_without_deep_tables(postgres_profile, unsupported, expected):
    profile = copy.deepcopy(postgres_profile)
    profile["run"]["analysis_level"] = "deep"
    for table in profile["tables"]:
        if unsupported:
            table["unsupported"].append(
                {"capability": "deep_elements", "detail": "not supported by this connector"}
            )
    report = render_quality_report(profile)
    section = report.split("## 6. Deep analysis: coverage and budget", 1)[1].split("## 7.")[0]
    assert section.strip() == expected
    assert "higher-order functions" not in report and "explode pass" not in report


def test_constraint_notes_follow_source_and_enforcement(postgres_profile, demo_profile):
    text = render_relationships(postgres_profile)
    assert "enforced by PostgreSQL for writes" in text and "NOT VALID" in text
    assert "Unity Catalog" not in text and "not checked by the database" not in text
    not_enforced = copy.deepcopy(postgres_profile)
    _table(not_enforced, "analytics.customers")["constraints"][0]["enforcement"] = "not_enforced"
    text = render_relationships(not_enforced)
    assert "not checked by the database" in text and "enforced by PostgreSQL" not in text
    # No constraint record: the Unity Catalog statement only for a Spark run.
    none = copy.deepcopy(postgres_profile)
    for table in none["tables"]:
        table["constraints"] = []
    text = render_relationships(none)
    assert "No declared constraints were visible." in text and "Unity" not in text
    assert "constraints in Unity Catalog are informational" in render_relationships(demo_profile)


def test_no_hyperloglog_claim_without_measured_distinct_counts(postgres_profile, demo_profile):
    profile = copy.deepcopy(postgres_profile)
    for table in profile["tables"]:
        for field in table["field_profiles"]:
            field["metrics"] = [m for m in field["metrics"] if m["name"] != "approx_distinct_count"]
    report = render_quality_report(profile)
    assert "HyperLogLog" not in report
    assert "no column distinct count was measured" in report
    assert "HyperLogLog" in render_quality_report(demo_profile)


def test_referential_summary_note_without_checked_targets(postgres_profile):
    from tabledossier.integrity import referential_summary

    config = postgres_profile["run"]["effective_config"]
    snapshot = referential_summary(
        [], config, planned=0, limited=[], target_mode="transaction_snapshot"
    )
    assert "transaction snapshot" in snapshot["notes"][0]
    assert "recorded version" in referential_summary([], config, planned=0, limited=[])["notes"][0]


def test_unknown_enforcement_is_not_described_as_not_enforced(postgres_profile):
    profile = copy.deepcopy(postgres_profile)
    _table(profile, "analytics.customers")["constraints"][0]["enforcement"] = "unknown"
    text = render_relationships(profile)
    assert "enforcement `unknown`" in text
    assert "marked `not_enforced`" not in text
