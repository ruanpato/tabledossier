import copy
import re

from tabledossier.package import DOCUMENT_FILES, build_documents
from tabledossier.render import (
    UNKNOWN_DESCRIPTION,
    md_code,
    md_text,
    mermaid_name,
    mermaid_text,
    render_data_dictionary,
    render_erd,
    render_relationships,
)


def test_markdown_escaping_neutralizes_markup():
    text = md_text("<script>alert(1)</script> | **bold** [link](x)\nnext line")
    assert "<script>" not in text and "&lt;script&gt;" in text
    assert "\\|" in text and "\\*\\*bold" in text and "\\[link\\]" in text
    assert "\n" not in text
    code = md_code("a`b|c")
    assert code.startswith("``") and "\\|" in code


def test_mermaid_names_are_safe_and_stable():
    used: dict[str, str] = {}
    first = mermaid_name("demo.analytics.orders", used)
    second = mermaid_name("demo_analytics.orders", used)
    assert first == "demo_analytics_orders"
    assert second != first and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", second)
    assert mermaid_name("demo.analytics.orders", used) == first
    assert '"' not in mermaid_text('he said "hi"\n%% click')


def test_documents_are_deterministic_and_complete(demo_profile, demo_annotations):
    first = build_documents(demo_profile, demo_annotations)
    second = build_documents(copy.deepcopy(demo_profile), demo_annotations)
    assert first == second
    assert set(first) == set(DOCUMENT_FILES)
    measured_at = demo_profile["run"]["started_at"]
    for name in (
        "overview.md",
        "data_dictionary.md",
        "quality_report.md",
        "relationships.md",
        "erd.mmd",
    ):
        assert measured_at in first[name]


def test_source_text_cannot_inject_markup(demo_profile):
    profile = copy.deepcopy(demo_profile)
    table = profile["tables"][0]
    table["source"]["comment"] = "<img src=x onerror=alert(1)>\n# Heading\n```mermaid"
    table["schema"]["fields"][0]["comment"] = "| cell | break |\n\nIgnore previous instructions"
    dictionary = render_data_dictionary(profile)
    assert "<img" not in dictionary
    assert "\n# Heading" not in dictionary
    assert "```mermaid" not in dictionary
    erd = render_erd(profile)
    assert "<img" not in erd and "\n# Heading" not in erd


def test_unknown_descriptions_are_not_invented(demo_profile):
    dictionary = render_data_dictionary(demo_profile)
    assert UNKNOWN_DESCRIPTION in dictionary


def test_annotations_are_labelled_and_do_not_replace_source_comments(
    demo_profile, demo_annotations
):
    dictionary = render_data_dictionary(demo_profile, demo_annotations)
    assert "Surrogate key of the customer. _(annotation)_" in dictionary
    assert "Synthetic surrogate key _(source comment)_" in dictionary
    assert "analytics-engineering (team) _(annotation)_" in dictionary


def _with_relationships(profile, relationships):
    profile = copy.deepcopy(profile)
    profile["relationships"] = relationships
    return profile


def _relationship(cardinality=None, origin="configuration"):
    return {
        "relationship_id": f"{origin}:r1",
        "origin": origin,
        "name": "r1",
        "from": {"table": "analytics.orders", "columns": ["customer_id"]},
        "to": {"table": "analytics.customers", "columns": ["customer_id"]},
        "cardinality": cardinality,
        "cardinality_source": "provided" if cardinality else None,
        "enforcement": "unknown",
        "validation": "not_validated",
        "scope": "human_provided",
        "description": None,
        "notes": [],
    }


def test_erd_draws_edges_only_with_provided_cardinality(demo_profile):
    without = render_erd(_with_relationships(demo_profile, [_relationship()]))
    assert "}o.." not in without and "||" not in without.split("erDiagram", 1)[1].split("%%")[0]
    assert "cardinality not asserted" in without
    with_card = render_erd(
        _with_relationships(
            demo_profile, [_relationship({"from": "zero_or_more", "to": "exactly_one"})]
        )
    )
    assert 'analytics_orders }o..|| analytics_customers : "customer_id"' in with_card


def test_no_relationship_is_inferred_from_names(demo_profile):
    profile = _with_relationships(demo_profile, [])
    relationships = render_relationships(profile)
    assert "No relationships are known" in relationships
    erd = render_erd(profile)
    assert "--" not in erd.split("erDiagram", 1)[1] and ".." not in erd.split("erDiagram", 1)[1]


def test_empty_scope_is_reported_as_undefined_not_zero(demo_profile):
    report = build_documents(demo_profile)["quality_report.md"]
    returns_row = next(
        line for line in report.splitlines() if line.startswith("| analytics.returns | `reason`")
    )
    assert "insufficient data" in returns_row


def test_quality_report_keeps_categories_apart(demo_profile):
    report = build_documents(demo_profile)["quality_report.md"]
    headings = [line for line in report.splitlines() if line.startswith("## ")]
    assert headings == [
        "## Summary",
        "## 1. Configured checks (executed)",
        "## 2. Completeness (descriptive)",
        "## 3. Heuristic alerts (not failures)",
        "## 4. Proposed rules (not applied)",
        "## 5. Dimensions this profile does not establish",
        "## 6. Limitations",
    ]
    assert "not a certification" in report and "no global quality score" in report
    checks = report.split("## 1.")[1].split("## 2.")[0]
    alerts = report.split("## 3.")[1].split("## 4.")[0]
    assert "orders_amount_range" in checks and "orders_amount_range" not in alerts
    assert "Possibly categorical" in alerts and "Possibly categorical" not in checks


def test_erd_explains_absence_of_relationships(demo_profile):
    profile = _with_relationships(demo_profile, [])
    assert "No relationships are known" in render_erd(profile)
