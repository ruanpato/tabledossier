"""Job summary: the small JSON returned with dbutils.notebook.exit (jobs.exit_summary)."""

import ast
import copy
import json

import pytest

from tabledossier.config import default_config, resolve_parameters, widget_defaults
from tabledossier.jsonutil import canonical_json
from tabledossier.notebook import CELL_SEPARATOR, generate_notebook
from tabledossier.package import job_summary
from tabledossier.resources import load_schema
from tabledossier.schemacheck import schema_errors
from tabledossier.validation import check_document


def test_summary_of_the_deep_demo_profile(deep_profile):
    summary = job_summary(deep_profile, "/Volumes/demo/analytics/results/run", [])
    assert check_document(summary, "job_summary") == []
    assert schema_errors(summary, load_schema("job_summary")) == []
    run = deep_profile["run"]
    counts = deep_profile["summary"]
    assert summary["kind"] == "tabledossier.job_summary"
    assert summary["run_id"] == run["run_id"] and summary["status"] == run["status"]
    assert summary["analysis_level"] == "deep"
    assert summary["run_dir"] == "/Volumes/demo/analytics/results/run"
    assert summary["profile_valid"] is True
    assert summary["tables"]["total"] == counts["tables_total"] == len(deep_profile["tables"])
    assert summary["checks"] == counts["checks"]
    assert summary["relationships"] == counts["relationships"]
    assert summary["relationship_hypotheses"] == len(
        deep_profile["relationship_hypotheses"]["hypotheses"]
    )


def test_summary_is_small_whatever_the_number_of_tables(deep_profile):
    many = copy.deepcopy(deep_profile)
    many["tables"] = many["tables"] * 50
    many["summary"]["tables_total"] = len(many["tables"])
    one = canonical_json(job_summary(deep_profile, "/x", []))
    fifty = canonical_json(job_summary(many, "/x", []))
    assert len(fifty) - len(one) <= 2, "only counts: the size does not grow with the tables"
    assert len(one) < 1024
    for table in deep_profile["tables"]:
        assert table["table_key"] not in one, "no table names"


def test_summary_of_an_invalid_profile_and_of_a_standard_run(demo_profile):
    summary = job_summary(demo_profile, "/x", ["$.run: something"])
    assert summary["profile_valid"] is False
    assert summary["analysis_level"] == "standard"
    assert set(summary["relationships"]) == {"validated", "violated", "not_validated"}
    assert check_document(summary, "job_summary") == []


def test_the_summary_schema_is_strict(deep_profile):
    summary = job_summary(deep_profile, "/x", [])
    for mutate in (
        lambda s: s.update(extra=1),
        lambda s: s["tables"].update(total=-1),
        lambda s: s.update(status="running"),
        lambda s: s.pop("run_dir"),
    ):
        document = copy.deepcopy(summary)
        mutate(document)
        assert check_document(document, "job_summary") != []
        assert schema_errors(document, load_schema("job_summary")) != []


def test_exit_summary_is_on_by_default_and_configurable():
    config = default_config()
    assert config["jobs"] == {"exit_summary": True}
    widgets = {**widget_defaults(config), "tables_json": '["demo.analytics.orders"]'}
    widgets["output_dir"] = "/Volumes/demo/analytics/results"
    widgets["config_json"] = json.dumps({"jobs": {"exit_summary": False}})
    effective, sources = resolve_parameters(config, widgets, load_schema("config"))
    assert effective["jobs"]["exit_summary"] is False
    assert sources["config_json_overrides"] == ["jobs"]
    widgets["config_json"] = json.dumps({"jobs": {"exit": True}})
    with pytest.raises(ValueError, match="exit"):
        resolve_parameters(config, widgets, load_schema("config"))


def test_the_notebook_ends_with_the_exit_call_outside_any_try_block():
    notebook, _ = generate_notebook(default_config())
    last = notebook.rstrip("\n").split(CELL_SEPARATOR)[-1]
    assert last.startswith("# DBTITLE 1,Job summary")
    tree = ast.parse(last)
    assert not any(isinstance(node, ast.Try) for node in ast.walk(tree))
    exits = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "dbutils.notebook.exit"
    ]
    assert len(exits) == 1
    assert isinstance(tree.body[-1], ast.If), "exit is the last statement of the notebook"
    assert notebook.count("dbutils.notebook.exit(") == 1
