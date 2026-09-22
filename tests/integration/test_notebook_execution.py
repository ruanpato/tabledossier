"""Execute the generated notebook file end to end with a local SparkSession."""

import json
from pathlib import Path

import pytest
from run_notebook_locally import run_notebook

from tabledossier.cli import main
from tabledossier.config import ConfigError
from tabledossier.package import PACKAGE_FILES, build_documents, job_summary
from tabledossier.validation import check_document, check_profile

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def notebook(tmp_path_factory):
    out = tmp_path_factory.mktemp("generated") / "profile_databricks.py"
    code = main(
        [
            "generate",
            "--config",
            str(ROOT / "examples" / "demo" / "demo.config.json"),
            "--output",
            str(out),
        ]
    )
    assert code == 0
    return out


def _widgets(tables, output_dir, **extra):
    values = {
        "tables_json": json.dumps(tables),
        "analysis_level": "standard",
        "output_dir": str(output_dir),
        "config_json": "{}",
    }
    values.update(extra)
    return values


def test_notebook_run_exports_a_valid_consistent_package(
    spark, spark_mode, demo_tables, notebook, tmp_path
):
    tables = [*demo_tables, "analytics.does_not_exist"]
    namespace = run_notebook(notebook, spark, _widgets(tables, tmp_path / "results"))
    export = namespace["td_export"]
    run_dir = Path(export["run_dir"])
    assert export["validation_errors"] == []
    assert sorted(p.name for p in run_dir.iterdir()) == sorted(PACKAGE_FILES)

    profile = json.loads((run_dir / "profile.json").read_text(encoding="utf-8"))
    assert check_profile(profile) == []
    assert profile["run"]["status"] == "partial"
    statuses = {t["table_key"]: t["status"] for t in profile["tables"]}
    assert statuses["analytics.does_not_exist"] == "failed"
    assert all(
        status == "succeeded"
        for key, status in statuses.items()
        if key != "analytics.does_not_exist"
    )
    failed = next(t for t in profile["tables"] if t["table_key"] == "analytics.does_not_exist")
    assert failed["errors"][0]["condition"] == "TABLE_OR_VIEW_NOT_FOUND"
    assert profile["run"]["environment"]["execution_context"] == "spark"
    # Recorded from the session the notebook actually received (Spark Connect or classic).
    assert profile["run"]["environment"]["spark_connect"] is (spark_mode == "connect")
    assert profile["run"]["generation"]["generation_id"].startswith("sha256:")

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert check_document(manifest, "manifest") == []
    assert manifest["status"] == "partial"
    assert manifest["profile_validation"] == {"checked": True, "valid": True, "errors": []}
    suggested = json.loads((run_dir / "suggested_rules.json").read_text(encoding="utf-8"))
    assert check_document(suggested, "suggested_rules") == []

    # Documents regenerated offline from the exported profile are identical to the notebook's.
    regenerated = build_documents(profile)
    for name, text in regenerated.items():
        assert (run_dir / name).read_text(encoding="utf-8") == text, name

    widgets = namespace["dbutils"].widgets
    assert widgets.created == [], "widgets passed in (as by a Job) must not be recreated"

    # The last cell returned the job summary with dbutils.notebook.exit, once.
    [value] = namespace["dbutils"].notebook.exits
    summary = json.loads(value)
    assert check_document(summary, "job_summary") == []
    assert summary == job_summary(profile, export["run_dir"], [])
    assert summary["status"] == "partial" and summary["run_dir"] == str(run_dir)
    assert summary["tables"] == {"total": 5, "succeeded": 4, "partial": 0, "failed": 1}
    assert namespace["td_exit_value"] == value
    assert "Returning the job summary" in namespace["td_exit_message"]


def test_job_summary_can_be_disabled_and_needs_dbutils_notebook(
    spark, demo_tables, notebook, tmp_path
):
    off = run_notebook(
        notebook,
        spark,
        _widgets(
            demo_tables[:1],
            tmp_path / "off",
            config_json=json.dumps({"jobs": {"exit_summary": False}}),
        ),
    )
    assert off["dbutils"].notebook.exits == []
    assert off["td_exit_value"] is None
    assert "jobs.exit_summary = false" in off["td_exit_message"]
    assert off["td_profile"]["run"]["effective_config"]["jobs"] == {"exit_summary": False}

    absent = run_notebook(
        notebook, spark, _widgets(demo_tables[:1], tmp_path / "absent"), notebook_utils=False
    )
    assert not hasattr(absent["dbutils"], "notebook")
    assert absent["td_exit_value"] is None
    assert "not available" in absent["td_exit_message"]
    assert absent["td_export"]["validation_errors"] == [], "the run itself is unaffected"


def test_render_cli_on_exported_profile(spark, demo_tables, notebook, tmp_path):
    namespace = run_notebook(notebook, spark, _widgets(demo_tables[:1], tmp_path / "results"))
    profile_path = Path(namespace["td_export"]["run_dir"]) / "profile.json"
    assert main(["validate", "--profile", str(profile_path)]) == 0
    out = tmp_path / "docs"
    assert main(["render", "--input", str(profile_path), "--output", str(out)]) == 0
    assert (out / "data_dictionary.md").read_text(encoding="utf-8") == (
        profile_path.parent / "data_dictionary.md"
    ).read_text(encoding="utf-8")


def test_missing_parameters_fail_before_reading_data(spark, notebook, tmp_path):
    # The notebook defines its own copy of ConfigError (embedded runtime), so compare by name.
    with pytest.raises(Exception) as excinfo:
        run_notebook(notebook, spark, {"tables_json": "[]", "output_dir": str(tmp_path / "out")})
    assert type(excinfo.value).__name__ == ConfigError.__name__
    assert "tables_json" in str(excinfo.value)
    assert not (tmp_path / "out").exists()


def test_unwritable_destination_fails_before_reading_data(spark, demo_tables, notebook, tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("not a directory", encoding="utf-8")
    namespace_error = None
    try:
        run_notebook(notebook, spark, _widgets(demo_tables[:1], blocker / "results"))
    except Exception as exc:  # noqa: BLE001 - asserting on the notebook's own exception type
        namespace_error = exc
    assert type(namespace_error).__name__ == "DestinationError"
    assert "/Volumes/" in str(namespace_error)


def test_metadata_level_notebook_run(spark, demo_tables, notebook, tmp_path):
    namespace = run_notebook(
        notebook, spark, _widgets(demo_tables, tmp_path / "results", analysis_level="metadata")
    )
    profile = namespace["td_profile"]
    assert profile["run"]["analysis_level"] == "metadata"
    for table in profile["tables"]:
        assert all(
            op["kind"] not in ("sample_collect", "aggregate_pass")
            for op in table["operations"]["planned"]
        )


def test_deep_notebook_run_exports_a_valid_1_2_package(
    spark, spark_mode, demo_tables, notebook, tmp_path
):
    namespace = run_notebook(
        notebook,
        spark,
        _widgets(
            demo_tables,
            tmp_path / "results",
            analysis_level="deep",
            config_json=json.dumps({"deep": {"max_extra_passes": 1}}),
        ),
    )
    export = namespace["td_export"]
    assert export["validation_errors"] == []
    run_dir = Path(export["run_dir"])
    profile = json.loads((run_dir / "profile.json").read_text(encoding="utf-8"))
    assert check_profile(profile) == []
    assert profile["schema_version"] == "1.2" and profile["run"]["analysis_level"] == "deep"
    assert profile["run"]["environment"]["spark_connect"] is (spark_mode == "connect")
    per_run = {"referential_check": 0, "relationship_hypothesis_check": 0}
    for table in profile["tables"]:
        deep = table["deep"]
        assert deep is not None and deep["extra_passes"]["planned"] <= 1
        reads = [op for op in table["operations"]["planned"] if op["reads_user_data"]]
        cross = [op for op in reads if op["kind"] in per_run]
        for op in cross:
            per_run[op["kind"]] += 1
        # per table: sample + 2 standard passes + 1 deep extra pass + 1 uniqueness pass (max_passes)
        assert len(reads) - len(cross) <= 1 + 2 + 1 + 1
        assert table["uniqueness"]["passes"]["planned"] <= 1
    # per run: at most max_relationships checks and max_pairs hypothesis pairs (defaults 5 and 5)
    assert 1 <= per_run["referential_check"] <= 5
    assert 1 <= per_run["relationship_hypothesis_check"] <= 5
    assert profile["referential_validation"]["planned"] == per_run["referential_check"]
    assert (
        profile["relationship_hypotheses"]["pairs_evaluated"]
        == per_run["relationship_hypothesis_check"]
    )
    statuses = {rel["name"]: rel["validation"] for rel in profile["relationships"]}
    assert statuses == {"orders_customer": "violated", "events_order": "validated"}
    hypotheses = profile["relationship_hypotheses"]["hypotheses"]
    assert [(h["from"]["columns"], h["to"]["columns"]) for h in hypotheses] == [
        (["referrer_id"], ["customer_id"])
    ]
    events = next(t for t in profile["tables"] if t["table_key"] == "analytics.order_events")
    event_id = next(k for k in events["uniqueness"]["keys"] if k["columns"] == ["event_id"])
    assert event_id["outcome"] == "duplicates"
    groups = next(m for m in event_id["metrics"] if m["name"] == "duplicate_key_groups")
    assert groups["value"] == 3, "three duplicated event ids are planted in the demo data"
    regenerated = build_documents(profile)
    for name, text in regenerated.items():
        assert (run_dir / name).read_text(encoding="utf-8") == text, name
    assert "## 6. Deep analysis: coverage and budget" in regenerated["quality_report.md"]
    assert "JSON paths (transient sample" in regenerated["data_dictionary.md"]
    plan_text = namespace["describe_plan"](namespace["td_ctx"])
    assert "deep level" in plan_text and "at most 1 extra pass(es)" in plan_text
