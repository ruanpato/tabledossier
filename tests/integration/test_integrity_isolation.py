"""Unexpected failures of the Deep II stages never lose the run or a table's profile."""

import json
from pathlib import Path

from tabledossier.config import validate_config
from tabledossier.contract import validate_profile
from tabledossier.resources import load_schema
from tabledossier.runtime import databricks as runtime_databricks
from tabledossier.runtime import spark as runtime_spark

DEMO_CONFIG = Path(__file__).resolve().parents[2] / "examples" / "demo" / "demo.config.json"


def test_unexpected_failures_are_isolated(spark, demo_tables, tmp_path, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(runtime_spark, "_sp_uniqueness", boom)
    monkeypatch.setattr(runtime_databricks, "validate_relationships", boom)
    monkeypatch.setattr(runtime_databricks, "evaluate_hypotheses", boom)
    schemas = {"config": load_schema("config"), "profile": load_schema("profile")}
    config = validate_config(json.loads(DEMO_CONFIG.read_text(encoding="utf-8")), schemas["config"])
    ctx = runtime_databricks.prepare_run(
        spark=spark,
        widget_values={
            "tables_json": json.dumps(demo_tables[:2]),
            "analysis_level": "deep",
            "output_dir": str(tmp_path / "results"),
            "config_json": "{}",
        },
        generated_config=config,
        schemas=schemas,
        generation={"generator_version": "test", "generation_id": None},
    )
    profile = runtime_databricks.execute_run(spark, ctx, log=lambda message: None)
    assert validate_profile(profile, schemas["profile"]) == []
    for table in profile["tables"]:
        assert table["status"] == "partial" and table["uniqueness"] is None
        assert table["field_profiles"] and table["table_metrics"], "the profile itself is kept"
        assert any(error["stage"] == "assemble" for error in table["errors"])
    assert profile["relationships"]
    for relationship in profile["relationships"]:
        assert relationship["validation"] == "not_validated"
        assert "failed unexpectedly" in relationship["validation_detail"]["reason"]
    assert "failed unexpectedly" in profile["relationship_hypotheses"]["reason"]
    assert profile["referential_validation"]["planned"] == 0
