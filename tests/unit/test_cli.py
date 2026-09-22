import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tabledossier.cli import (
    EXIT_CHECKS,
    EXIT_FAILED_RUN,
    EXIT_INVALID,
    EXIT_IO,
    EXIT_OK,
    EXIT_PARTIAL,
    main,
)


def _write(path, data):
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def test_quickstart_commands(tmp_path, capsys):
    config = tmp_path / "profile.config.json"
    notebook = tmp_path / "dist" / "profile_databricks.py"
    assert main(["init", "--output", str(config)]) == EXIT_OK
    assert main(["validate", "--config", str(config)]) == EXIT_OK
    assert main(["generate", "--config", str(config), "--output", str(notebook)]) == EXIT_OK
    assert notebook.exists()
    manifest = json.loads(
        (tmp_path / "dist" / "profile_databricks.generation.json").read_text(encoding="utf-8")
    )
    assert manifest["notebook"]["file_name"] == "profile_databricks.py"
    out = capsys.readouterr().out
    assert "tables_json" in out


def test_init_with_tables_and_no_overwrite(tmp_path):
    config = tmp_path / "c.json"
    args = [
        "init",
        "--output",
        str(config),
        "--tables",
        "demo.analytics.orders",
        "--output-dir",
        "/Volumes/demo/analytics/v/out",
    ]
    assert main(args) == EXIT_OK
    data = json.loads(config.read_text(encoding="utf-8"))
    assert data["tables"] == ["demo.analytics.orders"]
    assert main(args) == EXIT_IO
    assert main([*args, "--force"]) == EXIT_OK
    assert (
        main(["init", "--output", str(tmp_path / "x.json"), "--tables", "bad name"]) == EXIT_INVALID
    )


def test_generate_refuses_overwrite(tmp_path):
    config = tmp_path / "c.json"
    notebook = tmp_path / "n.py"
    main(["init", "--output", str(config)])
    assert main(["generate", "--config", str(config), "--output", str(notebook)]) == EXIT_OK
    assert main(["generate", "--config", str(config), "--output", str(notebook)]) == EXIT_IO
    assert (
        main(["generate", "--config", str(config), "--output", str(notebook), "--overwrite"])
        == EXIT_OK
    )


def test_invalid_inputs_have_stable_codes(tmp_path, capsys):
    assert main(["validate", "--config", str(tmp_path / "missing.json")]) == EXIT_IO
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert main(["validate", "--config", str(broken)]) == EXIT_INVALID
    bad = _write(
        tmp_path / "bad.json",
        {"kind": "tabledossier.config", "config_version": "1.0", "secret": "x"},
    )
    assert main(["validate", "--config", bad]) == EXIT_INVALID
    err = capsys.readouterr().err
    assert "unexpected property 'secret'" in err or "Additional properties" in err
    with pytest.raises(SystemExit) as excinfo:
        main(["validate"])
    assert excinfo.value.code == 2


def test_validate_profile_statuses(tmp_path, demo_profile):
    ok = _write(tmp_path / "ok.json", demo_profile)
    assert main(["validate", "--profile", ok]) == EXIT_OK
    assert main(["validate", "--profile", ok, "--fail-on-check-failures"]) == EXIT_CHECKS

    partial = copy.deepcopy(demo_profile)
    table = partial["tables"][-1]
    table["status"] = "partial"
    table["errors"] = [{"stage": "sample", "error_class": "X", "condition": None, "message": "m"}]
    partial["run"]["status"] = "partial"
    partial["summary"]["tables_succeeded"] -= 1
    partial["summary"]["tables_partial"] += 1
    assert main(["validate", "--profile", _write(tmp_path / "p.json", partial)]) == EXIT_PARTIAL

    failed = copy.deepcopy(demo_profile)
    for table in failed["tables"]:
        table["status"] = "failed"
        table["errors"] = [
            {"stage": "resolve", "error_class": "X", "condition": None, "message": "m"}
        ]
    failed["run"]["status"] = "failed"
    failed["summary"].update(
        tables_succeeded=0, tables_partial=0, tables_failed=len(failed["tables"])
    )
    assert main(["validate", "--profile", _write(tmp_path / "f.json", failed)]) == EXIT_FAILED_RUN

    future = copy.deepcopy(demo_profile)
    future["schema_version"] = "9.0"
    assert main(["validate", "--profile", _write(tmp_path / "v.json", future)]) == EXIT_INVALID


def test_render_offline_with_annotations(tmp_path, demo_profile, demo_annotations):
    profile = _write(tmp_path / "profile.json", demo_profile)
    annotations = _write(tmp_path / "annotations.json", demo_annotations)
    out = tmp_path / "docs"
    assert (
        main(["render", "--input", profile, "--output", str(out), "--annotations", annotations])
        == EXIT_OK
    )
    assert sorted(p.name for p in out.iterdir()) == sorted(
        [
            "overview.md",
            "data_dictionary.md",
            "quality_report.md",
            "relationships.md",
            "erd.mmd",
            "suggested_rules.json",
        ]
    )
    assert "(annotation)" in (out / "data_dictionary.md").read_text(encoding="utf-8")
    assert main(["render", "--input", profile, "--output", str(out)]) == EXIT_IO
    assert main(["render", "--input", profile, "--output", str(out), "--overwrite"]) == EXIT_OK


def test_render_refuses_invalid_profile(tmp_path, demo_profile):
    demo_profile["tables"][0]["status"] = "weird"
    profile = _write(tmp_path / "profile.json", demo_profile)
    out = tmp_path / "docs"
    assert main(["render", "--input", profile, "--output", str(out)]) == EXIT_INVALID
    assert not out.exists()


def test_schema_command(capsys):
    assert main(["schema", "profile"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["title"] == "TableDossier profile"


def test_module_entry_point_and_version():
    result = subprocess.run(
        [sys.executable, "-m", "tabledossier", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0 and result.stdout.startswith("tabledossier ")


def test_profiles_of_contract_1_0_are_still_accepted(tmp_path, capsys):
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "profile-1.0.json"
    profile = json.loads(fixture.read_text(encoding="utf-8"))
    assert profile["schema_version"] == "1.0" and profile["tool"]["version"] == "0.1.0"
    assert main(["validate", "--profile", str(fixture)]) == 0
    assert "schema 1.0" in capsys.readouterr().out
    out = tmp_path / "docs"
    assert main(["render", "--input", str(fixture), "--output", str(out)]) == 0
    dictionary = (out / "data_dictionary.md").read_text(encoding="utf-8")
    assert "analytics.orders" in dictionary and "JSON paths" not in dictionary
    # A 1.0 document may not use 1.1 additions: it is validated by the frozen 1.0 schema.
    profile["run"]["analysis_level"] = "deep"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", "--profile", str(bad)]) == 3
    profile["run"]["analysis_level"] = "standard"
    profile["schema_version"] = "1.1"
    relabelled = tmp_path / "relabelled.json"
    relabelled.write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", "--profile", str(relabelled)]) == 0, "1.1 only adds to 1.0"
    profile["schema_version"] = "9.9"
    relabelled.write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", "--profile", str(relabelled)]) == 3
    assert "supported: 1.0, 1.1, 1.2" in capsys.readouterr().err


def test_profiles_of_contract_1_1_are_still_accepted(tmp_path, capsys):
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "profile-1.1.json"
    profile = json.loads(fixture.read_text(encoding="utf-8"))
    assert profile["schema_version"] == "1.1" and profile["tool"]["version"] == "0.2.0"
    assert main(["validate", "--profile", str(fixture)]) == 0
    assert "schema 1.1" in capsys.readouterr().out
    out = tmp_path / "docs"
    assert main(["render", "--input", str(fixture), "--output", str(out)]) == 0
    report = (out / "quality_report.md").read_text(encoding="utf-8")
    assert "Deep analysis: coverage and budget" in report
    # A 1.1 document may not use 1.2 additions: it is validated by the frozen 1.1 schema.
    profile["tables"][0]["uniqueness"] = None
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", "--profile", str(bad)]) == 3
    del profile["tables"][0]["uniqueness"]
    profile["schema_version"] = "1.2"
    relabelled = tmp_path / "relabelled.json"
    relabelled.write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", "--profile", str(relabelled)]) == 0, "1.2 only adds to 1.1"


def test_schema_command_serves_every_profile_version(capsys):
    assert main(["schema", "profile"]) == 0
    assert '"const": "1.2"' in capsys.readouterr().out
    for version in ("1.0", "1.1"):
        assert main(["schema", f"profile-{version}"]) == 0
        assert f'"const": "{version}"' in capsys.readouterr().out
