"""The committed demo artefacts must be exactly what the current code produces.

If one of these tests fails after a code change, rebuild the demo with
``python examples/demo/build_demo_outputs.py`` (requires PySpark and Java).
"""

import json
from pathlib import Path

from tabledossier.notebook import generate_notebook
from tabledossier.package import build_documents
from tabledossier.validation import check_annotations, check_config, check_document, check_profile

DEMO = Path(__file__).resolve().parents[2] / "examples" / "demo"
OUTPUT = DEMO / "output"


def test_demo_inputs_are_valid(demo_annotations):
    normalized, errors = check_config(
        json.loads((DEMO / "demo.config.json").read_text(encoding="utf-8"))
    )
    assert errors == [] and normalized is not None
    assert check_annotations(demo_annotations) == []


def test_committed_notebook_matches_generator():
    normalized, _ = check_config(
        json.loads((DEMO / "demo.config.json").read_text(encoding="utf-8"))
    )
    notebook, manifest = generate_notebook(normalized)
    committed = (OUTPUT / "notebook" / "profile_databricks.py").read_text(encoding="utf-8")
    assert committed == notebook, "rebuild with: python examples/demo/build_demo_outputs.py"
    committed_manifest = json.loads(
        (OUTPUT / "notebook" / "profile_databricks.generation.json").read_text(encoding="utf-8")
    )
    manifest["notebook"]["file_name"] = "profile_databricks.py"
    assert committed_manifest == manifest
    assert check_document(committed_manifest, "manifest") == []


def test_committed_documents_match_renderer(demo_profile, demo_annotations):
    assert check_profile(demo_profile) == []
    for directory, annotations in (("run", None), ("annotated", demo_annotations)):
        documents = build_documents(demo_profile, annotations)
        for name, text in documents.items():
            committed = (OUTPUT / directory / name).read_text(encoding="utf-8")
            assert committed == text, f"{directory}/{name} is stale; rebuild the demo outputs"


def test_committed_manifest_describes_the_package():
    for directory in ("run", "deep"):
        manifest = json.loads((OUTPUT / directory / "manifest.json").read_text(encoding="utf-8"))
        assert check_document(manifest, "manifest") == []
        listed = {entry["path"] for entry in manifest["files"]}
        assert listed == {p.name for p in (OUTPUT / directory).iterdir()} - {"manifest.json"}
        assert manifest["profile_validation"] == {"checked": True, "valid": True, "errors": []}


def test_committed_deep_profile_is_valid_and_matches_renderer():
    profile = json.loads((OUTPUT / "deep" / "profile.json").read_text(encoding="utf-8"))
    assert profile["schema_version"] == "1.2"
    assert profile["run"]["analysis_level"] == "deep"
    assert check_profile(profile) == []
    for name, text in build_documents(profile).items():
        committed = (OUTPUT / "deep" / name).read_text(encoding="utf-8")
        assert committed == text, f"deep/{name} is stale; rebuild the demo outputs"
    orders = next(t for t in profile["tables"] if t["table_key"] == "analytics.orders")
    elements = [f for f in orders["field_profiles"] if f.get("element_context")]
    assert elements and all(f["profiled"] for f in elements)
    assert orders["deep"]["extra_passes"]["planned"] <= orders["deep"]["budget"]["max_extra_passes"]
    events = next(t for t in profile["tables"] if t["table_key"] == "analytics.order_events")
    payload = next(f for f in events["field_profiles"] if f["display_path"] == "payload")
    assert payload["json_paths"]["paths"], "the demo shows a JSON path catalogue"


def test_databricks_demo_config_mirrors_local_demo():
    local = json.loads((DEMO / "demo.config.json").read_text(encoding="utf-8"))
    remote_path = DEMO.parent / "databricks" / "demo.config.json"
    remote = json.loads(remote_path.read_text(encoding="utf-8"))
    normalized, errors = check_config(remote)
    assert errors == [] and normalized is not None
    expected = json.loads(json.dumps(local).replace('"analytics.', '"demo.analytics.'))
    expected["output_dir"] = remote["output_dir"]
    expected["purpose"] = remote["purpose"]
    assert remote == expected
