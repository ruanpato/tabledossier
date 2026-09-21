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
    manifest = json.loads((OUTPUT / "run" / "manifest.json").read_text(encoding="utf-8"))
    assert check_document(manifest, "manifest") == []
    listed = {entry["path"] for entry in manifest["files"]}
    assert listed == {p.name for p in (OUTPUT / "run").iterdir()} - {"manifest.json"}


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
