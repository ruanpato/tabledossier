import copy
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "examples" / "demo"
DEMO_PROFILE = DEMO / "output" / "run" / "profile.json"


@pytest.fixture(scope="session")
def demo_profile_data() -> dict:
    """Profile produced by executing the generated notebook on the synthetic demo tables."""
    return json.loads(DEMO_PROFILE.read_text(encoding="utf-8"))


@pytest.fixture
def demo_profile(demo_profile_data: dict) -> dict:
    return copy.deepcopy(demo_profile_data)


@pytest.fixture(scope="session")
def schemas() -> dict:
    from tabledossier.resources import SCHEMA_FILES, load_schema

    return {name: load_schema(name) for name in SCHEMA_FILES}


@pytest.fixture
def demo_annotations() -> dict:
    return json.loads((DEMO / "annotations.json").read_text(encoding="utf-8"))
