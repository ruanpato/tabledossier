import copy
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "examples" / "demo"
DEMO_PROFILE = DEMO / "output" / "run" / "profile.json"
DEEP_PROFILE = DEMO / "output" / "deep" / "profile.json"
PROFILE_1_0 = ROOT / "tests" / "fixtures" / "profile-1.0.json"
PROFILE_1_1 = ROOT / "tests" / "fixtures" / "profile-1.1.json"


@pytest.fixture(scope="session")
def demo_profile_data() -> dict:
    """Profile produced by executing the generated notebook on the synthetic demo tables."""
    return json.loads(DEMO_PROFILE.read_text(encoding="utf-8"))


@pytest.fixture
def demo_profile(demo_profile_data: dict) -> dict:
    return copy.deepcopy(demo_profile_data)


@pytest.fixture
def deep_profile() -> dict:
    """Deep-level profile (contract 1.2) produced by the same notebook on the demo tables."""
    return json.loads(DEEP_PROFILE.read_text(encoding="utf-8"))


@pytest.fixture
def profile_1_0() -> dict:
    """Profile written by TableDossier 0.1.0 (contract 1.0), kept to prove it is still read."""
    return json.loads(PROFILE_1_0.read_text(encoding="utf-8"))


@pytest.fixture
def profile_1_1() -> dict:
    """Deep profile written by TableDossier 0.2.0 (contract 1.1), kept to prove it is still read."""
    return json.loads(PROFILE_1_1.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def schemas() -> dict:
    from tabledossier.resources import SCHEMA_FILES, load_schema

    return {name: load_schema(name) for name in SCHEMA_FILES}


@pytest.fixture
def demo_annotations() -> dict:
    return json.loads((DEMO / "annotations.json").read_text(encoding="utf-8"))
