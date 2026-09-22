"""The stdlib validator embedded in notebooks must agree with jsonschema."""

import copy
import json

import pytest
from jsonschema import Draft202012Validator

from tabledossier.config import default_config
from tabledossier.schemacheck import schema_errors, unsupported_keywords

SCHEMA_NAMES = [
    "config",
    "profile",
    "profile-1.0",
    "profile-1.1",
    "annotations",
    "suggested_rules",
    "manifest",
]


@pytest.mark.parametrize("name", SCHEMA_NAMES)
def test_schemas_only_use_supported_keywords(schemas, name):
    assert unsupported_keywords(schemas[name]) == []


@pytest.mark.parametrize("name", SCHEMA_NAMES)
def test_schemas_are_valid_draft_2020_12(schemas, name):
    Draft202012Validator.check_schema(schemas[name])


def _agree(document, schema):
    formal = not list(Draft202012Validator(schema).iter_errors(document))
    local = not schema_errors(document, schema)
    assert formal == local, (formal, schema_errors(document, schema)[:3])
    return local


def _mutations(document):
    """Yield structurally broken variants of a valid document."""
    yield {**document, "unexpected": 1}
    for key in list(document)[:6]:
        broken = copy.deepcopy(document)
        del broken[key]
        yield broken
        wrong = copy.deepcopy(document)
        wrong[key] = 12345 if not isinstance(document[key], int) else "text"
        yield wrong


def test_profile_agreement_on_valid_and_mutated(demo_profile, schemas):
    schema = schemas["profile"]
    assert _agree(demo_profile, schema)
    for variant in _mutations(demo_profile):
        _agree(variant, schema)
    table = demo_profile["tables"][0]
    for variant_table in _mutations(table):
        _agree({**demo_profile, "tables": [variant_table]}, schema)
    metric = next(f for f in table["field_profiles"] if f["metrics"])["metrics"][0]
    for bad in (
        {**metric, "status": "measured", "value": None},
        {**metric, "status": "not_computed"},
        {**metric, "scope": "somewhere"},
        {**metric, "value": True, "value_type": "nonsense"},
    ):
        field = copy.deepcopy(table["field_profiles"][0])
        field["metrics"] = [bad]
        variant = copy.deepcopy(demo_profile)
        variant["tables"][0]["field_profiles"][0] = field
        assert not _agree(variant, schema)


def test_config_agreement(schemas):
    schema = schemas["config"]
    config = default_config()
    assert _agree(config, schema)
    for variant in _mutations(config):
        _agree(variant, schema)
    bad_filter = copy.deepcopy(config)
    bad_filter["table_options"] = {"a.b": {"filters": [{"column": "x", "operator": "drop"}]}}
    assert not _agree(bad_filter, schema)
    good_filter = copy.deepcopy(config)
    good_filter["table_options"] = {
        "a.b": {"filters": [{"column": ["x", "y"], "operator": "is_null"}]}
    }
    assert _agree(good_filter, schema)


def test_equality_semantics_match_json():
    schema = {"enum": [1, "1", True]}
    for value, expected in (
        (1, True),
        (1.0, True),
        (True, True),
        (False, False),
        ("1", True),
        (2, False),
    ):
        assert (not schema_errors(value, schema)) is expected
        assert (not list(Draft202012Validator(schema).iter_errors(value))) is expected


def test_unique_items_and_patterns():
    schema = {"type": "array", "uniqueItems": True, "items": {"type": "string", "pattern": "^a"}}
    for value in (["a", "ab"], ["a", "a"], ["b"], [1]):
        _agree(value, schema)


def test_messages_are_bounded():
    errors = schema_errors({"x": "y" * 1000}, {"properties": {"x": {"maxLength": 3}}})
    assert errors and len(errors[0]) < 200
    assert json.dumps(errors)


def test_profile_1_1_additions_agreement(deep_profile, schemas):
    schema = schemas["profile"]
    assert _agree(deep_profile, schema)
    orders = next(t for t in deep_profile["tables"] if t["table_key"] == "analytics.orders")
    for variant_table in _mutations(orders["deep"]):
        variant = copy.deepcopy(deep_profile)
        target = next(t for t in variant["tables"] if t["table_key"] == "analytics.orders")
        target["deep"] = variant_table
        _agree(variant, schema)
    element = next(f for f in orders["field_profiles"] if f.get("element_context"))
    for context in _mutations(element["element_context"]):
        variant = copy.deepcopy(deep_profile)
        target = next(t for t in variant["tables"] if t["table_key"] == "analytics.orders")
        field = next(f for f in target["field_profiles"] if f["field_id"] == element["field_id"])
        field["element_context"] = context
        assert not _agree(variant, schema)
    events = next(t for t in deep_profile["tables"] if t["table_key"] == "analytics.order_events")
    payload = next(f for f in events["field_profiles"] if f.get("json_paths"))
    for catalog in _mutations(payload["json_paths"]):
        variant = copy.deepcopy(deep_profile)
        target = next(t for t in variant["tables"] if t["table_key"] == "analytics.order_events")
        field = next(f for f in target["field_profiles"] if f["field_id"] == payload["field_id"])
        field["json_paths"] = catalog
        _agree(variant, schema)
    for path in _mutations(payload["json_paths"]["paths"][0]):
        variant = copy.deepcopy(deep_profile)
        target = next(t for t in variant["tables"] if t["table_key"] == "analytics.order_events")
        field = next(f for f in target["field_profiles"] if f["field_id"] == payload["field_id"])
        field["json_paths"]["paths"][0] = path
        assert not _agree(variant, schema)


def test_profile_1_1_agreement(profile_1_1, schemas):
    schema = schemas["profile-1.1"]
    assert _agree(profile_1_1, schema)
    for variant in _mutations(profile_1_1):
        _agree(variant, schema)
    assert not _agree({**profile_1_1, "schema_version": "1.2"}, schema)
    variant = copy.deepcopy(profile_1_1)
    variant["tables"][0]["uniqueness"] = None
    assert not _agree(variant, schema), "1.1 documents cannot use 1.2 additions"


def test_profile_1_0_agreement(profile_1_0, schemas):
    schema = schemas["profile-1.0"]
    assert _agree(profile_1_0, schema)
    for variant in _mutations(profile_1_0):
        _agree(variant, schema)
    assert not _agree({**profile_1_0, "schema_version": "1.1"}, schema)
