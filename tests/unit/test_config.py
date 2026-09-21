import json

import pytest

from tabledossier.config import (
    ConfigError,
    config_errors,
    default_config,
    execution_errors,
    normalize_config,
    resolve_parameters,
    table_options_for,
    validate_config,
    widget_defaults,
)
from tabledossier.paths import parse_table_identifier, table_lookup_key
from tabledossier.validation import check_config
from tabledossier.widgets import ensure_widgets, read_widgets


def _base(**overrides):
    config = {"kind": "tabledossier.config", "config_version": "1.0"}
    config.update(overrides)
    return config


def test_defaults_are_valid_and_conservative(schemas):
    config = default_config()
    assert config_errors(config, schemas["config"]) == []
    assert config["sampling"]["max_rows"] == 2000
    assert config["sampling"]["max_bytes"] == 16 * 1024 * 1024
    assert config["limits"]["max_depth"] == 3
    assert config["limits"]["max_fields"] == 200
    assert config["value_policy"]["persist_examples"] is False
    assert config["tables"] == []


def test_minimal_config_is_normalized(schemas):
    normalized = validate_config(_base(tables=["demo.analytics.orders"]), schemas["config"])
    assert normalized["analysis_level"] == "standard"
    assert normalized["tables"] == ["demo.analytics.orders"]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"tables": ["demo.analytics.order events"]}, "quote names"),
        ({"tables": ["a.b", "A.B"]}, "duplicate table"),
        ({"sampling": {"method": "random"}}, "random_fraction"),
        ({"password": "x"}, "unexpected property"),
        (
            {"table_options": {"a.b": {"filters": [{"column": "x", "operator": "eq"}]}}},
            "requires a value",
        ),
        (
            {
                "table_options": {
                    "a.b": {"filters": [{"column": "x", "operator": "in", "value": []}]}
                }
            },
            "non-empty list",
        ),
        (
            {
                "table_options": {
                    "a.b": {"filters": [{"column": "x", "operator": "between", "value": [1]}]}
                }
            },
            "[low, high]",
        ),
        (
            {
                "table_options": {
                    "a.b": {"filters": [{"column": "x", "operator": "is_null", "value": 1}]}
                }
            },
            "does not take a value",
        ),
        (
            {
                "table_options": {
                    "a.b": {
                        "filters": [
                            {
                                "column": "d",
                                "operator": "ge",
                                "value": "2026-02-30",
                                "value_type": "date",
                            }
                        ]
                    }
                }
            },
            "not a valid ISO 8601 date",
        ),
        (
            {
                "table_options": {
                    "a.b": {"checks": [{"id": "c", "type": "max_null_ratio", "max": 0.1}]}
                }
            },
            "requires 'column'",
        ),
        (
            {
                "table_options": {
                    "a.b": {
                        "checks": [{"id": "c", "type": "max_null_ratio", "column": "x", "max": 2}]
                    }
                }
            },
            "between 0 and 1",
        ),
        (
            {
                "relationships": [
                    {
                        "id": "r",
                        "from": {"table": "a.b", "columns": ["x", "y"]},
                        "to": {"table": "a.c", "columns": ["x"]},
                    }
                ]
            },
            "same number of columns",
        ),
        (
            {"value_policy": {"example_columns": [{"table": "a.b", "column": "x"}]}},
            "requires persist_examples",
        ),
    ],
)
def test_invalid_configurations(schemas, change, message):
    with pytest.raises(ConfigError) as excinfo:
        validate_config(_base(**change), schemas["config"])
    assert message in str(excinfo.value)


def test_cli_validation_agrees_for_valid_config():
    normalized, errors = check_config(_base(tables=["demo.analytics.orders"]))
    assert errors == [] and normalized is not None


def test_widget_precedence(schemas):
    generated = normalize_config(
        _base(tables=["demo.analytics.orders"], output_dir="/Volumes/demo/analytics/results")
    )
    widgets = widget_defaults(generated)
    widgets["tables_json"] = json.dumps(["demo.analytics.customers"])
    widgets["config_json"] = json.dumps({"sampling": {"max_rows": 10}})
    effective, sources = resolve_parameters(generated, widgets, schemas["config"])
    assert effective["tables"] == ["demo.analytics.customers"]
    assert effective["sampling"]["max_rows"] == 10
    assert effective["sampling"]["max_bytes"] == 16 * 1024 * 1024
    assert sources["config_json_overrides"] == ["sampling"]
    assert sources["widgets"]["tables_json"]["matches_generated_default"] is False
    assert sources["widgets"]["output_dir"]["matches_generated_default"] is True


@pytest.mark.parametrize(
    ("widgets", "message"),
    [
        ({"config_json": '{"tables": []}'}, "tables_json"),
        ({"config_json": "{not json"}, "invalid JSON"),
        ({"config_json": "[]"}, "JSON dict"),
        ({"tables_json": '"demo.analytics.orders"'}, "JSON list"),
        ({"tables_json": "[1]"}, "table identifier string"),
        ({"analysis_level": "deep"}, "is not one of"),
    ],
)
def test_invalid_widgets(schemas, widgets, message):
    generated = normalize_config(_base())
    values = widget_defaults(generated) | widgets
    with pytest.raises(ConfigError) as excinfo:
        resolve_parameters(generated, values, schemas["config"])
    assert message in str(excinfo.value)


def test_generation_allows_empty_tables_but_execution_explains():
    config = normalize_config(_base())
    problems = execution_errors(config)
    assert any(
        "tables_json" in problem and "demo.analytics.orders" in problem for problem in problems
    )
    assert any("output_dir" in problem for problem in problems)
    ok = normalize_config(_base(tables=["a.b"], output_dir="/Volumes/c/s/v/out"))
    assert execution_errors(ok) == []
    assert (
        "URI" in execution_errors(normalize_config(_base(tables=["a.b"], output_dir="dbfs:/x")))[0]
    )
    assert (
        "absolute"
        in execution_errors(normalize_config(_base(tables=["a.b"], output_dir="rel/x")))[0]
    )


def test_table_options_lookup_is_case_insensitive():
    config = normalize_config(_base(table_options={"Demo.Analytics.Orders": {"columns": ["a"]}}))
    lookup = table_lookup_key(parse_table_identifier("demo.analytics.orders"))
    assert table_options_for(config, lookup) == {"columns": ["a"]}


class _Widgets:
    def __init__(self, values):
        self.values = dict(values)
        self.calls = []

    def get(self, name):
        if name not in self.values:
            raise ValueError("InputWidgetNotDefined")
        return self.values[name]

    def text(self, name, default, label):
        self.calls.append(("text", name))
        self.values[name] = default

    def dropdown(self, name, default, choices, label):
        assert default in choices
        self.calls.append(("dropdown", name))
        self.values[name] = default


class _Dbutils:
    def __init__(self, values):
        self.widgets = _Widgets(values)


def test_existing_widget_values_are_never_reset():
    dbutils = _Dbutils({"tables_json": '["x.y.z"]', "analysis_level": "metadata"})
    created = ensure_widgets(dbutils, widget_defaults(normalize_config(_base())))
    assert created == ["output_dir", "config_json"]
    values = read_widgets(dbutils)
    assert values["tables_json"] == '["x.y.z"]'
    assert values["analysis_level"] == "metadata"
