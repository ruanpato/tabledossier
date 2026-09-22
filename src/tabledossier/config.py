"""Configuration defaults, merging, validation and notebook parameter precedence.

Part of the embedded runtime (standard library only). Structural validation
uses the JSON Schema passed in by the caller (the CLI loads it from the package;
the notebook embeds it as a literal generated from the same file).

Precedence, lowest to highest::

    built-in defaults < generated config < ``config_json`` widget
                      < ``tables_json`` / ``analysis_level`` / ``output_dir`` widgets
"""

import copy
import datetime
import json
import re
from collections.abc import Mapping
from typing import Any

from tabledossier.paths import (
    IdentifierError,
    column_reference_segments,
    parse_display_path,
    parse_table_identifier,
    table_lookup_key,
)
from tabledossier.schemacheck import schema_errors

CONFIG_KIND = "tabledossier.config"
CONFIG_VERSION = "1.0"
ANALYSIS_LEVELS = ("metadata", "standard", "deep")
ROW_READING_LEVELS = ("standard", "deep")
DEEP_ALL_TARGETS = "all_within_budget"
DEDICATED_WIDGET_KEYS = {
    "tables": "tables_json",
    "analysis_level": "analysis_level",
    "output_dir": "output_dir",
}
COLUMN_CHECK_TYPES = ("max_null_ratio", "max_null_count", "max_empty_string_ratio", "value_range")
TABLE_CHECK_TYPES = ("min_row_count", "max_row_count")
NO_VALUE_OPERATORS = ("is_null", "is_not_null")
LIST_VALUE_OPERATORS = ("in", "not_in")
MAX_LIST_FILTER_VALUES = 1000

DEFAULT_CONFIG: dict[str, Any] = {
    "kind": CONFIG_KIND,
    "config_version": CONFIG_VERSION,
    "tables": [],
    "analysis_level": "standard",
    "output_dir": "",
    "purpose": None,
    "limits": {
        "max_tables": 10,
        "max_fields": 200,
        "max_depth": 3,
        "max_expressions_per_pass": 800,
        "max_aggregate_passes": 2,
    },
    "sampling": {
        "method": "prefix",
        "max_rows": 2000,
        "max_bytes": 16 * 1024 * 1024,
        "max_value_chars": 8192,
        "max_columns": 100,
        "random_fraction": None,
        "seed": 42,
    },
    "consistency": {"pin_delta_version": True},
    "metrics": {
        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
        "quantile_accuracy": 10000,
        "approx_distinct_rsd": 0.02,
        "json_full_scope_validation": True,
    },
    "semantic": {
        "min_observations": 30,
        "detect_threshold": 0.95,
        "mixed_threshold": 0.2,
        "confidence_level": 0.95,
    },
    "thresholds": {
        "high_null_ratio": 0.5,
        "categorical_max_distinct": 20,
        "categorical_min_rows": 100,
        "identifier_min_distinct_ratio": 0.95,
        "identifier_min_rows": 30,
        "identifier_max_null_ratio": 0.01,
        "json_min_ratio": 0.8,
        "constant_min_rows": 2,
        "tail_iqr_multiplier": 10,
        "large_array_size": 1000,
    },
    "value_policy": {
        "persist_examples": False,
        "example_columns": [],
        "max_examples_per_column": 5,
        "max_example_chars": 64,
        "aggregate_extremes": "include",
        "json_key_names": "include",
        "redact_columns": [],
    },
    "deep": {
        "targets": DEEP_ALL_TARGETS,
        "collections": True,
        "json_paths": True,
        "element_distinct": "sample",
        "json_full_scope_validation": True,
        "max_extra_passes": 2,
        "max_explode_rows": 1000,
        "max_elements": 100000,
        "max_json_paths": 50,
        "max_json_depth": 3,
        "max_json_object_keys": 50,
        "uniqueness": {
            "keys": [],
            "declared_keys": False,
            "identifier_candidates": False,
            "max_keys": 5,
            "max_passes": 1,
        },
        "referential": {
            "configured": False,
            "declared": False,
            "mode": "full_scope",
            "max_relationships": 5,
            "max_sample_rows": 10000,
        },
        "relationship_hypotheses": {
            "enabled": False,
            "max_pairs": 5,
            "max_sample_rows": 10000,
            "inclusion_scope": "sample",
            "min_inclusion_ratio": 0.95,
        },
    },
    "jobs": {"exit_summary": True},
    "table_options": {},
    "relationships": [],
}


class ConfigError(ValueError):
    """Raised when configuration or notebook parameters are invalid."""

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = list(errors or [])

    def __str__(self) -> str:
        base = super().__str__()
        if not self.errors:
            return base
        return base + "\n" + "\n".join(f"  - {error}" for error in self.errors)


def default_config() -> dict[str, Any]:
    """Return a fresh copy of the built-in defaults."""
    return copy.deepcopy(DEFAULT_CONFIG)


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Merge ``override`` into ``base``: nested objects merge, other values replace."""
    merged = copy.deepcopy(dict(base))
    for key, value in override.items():
        if (
            isinstance(value, Mapping)
            and isinstance(merged.get(key), Mapping)
            and key != "table_options"
        ):
            merged[key] = deep_merge(merged[key], value)
        elif key == "table_options" and isinstance(value, Mapping):
            options = dict(merged.get(key) or {})
            options.update(copy.deepcopy(dict(value)))
            merged[key] = options
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def normalize_config(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``raw`` merged over the built-in defaults."""
    return deep_merge(DEFAULT_CONFIG, raw)


def _cfg_check_column_ref(reference: Any, where: str, errors: list[str]) -> None:
    try:
        column_reference_segments(reference)
    except IdentifierError as exc:
        errors.append(f"{where}: {exc}")


def _cfg_check_iso(value: Any, value_type: str, where: str, errors: list[str]) -> None:
    if not isinstance(value, str):
        errors.append(f"{where}: {value_type} values must be ISO 8601 strings")
        return
    try:
        if value_type == "date":
            datetime.date.fromisoformat(value)
        else:
            normalized = value.replace("Z", "+00:00")
            normalized = re.sub(r"(\.\d{6})\d+", r"\1", normalized)
            datetime.datetime.fromisoformat(normalized)
    except ValueError:
        errors.append(f"{where}: {value!r} is not a valid ISO 8601 {value_type}")


def _cfg_check_filter(item: Mapping[str, Any], where: str, errors: list[str]) -> None:
    _cfg_check_column_ref(item.get("column"), f"{where}.column", errors)
    operator = item.get("operator")
    has_value = "value" in item
    value = item.get("value")
    value_type = item.get("value_type")
    if operator in NO_VALUE_OPERATORS:
        if has_value:
            errors.append(f"{where}: operator {operator!r} does not take a value")
        return
    if not has_value:
        errors.append(f"{where}: operator {operator!r} requires a value")
        return
    if operator in LIST_VALUE_OPERATORS:
        if not isinstance(value, list) or not value or len(value) > MAX_LIST_FILTER_VALUES:
            errors.append(
                f"{where}: operator {operator!r} requires a non-empty list "
                f"(at most {MAX_LIST_FILTER_VALUES} values)"
            )
            return
        scalars = value
    elif operator == "between":
        if not isinstance(value, list) or len(value) != 2:
            errors.append(f"{where}: operator 'between' requires a list [low, high]")
            return
        scalars = value
    else:
        if isinstance(value, list):
            errors.append(f"{where}: operator {operator!r} requires a single value")
            return
        scalars = [value]
    for scalar in scalars:
        if isinstance(scalar, (list, dict)) or scalar is None:
            errors.append(f"{where}: filter values must be strings, numbers or booleans")
            return
        if operator == "like" and not isinstance(scalar, str):
            errors.append(f"{where}: operator 'like' requires a string pattern")
        if value_type in ("date", "timestamp"):
            _cfg_check_iso(scalar, value_type, f"{where}.value", errors)


def _cfg_check_check(item: Mapping[str, Any], where: str, errors: list[str]) -> None:
    check_type = item.get("type")
    has_column = "column" in item
    if check_type in COLUMN_CHECK_TYPES:
        if not has_column:
            errors.append(f"{where}: check type {check_type!r} requires 'column'")
        else:
            _cfg_check_column_ref(item["column"], f"{where}.column", errors)
    elif check_type in TABLE_CHECK_TYPES and has_column:
        errors.append(f"{where}: check type {check_type!r} does not take 'column'")
    low, high = item.get("min"), item.get("max")
    max_only = ("max_null_ratio", "max_empty_string_ratio", "max_null_count", "max_row_count")
    if check_type in max_only and (high is None or low is not None):
        errors.append(f"{where}: check type {check_type!r} requires 'max' only")
    if check_type == "min_row_count" and (low is None or high is not None):
        errors.append(f"{where}: check type 'min_row_count' requires 'min' only")
    if check_type == "value_range" and low is None and high is None:
        errors.append(f"{where}: check type 'value_range' requires 'min' and/or 'max'")
    ratio_check = check_type in ("max_null_ratio", "max_empty_string_ratio")
    if ratio_check and high is not None and not 0 <= high <= 1:
        errors.append(f"{where}: ratio thresholds must be between 0 and 1")
    if check_type in ("max_null_count", "max_row_count", "min_row_count"):
        bound = high if low is None else low
        if bound is not None and (bound < 0 or float(bound) != int(bound)):
            errors.append(f"{where}: count thresholds must be non-negative integers")
    if low is not None and high is not None and low > high:
        errors.append(f"{where}: 'min' is greater than 'max'")


def _cfg_check_table(text: Any, where: str, errors: list[str]) -> str | None:
    try:
        return table_lookup_key(parse_table_identifier(text))
    except IdentifierError as exc:
        errors.append(f"{where}: {exc}")
        return None


def config_errors(config: Mapping[str, Any], schema: Mapping[str, Any]) -> list[str]:
    """Return all structural and semantic errors of a (normalized) configuration."""
    errors = schema_errors(config, schema)
    if errors:
        return errors
    seen_tables: set[str] = set()
    for index, table in enumerate(config.get("tables", [])):
        key = _cfg_check_table(table, f"$.tables[{index}]", errors)
        if key is not None:
            if key in seen_tables:
                errors.append(f"$.tables[{index}]: duplicate table {table!r}")
            seen_tables.add(key)
    limits = config.get("limits", {})
    max_tables = limits.get("max_tables", DEFAULT_CONFIG["limits"]["max_tables"])
    if len(config.get("tables", [])) > max_tables:
        errors.append(
            f"$.tables: {len(config['tables'])} tables exceed limits.max_tables={max_tables}; "
            "profile a smaller batch or raise the limit explicitly"
        )
    sampling = config.get("sampling", {})
    if sampling.get("method") == "random" and not sampling.get("random_fraction"):
        errors.append("$.sampling: method 'random' requires random_fraction (0 < f <= 1)")
    if sampling.get("max_value_chars", 0) > sampling.get("max_bytes", 1 << 62):
        errors.append("$.sampling: max_value_chars must not exceed max_bytes")
    semantic = config.get("semantic", {})
    if semantic.get("mixed_threshold", 0) >= semantic.get("detect_threshold", 1):
        errors.append("$.semantic: mixed_threshold must be lower than detect_threshold")

    for key, options in config.get("table_options", {}).items():
        where = f"$.table_options[{key!r}]"
        _cfg_check_table(key, where, errors)
        for index, column in enumerate(options.get("columns") or []):
            _cfg_check_column_ref(column, f"{where}.columns[{index}]", errors)
        for index, item in enumerate(options.get("filters", [])):
            _cfg_check_filter(item, f"{where}.filters[{index}]", errors)
        check_ids: set[str] = set()
        for index, item in enumerate(options.get("checks", [])):
            _cfg_check_check(item, f"{where}.checks[{index}]", errors)
            if item["id"] in check_ids:
                errors.append(f"{where}.checks[{index}]: duplicate check id {item['id']!r}")
            check_ids.add(item["id"])

    targets = config.get("deep", {}).get("targets", DEEP_ALL_TARGETS)
    if isinstance(targets, list):
        for index, item in enumerate(targets):
            where = f"$.deep.targets[{index}]"
            _cfg_check_table(item["table"], where, errors)
            try:
                parse_display_path(item["column"])
            except IdentifierError as exc:
                errors.append(f"{where}.column: {exc}")

    key_ids: set[str] = set()
    for index, item in enumerate(config.get("deep", {}).get("uniqueness", {}).get("keys", [])):
        where = f"$.deep.uniqueness.keys[{index}]"
        _cfg_check_table(item["table"], where, errors)
        for col_index, column in enumerate(item["columns"]):
            _cfg_check_column_ref(column, f"{where}.columns[{col_index}]", errors)
        if item.get("id") is not None:
            if item["id"] in key_ids:
                errors.append(f"{where}: duplicate key id {item['id']!r}")
            key_ids.add(item["id"])

    policy = config.get("value_policy", {})
    for list_name in ("example_columns", "redact_columns"):
        for index, item in enumerate(policy.get(list_name, [])):
            where = f"$.value_policy.{list_name}[{index}]"
            _cfg_check_table(item["table"], where, errors)
            try:
                parse_display_path(item["column"])
            except IdentifierError as exc:
                errors.append(f"{where}.column: {exc}")
    if policy.get("example_columns") and not policy.get("persist_examples"):
        errors.append("$.value_policy: example_columns requires persist_examples=true")

    relationship_ids: set[str] = set()
    for index, item in enumerate(config.get("relationships", [])):
        where = f"$.relationships[{index}]"
        if item["id"] in relationship_ids:
            errors.append(f"{where}: duplicate relationship id {item['id']!r}")
        relationship_ids.add(item["id"])
        for end in ("from", "to"):
            _cfg_check_table(item[end]["table"], f"{where}.{end}.table", errors)
            for col_index, column in enumerate(item[end]["columns"]):
                _cfg_check_column_ref(column, f"{where}.{end}.columns[{col_index}]", errors)
        if len(item["from"]["columns"]) != len(item["to"]["columns"]):
            errors.append(f"{where}: 'from' and 'to' must list the same number of columns")
    return errors


def validate_config(raw: Mapping[str, Any], schema: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a raw configuration and return it normalized, or raise ConfigError."""
    errors = schema_errors(raw, schema)
    if not errors:
        normalized = normalize_config(raw)
        errors = config_errors(normalized, schema)
        if not errors:
            return normalized
    raise ConfigError("invalid TableDossier configuration", errors)


def widget_defaults(config: Mapping[str, Any]) -> dict[str, str]:
    """Return the string defaults for the notebook widgets."""
    return {
        "tables_json": json.dumps(list(config.get("tables", [])), ensure_ascii=True),
        "analysis_level": str(config.get("analysis_level", "standard")),
        "output_dir": str(config.get("output_dir", "")),
        "config_json": "{}",
    }


def _cfg_parse_json(text: str, name: str, expected: type, errors: list[str]) -> Any:
    try:
        value = json.loads(text) if text.strip() else expected()
    except json.JSONDecodeError as exc:
        errors.append(f"widget {name!r}: invalid JSON ({exc.msg} at position {exc.pos})")
        return None
    if not isinstance(value, expected):
        errors.append(f"widget {name!r}: expected a JSON {expected.__name__}")
        return None
    return value


def resolve_parameters(
    generated_config: Mapping[str, Any],
    widget_values: Mapping[str, str],
    schema: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Merge widget values over the generated configuration.

    Returns ``(effective_config, parameter_sources)`` or raises
    :class:`ConfigError` listing every problem found.
    """
    errors: list[str] = []
    defaults = widget_defaults(generated_config)
    overrides = _cfg_parse_json(widget_values.get("config_json", "{}"), "config_json", dict, errors)
    tables = _cfg_parse_json(widget_values.get("tables_json", "[]"), "tables_json", list, errors)
    if errors:
        raise ConfigError("invalid notebook parameters", errors)
    assert overrides is not None and tables is not None
    for key, widget in DEDICATED_WIDGET_KEYS.items():
        if key in overrides:
            errors.append(f"widget 'config_json': set {key!r} with the {widget!r} widget instead")
    for key in ("kind", "config_version"):
        if key in overrides:
            errors.append(f"widget 'config_json': {key!r} cannot be overridden")
    if not all(isinstance(item, str) for item in tables):
        errors.append("widget 'tables_json': every item must be a table identifier string")
    level = widget_values.get("analysis_level", defaults["analysis_level"]).strip()
    output_dir = widget_values.get("output_dir", defaults["output_dir"]).strip()
    if errors:
        raise ConfigError("invalid notebook parameters", errors)

    merged = deep_merge(normalize_config(generated_config), overrides)
    merged["tables"] = [item.strip() for item in tables]
    merged["analysis_level"] = level
    merged["output_dir"] = output_dir
    problems = config_errors(merged, schema)
    if problems:
        raise ConfigError("invalid effective configuration", problems)
    sources = {
        "precedence": [
            "built-in defaults",
            "generated config",
            "config_json widget",
            "tables_json/analysis_level/output_dir widgets",
        ],
        "config_json_overrides": sorted(overrides),
        "widgets": {
            name: {
                "source": "widget",
                "matches_generated_default": widget_values.get(name, defaults[name]).strip()
                == defaults[name].strip(),
            }
            for name in ("tables_json", "analysis_level", "output_dir")
        },
    }
    return merged, sources


def execution_errors(config: Mapping[str, Any]) -> list[str]:
    """Return problems that allow generation but block execution."""
    errors: list[str] = []
    if not config.get("tables"):
        errors.append(
            "no tables to profile: set the 'tables_json' widget (or the Job parameter of the same "
            'name) to a JSON list such as ["demo.analytics.orders"] (catalog.schema.table) and run '
            "the notebook again"
        )
    output_dir = str(config.get("output_dir", ""))
    if not output_dir:
        errors.append(
            "no output directory: set the 'output_dir' widget (or the Job parameter of the same "
            "name), for example /Volumes/<catalog>/<schema>/<volume>/tabledossier"
        )
    elif re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", output_dir):
        errors.append(
            f"output_dir {output_dir!r} looks like a URI; use a POSIX path such as "
            "/Volumes/<catalog>/<schema>/<volume>/tabledossier"
        )
    elif not output_dir.startswith("/"):
        errors.append(f"output_dir {output_dir!r} must be an absolute path")
    return errors


def table_options_for(config: Mapping[str, Any], table_lookup: str) -> dict[str, Any]:
    """Return the options configured for a table (matched case-insensitively)."""
    for key, options in config.get("table_options", {}).items():
        try:
            if table_lookup_key(parse_table_identifier(key)) == table_lookup:
                return dict(options)
        except IdentifierError:
            continue
    return {}


def sanitized_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Return the configuration as recorded in profiles.

    The schema forbids unknown keys, so configurations cannot carry credentials;
    this copy exists so callers never mutate the effective configuration.
    Filter values are kept because they define the analysed population.
    """
    return copy.deepcopy(dict(config))
