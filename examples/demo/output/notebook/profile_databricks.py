# Databricks notebook source
# MAGIC %md
# MAGIC # TableDossier profiling notebook
# MAGIC
# MAGIC Generated offline by TableDossier 0.2.0 (generation id `sha256:97a5554e40f47ca66bf9b8e4d8e1d0842c608a86e7e8173ad86093578dd76f51`).
# MAGIC
# MAGIC **This notebook contains no results yet.** It was generated without access to your data; metrics exist only after you run it here.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Reads the parameters (widgets) at the top of the notebook.
# MAGIC 2. Validates them and creates a new results directory under `output_dir` before reading any table.
# MAGIC 3. Profiles each table sequentially: catalog metadata and, at the `standard` level, one bounded sample and a bounded number of shared aggregation passes. The `deep` level adds array/map element metrics and JSON paths within explicit budgets.
# MAGIC 4. Writes `profile.json`, `manifest.json` and the derived documentation to the results directory.
# MAGIC
# MAGIC **Safety.** Sources are only read. The notebook never alters schemas or constraints, never runs OPTIMIZE or ANALYZE TABLE and never modifies data. It writes only to its own results directory and never overwrites existing files. It installs nothing and downloads nothing.
# MAGIC
# MAGIC **Requirements.** A Databricks Runtime with Python and PySpark (targets: 16.4 LTS, 15.4 LTS, 17.3 LTS; see docs/compatibility.md for what has been validated).
# MAGIC
# MAGIC **How to use.** 4 default table(s) were configured at generation. Set `tables_json` (for example `["demo.analytics.orders"]`) and `output_dir` (for example `/Volumes/<catalog>/<schema>/<volume>/tabledossier`), then choose *Run all*.
# MAGIC
# MAGIC **License.** The embedded runtime cells are TableDossier source code, licensed under the Apache License, Version 2.0. Your data, parameters and results are yours and are not subject to that license.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC | --- | --- |
# MAGIC | `tables_json` | JSON list of `catalog.schema.table` identifiers (quote unusual names with backticks). |
# MAGIC | `analysis_level` | `metadata` (no row reads), `standard` (sample + aggregations) or `deep` (standard + elements and JSON paths, budgeted). |
# MAGIC | `output_dir` | Directory where a new `<run_id>/` folder is created. |
# MAGIC | `config_json` | Optional JSON object merged over the generated configuration (no secrets). |
# MAGIC
# MAGIC Precedence: built-in defaults < generated configuration < `config_json` < the three dedicated widgets. Existing widget values (typed by you or passed by a Job) are never reset.

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.widgets
# TableDossier 0.2.0 embedded runtime: module tabledossier.widgets
# Source: src/tabledossier/widgets.py (sha256:a2e3f175e63ab43eb0dab0b1099b42ac55168a36686335ac2e43fb03457dc116)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Databricks notebook widgets (the notebook's parameters).

Part of the embedded runtime (standard library only). It is placed in the
*Parameters* section of the generated notebook, before the other runtime
modules, so it must not depend on them.

Widgets are created only when missing: values typed by a user or passed by a
Job are never reset by re-running the notebook.
"""

from collections.abc import Mapping
from typing import Any

NOTEBOOK_WIDGETS = (
    ("tables_json", "1. Tables (JSON list of catalog.schema.table)"),
    ("analysis_level", "2. Analysis level"),
    ("output_dir", "3. Output directory (e.g. /Volumes/<catalog>/<schema>/<volume>/tabledossier)"),
    ("config_json", "4. Extra configuration (JSON object, no secrets)"),
)
ANALYSIS_LEVEL_CHOICES = ("metadata", "standard", "deep")


def ensure_widgets(dbutils: Any, defaults: Mapping[str, str]) -> list[str]:
    """Create missing widgets with the generated defaults; return the names created."""
    created = []
    for name, label in NOTEBOOK_WIDGETS:
        try:
            dbutils.widgets.get(name)
            continue
        except Exception:  # noqa: BLE001 - Databricks raises when a widget is not defined
            pass
        if name == "analysis_level":
            dbutils.widgets.dropdown(name, defaults[name], list(ANALYSIS_LEVEL_CHOICES), label)
        else:
            dbutils.widgets.text(name, defaults[name], label)
        created.append(name)
    return created


def read_widgets(dbutils: Any) -> dict[str, str]:
    """Return the current string value of every TableDossier widget."""
    return {name: str(dbutils.widgets.get(name)) for name, _ in NOTEBOOK_WIDGETS}

# COMMAND ----------

# DBTITLE 1,Parameters
# Configuration captured at generation time (defaults for the widgets below).
TD_GENERATED_CONFIG = {'kind': 'tabledossier.config',
 'config_version': '1.0',
 'tables': ['analytics.customers',
            'analytics.orders',
            'analytics.order_events',
            'analytics.returns'],
 'analysis_level': 'standard',
 'output_dir': '',
 'purpose': 'Synthetic demo: document the demo analytics tables.',
 'limits': {'max_tables': 10,
            'max_fields': 200,
            'max_depth': 3,
            'max_expressions_per_pass': 800,
            'max_aggregate_passes': 2},
 'sampling': {'method': 'prefix',
              'max_rows': 2000,
              'max_bytes': 16777216,
              'max_value_chars': 8192,
              'max_columns': 100,
              'random_fraction': None,
              'seed': 42},
 'consistency': {'pin_delta_version': True},
 'metrics': {'quantiles': [0.05, 0.25, 0.5, 0.75, 0.95],
             'quantile_accuracy': 10000,
             'approx_distinct_rsd': 0.02,
             'json_full_scope_validation': True},
 'semantic': {'min_observations': 30,
              'detect_threshold': 0.95,
              'mixed_threshold': 0.2,
              'confidence_level': 0.95},
 'thresholds': {'high_null_ratio': 0.5,
                'categorical_max_distinct': 20,
                'categorical_min_rows': 100,
                'identifier_min_distinct_ratio': 0.95,
                'identifier_min_rows': 30,
                'identifier_max_null_ratio': 0.01,
                'json_min_ratio': 0.8,
                'constant_min_rows': 2,
                'tail_iqr_multiplier': 10,
                'large_array_size': 1000},
 'value_policy': {'persist_examples': True,
                  'example_columns': [{'table': 'analytics.customers', 'column': 'segment'}],
                  'max_examples_per_column': 5,
                  'max_example_chars': 64,
                  'aggregate_extremes': 'include',
                  'json_key_names': 'include',
                  'redact_columns': []},
 'deep': {'targets': 'all_within_budget',
          'collections': True,
          'json_paths': True,
          'element_distinct': 'sample',
          'json_full_scope_validation': True,
          'max_extra_passes': 2,
          'max_explode_rows': 1000,
          'max_elements': 100000,
          'max_json_paths': 50,
          'max_json_depth': 3,
          'max_json_object_keys': 50,
          'uniqueness': {'keys': [{'table': 'analytics.customers',
                                   'columns': ['customer_id'],
                                   'id': 'customers_key'},
                                  {'table': 'analytics.orders',
                                   'columns': ['order_id'],
                                   'id': 'orders_key'},
                                  {'table': 'analytics.order_events',
                                   'columns': ['event_id'],
                                   'id': 'events_key'},
                                  {'table': 'analytics.order_events',
                                   'columns': ['order_id', 'event_type'],
                                   'id': 'events_per_order_and_type'}],
                         'declared_keys': True,
                         'identifier_candidates': True,
                         'max_keys': 5,
                         'max_passes': 1},
          'referential': {'configured': True,
                          'declared': True,
                          'mode': 'full_scope',
                          'max_relationships': 5,
                          'max_sample_rows': 10000},
          'relationship_hypotheses': {'enabled': True,
                                      'max_pairs': 5,
                                      'max_sample_rows': 10000,
                                      'inclusion_scope': 'sample',
                                      'min_inclusion_ratio': 0.95}},
 'table_options': {'analytics.customers': {'checks': [{'id': 'customers_email_nulls',
                                                       'type': 'max_null_ratio',
                                                       'column': 'email',
                                                       'max': 0.1}]},
                   'analytics.orders': {'purpose': 'Orders placed since 2025-01-01 (synthetic).',
                                        'filters': [{'column': 'order_ts',
                                                     'operator': 'ge',
                                                     'value': '2025-01-01T00:00:00Z',
                                                     'value_type': 'timestamp'}],
                                        'checks': [{'id': 'orders_min_rows',
                                                    'type': 'min_row_count',
                                                    'min': 1},
                                                   {'id': 'orders_customer_not_null',
                                                    'type': 'max_null_ratio',
                                                    'column': 'customer_id',
                                                    'max': 0.0},
                                                   {'id': 'orders_amount_range',
                                                    'type': 'value_range',
                                                    'column': 'amount',
                                                    'min': 0,
                                                    'max': 10000,
                                                    'description': 'Synthetic business bound; the '
                                                                   'demo data contains one planted '
                                                                   'outlier.'}]},
                   'analytics.returns': {'checks': [{'id': 'returns_reason_nulls',
                                                     'type': 'max_null_ratio',
                                                     'column': 'reason',
                                                     'max': 0.1}]}},
 'relationships': [{'id': 'orders_customer',
                    'from': {'table': 'analytics.orders', 'columns': ['customer_id']},
                    'to': {'table': 'analytics.customers', 'columns': ['customer_id']},
                    'cardinality': {'from': 'zero_or_more', 'to': 'exactly_one'},
                    'description': 'Each order references one customer (stated by the demo '
                                   'author).'},
                   {'id': 'events_order',
                    'from': {'table': 'analytics.order_events', 'columns': ['order_id']},
                    'to': {'table': 'analytics.orders', 'columns': ['order_id']},
                    'description': 'Events reference orders; cardinality intentionally not '
                                   'asserted.'}]}

TD_GENERATION = {'generator_version': '0.2.0',
 'generation_id': 'sha256:97a5554e40f47ca66bf9b8e4d8e1d0842c608a86e7e8173ad86093578dd76f51'}

TD_WIDGET_DEFAULTS = {'tables_json': '["analytics.customers", "analytics.orders", "analytics.order_events", '
                '"analytics.returns"]',
 'analysis_level': 'standard',
 'output_dir': '',
 'config_json': '{}'}

ensure_widgets(dbutils, TD_WIDGET_DEFAULTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Runtime definitions
# MAGIC
# MAGIC The following cells define the TableDossier runtime. Each cell is a module of the TableDossier package copied verbatim from the source file named in its header (with its SHA-256), except that `from tabledossier... import ...` lines were removed because earlier cells define those names. They use only the Python standard library and the PySpark provided by the runtime.
# MAGIC
# MAGIC Run them as they are; to change behaviour, change the configuration or regenerate the notebook.

# COMMAND ----------

# DBTITLE 1,Runtime: JSON Schemas
# JSON Schemas shipped with TableDossier 0.2.0: the exact text of the files in
# src/tabledossier/schemas/ (SHA-256 below), parsed with json.loads.
import json

TD_SCHEMAS = {}
# config.schema.json: sha256:d90470488296d9767f7540c63dcda0749af4307e69153d744e66bc7e31bc545f
TD_SCHEMAS['config'] = json.loads(r'''{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:tabledossier:schema:config:1.0",
  "title": "TableDossier configuration",
  "description": "Configuration used to generate a profiling notebook and, at run time, merged with notebook widget values. Never put secrets here.",
  "type": "object",
  "required": ["kind", "config_version"],
  "additionalProperties": false,
  "properties": {
    "$schema": {"type": "string"},
    "kind": {"const": "tabledossier.config"},
    "config_version": {"const": "1.0"},
    "tables": {
      "description": "Default table identifiers (catalog.schema.table). May be empty when generating; execution requires at least one.",
      "type": "array",
      "items": {"type": "string", "minLength": 1, "maxLength": 800},
      "maxItems": 100
    },
    "analysis_level": {"enum": ["metadata", "standard", "deep"]},
    "output_dir": {
      "description": "Destination directory in the execution environment, e.g. /Volumes/<catalog>/<schema>/<volume>/tabledossier. May be empty when generating.",
      "type": "string",
      "maxLength": 1024
    },
    "purpose": {"type": ["string", "null"], "maxLength": 2000},
    "limits": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "max_tables": {"type": "integer", "minimum": 1, "maximum": 100},
        "max_fields": {"type": "integer", "minimum": 1, "maximum": 5000},
        "max_depth": {"type": "integer", "minimum": 1, "maximum": 10},
        "max_expressions_per_pass": {"type": "integer", "minimum": 10, "maximum": 5000},
        "max_aggregate_passes": {"type": "integer", "minimum": 1, "maximum": 20}
      }
    },
    "sampling": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "method": {"enum": ["prefix", "random", "none"]},
        "max_rows": {"type": "integer", "minimum": 1, "maximum": 1000000},
        "max_bytes": {"type": "integer", "minimum": 1024, "maximum": 1073741824},
        "max_value_chars": {"type": "integer", "minimum": 16, "maximum": 1048576},
        "max_columns": {"type": "integer", "minimum": 1, "maximum": 1000},
        "random_fraction": {"type": ["number", "null"], "exclusiveMinimum": 0, "maximum": 1},
        "seed": {"type": "integer"}
      }
    },
    "consistency": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "pin_delta_version": {"type": "boolean"}
      }
    },
    "metrics": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "quantiles": {
          "type": "array",
          "items": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 1},
          "maxItems": 20,
          "uniqueItems": true
        },
        "quantile_accuracy": {"type": "integer", "minimum": 100, "maximum": 1000000},
        "approx_distinct_rsd": {"type": "number", "minimum": 0.01, "maximum": 0.3},
        "json_full_scope_validation": {"type": "boolean"}
      }
    },
    "semantic": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "min_observations": {"type": "integer", "minimum": 1, "maximum": 1000000},
        "detect_threshold": {"type": "number", "exclusiveMinimum": 0.5, "maximum": 1},
        "mixed_threshold": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "confidence_level": {"type": "number", "minimum": 0.5, "maximum": 0.999}
      }
    },
    "thresholds": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "high_null_ratio": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "categorical_max_distinct": {"type": "integer", "minimum": 2},
        "categorical_min_rows": {"type": "integer", "minimum": 1},
        "identifier_min_distinct_ratio": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "identifier_min_rows": {"type": "integer", "minimum": 1},
        "identifier_max_null_ratio": {"type": "number", "minimum": 0, "maximum": 1},
        "json_min_ratio": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "constant_min_rows": {"type": "integer", "minimum": 2},
        "tail_iqr_multiplier": {"type": "number", "exclusiveMinimum": 0},
        "large_array_size": {"type": "integer", "minimum": 1}
      }
    },
    "value_policy": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "persist_examples": {"type": "boolean"},
        "example_columns": {"type": "array", "items": {"$ref": "#/$defs/qualifiedColumn"}, "maxItems": 200},
        "max_examples_per_column": {"type": "integer", "minimum": 1, "maximum": 50},
        "max_example_chars": {"type": "integer", "minimum": 1, "maximum": 1000},
        "aggregate_extremes": {"enum": ["include", "redact"]},
        "json_key_names": {"enum": ["include", "redact"]},
        "redact_columns": {"type": "array", "items": {"$ref": "#/$defs/qualifiedColumn"}, "maxItems": 1000}
      }
    },
    "deep": {
      "description": "Opt-in operations of the deep level (standard + element and JSON path profiling, exact uniqueness, referential validation and relationship hypotheses). Ignored at other levels. See docs/configuration.md.",
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "targets": {
          "description": "all_within_budget, or an explicit list of collection (array/map) and string fields per table.",
          "oneOf": [
            {"const": "all_within_budget"},
            {"type": "array", "items": {"$ref": "#/$defs/qualifiedColumn"}, "maxItems": 1000}
          ]
        },
        "collections": {"type": "boolean"},
        "json_paths": {"type": "boolean"},
        "element_distinct": {"enum": ["sample", "full_scope", "off"]},
        "json_full_scope_validation": {"type": "boolean"},
        "max_extra_passes": {"type": "integer", "minimum": 0, "maximum": 10},
        "max_explode_rows": {"type": "integer", "minimum": 1, "maximum": 1000000},
        "max_elements": {"type": "integer", "minimum": 1, "maximum": 100000000},
        "max_json_paths": {"type": "integer", "minimum": 1, "maximum": 1000},
        "max_json_depth": {"type": "integer", "minimum": 1, "maximum": 10},
        "max_json_object_keys": {"type": "integer", "minimum": 1, "maximum": 1000},
        "uniqueness": {
          "description": "Exact uniqueness of keys: explicit keys, declared PRIMARY KEY/UNIQUE constraints and identifier candidates, within max_keys and max_passes per table.",
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "keys": {"type": "array", "items": {"$ref": "#/$defs/uniquenessKey"}, "maxItems": 1000},
            "declared_keys": {"type": "boolean"},
            "identifier_candidates": {"type": "boolean"},
            "max_keys": {"type": "integer", "minimum": 1, "maximum": 100},
            "max_passes": {"type": "integer", "minimum": 1, "maximum": 10}
          }
        },
        "referential": {
          "description": "Referential validation of configured relationships and declared foreign keys (one Spark action per relationship, at most max_relationships per run).",
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "configured": {"type": "boolean"},
            "declared": {"type": "boolean"},
            "mode": {"enum": ["full_scope", "sample"]},
            "max_relationships": {"type": "integer", "minimum": 0, "maximum": 100},
            "max_sample_rows": {"type": "integer", "minimum": 1, "maximum": 100000000}
          }
        },
        "relationship_hypotheses": {
          "description": "Data-driven relationship hypotheses (off by default; one Spark action per evaluated pair, at most max_pairs per run).",
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "enabled": {"type": "boolean"},
            "max_pairs": {"type": "integer", "minimum": 0, "maximum": 100},
            "max_sample_rows": {"type": "integer", "minimum": 1, "maximum": 100000000},
            "inclusion_scope": {"enum": ["sample", "full_scope"]},
            "min_inclusion_ratio": {"type": "number", "minimum": 0, "maximum": 1}
          }
        }
      }
    },
    "table_options": {
      "description": "Per-table options keyed by table identifier.",
      "type": "object",
      "additionalProperties": {"$ref": "#/$defs/tableOptions"}
    },
    "relationships": {
      "type": "array",
      "items": {"$ref": "#/$defs/relationship"},
      "maxItems": 500
    }
  },
  "$defs": {
    "columnRef": {
      "description": "A string is a literal top-level column name (dots are not split). A list navigates nested struct fields.",
      "oneOf": [
        {"type": "string", "minLength": 1, "maxLength": 255},
        {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 255}, "minItems": 1, "maxItems": 10}
      ]
    },
    "qualifiedColumn": {
      "type": "object",
      "additionalProperties": false,
      "required": ["table", "column"],
      "properties": {
        "table": {"type": "string", "minLength": 1},
        "column": {"type": "string", "minLength": 1, "description": "Display path, e.g. customer.email or `odd.name`."}
      }
    },
    "filter": {
      "type": "object",
      "additionalProperties": false,
      "required": ["column", "operator"],
      "properties": {
        "column": {"$ref": "#/$defs/columnRef"},
        "operator": {"enum": ["eq", "ne", "lt", "le", "gt", "ge", "in", "not_in", "between", "is_null", "is_not_null", "like"]},
        "value": {"type": ["string", "number", "boolean", "array"]},
        "value_type": {"enum": ["string", "integer", "double", "decimal", "boolean", "date", "timestamp"]}
      }
    },
    "check": {
      "type": "object",
      "additionalProperties": false,
      "required": ["id", "type"],
      "properties": {
        "id": {"type": "string", "pattern": "^[A-Za-z0-9_.-]{1,100}$"},
        "type": {"enum": ["min_row_count", "max_row_count", "max_null_ratio", "max_null_count", "max_empty_string_ratio", "value_range"]},
        "column": {"$ref": "#/$defs/columnRef"},
        "min": {"type": "number"},
        "max": {"type": "number"},
        "description": {"type": "string", "maxLength": 1000}
      }
    },
    "tableOptions": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "columns": {
          "description": "Top-level columns to profile (null or absent = all).",
          "type": ["array", "null"],
          "items": {"type": "string", "minLength": 1, "maxLength": 255},
          "maxItems": 5000
        },
        "filters": {"type": "array", "items": {"$ref": "#/$defs/filter"}, "maxItems": 50},
        "purpose": {"type": ["string", "null"], "maxLength": 2000},
        "checks": {"type": "array", "items": {"$ref": "#/$defs/check"}, "maxItems": 500}
      }
    },
    "cardinality": {"enum": ["zero_or_one", "exactly_one", "zero_or_more", "one_or_more"]},
    "uniquenessKey": {
      "type": "object",
      "additionalProperties": false,
      "required": ["table", "columns"],
      "properties": {
        "id": {"type": "string", "pattern": "^[A-Za-z0-9_.-]{1,100}$"},
        "table": {"type": "string", "minLength": 1},
        "columns": {"type": "array", "items": {"$ref": "#/$defs/columnRef"}, "minItems": 1, "maxItems": 32}
      }
    },
    "relationshipEnd": {
      "type": "object",
      "additionalProperties": false,
      "required": ["table", "columns"],
      "properties": {
        "table": {"type": "string", "minLength": 1},
        "columns": {"type": "array", "items": {"$ref": "#/$defs/columnRef"}, "minItems": 1, "maxItems": 32}
      }
    },
    "relationship": {
      "type": "object",
      "additionalProperties": false,
      "required": ["id", "from", "to"],
      "properties": {
        "id": {"type": "string", "pattern": "^[A-Za-z0-9_.-]{1,100}$"},
        "from": {"$ref": "#/$defs/relationshipEnd"},
        "to": {"$ref": "#/$defs/relationshipEnd"},
        "cardinality": {
          "type": "object",
          "additionalProperties": false,
          "required": ["from", "to"],
          "properties": {
            "from": {"$ref": "#/$defs/cardinality"},
            "to": {"$ref": "#/$defs/cardinality"}
          }
        },
        "description": {"type": "string", "maxLength": 2000}
      }
    }
  }
}''')
# profile.schema.json: sha256:b949f622ebbe2489cae299ba95852d7cb03a7c9e0593808b353c150c9bab77ad
TD_SCHEMAS['profile'] = json.loads(r'''{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:tabledossier:schema:profile:1.2",
  "title": "TableDossier profile",
  "description": "Canonical, engine-independent result of one profiling run. Documentation is derived from this document only. Version 1.2 only adds to 1.1 (exact uniqueness, referential validation, relationship hypotheses), which only adds to 1.0 (deep level). See docs/contract.md.",
  "type": "object",
  "required": ["kind", "schema_version", "tool", "run", "value_exposure", "tables", "relationships", "summary"],
  "additionalProperties": false,
  "properties": {
    "kind": {"const": "tabledossier.profile"},
    "schema_version": {"const": "1.2"},
    "tool": {
      "type": "object",
      "required": ["name", "version"],
      "additionalProperties": false,
      "properties": {
        "name": {"const": "tabledossier"},
        "version": {"type": "string", "minLength": 1}
      }
    },
    "run": {"$ref": "#/$defs/run"},
    "value_exposure": {"$ref": "#/$defs/valueExposure"},
    "tables": {"type": "array", "items": {"$ref": "#/$defs/table"}},
    "relationships": {"type": "array", "items": {"$ref": "#/$defs/relationship"}},
    "summary": {"$ref": "#/$defs/summary"},
    "referential_validation": {"description": "1.2: run-level record of referential validation (null below the deep level).", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/referentialValidation"}]},
    "relationship_hypotheses": {"description": "1.2: data-driven relationship hypotheses, kept apart from known relationships (null below the deep level).", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/relationshipHypotheses"}]}
  },
  "$defs": {
    "utcTimestamp": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$"},
    "fingerprint": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
    "stringList": {"type": "array", "items": {"type": "string"}},
    "nullableString": {"type": ["string", "null"]},
    "run": {
      "type": "object",
      "required": [
        "run_id", "status", "analysis_level", "started_at", "finished_at", "duration_ms",
        "reference_time", "environment", "effective_config", "config_fingerprint",
        "parameter_sources", "generation", "purpose", "capabilities"
      ],
      "additionalProperties": false,
      "properties": {
        "run_id": {"type": "string", "pattern": "^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$"},
        "status": {"enum": ["succeeded", "partial", "failed"]},
        "analysis_level": {"enum": ["metadata", "standard", "deep"]},
        "started_at": {"$ref": "#/$defs/utcTimestamp"},
        "finished_at": {"$ref": "#/$defs/utcTimestamp"},
        "duration_ms": {"type": "integer", "minimum": 0},
        "reference_time": {"$ref": "#/$defs/utcTimestamp", "description": "Reference instant used by temporal metrics (after_reference_count)."},
        "environment": {"$ref": "#/$defs/environment"},
        "effective_config": {"type": "object", "description": "Configuration after applying defaults, generated values and widgets. It never contains credentials."},
        "config_fingerprint": {"$ref": "#/$defs/fingerprint"},
        "parameter_sources": {"type": "object"},
        "generation": {
          "type": "object",
          "required": ["generator_version", "generation_id"],
          "additionalProperties": false,
          "properties": {
            "generator_version": {"$ref": "#/$defs/nullableString"},
            "generation_id": {"type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$"}
          }
        },
        "purpose": {"$ref": "#/$defs/nullableString"},
        "capabilities": {
          "type": "object",
          "additionalProperties": {
            "type": "object",
            "required": ["available", "detail"],
            "additionalProperties": false,
            "properties": {
              "available": {"type": "boolean"},
              "detail": {"type": "string"}
            }
          }
        }
      }
    },
    "environment": {
      "type": "object",
      "required": ["engine", "execution_context", "python_version"],
      "additionalProperties": false,
      "properties": {
        "engine": {"enum": ["spark", "none"]},
        "execution_context": {"enum": ["databricks", "spark", "synthetic"]},
        "python_version": {"type": "string"},
        "spark_version": {"$ref": "#/$defs/nullableString"},
        "databricks_runtime_version": {"$ref": "#/$defs/nullableString"},
        "session_timezone": {"$ref": "#/$defs/nullableString"},
        "ansi_mode": {"type": ["boolean", "null"]},
        "spark_connect": {"type": ["boolean", "null"]}
      }
    },
    "valueExposure": {
      "type": "object",
      "required": ["raw_values_persisted", "example_columns", "aggregate_extremes", "json_key_names", "redacted_columns", "notes"],
      "additionalProperties": false,
      "properties": {
        "raw_values_persisted": {"type": "boolean"},
        "example_columns": {"type": "array", "items": {"type": "object"}},
        "aggregate_extremes": {"enum": ["include", "redact"]},
        "json_key_names": {"enum": ["include", "redact"]},
        "redacted_columns": {"type": "array", "items": {"type": "object"}},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "error": {
      "type": "object",
      "required": ["stage", "error_class", "condition", "message"],
      "additionalProperties": false,
      "properties": {
        "stage": {"enum": ["resolve", "metadata", "snapshot", "sample", "semantic", "aggregate", "checks", "assemble"]},
        "error_class": {"type": "string"},
        "condition": {"$ref": "#/$defs/nullableString"},
        "message": {"type": "string", "maxLength": 1000}
      }
    },
    "table": {
      "type": "object",
      "required": [
        "table_id", "table_key", "identifier", "status", "errors", "purpose", "source",
        "consistency", "scope", "sample", "summary", "table_metrics", "schema",
        "field_profiles", "constraints", "findings", "quality_checks", "suggested_rules",
        "omissions", "unsupported", "operations", "timings_ms", "notes"
      ],
      "additionalProperties": false,
      "properties": {
        "table_id": {"type": "string", "pattern": "^t_[0-9a-f]{12}$"},
        "table_key": {"type": "string", "minLength": 1},
        "identifier": {
          "type": "object",
          "required": ["input", "parts", "quoted"],
          "additionalProperties": false,
          "properties": {
            "input": {"type": "string"},
            "parts": {"$ref": "#/$defs/stringList"},
            "quoted": {"type": "string"}
          }
        },
        "status": {"enum": ["succeeded", "partial", "failed"]},
        "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        "purpose": {"$ref": "#/$defs/nullableString"},
        "source": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/source"}]},
        "consistency": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/consistency"}]},
        "scope": {"$ref": "#/$defs/scope"},
        "sample": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/sample"}]},
        "summary": {
          "type": "object",
          "additionalProperties": {"type": ["integer", "null"], "minimum": 0}
        },
        "table_metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}},
        "schema": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/schemaTree"}]},
        "field_profiles": {"type": "array", "items": {"$ref": "#/$defs/fieldProfile"}},
        "constraints": {"type": "array", "items": {"$ref": "#/$defs/constraint"}},
        "findings": {"type": "array", "items": {"$ref": "#/$defs/finding"}},
        "quality_checks": {"type": "array", "items": {"$ref": "#/$defs/check"}},
        "suggested_rules": {"type": "array", "items": {"$ref": "#/$defs/rule"}},
        "omissions": {"type": "array", "items": {"$ref": "#/$defs/omission"}},
        "unsupported": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["capability", "detail"],
            "additionalProperties": false,
            "properties": {"capability": {"type": "string"}, "detail": {"type": "string"}}
          }
        },
        "operations": {"$ref": "#/$defs/operations"},
        "timings_ms": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "notes": {"$ref": "#/$defs/stringList"},
        "deep": {"description": "1.1: what the deep level covered and what its budgets limited (null at other levels).", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/deepCoverage"}]},
        "uniqueness": {"description": "1.2: exact uniqueness of keys (null below the deep level).", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/uniqueness"}]}
      }
    },
    "source": {
      "type": "object",
      "required": ["source_type", "table_type", "provider", "comment", "created_at", "last_modified", "partition_columns", "clustering_columns", "size_in_bytes", "file_count", "metadata_state", "captured_at", "notes"],
      "additionalProperties": false,
      "properties": {
        "source_type": {"enum": ["table", "view", "unknown"]},
        "table_type": {"$ref": "#/$defs/nullableString"},
        "provider": {"$ref": "#/$defs/nullableString"},
        "comment": {"$ref": "#/$defs/nullableString"},
        "created_at": {"$ref": "#/$defs/nullableString"},
        "last_modified": {"$ref": "#/$defs/nullableString"},
        "partition_columns": {"$ref": "#/$defs/stringList"},
        "clustering_columns": {"$ref": "#/$defs/stringList"},
        "size_in_bytes": {"$ref": "#/$defs/metric"},
        "file_count": {"$ref": "#/$defs/metric"},
        "metadata_state": {"enum": ["current_at_capture"]},
        "captured_at": {"$ref": "#/$defs/utcTimestamp"},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "consistency": {
      "type": "object",
      "required": ["mode", "delta_version", "version_timestamp", "guarantee", "notes"],
      "additionalProperties": false,
      "properties": {
        "mode": {"enum": ["pinned_delta_version", "unpinned", "metadata_only"]},
        "delta_version": {"type": ["integer", "null"], "minimum": 0},
        "version_timestamp": {"$ref": "#/$defs/nullableString"},
        "guarantee": {"type": "string"},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "scope": {
      "type": "object",
      "required": ["population", "scope_label", "filters", "selected_columns", "row_semantics"],
      "additionalProperties": false,
      "properties": {
        "population": {"enum": ["full", "filtered"]},
        "scope_label": {"enum": ["full_table", "filtered_table", "full_snapshot", "filtered_snapshot", "table_metadata"]},
        "filters": {"type": "array", "items": {"type": "object"}},
        "selected_columns": {"type": ["array", "null"], "items": {"type": "string"}},
        "row_semantics": {"type": "string"}
      }
    },
    "sample": {
      "type": "object",
      "required": ["enabled", "method", "biased", "reads_full_source", "max_rows", "max_bytes", "max_value_chars", "rows_collected", "bytes_retained", "values_truncated", "stopped_reason", "fields_sampled", "fields_omitted", "notes"],
      "additionalProperties": false,
      "properties": {
        "enabled": {"type": "boolean"},
        "method": {"enum": ["prefix", "random", "none"]},
        "biased": {"type": ["boolean", "null"]},
        "reads_full_source": {"type": ["boolean", "null"]},
        "max_rows": {"type": "integer", "minimum": 1},
        "max_bytes": {"type": "integer", "minimum": 1},
        "max_value_chars": {"type": "integer", "minimum": 1},
        "random_fraction": {"type": ["number", "null"]},
        "seed": {"type": ["integer", "null"]},
        "rows_collected": {"type": ["integer", "null"], "minimum": 0},
        "bytes_retained": {"type": ["integer", "null"], "minimum": 0},
        "values_truncated": {"type": ["integer", "null"], "minimum": 0},
        "stopped_reason": {"enum": ["row_limit", "byte_budget", "exhausted", "disabled", "not_applicable", "error"]},
        "fields_sampled": {"type": "integer", "minimum": 0},
        "fields_omitted": {"type": "integer", "minimum": 0},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "segment": {
      "type": "object",
      "required": ["kind"],
      "additionalProperties": false,
      "properties": {
        "kind": {"enum": ["field", "array_element", "map_key", "map_value"]},
        "name": {"type": "string", "minLength": 1}
      },
      "if": {"properties": {"kind": {"const": "field"}}},
      "then": {"required": ["name"]},
      "else": {"not": {"required": ["name"]}}
    },
    "path": {"type": "array", "items": {"$ref": "#/$defs/segment"}, "minItems": 1},
    "shallowType": {
      "type": "object",
      "required": ["kind", "physical_type"],
      "additionalProperties": false,
      "properties": {
        "kind": {"enum": ["integer", "float", "decimal", "string", "boolean", "date", "timestamp", "timestamp_ntz", "binary", "struct", "array", "map", "variant", "interval", "null", "other"]},
        "physical_type": {"type": "string"},
        "precision": {"type": "integer"},
        "scale": {"type": "integer"}
      }
    },
    "schemaTree": {
      "type": "object",
      "required": ["fields", "nodes_total", "nodes_omitted", "top_level_omitted", "limits", "captured_from"],
      "additionalProperties": false,
      "properties": {
        "fields": {"type": "array", "items": {"$ref": "#/$defs/schemaNode"}},
        "nodes_total": {"type": "integer", "minimum": 0},
        "nodes_omitted": {"type": "integer", "minimum": 0},
        "top_level_omitted": {"type": "integer", "minimum": 0},
        "limits": {"type": "object"},
        "captured_from": {"enum": ["pinned_snapshot", "current_table"]}
      }
    },
    "schemaNode": {
      "type": "object",
      "required": ["field_id", "name", "segment_kind", "path", "display_path", "parent_field_id", "depth", "type", "nullable", "comment", "children", "children_omitted", "children_omitted_reason"],
      "additionalProperties": false,
      "properties": {
        "field_id": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"},
        "name": {"$ref": "#/$defs/nullableString"},
        "segment_kind": {"enum": ["field", "array_element", "map_key", "map_value"]},
        "path": {"$ref": "#/$defs/path"},
        "display_path": {"type": "string"},
        "parent_field_id": {"$ref": "#/$defs/nullableString"},
        "depth": {"type": "integer", "minimum": 1},
        "type": {"$ref": "#/$defs/shallowType"},
        "nullable": {"type": ["boolean", "null"], "description": "Nullability declared by the source schema (not observed)."},
        "comment": {"$ref": "#/$defs/nullableString"},
        "children": {"type": "array", "items": {"$ref": "#/$defs/schemaNode"}},
        "children_omitted": {"type": "integer", "minimum": 0},
        "children_omitted_reason": {"$ref": "#/$defs/nullableString"}
      }
    },
    "metric": {
      "type": "object",
      "required": ["name", "status", "value", "unit", "scope", "accuracy", "source"],
      "additionalProperties": false,
      "properties": {
        "name": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
        "status": {"enum": ["measured", "not_computed", "unsupported", "insufficient_data", "redacted", "error", "unavailable"]},
        "value": {"type": ["number", "string", "boolean", "array", "null"]},
        "value_type": {"enum": ["integer", "float", "decimal", "boolean", "string", "date", "timestamp", "timestamp_ntz", "ratio", "quantiles"]},
        "unit": {"$ref": "#/$defs/nullableString"},
        "scope": {"enum": ["table_metadata", "full_table", "filtered_table", "full_snapshot", "filtered_snapshot", "sample"]},
        "accuracy": {"enum": ["exact", "approximate", "as_recorded", "unknown"]},
        "source": {"enum": ["catalog_metadata", "table_statistics", "table_history", "aggregate", "sample", "derived"]},
        "method": {"type": "string"},
        "denominator": {"type": "integer", "minimum": 0},
        "denominator_unit": {"$ref": "#/$defs/nullableString"},
        "reason": {"type": "string"},
        "details": {"type": "object"}
      },
      "if": {"properties": {"status": {"const": "measured"}}},
      "then": {
        "required": ["value_type", "method"],
        "properties": {"value": {"type": ["number", "string", "boolean", "array"]}}
      },
      "else": {
        "required": ["reason"],
        "properties": {"value": {"type": "null"}}
      }
    },
    "observedFormat": {
      "type": "object",
      "required": ["status", "format", "scope", "eligible_observations", "candidates", "method", "limitations"],
      "additionalProperties": true,
      "properties": {
        "status": {"enum": ["detected", "mixed", "ambiguous", "unknown", "insufficient_data", "not_computed"]},
        "format": {"type": ["string", "null"]},
        "scope": {"const": "sample"},
        "eligible_observations": {"type": "integer", "minimum": 0},
        "candidates": {"type": "array", "items": {"type": "object", "required": ["format", "matches", "match_ratio"]}},
        "method": {"type": "string"},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    },
    "fieldProfile": {
      "type": "object",
      "required": ["field_id", "display_path", "path", "type_kind", "physical_type", "nullable", "comment", "profiled", "omission_reason", "metrics", "semantics", "json_profile", "concentration", "examples"],
      "additionalProperties": false,
      "properties": {
        "field_id": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"},
        "display_path": {"type": "string"},
        "path": {"$ref": "#/$defs/path"},
        "type_kind": {"type": "string"},
        "physical_type": {"type": "string"},
        "nullable": {"type": ["boolean", "null"]},
        "comment": {"$ref": "#/$defs/nullableString"},
        "profiled": {"type": "boolean"},
        "omission_reason": {"$ref": "#/$defs/nullableString"},
        "metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}},
        "semantics": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["observed_format", "candidate_roles"],
              "additionalProperties": false,
              "properties": {
                "observed_format": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/observedFormat"}]},
                "candidate_roles": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "required": ["role", "evidence", "limitations"],
                    "additionalProperties": false,
                    "properties": {
                      "role": {"enum": ["identifier_candidate", "categorical_candidate", "json_document_candidate"]},
                      "evidence": {"type": "array"},
                      "limitations": {"type": "string"}
                    }
                  }
                }
              }
            }
          ]
        },
        "json_profile": {"type": ["object", "null"]},
        "concentration": {"type": ["object", "null"]},
        "examples": {"type": ["object", "null"]},
        "element_context": {"description": "1.1: set for profiled array elements and map entries; their denominators are elements or entries.", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/elementContext"}]},
        "json_paths": {"description": "1.1: JSON path catalogue of a string field (deep level).", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/jsonPaths"}]}
      }
    },
    "constraint": {
      "type": "object",
      "required": ["name", "constraint_type", "columns", "expression", "referenced", "enforcement", "source"],
      "additionalProperties": false,
      "properties": {
        "name": {"type": "string"},
        "constraint_type": {"enum": ["primary_key", "foreign_key", "unique", "check", "not_null"]},
        "columns": {"$ref": "#/$defs/stringList"},
        "expression": {"$ref": "#/$defs/nullableString"},
        "referenced": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["table", "columns"],
              "additionalProperties": false,
              "properties": {"table": {"type": "string"}, "columns": {"$ref": "#/$defs/stringList"}}
            }
          ]
        },
        "enforcement": {"enum": ["enforced", "not_enforced", "unknown"]},
        "source": {"enum": ["information_schema", "delta_table_property", "source_schema"]}
      }
    },
    "finding": {
      "type": "object",
      "required": ["finding_id", "code", "severity", "field_id", "display_path", "title", "message", "evidence", "threshold", "severity_reason", "limitations"],
      "additionalProperties": false,
      "properties": {
        "finding_id": {"type": "string"},
        "code": {"enum": ["all_null", "high_null_ratio", "possible_constant", "possible_categorical", "identifier_candidate", "probable_json", "extreme_numeric_tail", "large_arrays"]},
        "severity": {"enum": ["info", "warning"]},
        "field_id": {"type": "string"},
        "display_path": {"type": "string"},
        "title": {"type": "string"},
        "message": {"type": "string"},
        "evidence": {"type": "array"},
        "threshold": {"type": ["object", "null"]},
        "severity_reason": {"type": "string"},
        "limitations": {"type": "string"}
      }
    },
    "check": {
      "type": "object",
      "required": ["check_id", "type", "origin", "target", "parameters", "status", "observed", "message", "description"],
      "additionalProperties": false,
      "properties": {
        "check_id": {"type": "string"},
        "type": {"enum": ["min_row_count", "max_row_count", "max_null_ratio", "max_null_count", "max_empty_string_ratio", "value_range"]},
        "origin": {"const": "configured"},
        "target": {
          "type": "object",
          "required": ["table", "column"],
          "additionalProperties": false,
          "properties": {"table": {"type": "string"}, "column": {"$ref": "#/$defs/nullableString"}}
        },
        "parameters": {"type": "object"},
        "status": {"enum": ["pass", "fail", "not_evaluated", "error"]},
        "observed": {"type": ["object", "null"]},
        "message": {"type": "string"},
        "description": {"$ref": "#/$defs/nullableString"}
      }
    },
    "rule": {
      "type": "object",
      "required": ["rule_id", "table", "column", "rule_type", "parameters", "status", "requires_review", "rationale", "evidence"],
      "additionalProperties": false,
      "properties": {
        "rule_id": {"type": "string"},
        "table": {"type": "string"},
        "column": {"$ref": "#/$defs/nullableString"},
        "rule_type": {"enum": ["not_null", "unique", "accepted_values", "valid_json", "matches_format", "value_range", "min_row_count"]},
        "parameters": {"type": "object"},
        "status": {"const": "proposed"},
        "requires_review": {"const": true},
        "rationale": {"type": "string"},
        "evidence": {"type": "array"}
      }
    },
    "omission": {
      "type": "object",
      "required": ["field_id", "display_path", "reason", "detail", "metrics"],
      "additionalProperties": false,
      "properties": {
        "field_id": {"$ref": "#/$defs/nullableString"},
        "display_path": {"$ref": "#/$defs/nullableString"},
        "reason": {"type": "string"},
        "detail": {"type": "string"},
        "metrics": {"$ref": "#/$defs/stringList"}
      }
    },
    "operations": {
      "type": "object",
      "required": ["planned", "observed", "physical_scans", "bytes_read", "monetary_cost"],
      "additionalProperties": false,
      "properties": {
        "planned": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["operation_id", "kind", "description", "reads_user_data"],
            "additionalProperties": false,
            "properties": {
              "operation_id": {"type": "string"},
              "kind": {"enum": ["catalog_metadata", "table_detail", "table_history", "information_schema", "sample_collect", "aggregate_pass", "deep_aggregate_pass", "element_explode_pass", "uniqueness_pass", "referential_check", "relationship_hypothesis_check"]},
              "description": {"type": "string"},
              "reads_user_data": {"type": "boolean"},
              "details": {"type": "object"}
            }
          }
        },
        "observed": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["operation_id", "status", "duration_ms"],
            "additionalProperties": false,
            "properties": {
              "operation_id": {"type": "string"},
              "status": {"enum": ["succeeded", "failed", "skipped"]},
              "duration_ms": {"type": ["integer", "null"], "minimum": 0},
              "rows_returned": {"type": ["integer", "null"], "minimum": 0},
              "detail": {"$ref": "#/$defs/nullableString"}
            }
          }
        },
        "physical_scans": {"oneOf": [{"const": "unknown"}, {"type": "integer", "minimum": 0}]},
        "bytes_read": {"oneOf": [{"const": "unknown"}, {"type": "integer", "minimum": 0}]},
        "monetary_cost": {"const": "unknown"}
      }
    },
    "elementContext": {
      "type": "object",
      "required": ["collection_field_id", "collection_display_path", "collection_kind", "segment", "inner", "unit"],
      "additionalProperties": false,
      "properties": {
        "collection_field_id": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"},
        "collection_display_path": {"type": "string"},
        "collection_kind": {"enum": ["array", "map"]},
        "segment": {"enum": ["array_element", "map_key", "map_value"]},
        "inner": {"$ref": "#/$defs/stringList"},
        "unit": {"enum": ["elements", "entries"]}
      }
    },
    "jsonSegment": {
      "type": "object",
      "required": ["kind"],
      "additionalProperties": false,
      "properties": {
        "kind": {"enum": ["key", "items", "any_key"]},
        "name": {"type": "string"}
      },
      "if": {"properties": {"kind": {"const": "key"}}},
      "then": {"required": ["name"]},
      "else": {"not": {"required": ["name"]}}
    },
    "jsonPath": {
      "type": "object",
      "required": ["path", "segments", "depth", "present_in", "presence_ratio", "occurrences", "types", "heterogeneous", "dominant_type", "map_like", "full_scope"],
      "additionalProperties": false,
      "properties": {
        "path": {"type": "string", "minLength": 1},
        "segments": {"type": "array", "items": {"$ref": "#/$defs/jsonSegment"}, "minItems": 1},
        "depth": {"type": "integer", "minimum": 1},
        "present_in": {"type": "integer", "minimum": 0},
        "presence_ratio": {"type": ["number", "null"], "minimum": 0, "maximum": 1},
        "occurrences": {"type": "integer", "minimum": 0},
        "types": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "heterogeneous": {"type": "boolean"},
        "dominant_type": {"enum": ["object", "array", "string", "number", "boolean", null]},
        "map_like": {"type": "boolean"},
        "full_scope": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["status", "reason", "metrics"],
              "additionalProperties": false,
              "properties": {
                "status": {"enum": ["measured", "incomplete", "not_computed"]},
                "reason": {"$ref": "#/$defs/nullableString"},
                "metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}}
              }
            }
          ]
        }
      }
    },
    "jsonPaths": {
      "type": "object",
      "required": ["scope", "source", "eligible_observations", "excluded", "counts", "documents", "max_depth", "depth_truncated", "paths_observed", "paths_listed", "paths_omitted", "tracking_truncated", "map_like_paths", "heterogeneous_paths", "paths", "paths_omitted_reason", "full_scope", "method", "limitations"],
      "additionalProperties": false,
      "properties": {
        "scope": {"const": "sample"},
        "source": {"const": "sample"},
        "eligible_observations": {"type": "integer", "minimum": 0},
        "excluded": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "counts": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "documents": {"type": "integer", "minimum": 0},
        "max_depth": {"type": "integer", "minimum": 1},
        "depth_truncated": {"type": "boolean"},
        "paths_observed": {"type": "integer", "minimum": 0},
        "paths_listed": {"type": "integer", "minimum": 0},
        "paths_omitted": {"type": "integer", "minimum": 0},
        "tracking_truncated": {"type": "boolean"},
        "map_like_paths": {"type": "integer", "minimum": 0},
        "heterogeneous_paths": {"type": "integer", "minimum": 0},
        "paths": {"oneOf": [{"type": "null"}, {"type": "array", "items": {"$ref": "#/$defs/jsonPath"}}]},
        "paths_omitted_reason": {"$ref": "#/$defs/nullableString"},
        "full_scope": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["method", "status", "reason", "documents", "limitations"],
              "additionalProperties": false,
              "properties": {
                "method": {"enum": ["variant", "get_json_object", null]},
                "status": {"enum": ["measured", "not_computed", "unsupported"]},
                "reason": {"$ref": "#/$defs/nullableString"},
                "documents": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/metric"}]},
                "method_description": {"type": "string"},
                "limitations": {"$ref": "#/$defs/stringList"}
              }
            }
          ]
        },
        "method": {"type": "string"},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    },
    "deepCoverage": {
      "type": "object",
      "required": ["targets", "requested_targets", "not_eligible", "collections", "json_fields", "budget", "extra_passes", "expressions", "element_distinct", "limited", "notes"],
      "additionalProperties": false,
      "properties": {
        "targets": {"enum": ["all_within_budget", "explicit"]},
        "requested_targets": {"type": ["array", "null"], "items": {"type": "string"}},
        "not_eligible": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["display_path", "reason"],
            "additionalProperties": false,
            "properties": {"display_path": {"type": "string"}, "reason": {"type": "string"}}
          }
        },
        "collections": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["field_id", "display_path", "kind", "element_fields_profiled", "element_fields_omitted"],
            "additionalProperties": false,
            "properties": {
              "field_id": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"},
              "display_path": {"type": "string"},
              "kind": {"enum": ["array", "map"]},
              "element_fields_profiled": {"type": "integer", "minimum": 0},
              "element_fields_omitted": {"type": "integer", "minimum": 0}
            }
          }
        },
        "json_fields": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["field_id", "display_path", "status", "reason", "paths_listed", "paths_validated", "full_scope_method"],
            "additionalProperties": false,
            "properties": {
              "field_id": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"},
              "display_path": {"type": "string"},
              "status": {"enum": ["catalogued", "not_catalogued"]},
              "reason": {"$ref": "#/$defs/nullableString"},
              "paths_listed": {"type": "integer", "minimum": 0},
              "paths_validated": {"type": "integer", "minimum": 0},
              "full_scope_method": {"enum": ["variant", "get_json_object", null]}
            }
          }
        },
        "budget": {
          "type": "object",
          "required": ["max_extra_passes", "max_explode_rows", "max_elements", "max_json_paths", "max_json_depth", "max_json_object_keys"],
          "additionalProperties": false,
          "properties": {
            "max_extra_passes": {"type": "integer", "minimum": 0},
            "max_explode_rows": {"type": "integer", "minimum": 1},
            "max_elements": {"type": "integer", "minimum": 1},
            "max_json_paths": {"type": "integer", "minimum": 1},
            "max_json_depth": {"type": "integer", "minimum": 1},
            "max_json_object_keys": {"type": "integer", "minimum": 1}
          }
        },
        "extra_passes": {
          "type": "object",
          "required": ["budget", "planned", "aggregate", "element_explode"],
          "additionalProperties": false,
          "properties": {
            "budget": {"type": "integer", "minimum": 0},
            "planned": {"type": "integer", "minimum": 0},
            "aggregate": {"type": "integer", "minimum": 0},
            "element_explode": {"type": "integer", "minimum": 0}
          }
        },
        "expressions": {
          "type": "object",
          "required": ["planned", "omitted"],
          "additionalProperties": false,
          "properties": {
            "planned": {"type": "integer", "minimum": 0},
            "omitted": {"type": "integer", "minimum": 0}
          }
        },
        "element_distinct": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["mode", "status", "reason", "fields", "max_rows", "max_elements", "rows_with_elements", "elements_examined", "stopped_reason"],
              "additionalProperties": false,
              "properties": {
                "mode": {"enum": ["sample", "full_scope", "off"]},
                "status": {"enum": ["measured", "not_computed", "error"]},
                "reason": {"$ref": "#/$defs/nullableString"},
                "fields": {"type": "integer", "minimum": 0},
                "method": {"enum": ["prefix", "random", "none"]},
                "max_rows": {"type": ["integer", "null"], "minimum": 1},
                "max_elements": {"type": "integer", "minimum": 1},
                "rows_with_elements": {"type": ["integer", "null"], "minimum": 0},
                "elements_examined": {"type": ["integer", "null"], "minimum": 0},
                "stopped_reason": {"enum": ["element_limit", "exhausted", "not_applicable", "error"]}
              }
            }
          ]
        },
        "limited": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["item", "reason", "detail"],
            "additionalProperties": false,
            "properties": {
              "item": {"type": "string"},
              "reason": {"type": "string"},
              "detail": {"type": "string"}
            }
          }
        },
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "relationshipEnd": {
      "type": "object",
      "required": ["table", "columns"],
      "additionalProperties": false,
      "properties": {
        "table": {"type": "string"},
        "columns": {"type": "array", "items": {"type": "string"}, "minItems": 1}
      }
    },
    "cardinality": {"enum": ["zero_or_one", "exactly_one", "zero_or_more", "one_or_more"]},
    "relationship": {
      "type": "object",
      "required": ["relationship_id", "origin", "name", "from", "to", "cardinality", "cardinality_source", "enforcement", "validation", "scope", "description", "notes"],
      "additionalProperties": false,
      "properties": {
        "relationship_id": {"type": "string"},
        "origin": {"enum": ["declared_constraint", "configuration", "annotation"]},
        "name": {"type": "string"},
        "from": {"$ref": "#/$defs/relationshipEnd"},
        "to": {"$ref": "#/$defs/relationshipEnd"},
        "cardinality": {
          "oneOf": [
            {"type": "null"},
            {
              "type": "object",
              "required": ["from", "to"],
              "additionalProperties": false,
              "properties": {"from": {"$ref": "#/$defs/cardinality"}, "to": {"$ref": "#/$defs/cardinality"}}
            }
          ]
        },
        "cardinality_source": {"enum": ["provided", null]},
        "enforcement": {"enum": ["enforced", "not_enforced", "unknown"]},
        "validation": {"enum": ["not_validated", "validated", "violated"]},
        "scope": {"enum": ["declared_metadata", "human_provided"]},
        "description": {"$ref": "#/$defs/nullableString"},
        "notes": {"$ref": "#/$defs/stringList"},
        "validation_detail": {"description": "1.2: why the relationship is validated, violated or not validated.", "oneOf": [{"type": "null"}, {"$ref": "#/$defs/validationDetail"}]}
      }
    },
    "summary": {
      "type": "object",
      "required": ["tables_total", "tables_succeeded", "tables_partial", "tables_failed", "fields_profiled", "checks", "findings", "suggested_rules"],
      "additionalProperties": false,
      "properties": {
        "tables_total": {"type": "integer", "minimum": 0},
        "tables_succeeded": {"type": "integer", "minimum": 0},
        "tables_partial": {"type": "integer", "minimum": 0},
        "tables_failed": {"type": "integer", "minimum": 0},
        "fields_profiled": {"type": "integer", "minimum": 0},
        "checks": {
          "type": "object",
          "required": ["pass", "fail", "not_evaluated", "error"],
          "additionalProperties": false,
          "properties": {
            "pass": {"type": "integer", "minimum": 0},
            "fail": {"type": "integer", "minimum": 0},
            "not_evaluated": {"type": "integer", "minimum": 0},
            "error": {"type": "integer", "minimum": 0}
          }
        },
        "findings": {
          "type": "object",
          "required": ["info", "warning"],
          "additionalProperties": false,
          "properties": {
            "info": {"type": "integer", "minimum": 0},
            "warning": {"type": "integer", "minimum": 0}
          }
        },
        "suggested_rules": {"type": "integer", "minimum": 0},
        "uniqueness": {
          "description": "1.2: measured keys by outcome.",
          "type": "object",
          "required": ["keys_measured", "unique", "unique_non_null", "duplicates", "empty"],
          "additionalProperties": false,
          "properties": {
            "keys_measured": {"type": "integer", "minimum": 0},
            "unique": {"type": "integer", "minimum": 0},
            "unique_non_null": {"type": "integer", "minimum": 0},
            "duplicates": {"type": "integer", "minimum": 0},
            "empty": {"type": "integer", "minimum": 0}
          }
        },
        "relationships": {
          "description": "1.2: relationships by validation status.",
          "type": "object",
          "required": ["validated", "violated", "not_validated"],
          "additionalProperties": false,
          "properties": {
            "validated": {"type": "integer", "minimum": 0},
            "violated": {"type": "integer", "minimum": 0},
            "not_validated": {"type": "integer", "minimum": 0}
          }
        },
        "relationship_hypotheses": {"description": "1.2: hypotheses listed.", "type": "integer", "minimum": 0}
      }
    },
    "limitedItems": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["item", "reason", "detail"],
        "additionalProperties": false,
        "properties": {"item": {"type": "string"}, "reason": {"type": "string"}, "detail": {"type": "string"}}
      }
    },
    "uniquenessKey": {
      "description": "1.2: exact uniqueness of one key (one or more columns) over the analysed scope. Only counts are recorded, never key values.",
      "type": "object",
      "required": ["key_id", "origins", "names", "columns", "field_ids", "status", "reason", "outcome", "scope", "operation_id", "metrics", "limitations"],
      "additionalProperties": false,
      "properties": {
        "key_id": {"type": "string", "pattern": "^k_[0-9a-f]{12}$"},
        "origins": {"type": "array", "minItems": 1, "items": {"enum": ["configured", "declared_primary_key", "declared_unique", "identifier_candidate"]}},
        "names": {"$ref": "#/$defs/stringList"},
        "columns": {"$ref": "#/$defs/stringList"},
        "field_ids": {"type": "array", "items": {"type": "string", "pattern": "^f_[0-9a-f]{12}$"}},
        "status": {"enum": ["measured", "not_computed", "not_eligible", "error"]},
        "reason": {"$ref": "#/$defs/nullableString"},
        "outcome": {"enum": ["unique", "unique_non_null", "duplicates", "empty", null]},
        "scope": {"enum": ["table_metadata", "full_table", "filtered_table", "full_snapshot", "filtered_snapshot", "sample"]},
        "operation_id": {"$ref": "#/$defs/nullableString"},
        "metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    },
    "uniqueness": {
      "type": "object",
      "required": ["keys", "sources", "budget", "passes", "limited", "null_semantics", "notes"],
      "additionalProperties": false,
      "properties": {
        "keys": {"type": "array", "items": {"$ref": "#/$defs/uniquenessKey"}},
        "sources": {
          "type": "object",
          "required": ["configured", "declared_keys", "identifier_candidates"],
          "additionalProperties": false,
          "properties": {"configured": {"type": "boolean"}, "declared_keys": {"type": "boolean"}, "identifier_candidates": {"type": "boolean"}}
        },
        "budget": {
          "type": "object",
          "required": ["max_keys", "max_passes"],
          "additionalProperties": false,
          "properties": {"max_keys": {"type": "integer", "minimum": 1}, "max_passes": {"type": "integer", "minimum": 1}}
        },
        "passes": {
          "type": "object",
          "required": ["budget", "planned"],
          "additionalProperties": false,
          "properties": {"budget": {"type": "integer", "minimum": 0}, "planned": {"type": "integer", "minimum": 0}}
        },
        "limited": {"$ref": "#/$defs/limitedItems"},
        "null_semantics": {"type": "string"},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "validationSide": {
      "type": "object",
      "required": ["table", "scope", "consistency_mode", "delta_version"],
      "additionalProperties": false,
      "properties": {
        "table": {"type": "string"},
        "scope": {"enum": ["table_metadata", "full_table", "filtered_table", "full_snapshot", "filtered_snapshot", "sample"]},
        "consistency_mode": {"enum": ["pinned_delta_version", "unpinned", "metadata_only"]},
        "delta_version": {"type": ["integer", "null"], "minimum": 0}
      }
    },
    "typeCompatibility": {
      "type": "object",
      "required": ["from_column", "from_type", "to_column", "to_type", "compatible", "rule"],
      "additionalProperties": false,
      "properties": {
        "from_column": {"type": "string"},
        "from_type": {"type": "string"},
        "to_column": {"type": "string"},
        "to_type": {"type": "string"},
        "compatible": {"type": "boolean"},
        "rule": {"type": "string"}
      }
    },
    "validationDetail": {
      "type": "object",
      "required": ["status", "reason", "mode", "from", "to", "operation_id", "type_compatibility", "target_key_unique", "metrics", "limitations"],
      "additionalProperties": false,
      "properties": {
        "status": {"enum": ["not_validated", "validated", "violated"]},
        "reason": {"$ref": "#/$defs/nullableString"},
        "mode": {"enum": ["full_scope", "sample", null]},
        "from": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/validationSide"}]},
        "to": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/validationSide"}]},
        "operation_id": {"$ref": "#/$defs/nullableString"},
        "type_compatibility": {"type": "array", "items": {"$ref": "#/$defs/typeCompatibility"}},
        "target_key_unique": {"type": ["boolean", "null"]},
        "metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    },
    "referentialValidation": {
      "type": "object",
      "required": ["requested", "mode", "budget", "planned", "validated", "violated", "not_validated", "limited", "notes"],
      "additionalProperties": false,
      "properties": {
        "requested": {
          "type": "object",
          "required": ["configured", "declared"],
          "additionalProperties": false,
          "properties": {"configured": {"type": "boolean"}, "declared": {"type": "boolean"}}
        },
        "mode": {"enum": ["full_scope", "sample"]},
        "budget": {
          "type": "object",
          "required": ["max_relationships", "max_sample_rows"],
          "additionalProperties": false,
          "properties": {"max_relationships": {"type": "integer", "minimum": 0}, "max_sample_rows": {"type": "integer", "minimum": 1}}
        },
        "planned": {"type": "integer", "minimum": 0},
        "validated": {"type": "integer", "minimum": 0},
        "violated": {"type": "integer", "minimum": 0},
        "not_validated": {"type": "integer", "minimum": 0},
        "limited": {"$ref": "#/$defs/limitedItems"},
        "notes": {"$ref": "#/$defs/stringList"}
      }
    },
    "hypothesis": {
      "description": "1.2: a data-driven relationship hypothesis. It is never a known relationship, has no cardinality and is never drawn as an ER edge.",
      "type": "object",
      "required": ["hypothesis_id", "status", "from", "to", "cardinality", "evidence", "operation_id", "limitations"],
      "additionalProperties": false,
      "properties": {
        "hypothesis_id": {"type": "string", "pattern": "^hyp_[0-9]+$"},
        "status": {"const": "hypothesis"},
        "from": {"$ref": "#/$defs/relationshipEnd"},
        "to": {"$ref": "#/$defs/relationshipEnd"},
        "cardinality": {"type": "null"},
        "evidence": {
          "type": "object",
          "required": ["inclusion_scope", "from", "to", "metrics", "target_key_id", "target_key_unique", "type_compatibility", "range_relation", "range_basis"],
          "additionalProperties": false,
          "properties": {
            "inclusion_scope": {"enum": ["sample", "full_scope"]},
            "from": {"$ref": "#/$defs/validationSide"},
            "to": {"$ref": "#/$defs/validationSide"},
            "metrics": {"type": "array", "items": {"$ref": "#/$defs/metric"}},
            "target_key_id": {"type": "string", "pattern": "^k_[0-9a-f]{12}$"},
            "target_key_unique": {"type": "boolean"},
            "type_compatibility": {"type": "array", "items": {"$ref": "#/$defs/typeCompatibility"}},
            "range_relation": {"enum": ["contained", "overlapping", "not_compared"]},
            "range_basis": {"enum": ["values", "lengths"]}
          }
        },
        "operation_id": {"type": "string"},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    },
    "relationshipHypotheses": {
      "type": "object",
      "required": ["enabled", "reason", "budget", "targets", "pairs_considered", "pairs_evaluated", "pairs_not_evaluated", "pairs_known_excluded", "pairs_disjoint_excluded", "pairs_rejected", "hypotheses", "method", "limitations"],
      "additionalProperties": false,
      "properties": {
        "enabled": {"type": "boolean"},
        "reason": {"$ref": "#/$defs/nullableString"},
        "budget": {
          "type": "object",
          "required": ["max_pairs", "max_sample_rows", "inclusion_scope", "min_inclusion_ratio"],
          "additionalProperties": false,
          "properties": {
            "max_pairs": {"type": "integer", "minimum": 0},
            "max_sample_rows": {"type": "integer", "minimum": 1},
            "inclusion_scope": {"enum": ["sample", "full_scope"]},
            "min_inclusion_ratio": {"type": "number", "minimum": 0, "maximum": 1}
          }
        },
        "targets": {"type": "integer", "minimum": 0},
        "pairs_considered": {"type": "integer", "minimum": 0},
        "pairs_evaluated": {"type": "integer", "minimum": 0},
        "pairs_not_evaluated": {"type": "integer", "minimum": 0},
        "pairs_known_excluded": {"type": "integer", "minimum": 0},
        "pairs_disjoint_excluded": {"type": "integer", "minimum": 0},
        "pairs_rejected": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "hypotheses": {"type": "array", "items": {"$ref": "#/$defs/hypothesis"}},
        "method": {"type": "string"},
        "limitations": {"$ref": "#/$defs/stringList"}
      }
    }
  }
}''')

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier._version
# TableDossier 0.2.0 embedded runtime: module tabledossier._version
# Source: src/tabledossier/_version.py (sha256:e26998b42babbc4da3f1df4345ca45a7dacc0986d7836f2b030b811d972bc9c3)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Single source of the TableDossier version string."""

__version__ = "0.2.0"

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.jsonutil
# TableDossier 0.2.0 embedded runtime: module tabledossier.jsonutil
# Source: src/tabledossier/jsonutil.py (sha256:99d26d9fabd2890d26071f5d37027b638166f0f957567a139526ddd34545920e)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""JSON serialization conventions shared by the CLI and the notebook runtime.

This module is part of the embedded runtime: it is copied verbatim (minus
intra-package imports) into generated notebooks, so it must only use the
Python standard library.

Conventions (see ``docs/contract.md``):

* integers are JSON integers (consumers should parse them with arbitrary
  precision when values may exceed 2**53);
* decimals are JSON strings holding the canonical ``str(Decimal)`` form, so
  precision and scale are never lost;
* finite floats are JSON numbers; ``NaN``, ``Infinity`` and ``-Infinity`` are
  the JSON strings ``"NaN"``, ``"Infinity"`` and ``"-Infinity"``;
* dates are ``YYYY-MM-DD`` strings; timestamps are ISO 8601 strings, with an
  offset when the value is an instant and without one for local
  (timezone-less) timestamps.
"""

import datetime
import hashlib
import json
import math
from decimal import Decimal
from typing import Any

FLOAT_SPECIALS = ("NaN", "Infinity", "-Infinity")


def canonical_json(obj: Any) -> str:
    """Return a deterministic, compact JSON encoding used for fingerprints."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def pretty_json(obj: Any) -> str:
    """Return human-readable JSON (insertion order kept) ending with a newline."""
    return json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False) + "\n"


def fingerprint(obj: Any) -> str:
    """Return ``sha256:<hex>`` of the canonical JSON encoding of ``obj``."""
    return "sha256:" + hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def text_fingerprint(text: str) -> str:
    """Return ``sha256:<hex>`` of UTF-8 ``text``."""
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def short_hash(obj: Any, length: int = 12) -> str:
    """Return a short, stable hexadecimal digest of ``obj`` (for identifiers)."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()[:length]


def format_utc(moment: datetime.datetime) -> str:
    """Format an aware datetime as ``YYYY-MM-DDTHH:MM:SS.mmmZ`` in UTC."""
    if moment.tzinfo is None:
        raise ValueError("format_utc requires a timezone-aware datetime")
    utc = moment.astimezone(datetime.timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def utc_now() -> datetime.datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


def encode_float(value: float) -> float | str:
    """Encode a float, mapping non-finite values to their string markers."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def encode_scalar(value: Any) -> tuple[Any, str]:
    """Encode a Python scalar into ``(json_value, value_type)``.

    ``value_type`` is one of ``boolean``, ``integer``, ``float``, ``decimal``,
    ``string``, ``date``, ``timestamp`` or ``timestamp_ntz``.
    """
    if isinstance(value, bool):
        return value, "boolean"
    if isinstance(value, int):
        return value, "integer"
    if isinstance(value, float):
        return encode_float(value), "float"
    if isinstance(value, Decimal):
        if value.is_nan():
            return "NaN", "decimal"
        return str(value), "decimal"
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.isoformat(), "timestamp_ntz"
        return value.isoformat().replace("+00:00", "Z"), "timestamp"
    if isinstance(value, datetime.date):
        return value.isoformat(), "date"
    if isinstance(value, str):
        return value, "string"
    raise TypeError(f"unsupported scalar type for encoding: {type(value).__name__}")

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.schemacheck
# TableDossier 0.2.0 embedded runtime: module tabledossier.schemacheck
# Source: src/tabledossier/schemacheck.py (sha256:73db83695ac1e7677801f230cbec851f49f1c56e6ff6a6f80c353274e08791ba)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Standard-library validator for the JSON Schema subset used by TableDossier.

Generated notebooks cannot install ``jsonschema``, so they validate the
configuration and the exported profile with this small interpreter of the
*same* schema files shipped in ``tabledossier/schemas``. The CLI and CI also
run the full ``jsonschema`` implementation; tests check that both agree.

Only the keywords listed in :data:`SUPPORTED_KEYWORDS` may appear in
TableDossier schemas. :func:`unsupported_keywords` lets tests prove that no
schema relies on a keyword this module would silently ignore.
"""

import re
from collections.abc import Iterator, Mapping
from typing import Any

SUPPORTED_KEYWORDS = frozenset(
    {
        "$ref",
        "$defs",
        "type",
        "enum",
        "const",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minProperties",
        "oneOf",
        "anyOf",
        "allOf",
        "not",
        "if",
        "then",
        "else",
    }
)
ANNOTATION_KEYWORDS = frozenset(
    {"$schema", "$id", "$comment", "title", "description", "default", "examples", "format"}
)

_SC_MAX_REPR = 80


def _sc_repr(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _SC_MAX_REPR else text[: _SC_MAX_REPR - 3] + "..."


def _sc_is_type(value: Any, type_name: str) -> bool:
    if type_name == "null":
        return value is None
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        if isinstance(value, bool):
            return False
        return isinstance(value, int) or (isinstance(value, float) and value.is_integer())
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "object":
        return isinstance(value, dict)
    raise ValueError(f"unknown JSON Schema type: {type_name}")


def _sc_equal(left: Any, right: Any) -> bool:
    """JSON equality: booleans never equal numbers; 1 equals 1.0."""
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _sc_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_sc_equal(left[k], right[k]) for k in left)
    return type(left) is type(right) and bool(left == right)


def _sc_resolve(ref: str, root: Mapping[str, Any]) -> Mapping[str, Any]:
    if not ref.startswith("#/"):
        raise ValueError(f"only local references are supported: {ref}")
    node: Any = root
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        node = node[token]
    if not isinstance(node, Mapping):
        raise ValueError(f"reference does not point to a schema: {ref}")
    return node


def _sc_validate(
    value: Any,
    schema: Mapping[str, Any] | bool,
    root: Mapping[str, Any],
    where: str,
    errors: list[str],
) -> None:
    if schema is True:
        return
    if schema is False:
        errors.append(f"{where}: no value is allowed here")
        return
    if "$ref" in schema:
        _sc_validate(value, _sc_resolve(schema["$ref"], root), root, where, errors)

    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_sc_is_type(value, t) for t in types):
            errors.append(f"{where}: expected {' or '.join(types)}, got {_sc_repr(value)}")
            return
    if "const" in schema and not _sc_equal(value, schema["const"]):
        errors.append(f"{where}: expected {_sc_repr(schema['const'])}, got {_sc_repr(value)}")
    if "enum" in schema and not any(_sc_equal(value, option) for option in schema["enum"]):
        errors.append(f"{where}: {_sc_repr(value)} is not one of {_sc_repr(schema['enum'])}")

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(f"{where}: shorter than {schema['minLength']} characters")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{where}: longer than {schema['maxLength']} characters")
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            errors.append(f"{where}: {_sc_repr(value)} does not match {schema['pattern']!r}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{where}: {value} is below the minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{where}: {value} is above the maximum {schema['maximum']}")
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            errors.append(f"{where}: {value} must be greater than {schema['exclusiveMinimum']}")
        if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
            errors.append(f"{where}: {value} must be less than {schema['exclusiveMaximum']}")

    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{where}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{where}: more than {schema['maxItems']} items")
        if schema.get("uniqueItems"):
            for index, item in enumerate(value):
                if any(_sc_equal(item, other) for other in value[:index]):
                    errors.append(f"{where}[{index}]: duplicate item")
        if "items" in schema:
            for index, item in enumerate(value):
                _sc_validate(item, schema["items"], root, f"{where}[{index}]", errors)

    if isinstance(value, dict):
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            errors.append(f"{where}: fewer than {schema['minProperties']} properties")
        for name in schema.get("required", ()):
            if name not in value:
                errors.append(f"{where}: missing required property {name!r}")
        properties = schema.get("properties", {})
        for name, item in value.items():
            if name in properties:
                _sc_validate(item, properties[name], root, f"{where}.{name}", errors)
            elif "additionalProperties" in schema:
                extra = schema["additionalProperties"]
                if extra is False:
                    errors.append(f"{where}: unexpected property {name!r}")
                else:
                    _sc_validate(item, extra, root, f"{where}.{name}", errors)

    for sub in schema.get("allOf", ()):
        _sc_validate(value, sub, root, where, errors)
    if "anyOf" in schema and not any(
        not _sc_collect(value, sub, root, where) for sub in schema["anyOf"]
    ):
        errors.append(f"{where}: does not match any allowed alternative")
    if "oneOf" in schema:
        matches = sum(1 for sub in schema["oneOf"] if not _sc_collect(value, sub, root, where))
        if matches != 1:
            errors.append(f"{where}: must match exactly one alternative (matched {matches})")
    if "not" in schema and not _sc_collect(value, schema["not"], root, where):
        errors.append(f"{where}: matches a forbidden schema")
    if "if" in schema:
        if not _sc_collect(value, schema["if"], root, where):
            if "then" in schema:
                _sc_validate(value, schema["then"], root, where, errors)
        elif "else" in schema:
            _sc_validate(value, schema["else"], root, where, errors)


def _sc_collect(
    value: Any, schema: Mapping[str, Any] | bool, root: Mapping[str, Any], where: str
) -> list[str]:
    errors: list[str] = []
    _sc_validate(value, schema, root, where, errors)
    return errors


def schema_errors(instance: Any, schema: Mapping[str, Any], max_errors: int = 50) -> list[str]:
    """Validate ``instance`` against ``schema`` and return readable error strings.

    An empty list means the instance is valid. At most ``max_errors`` messages
    are returned.
    """
    errors = _sc_collect(instance, schema, schema, "$")
    return errors[:max_errors]


def _sc_walk(schema: Any, where: str) -> Iterator[tuple[str, str]]:
    if isinstance(schema, bool):
        return
    if not isinstance(schema, Mapping):
        raise ValueError(f"{where}: schema must be an object or boolean")
    for key, sub in schema.items():
        if key not in SUPPORTED_KEYWORDS and key not in ANNOTATION_KEYWORDS:
            yield where, key
        if key in ("properties", "$defs"):
            for name, child in sub.items():
                yield from _sc_walk(child, f"{where}/{key}/{name}")
        elif key in ("items", "additionalProperties", "not", "if", "then", "else"):
            yield from _sc_walk(sub, f"{where}/{key}")
        elif key in ("oneOf", "anyOf", "allOf"):
            for index, child in enumerate(sub):
                yield from _sc_walk(child, f"{where}/{key}/{index}")


def unsupported_keywords(schema: Mapping[str, Any]) -> list[str]:
    """Return ``location:keyword`` entries this validator would not enforce."""
    return [f"{where}:{key}" for where, key in _sc_walk(schema, "#")]

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.errors
# TableDossier 0.2.0 embedded runtime: module tabledossier.errors
# Source: src/tabledossier/errors.py (sha256:b108b4cf3d80cefa0ea5538173f6b69387e4e354ad7871d1a21a596d06eb9cb1)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Sanitized error records.

Part of the embedded runtime (standard library only). Engine messages can
echo data values (for example cast errors quote the offending value), so only
the first line is kept, quoted literals and URIs are replaced, and the length
is bounded. Backtick-quoted identifiers are kept because they name objects the
user asked to profile.
"""

import re
from typing import Any

_ER_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_ER_URI = re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://\S+")
# Engine messages start with their error condition, e.g. "[TABLE_OR_VIEW_NOT_FOUND] ...".
_ER_CONDITION = re.compile(r"^\s*\[([A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*)\]")


def sanitize_message(text: str, limit: int = 500) -> str:
    """Return the first line of ``text`` with quoted literals and URIs removed."""
    stripped = (text or "").strip()
    first = stripped.splitlines()[0] if stripped else ""
    first = _ER_URI.sub("<uri>", first)
    first = _ER_QUOTED.sub("<redacted>", first)
    return first[:limit]


def error_record(exc: BaseException, stage: str) -> dict[str, Any]:
    """Return a sanitized error record (stage, class, engine condition, message)."""
    condition = None
    for attribute in ("getCondition", "getErrorClass"):
        getter = getattr(exc, attribute, None)
        if callable(getter):
            try:
                condition = getter()
            except Exception:  # noqa: BLE001 - optional metadata only
                condition = None
            if condition:
                break
    if not condition:
        # Spark Connect clients (e.g. PySpark 3.5) may not expose the condition as an
        # attribute; the server still prefixes the message with it.
        match = _ER_CONDITION.match(str(exc))
        condition = match.group(1) if match else None
    message = sanitize_message(str(exc)) or type(exc).__name__
    return {
        "stage": stage,
        "error_class": type(exc).__name__,
        "condition": condition,
        "message": message,
    }

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.paths
# TableDossier 0.2.0 embedded runtime: module tabledossier.paths
# Source: src/tabledossier/paths.py (sha256:4dcb81311c66016927c7894a0639b34d3ff2cb78ed20a4d5fd9986dfbbec5e78)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Table identifiers and typed field paths.

Part of the embedded runtime (standard library only).

Table identifiers
    ``catalog.schema.table`` with optional backtick quoting for parts that
    contain other characters, e.g. ``demo.analytics.`order events```. A doubled
    backtick inside a quoted part is a literal backtick.

Field paths
    A field path is a list of typed segments, so that a column literally
    named ``a.b`` is distinct from field ``b`` nested inside struct ``a``::

        [{"kind": "field", "name": "a.b"}]                       -> `a.b`
        [{"kind": "field", "name": "a"}, {"kind": "field", "name": "b"}] -> a.b
        [{"kind": "field", "name": "items"}, {"kind": "array_element"}] -> items[]
        [{"kind": "field", "name": "attrs"}, {"kind": "map_value"}]     -> attrs{value}

    The display form is for documentation and annotation keys. It is not
    guaranteed to be valid SQL.
"""

import re
from typing import Any


SEGMENT_KINDS = ("field", "array_element", "map_key", "map_value")
_PT_BARE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PT_UNQUOTED_PART = re.compile(r"^[A-Za-z0-9_]+$")
_PT_CONTROL = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]")
MAX_IDENTIFIER_PART_LENGTH = 255


class IdentifierError(ValueError):
    """Raised when a table identifier or field path cannot be parsed."""


def _pt_check_part(part: str, text: str) -> None:
    if part == "":
        raise IdentifierError(f"empty name part in {text!r}")
    if _PT_CONTROL.search(part):
        raise IdentifierError(f"control characters are not allowed in {text!r}")
    if len(part) > MAX_IDENTIFIER_PART_LENGTH:
        raise IdentifierError(f"name part longer than {MAX_IDENTIFIER_PART_LENGTH} characters")


def _pt_read_quoted(text: str, start: int) -> tuple[str, int]:
    """Read a backtick-quoted name starting at ``text[start] == '`'``."""
    chars: list[str] = []
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "`":
            if index + 1 < len(text) and text[index + 1] == "`":
                chars.append("`")
                index += 2
                continue
            return "".join(chars), index + 1
        chars.append(char)
        index += 1
    raise IdentifierError(f"unterminated backtick quote in {text!r}")


def parse_table_identifier(text: str) -> list[str]:
    """Parse ``catalog.schema.table`` (1 to 3 parts) into its name parts."""
    if not isinstance(text, str):
        raise IdentifierError("table identifier must be a string")
    source = text.strip()
    if not source:
        raise IdentifierError("table identifier is empty")
    parts: list[str] = []
    index = 0
    while True:
        if index < len(source) and source[index] == "`":
            part, index = _pt_read_quoted(source, index)
        else:
            end = source.find(".", index)
            end = len(source) if end == -1 else end
            part = source[index:end]
            if part and not _PT_UNQUOTED_PART.match(part):
                raise IdentifierError(
                    f"invalid unquoted name {part!r} in {text!r}; "
                    "quote names with other characters using backticks"
                )
            index = end
        _pt_check_part(part, text)
        parts.append(part)
        if index == len(source):
            break
        if source[index] != ".":
            raise IdentifierError(f"expected '.' after a quoted name in {text!r}")
        index += 1
        if index == len(source):
            raise IdentifierError(f"identifier ends with '.': {text!r}")
    if len(parts) > 3:
        raise IdentifierError(f"expected at most 3 name parts (catalog.schema.table): {text!r}")
    return parts


def quote_name(name: str) -> str:
    """Quote one SQL identifier part with backticks (inner backticks doubled)."""
    return "`" + name.replace("`", "``") + "`"


def quote_table_identifier(parts: list[str]) -> str:
    """Return a fully quoted multi-part identifier safe to embed in Spark SQL."""
    return ".".join(quote_name(part) for part in parts)


def _pt_display_name(name: str) -> str:
    return name if _PT_BARE_NAME.match(name) else quote_name(name)


def table_key(parts: list[str]) -> str:
    """Return the canonical display form of a table identifier."""
    return ".".join(_pt_display_name(part) for part in parts)


def table_lookup_key(parts: list[str]) -> str:
    """Case-insensitive key used to match the same table spelled differently."""
    return table_key([part.casefold() for part in parts])


def table_id(parts: list[str]) -> str:
    """Return a stable identifier for a table (used for anchors and references)."""
    return "t_" + short_hash([part.casefold() for part in parts])


def field_segment(name: str) -> dict[str, Any]:
    """Return a struct/column field segment."""
    return {"kind": "field", "name": name}


def display_path(segments: list[dict[str, Any]]) -> str:
    """Return the human-readable form of a typed field path."""
    out = ""
    for segment in segments:
        kind = segment["kind"]
        if kind == "field":
            name = _pt_display_name(segment["name"])
            out = name if out == "" else f"{out}.{name}"
        elif kind == "array_element":
            out += "[]"
        elif kind == "map_key":
            out += "{key}"
        elif kind == "map_value":
            out += "{value}"
        else:
            raise IdentifierError(f"unknown path segment kind: {kind!r}")
    return out


def parse_display_path(text: str) -> list[dict[str, Any]]:
    """Parse a display path (as produced by :func:`display_path`) into segments."""
    if not isinstance(text, str) or not text:
        raise IdentifierError("field path must be a non-empty string")
    segments: list[dict[str, Any]] = []
    index = 0
    expect_name = True
    while index < len(text):
        if expect_name:
            if text[index] == "`":
                name, index = _pt_read_quoted(text, index)
            else:
                match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[index:])
                if not match:
                    raise IdentifierError(f"invalid field name at position {index} in {text!r}")
                name = match.group(0)
                index += len(name)
            _pt_check_part(name, text)
            segments.append(field_segment(name))
            expect_name = False
            continue
        if text.startswith("[]", index):
            segments.append({"kind": "array_element"})
            index += 2
        elif text.startswith("{key}", index):
            segments.append({"kind": "map_key"})
            index += 5
        elif text.startswith("{value}", index):
            segments.append({"kind": "map_value"})
            index += 7
        elif text[index] == ".":
            index += 1
            expect_name = True
            if index == len(text):
                raise IdentifierError(f"field path ends with '.': {text!r}")
        else:
            raise IdentifierError(f"unexpected character at position {index} in {text!r}")
    return segments


def column_reference_segments(reference: Any) -> list[dict[str, Any]]:
    """Convert a configuration column reference into typed segments.

    A string is a *literal* top-level column name (dots are not split); a list
    of strings navigates nested struct fields.
    """
    if isinstance(reference, str):
        _pt_check_part(reference, reference)
        return [field_segment(reference)]
    if isinstance(reference, list) and reference and all(isinstance(p, str) for p in reference):
        for part in reference:
            _pt_check_part(part, ".".join(reference))
        return [field_segment(part) for part in reference]
    raise IdentifierError(
        "a column reference must be a column name or a non-empty list of nested field names"
    )


def field_id(segments: list[dict[str, Any]]) -> str:
    """Return a stable identifier for a field path within a table."""
    return "f_" + short_hash(segments)

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.config
# TableDossier 0.2.0 embedded runtime: module tabledossier.config
# Source: src/tabledossier/config.py (sha256:75448e0ce0a5f3d51b79efeb7ebeed694da6b5aa63485b975fd074edfb5a55ef)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

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


CONFIG_KIND = "tabledossier.config"
CONFIG_VERSION = "1.0"
ANALYSIS_LEVELS = ("metadata", "standard", "deep")
ROW_READING_LEVELS = ("standard", "deep")
DEEP_ALL_TARGETS = "all_within_budget"
WIDGET_NAMES = tuple(name for name, _ in NOTEBOOK_WIDGETS)
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
            "no tables to profile: set the 'tables_json' widget to a JSON list such as "
            '["demo.analytics.orders"] (catalog.schema.table) and run the notebook again'
        )
    output_dir = str(config.get("output_dir", ""))
    if not output_dir:
        errors.append(
            "no output directory: set the 'output_dir' widget, for example "
            "/Volumes/<catalog>/<schema>/<volume>/tabledossier"
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

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.metrics
# TableDossier 0.2.0 embedded runtime: module tabledossier.metrics
# Source: src/tabledossier/metrics.py (sha256:c249a393132d3e995c1f45327360d5599d18d4e8485da31174cbd195c4162377)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Metric records of the canonical profile contract.

Part of the embedded runtime (standard library only).

Every metric states four independent axes:

* ``source``   - where the number came from (aggregate, sample, catalog...);
* ``scope``    - which population it describes (filtered snapshot, sample...);
* ``accuracy`` - exact, approximate, as recorded by metadata, or unknown;
* ``status``   - measured, or why there is no value.

A status other than ``measured`` never carries a value, so "not computed" can
never be confused with zero.
"""

from collections.abc import Iterable, Mapping
from typing import Any


METRIC_STATUSES = (
    "measured",
    "not_computed",
    "unsupported",
    "insufficient_data",
    "redacted",
    "error",
    "unavailable",
)
METRIC_SCOPES = (
    "table_metadata",
    "full_table",
    "filtered_table",
    "full_snapshot",
    "filtered_snapshot",
    "sample",
)
METRIC_ACCURACIES = ("exact", "approximate", "as_recorded", "unknown")
METRIC_SOURCES = (
    "catalog_metadata",
    "table_statistics",
    "table_history",
    "aggregate",
    "sample",
    "derived",
)


def measured(
    name: str,
    value: Any,
    *,
    unit: str | None,
    scope: str,
    accuracy: str,
    source: str,
    method: str,
    value_type: str | None = None,
    denominator: int | None = None,
    denominator_unit: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a measured metric, encoding ``value`` per the JSON conventions."""
    if value is None:
        raise ValueError(f"measured metric {name!r} requires a value")
    if value_type is None:
        encoded, value_type = encode_scalar(value)
    elif value_type in ("ratio", "quantiles"):
        encoded = value
    else:
        encoded, _ = encode_scalar(value)
    metric: dict[str, Any] = {
        "name": name,
        "status": "measured",
        "value": encoded,
        "value_type": value_type,
        "unit": unit,
        "scope": scope,
        "accuracy": accuracy,
        "source": source,
        "method": method,
    }
    if denominator is not None:
        metric["denominator"] = denominator
        metric["denominator_unit"] = denominator_unit
    if details:
        metric["details"] = dict(details)
    return metric


def not_measured(
    name: str,
    status: str,
    reason: str,
    *,
    scope: str,
    source: str,
    method: str | None = None,
    unit: str | None = None,
) -> dict[str, Any]:
    """Return a metric without a value, stating why (``status`` and ``reason``)."""
    if status == "measured" or status not in METRIC_STATUSES:
        raise ValueError(f"invalid non-measured status: {status!r}")
    metric: dict[str, Any] = {
        "name": name,
        "status": status,
        "value": None,
        "unit": unit,
        "scope": scope,
        "accuracy": "unknown",
        "source": source,
        "reason": reason,
    }
    if method:
        metric["method"] = method
    return metric


def ratio(
    name: str,
    numerator: int | None,
    denominator: int | None,
    *,
    scope: str,
    source: str,
    method: str,
    denominator_unit: str,
    accuracy: str = "exact",
) -> dict[str, Any]:
    """Return a ratio metric, or ``insufficient_data`` when the denominator is 0."""
    if numerator is None or denominator is None:
        return not_measured(
            name, "not_computed", "an input count is not available", scope=scope, source=source
        )
    if denominator == 0:
        return not_measured(
            name,
            "insufficient_data",
            f"undefined: no {denominator_unit} in scope (denominator is 0)",
            scope=scope,
            source=source,
            method=method,
            unit="ratio",
        )
    return measured(
        name,
        numerator / denominator,
        value_type="ratio",
        unit="ratio",
        scope=scope,
        accuracy=accuracy,
        source=source,
        method=method,
        denominator=denominator,
        denominator_unit=denominator_unit,
    )


def find_metric(metrics: Iterable[Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    """Return the first metric called ``name`` or ``None``."""
    for metric in metrics:
        if metric.get("name") == name:
            return metric
    return None


def metric_value(metrics: Iterable[Mapping[str, Any]], name: str) -> Any:
    """Return the value of a *measured* metric, else ``None``."""
    metric = find_metric(metrics, name)
    if metric is None or metric.get("status") != "measured":
        return None
    return metric.get("value")


def numeric_value(metrics: Iterable[Mapping[str, Any]], name: str) -> float | None:
    """Return a measured metric as a finite float (decimals parsed), else ``None``."""
    value = metric_value(metrics, name)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return None
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    return None

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.planning
# TableDossier 0.2.0 embedded runtime: module tabledossier.planning
# Source: src/tabledossier/planning.py (sha256:970ba12c986e535097884051dc5a72ebdf514b2ec1bbd7412608cf06151c40c1)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Engine-neutral planning: schema tree, field selection, sample and aggregate plans.

Part of the embedded runtime (standard library only).

The planner only *describes* work as declarative specs (``op`` names with
parameters). An engine adapter (``tabledossier.runtime.spark``) compiles the
specs into expressions and performs the actions. Adding metrics therefore adds
expressions to a bounded number of shared aggregation passes; it never adds one
Spark action per column. A single aggregation call is an organizational goal,
not a promise of a single physical scan or of a cost similar to ``count(*)``.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any


NUMERIC_KINDS = ("integer", "float", "decimal")
TEMPORAL_KINDS = ("date", "timestamp", "timestamp_ntz")
ORDERABLE_KINDS = (*NUMERIC_KINDS, "string", "boolean", *TEMPORAL_KINDS, "binary")
EXTREME_METRICS = ("min", "max", "mean", "stddev", "quantiles")

# (metric name, op, tier, expression cost). Lower tiers are kept first when the
# expression budget is exceeded. Tier 0 is reserved for the table row count.
_PL_COMMON = [("null_count", "count_null", 1, 1)]
_PL_ORDERABLE = [
    ("approx_distinct_count", "approx_distinct", 3, 1),
    ("all_values_equal", "all_values_equal", 3, 2),
]
_PL_NUMERIC = [
    ("min", "min_value", 2, 1),
    ("max", "max_value", 2, 1),
    ("mean", "mean_value", 2, 1),
    ("zero_count", "count_zero", 2, 1),
    ("negative_count", "count_negative", 2, 1),
    ("positive_count", "count_positive", 2, 1),
    ("stddev", "stddev_value", 4, 1),
    ("quantiles", "quantiles", 4, 1),
]
_PL_FLOAT_EXTRA = [
    ("nan_count", "count_nan", 2, 1),
    ("positive_infinity_count", "count_pos_inf", 2, 1),
    ("negative_infinity_count", "count_neg_inf", 2, 1),
    ("finite_count", "count_finite", 2, 1),
]
_PL_STRING = [
    ("empty_count", "count_empty_string", 2, 1),
    ("min_length", "min_length", 2, 1),
    ("max_length", "max_length", 2, 1),
    ("mean_length", "mean_length", 2, 1),
    ("whitespace_only_count", "count_whitespace_only", 4, 1),
    ("length_quantiles", "length_quantiles", 4, 1),
]
_PL_BOOLEAN = [("true_count", "count_true", 2, 1), ("false_count", "count_false", 2, 1)]
_PL_TEMPORAL = [("min", "min_value", 2, 1), ("max", "max_value", 2, 1)]
_PL_BINARY = [("min_length", "min_length", 2, 1), ("max_length", "max_length", 2, 1)]
_PL_ARRAY = [
    ("empty_count", "count_empty_collection", 2, 1),
    ("min_size", "min_size", 2, 1),
    ("max_size", "max_size", 2, 1),
    ("mean_size", "mean_size", 2, 1),
    ("total_element_count", "sum_size", 2, 1),
    ("null_element_count", "count_null_elements", 3, 1),
]
_PL_MAP = [
    ("empty_count", "count_empty_collection", 2, 1),
    ("min_size", "min_size", 2, 1),
    ("max_size", "max_size", 2, 1),
    ("mean_size", "mean_size", 2, 1),
    ("total_entry_count", "sum_size", 2, 1),
    ("null_value_count", "count_null_map_values", 3, 1),
]


def scope_label(pinned: bool, filtered: bool) -> str:
    """Return the metric scope for a population (snapshot pinned or not, filtered or not)."""
    if pinned:
        return "filtered_snapshot" if filtered else "full_snapshot"
    return "filtered_table" if filtered else "full_table"


def _pl_shallow_type(ntype: Mapping[str, Any]) -> dict[str, Any]:
    shallow = {"kind": ntype["kind"], "physical_type": ntype.get("physical_type", ntype["kind"])}
    for key in ("precision", "scale"):
        if key in ntype:
            shallow[key] = ntype[key]
    return shallow


def _pl_children(ntype: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return child descriptors ``(segment, type, nullable, comment)`` of a type."""
    kind = ntype["kind"]
    if kind == "struct":
        return [
            {
                "segment": field_segment(child["name"]),
                "type": child["type"],
                "nullable": child.get("nullable"),
                "comment": child.get("comment"),
            }
            for child in ntype.get("fields", [])
        ]
    if kind == "array":
        return [
            {
                "segment": {"kind": "array_element"},
                "type": ntype["element_type"],
                "nullable": ntype.get("contains_null"),
                "comment": None,
            }
        ]
    if kind == "map":
        return [
            {
                "segment": {"kind": "map_key"},
                "type": ntype["key_type"],
                "nullable": False,
                "comment": None,
            },
            {
                "segment": {"kind": "map_value"},
                "type": ntype["value_type"],
                "nullable": ntype.get("value_contains_null"),
                "comment": None,
            },
        ]
    return []


def _pl_make_node(
    parent: Mapping[str, Any] | None, descriptor: Mapping[str, Any], depth: int
) -> dict[str, Any]:
    path = [*(parent["path"] if parent else []), descriptor["segment"]]
    segment = descriptor["segment"]
    return {
        "field_id": field_id(path),
        "name": segment.get("name"),
        "segment_kind": segment["kind"],
        "path": path,
        "display_path": display_path(path),
        "parent_field_id": parent["field_id"] if parent else None,
        "depth": depth,
        "type": _pl_shallow_type(descriptor["type"]),
        "nullable": descriptor.get("nullable"),
        "comment": descriptor.get("comment"),
        "children": [],
        "children_omitted": 0,
        "children_omitted_reason": None,
        "_ntype": descriptor["type"],
    }


def build_schema_tree(
    fields: Sequence[Mapping[str, Any]], *, max_depth: int, max_fields: int
) -> dict[str, Any]:
    """Build the documented schema tree breadth-first within depth and field limits.

    ``fields`` are neutral top-level field descriptors
    (``name``/``type``/``nullable``/``comment``) produced by an engine adapter.
    Breadth-first order keeps every top-level column before nested fields when
    the field budget is reached.
    """
    top: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = []
    count = 0
    omitted = 0
    top_omitted = 0
    for field in fields:
        if count >= max_fields:
            top_omitted += 1
            omitted += 1
            continue
        descriptor = {
            "segment": field_segment(field["name"]),
            "type": field["type"],
            "nullable": field.get("nullable"),
            "comment": field.get("comment"),
        }
        node = _pl_make_node(None, descriptor, 1)
        top.append(node)
        queue.append(node)
        count += 1
    position = 0
    while position < len(queue):
        node = queue[position]
        position += 1
        children = _pl_children(node["_ntype"])
        if not children:
            continue
        if node["depth"] >= max_depth:
            node["children_omitted"] = len(children)
            node["children_omitted_reason"] = "max_depth"
            omitted += len(children)
            continue
        for descriptor in children:
            if count >= max_fields:
                node["children_omitted"] += 1
                node["children_omitted_reason"] = "max_fields"
                omitted += 1
                continue
            child = _pl_make_node(node, descriptor, node["depth"] + 1)
            node["children"].append(child)
            queue.append(child)
            count += 1
    for node in queue:
        node.pop("_ntype", None)
    return {
        "fields": top,
        "nodes_total": count,
        "nodes_omitted": omitted,
        "top_level_omitted": top_omitted,
        "limits": {"max_depth": max_depth, "max_fields": max_fields},
    }


def iter_nodes(nodes: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Yield schema tree nodes depth-first in schema order."""
    for node in nodes:
        yield node
        yield from iter_nodes(node["children"])


def _pl_in_collection(node: Mapping[str, Any]) -> bool:
    return any(segment["kind"] != "field" for segment in node["path"])


def select_profile_fields(
    tree: Mapping[str, Any], selected_columns: list[str] | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(profiled_nodes, omissions)`` for the standard level.

    Fields inside array elements or map entries are documented in the schema
    tree but not profiled (element-level profiling belongs to ``deep``).
    """
    wanted = None if selected_columns is None else {name for name in selected_columns}
    profiled: list[dict[str, Any]] = []
    omissions: list[dict[str, Any]] = []
    for node in iter_nodes(tree["fields"]):
        top_name = node["path"][0]["name"]
        if wanted is not None and top_name not in wanted:
            if node["depth"] == 1:
                omissions.append(
                    _pl_omission(node, "not_selected", "column not in the configured selection")
                )
            continue
        if _pl_in_collection(node):
            parent_segment_kinds = [s["kind"] for s in node["path"][:-1]]
            if all(kind == "field" for kind in parent_segment_kinds):
                omissions.append(
                    _pl_omission(
                        node,
                        "inside_collection",
                        "array elements and map entries are profiled at collection level only; "
                        "element-level profiling requires analysis_level = deep",
                    )
                )
            continue
        profiled.append(node)
        if node["children_omitted"]:
            omissions.append(
                _pl_omission(
                    node,
                    node["children_omitted_reason"] or "limit",
                    f"{node['children_omitted']} child field(s) not documented or profiled "
                    f"({node['children_omitted_reason']} limit)",
                )
            )
    if tree.get("top_level_omitted"):
        omissions.append(
            {
                "field_id": None,
                "display_path": None,
                "reason": "max_fields",
                "detail": (
                    f"{tree['top_level_omitted']} top-level column(s) beyond limits.max_fields"
                ),
                "metrics": [],
            }
        )
    return profiled, omissions


def _pl_omission(node: Mapping[str, Any], reason: str, detail: str) -> dict[str, Any]:
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "reason": reason,
        "detail": detail,
        "metrics": [],
    }


def plan_sample(profiled: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> dict[str, Any]:
    """Plan the transient sample used for semantic and JSON inference (strings only)."""
    sampling = config["sampling"]
    strings = [node for node in profiled if node["type"]["kind"] == "string"]
    plan: dict[str, Any] = {
        "enabled": False,
        "method": sampling["method"],
        "max_rows": sampling["max_rows"],
        "max_bytes": sampling["max_bytes"],
        "max_value_chars": sampling["max_value_chars"],
        "random_fraction": sampling.get("random_fraction"),
        "seed": sampling.get("seed"),
        "field_ids": [],
        "omitted_field_ids": [],
        "reason": None,
    }
    if config["analysis_level"] not in ("standard", "deep"):
        plan["reason"] = "the metadata level does not read table rows"
    elif sampling["method"] == "none":
        plan["reason"] = "sampling disabled by configuration (sampling.method = none)"
    elif not strings:
        plan["reason"] = "no string fields to inspect"
    else:
        plan["enabled"] = True
        limit = sampling["max_columns"]
        plan["field_ids"] = [node["field_id"] for node in strings[:limit]]
        plan["omitted_field_ids"] = [node["field_id"] for node in strings[limit:]]
    return plan


def _pl_catalog(kind: str) -> list[tuple[str, str, int, int]]:
    specs = list(_PL_COMMON)
    if kind in ORDERABLE_KINDS:
        specs += _PL_ORDERABLE
    if kind == "float":
        specs += _PL_FLOAT_EXTRA
    if kind in NUMERIC_KINDS:
        specs += _PL_NUMERIC
    elif kind == "string":
        specs += _PL_STRING
    elif kind == "boolean":
        specs += _PL_BOOLEAN
    elif kind in TEMPORAL_KINDS:
        specs += _PL_TEMPORAL
    elif kind == "binary":
        specs += _PL_BINARY
    elif kind == "array":
        specs += _PL_ARRAY
    elif kind == "map":
        specs += _PL_MAP
    return specs


def plan_aggregates(
    profiled: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any],
    capabilities: Mapping[str, bool],
    scope: str,
    json_field_ids: set[str] | frozenset[str] = frozenset(),
    redacted_field_ids: set[str] | frozenset[str] = frozenset(),
    deep_candidates: Sequence[Mapping[str, Any]] = (),
    max_extra_passes: int = 0,
) -> dict[str, Any]:
    """Plan shared aggregation passes (``standard`` metrics plus optional ``deep`` ones).

    Returns a dict with ``specs`` (alias, metric, op, field, tier, cost,
    params), ``passes`` (lists of aliases), ``static_metrics`` (metrics that
    are known up front not to be computed, with reasons) and ``omissions``.

    Standard metrics are planned exactly as at the ``standard`` level, within
    ``max_expressions_per_pass x max_aggregate_passes``. ``deep_candidates``
    (tiers 5-7, see ``tabledossier.deep``) only use the room left in the last
    standard pass plus at most ``max_extra_passes`` additional passes; when
    they do not fit, the highest tiers are dropped first and recorded as
    ``deep_budget`` omissions. ``extra_passes`` reports the passes added.
    """
    metrics_cfg = config["metrics"]
    limits = config["limits"]
    by_id = {node["field_id"]: node for node in profiled}
    candidates: list[dict[str, Any]] = [
        {
            "metric": "row_count",
            "op": "count_all",
            "field_id": None,
            "tier": 0,
            "cost": 1,
            "params": {},
        }
    ]
    static: list[dict[str, Any]] = []
    for node in profiled:
        kind = node["type"]["kind"]
        fid = node["field_id"]
        for metric, op, tier, cost in _pl_catalog(kind):
            params: dict[str, Any] = {}
            if metric in EXTREME_METRICS and fid in redacted_field_ids:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            metric,
                            "redacted",
                            "value policy redacts extremes and distribution values for this field",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
                continue
            if op in ("quantiles", "length_quantiles"):
                params = {
                    "probabilities": list(metrics_cfg["quantiles"]),
                    "accuracy": metrics_cfg["quantile_accuracy"],
                }
                if not params["probabilities"]:
                    continue
            if op == "approx_distinct":
                params = {"rsd": metrics_cfg["approx_distinct_rsd"]}
            candidates.append(
                {
                    "metric": metric,
                    "op": op,
                    "field_id": fid,
                    "tier": tier,
                    "cost": cost,
                    "params": params,
                }
            )
        parent = by_id.get(node["parent_field_id"] or "")
        if parent is not None and parent["type"]["kind"] == "struct":
            candidates.append(
                {
                    "metric": "null_count_parent_present",
                    "op": "count_null_parent_present",
                    "field_id": fid,
                    "tier": 1,
                    "cost": 1,
                    "params": {"parent_field_id": parent["field_id"]},
                }
            )
        if kind in ("date", "timestamp"):
            candidates.append(
                {
                    "metric": "after_reference_count",
                    "op": "count_after_reference",
                    "field_id": fid,
                    "tier": 3,
                    "cost": 1,
                    "params": {},
                }
            )
        elif kind == "timestamp_ntz":
            static.append(
                {
                    "field_id": fid,
                    "metric": not_measured(
                        "after_reference_count",
                        "not_computed",
                        "timezone-less timestamps have no unambiguous reference instant",
                        scope=scope,
                        source="aggregate",
                    ),
                }
            )
        if kind == "string" and fid in json_field_ids:
            if not metrics_cfg.get("json_full_scope_validation", True):
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            "json_invalid_count",
                            "not_computed",
                            "full-scope JSON validation disabled by configuration",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
            elif capabilities.get("try_parse_json"):
                candidates.append(
                    {
                        "metric": "json_invalid_count",
                        "op": "count_json_invalid",
                        "field_id": fid,
                        "tier": 3,
                        "cost": 1,
                        "params": {},
                    }
                )
            else:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            "json_invalid_count",
                            "unsupported",
                            "try_parse_json is not available in this runtime; "
                            "JSON validity is reported for the sample only",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )

    capacity = limits["max_expressions_per_pass"] * limits["max_aggregate_passes"]
    kept, dropped = _pl_apply_budget(candidates, capacity)
    omissions: dict[str, dict[str, Any]] = {}
    for spec in dropped:
        fid = spec["field_id"]
        node = by_id[fid]
        entry = omissions.setdefault(
            fid,
            {
                "field_id": fid,
                "display_path": node["display_path"],
                "reason": "expression_budget",
                "detail": (
                    f"aggregate expression budget of {capacity} "
                    f"({limits['max_expressions_per_pass']} per pass x "
                    f"{limits['max_aggregate_passes']} passes) exceeded"
                ),
                "metrics": [],
            },
        )
        entry["metrics"].append(spec["metric"])
        static.append(
            {
                "field_id": fid,
                "metric": not_measured(
                    spec["metric"],
                    "not_computed",
                    "omitted to respect the aggregate expression budget",
                    scope=scope,
                    source="aggregate",
                ),
            }
        )
    per_pass = limits["max_expressions_per_pass"]
    for index, spec in enumerate(kept):
        spec["alias"] = f"a{index:04d}"
    costs = {spec["alias"]: spec["cost"] for spec in kept}
    passes = _pl_pack(kept, per_pass, [], costs)
    standard_passes = len(passes)
    kept_deep: list[dict[str, Any]] = []
    dropped_deep: list[dict[str, Any]] = []
    if deep_candidates:
        room = per_pass - sum(costs[alias] for alias in passes[-1]) if passes else per_pass
        deep_cost = sum(spec["cost"] for spec in deep_candidates)
        needed = -(-max(0, deep_cost - room) // per_pass)
        extra = min(needed, max(0, max_extra_passes))
        kept_deep, dropped_deep = _pl_apply_budget(
            [dict(spec) for spec in deep_candidates], room + per_pass * extra
        )
        for spec in dropped_deep:
            fid = spec["field_id"]
            entry = omissions.setdefault(
                f"deep:{fid}",
                {
                    "field_id": fid,
                    "display_path": by_id[fid]["display_path"]
                    if fid in by_id
                    else spec.get("display_path"),
                    "reason": "deep_budget",
                    "detail": (
                        "deep expressions beyond the room left in the standard passes plus "
                        f"deep.max_extra_passes = {max_extra_passes} extra pass(es) of "
                        f"{per_pass} expressions"
                    ),
                    "metrics": [],
                },
            )
            label = spec["metric"] if spec.get("group") != "json_path" else "json_path_validation"
            if label not in entry["metrics"]:
                entry["metrics"].append(label)
            if spec.get("group") == "element":
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            spec["metric"],
                            "not_computed",
                            "omitted to respect the deep expression and pass budgets",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
        for index, spec in enumerate(kept_deep, start=len(kept)):
            spec["alias"] = f"a{index:04d}"
            costs[spec["alias"]] = spec["cost"]
        passes = _pl_pack(kept_deep, per_pass, passes, costs)
    specs = [*kept, *kept_deep]
    return {
        "specs": specs,
        "passes": passes,
        "static_metrics": static,
        "omissions": list(omissions.values()),
        "expression_count": sum(spec["cost"] for spec in specs),
        "expression_capacity": capacity,
        "standard_passes": standard_passes,
        "extra_passes": len(passes) - standard_passes,
        "deep_expressions": sum(spec["cost"] for spec in kept_deep),
        "deep_dropped": dropped_deep,
    }


def _pl_pack(
    specs: Sequence[Mapping[str, Any]],
    per_pass: int,
    passes: list[list[str]],
    costs: Mapping[str, int],
) -> list[list[str]]:
    """Append aliases of ``specs`` to ``passes``, filling the last pass before opening one."""
    out = [list(item) for item in passes]
    used = sum(costs[alias] for alias in out[-1]) if out else 0
    for spec in specs:
        if not out or used + spec["cost"] > per_pass:
            out.append([])
            used = 0
        out[-1].append(spec["alias"])
        used += spec["cost"]
    return out


def _pl_apply_budget(
    candidates: list[dict[str, Any]], capacity: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    total = sum(spec["cost"] for spec in candidates)
    if total <= capacity:
        return candidates, []
    dropped_ids: set[int] = set()
    tiers = sorted({spec["tier"] for spec in candidates if spec["tier"] > 0}, reverse=True)
    for tier in tiers:
        for index in range(len(candidates) - 1, -1, -1):
            if total <= capacity:
                break
            spec = candidates[index]
            if spec["tier"] == tier:
                dropped_ids.add(index)
                total -= spec["cost"]
        if total <= capacity:
            break
    kept = [spec for index, spec in enumerate(candidates) if index not in dropped_ids]
    dropped = [spec for index, spec in enumerate(candidates) if index in dropped_ids]
    return kept, dropped


def operation(
    op_id: str, kind: str, description: str, *, reads_user_data: bool, **details: Any
) -> dict[str, Any]:
    """Return a planned-operation record (what will be asked of the engine)."""
    record: dict[str, Any] = {
        "operation_id": op_id,
        "kind": kind,
        "description": description,
        "reads_user_data": reads_user_data,
    }
    if details:
        record["details"] = details
    return record

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.semantic
# TableDossier 0.2.0 embedded runtime: module tabledossier.semantic
# Source: src/tabledossier/semantic.py (sha256:14e1b52707b12164c1eda7d2cdbb06f534676f56df0a9420ee44f329096edcc6)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Observed formats, JSON shape and candidate roles.

Part of the embedded runtime (standard library only).

Three concepts are kept apart:

* the *physical type* declared by the source (for example ``string``);
* the *observed format* of sampled values (UUID, JSON object, ISO date...);
* a *candidate role* (identifier, categorical) suggested by measurements.

None of them establishes business meaning, which requires source comments or
human annotations. Detectors use real parsing where it matters (JSON, dates)
and report ``unknown``, ``mixed``, ``ambiguous`` or ``insufficient_data``
instead of guessing. Sampled values are transient: they are inspected inside
the execution environment and never persisted unless the value policy
allow-lists a column.
"""

import datetime
import json
import math
import re
import statistics
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit


FORMAT_DETECTORS = (
    "json_object",
    "json_array",
    "uuid",
    "numeric_string",
    "boolean_string",
    "iso_date",
    "iso_timestamp",
    "url",
    "email_candidate",
)
_SEM_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SEM_NUMERIC = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")
_SEM_BOOLEAN = frozenset({"true", "false", "t", "f", "yes", "no", "y", "n", "0", "1"})
_SEM_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SEM_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)(?:[.,](\d{1,9}))?"
    r"(Z|[+-]\d{2}(?::?\d{2})?)?$"
)
_SEM_URL = re.compile(r"^(?:https?|ftp)://[^\s/?#]+[^\s]*$", re.IGNORECASE)
_SEM_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SEM_KEY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-. ]{0,63}$")
MAX_LISTED_JSON_KEYS = 50


def _sem_reject_constant(token: str) -> Any:
    raise ValueError(f"non-standard JSON constant {token}")


def parse_json_text(text: str) -> tuple[bool, Any]:
    """Parse strict JSON (``NaN``/``Infinity`` rejected); return ``(ok, value)``."""
    try:
        return True, json.loads(text, parse_constant=_sem_reject_constant)
    except (ValueError, RecursionError):
        return False, None


def _sem_is_timestamp(value: str) -> tuple[bool, bool]:
    """Return ``(is_iso_timestamp, has_offset)``."""
    match = _SEM_TIMESTAMP.match(value)
    if not match:
        return False, False
    day, clock, fraction, offset = match.groups()
    if len(clock) == 5:
        clock += ":00"
    text = f"{day}T{clock}"
    if fraction:
        text += "." + fraction[:6].ljust(6, "0")
    if offset:
        if offset == "Z":
            text += "+00:00"
        elif len(offset) == 3:
            text += offset + ":00"
        elif ":" not in offset:
            text += offset[:3] + ":" + offset[3:]
        else:
            text += offset
    try:
        datetime.datetime.fromisoformat(text)
    except ValueError:
        return False, False
    return True, bool(offset)


def detect_formats(value: str) -> set[str]:
    """Return the set of detector names matching ``value`` (exact, unstripped)."""
    found: set[str] = set()
    stripped = value.strip()
    if stripped[:1] in ("{", "["):
        ok, parsed = parse_json_text(stripped)
        if ok and isinstance(parsed, dict):
            found.add("json_object")
        elif ok and isinstance(parsed, list):
            found.add("json_array")
    if _SEM_UUID.match(value):
        found.add("uuid")
    if _SEM_NUMERIC.match(value):
        found.add("numeric_string")
    if value.casefold() in _SEM_BOOLEAN:
        found.add("boolean_string")
    if _SEM_DATE.match(value):
        try:
            datetime.date.fromisoformat(value)
            found.add("iso_date")
        except ValueError:
            pass
    elif _sem_is_timestamp(value)[0]:
        found.add("iso_timestamp")
    if _SEM_URL.match(value) and urlsplit(value).netloc:
        found.add("url")
    if len(value) <= 254 and _SEM_EMAIL.match(value):
        found.add("email_candidate")
    return found


def wilson_interval(successes: int, trials: int, level: float = 0.95) -> dict[str, Any] | None:
    """Return the Wilson score interval for an observed proportion.

    The interval describes uncertainty of the *observed* proportion under an
    independent-sampling assumption. It does not correct sampling bias (a
    prefix sample is not random) and says nothing about business meaning.
    """
    if trials <= 0:
        return None
    z = statistics.NormalDist().inv_cdf(0.5 + level / 2)
    p = successes / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denominator
    return {
        "method": "wilson_score",
        "level": level,
        "low": max(0.0, center - half),
        "high": min(1.0, center + half),
    }


def sample_limitations(sample_method: str, eligible: int) -> list[str]:
    """Return the standard limitation statements for sample-based inference."""
    notes = [
        f"Inferred from {eligible} transient sampled value(s); not guaranteed for the whole "
        "population.",
        "Confidence intervals assume independent sampling; they do not remove sampling bias "
        "and do not establish business meaning.",
    ]
    if sample_method == "prefix":
        notes.append(
            "The prefix sample returns the first rows produced by the engine and may be biased."
        )
    return notes


def infer_format(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    sample_method: str,
    semantic_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Infer the observed format of sampled string values.

    ``values`` are complete (non-truncated, non-null) sampled values. Empty and
    whitespace-only values are excluded from the eligible denominator.
    """
    blank = sum(1 for value in values if value.strip() == "")
    eligible_values = [value for value in values if value.strip() != ""]
    eligible = len(eligible_values)
    min_obs = semantic_config["min_observations"]
    detect = semantic_config["detect_threshold"]
    mixed = semantic_config["mixed_threshold"]
    level = semantic_config["confidence_level"]
    counts: Counter[str] = Counter()
    with_offset = 0
    for value in eligible_values:
        formats = detect_formats(value)
        counts.update(formats)
        if "iso_timestamp" in formats and _sem_is_timestamp(value)[1]:
            with_offset += 1
    candidates = []
    for name in FORMAT_DETECTORS:
        matches = counts.get(name, 0)
        if matches == 0 or eligible == 0:
            continue
        candidate: dict[str, Any] = {
            "format": name,
            "matches": matches,
            "match_ratio": matches / eligible,
            "interval": wilson_interval(matches, eligible, level),
        }
        if name == "iso_timestamp":
            candidate["with_offset"] = with_offset
        candidates.append(candidate)
    candidates.sort(key=lambda item: (-item["matches"], FORMAT_DETECTORS.index(item["format"])))
    result: dict[str, Any] = {
        "status": "unknown",
        "format": None,
        "scope": "sample",
        "eligible_observations": eligible,
        "excluded": {"sql_null": sql_nulls, "truncated": truncated, "blank": blank},
        "min_observations": min_obs,
        "thresholds": {"detect": detect, "mixed": mixed},
        "candidates": candidates,
        "method": "rule-based detectors with strict parsing (JSON, ISO dates) on sampled values",
        "limitations": sample_limitations(sample_method, eligible),
    }
    if eligible < min_obs:
        result["status"] = "insufficient_data"
        result["reason"] = f"{eligible} eligible observation(s); at least {min_obs} required"
        return result
    strong = [item for item in candidates if item["match_ratio"] >= detect]
    if len(strong) > 1:
        result["status"] = "ambiguous"
        result["reason"] = "several formats match: " + ", ".join(item["format"] for item in strong)
    elif strong:
        result["status"] = "detected"
        result["format"] = strong[0]["format"]
    elif candidates and candidates[0]["match_ratio"] >= mixed:
        result["status"] = "mixed"
        result["reason"] = "values match formats only partially"
    return result


def _sem_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def is_json_like(values: list[str], threshold: float = 0.5) -> bool:
    """Return True when at least ``threshold`` of non-blank values start like JSON."""
    eligible = [value.lstrip() for value in values if value.strip()]
    if not eligible:
        return False
    starts = sum(1 for value in eligible if value[:1] in ("{", "["))
    return starts / len(eligible) >= threshold


def json_shape(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    include_key_names: bool,
    sample_method: str,
) -> dict[str, Any]:
    """Classify sampled values by real JSON parsing and summarize their shape.

    SQL NULLs and values truncated by the sampling policy are excluded (they
    are reported separately and are never counted as invalid JSON).
    """
    counts = {"object": 0, "array": 0, "scalar": 0, "json_null_literal": 0, "invalid": 0}
    key_presence: Counter[str] = Counter()
    key_types: dict[str, Counter[str]] = {}
    key_sets: Counter[tuple[str, ...]] = Counter()
    for value in values:
        ok, parsed = parse_json_text(value.strip()) if value.strip() else (False, None)
        if not ok:
            counts["invalid"] += 1
        elif parsed is None:
            counts["json_null_literal"] += 1
        elif isinstance(parsed, dict):
            counts["object"] += 1
            key_sets[tuple(sorted(parsed))] += 1
            for key, item in parsed.items():
                key_presence[key] += 1
                key_types.setdefault(key, Counter())[_sem_json_type(item)] += 1
        elif isinstance(parsed, list):
            counts["array"] += 1
        else:
            counts["scalar"] += 1
    eligible = len(values)
    kinds_present = sum(
        1 for name in ("object", "array", "scalar", "json_null_literal") if counts[name]
    )
    conflicting = sum(
        1 for types in key_types.values() if len([t for t in types if t != "null"]) > 1
    )
    shape: dict[str, Any] = {
        "scope": "sample",
        "eligible_observations": eligible,
        "excluded": {"sql_null": sql_nulls, "truncated": truncated},
        "counts": counts,
        "valid_ratio": (eligible - counts["invalid"]) / eligible if eligible else None,
        "heterogeneity": {
            "top_level_kinds_present": kinds_present,
            "distinct_object_key_sets": len(key_sets),
            "keys_with_conflicting_types": conflicting,
        },
        "keys": None,
        "keys_omitted_reason": None,
        "limitations": [
            *sample_limitations(sample_method, eligible),
            "Shape inferred from the sample is not a complete or guaranteed schema.",
            "Values truncated by sampling.max_value_chars are excluded, never counted as invalid.",
        ],
    }
    if not key_presence:
        shape["keys_omitted_reason"] = "no JSON objects observed"
    elif not include_key_names:
        shape["keys_omitted_reason"] = "value policy (json_key_names = redact)"
    elif len(key_presence) > MAX_LISTED_JSON_KEYS:
        shape["keys_omitted_reason"] = (
            f"{len(key_presence)} distinct keys (more than {MAX_LISTED_JSON_KEYS}); "
            "objects look map-like, so key names are not listed"
        )
    elif not all(_SEM_KEY_NAME.match(key) for key in key_presence):
        shape["keys_omitted_reason"] = "some keys do not look like structural field names"
    else:
        objects = counts["object"]
        shape["keys"] = [
            {
                "key": key,
                "present_in": present,
                "presence_ratio": present / objects,
                "types": dict(sorted(key_types[key].items())),
            }
            for key, present in sorted(key_presence.items(), key=lambda item: (-item[1], item[0]))
        ]
    return shape


def value_concentration(values: list[str]) -> dict[str, Any]:
    """Summarize how concentrated sampled values are, without exposing labels."""
    counter = Counter(values)
    total = len(values)
    top = [count for _, count in counter.most_common(5)]
    return {
        "scope": "sample",
        "accuracy": "exact_for_sample",
        "observations": total,
        "sample_distinct_count": len(counter),
        "top_1_share": top[0] / total if total else None,
        "top_5_share": sum(top) / total if total else None,
        "note": "Exact for the sample only; not extrapolated to the population.",
    }


def value_examples(values: list[str], *, max_examples: int, max_chars: int) -> dict[str, Any]:
    """Return the most frequent sampled values (only for allow-listed columns)."""
    counter = Counter(values)
    examples = []
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[
        :max_examples
    ]:
        examples.append(
            {"value": value[:max_chars], "sample_count": count, "truncated": len(value) > max_chars}
        )
    return {
        "scope": "sample",
        "policy": "allow-listed by value_policy.example_columns",
        "values": examples,
    }


def candidate_roles(
    type_kind: str,
    metrics: Iterable[Mapping[str, Any]],
    observed_format: Mapping[str, Any] | None,
    thresholds: Mapping[str, Any],
    physical_type: str = "",
) -> list[dict[str, Any]]:
    """Suggest candidate roles from measured metrics and observed format.

    Identifier candidates are limited to integers, strings and decimals with
    scale 0, with few nulls and an approximate distinct count close to the
    number of non-null values.
    """
    metric_list = list(metrics)
    roles: list[dict[str, Any]] = []
    non_null = metric_value(metric_list, "non_null_count")
    distinct = metric_value(metric_list, "approx_distinct_count")
    null_ratio = metric_value(metric_list, "null_ratio")
    fmt = (
        (observed_format or {}).get("format")
        if (observed_format or {}).get("status") == "detected"
        else None
    )
    scale = re.search(r"decimal\(\s*\d+\s*,\s*(\d+)\s*\)", physical_type)
    key_like = type_kind in ("integer", "string") or (
        type_kind == "decimal" and scale is not None and scale.group(1) == "0"
    )
    if isinstance(non_null, int) and isinstance(distinct, int) and non_null > 0:
        ratio = distinct / non_null
        if (
            key_like
            and non_null >= thresholds["identifier_min_rows"]
            and ratio >= thresholds["identifier_min_distinct_ratio"]
            and isinstance(null_ratio, (int, float))
            and null_ratio <= thresholds["identifier_max_null_ratio"]
        ):
            roles.append(
                {
                    "role": "identifier_candidate",
                    "evidence": [
                        {"metric": "approx_distinct_count", "value": distinct},
                        {"metric": "non_null_count", "value": non_null},
                        {"metric": "null_ratio", "value": null_ratio},
                        *([{"observed_format": fmt}] if fmt else []),
                    ],
                    "limitations": (
                        "Approximate distinct counts do not prove uniqueness or a primary key; "
                        "exact validation requires an additional read (deep.uniqueness at the "
                        "deep level)."
                    ),
                }
            )
        if (
            type_kind in ("string", "integer")
            and non_null >= thresholds["categorical_min_rows"]
            and 2 <= distinct <= thresholds["categorical_max_distinct"]
        ):
            roles.append(
                {
                    "role": "categorical_candidate",
                    "evidence": [
                        {"metric": "approx_distinct_count", "value": distinct},
                        {"metric": "non_null_count", "value": non_null},
                    ],
                    "limitations": (
                        "Few distinct values in the analysed scope; the set of allowed values "
                        "must be defined by a data owner."
                    ),
                }
            )
    if fmt in ("json_object", "json_array"):
        roles.append(
            {
                "role": "json_document_candidate",
                "evidence": [{"observed_format": fmt}],
                "limitations": "Based on a sample; see json_profile for validity and shape.",
            }
        )
    return roles

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.deep
# TableDossier 0.2.0 embedded runtime: module tabledossier.deep
# Source: src/tabledossier/deep.py (sha256:504fb2edd4b5d0ed40a6dbe06f0a7967a523c5c6f5e82767bb51c866cdcdcdbb)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Deep level, part I: elements of arrays and maps, and JSON paths of string fields.

Part of the embedded runtime (standard library only). Like ``planning``, this
module only *describes* work; the Spark adapter compiles and executes it.

``deep`` is ``standard`` plus opt-in operations, each bounded by the ``deep``
section of the configuration and declared in ``operations.planned``:

* **Element metrics** (``items[]``, ``items[].sku``, ``attrs{key}``,
  ``attrs{value}``) are per-row aggregations with higher-order functions
  (``filter``, ``transform``, ``array_min``...) summed over rows. They join the
  shared aggregation passes (lowest priority), are exact over the analysed
  scope and never use ``explode``. Their denominators are elements or entries,
  never rows.
* **Element distinct counts** need one row per element, so they run in one
  separate pass that explodes a bounded sample (or the full scope when its
  measured size fits the element budget). Scope and accuracy say so.
* **JSON paths** of string fields are catalogued from the transient sample
  already collected by ``standard`` (presence, types, heterogeneity), then
  optionally validated over the full scope with engine functions detected at
  run time. A sample catalogue is never presented as a complete schema.

Increasing metrics adds expressions to bounded passes; it never adds one Spark
action per column.
"""

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


COLLECTION_KINDS = ("array", "map")
ELEMENT_SEGMENTS = ("array_element", "map_key", "map_value")
DEEP_OPERATION_KINDS = ("deep_aggregate_pass", "element_explode_pass")
# Deep candidates are always cheaper to drop than any standard metric (tiers 0-4).
DEEP_TIERS = (5, 6, 7)
JSON_TYPE_PATTERNS = {
    "object": "^OBJECT",
    "array": "^ARRAY",
    "string": "^STRING$",
    "boolean": "^BOOLEAN$",
    "number": "^(TINYINT|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DECIMAL)",
    "null": "^VOID$",
}
_DP_KEY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-. ]{0,63}$")
# A key is listed by name only when it occurs in at least this share of the sampled
# documents that contain its object (and in at least two documents).
JSON_MIN_KEY_PRESENCE = 0.1
_DP_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DP_UNQUOTABLE = re.compile(r"['\\\[\]]")

# (metric, op, tier, cost) per element kind; evaluated per row with higher-order functions.
_DP_COMMON = [("null_count", "el_count_null", 5, 1)]
_DP_NUMERIC = [
    ("min", "el_min", 6, 1),
    ("max", "el_max", 6, 1),
    ("zero_count", "el_count_zero", 6, 1),
    ("negative_count", "el_count_negative", 6, 1),
    ("positive_count", "el_count_positive", 6, 1),
]
_DP_FLOAT_EXTRA = [
    ("nan_count", "el_count_nan", 6, 1),
    ("positive_infinity_count", "el_count_pos_inf", 6, 1),
    ("negative_infinity_count", "el_count_neg_inf", 6, 1),
    ("finite_count", "el_count_finite", 5, 1),
]
_DP_STRING = [
    ("empty_count", "el_count_empty", 6, 1),
    ("min_length", "el_min_length", 6, 1),
    ("max_length", "el_max_length", 6, 1),
    ("whitespace_only_count", "el_count_whitespace_only", 7, 1),
]
_DP_BOOLEAN = [("true_count", "el_count_true", 6, 1), ("false_count", "el_count_false", 6, 1)]
_DP_TEMPORAL = [("min", "el_min", 6, 1), ("max", "el_max", 6, 1)]
_DP_BINARY = [("min_length", "el_min_length", 6, 1), ("max_length", "el_max_length", 6, 1)]
DEEP_EXTREME_METRICS = ("min", "max")


def deep_enabled(config: Mapping[str, Any]) -> bool:
    """Return True when the run uses the deep level."""
    return config.get("analysis_level") == "deep"


def collection_unit(kind: str) -> str:
    """Denominator unit of a collection kind: ``elements`` (arrays) or ``entries`` (maps)."""
    return "entries" if kind == "map" else "elements"


# --------------------------------------------------------------------------- targets


def _dp_requested(config: Mapping[str, Any], table_lookup: str) -> list[str] | None:
    targets = config["deep"]["targets"]
    if targets == DEEP_ALL_TARGETS:
        return None
    requested = []
    for item in targets:
        try:
            if table_lookup_key(parse_table_identifier(item["table"])) == table_lookup:
                requested.append(item["column"])
        except IdentifierError:
            continue
    return requested


def _dp_collection_segments(path: Sequence[Mapping[str, Any]]) -> list[int]:
    return [index for index, segment in enumerate(path) if segment["kind"] != "field"]


def _dp_omission(node: Mapping[str, Any], reason: str, detail: str) -> dict[str, Any]:
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "reason": reason,
        "detail": detail,
        "metrics": [],
    }


def plan_deep_elements(
    tree: Mapping[str, Any],
    profiled: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    table_lookup: str,
) -> dict[str, Any]:
    """Select deep targets of one table and the element nodes to profile.

    Returns ``mode``, ``requested`` display paths (explicit mode), the
    targeted ``collections``, explicitly requested ``json_nodes`` (``None`` in
    ``all_within_budget`` mode, where probable-JSON fields are chosen after the
    sample), ``not_eligible`` targets, ``element_nodes`` with their
    ``contexts``, and the ``omissions`` that replace ``inside_collection``.
    """
    deep = config["deep"]
    requested = _dp_requested(config, table_lookup)
    nodes = list(iter_nodes(list(tree["fields"])))
    by_id = {node["field_id"]: node for node in nodes}
    profiled_ids = {node["field_id"] for node in profiled}
    collections: list[dict[str, Any]] = []
    json_nodes: list[dict[str, Any]] | None = None
    not_eligible: list[dict[str, str]] = []
    if requested is None:
        if deep["collections"]:
            collections = [dict(n) for n in profiled if n["type"]["kind"] in COLLECTION_KINDS]
    else:
        json_nodes = []
        for text in requested:
            try:
                fid = field_id(parse_display_path(text))
            except IdentifierError:
                not_eligible.append({"display_path": text, "reason": "invalid display path"})
                continue
            found = by_id.get(fid)
            kind = found["type"]["kind"] if found else None
            if found is None:
                reason = "not found in the documented schema tree"
            elif fid not in profiled_ids:
                reason = "not profiled at the standard level (not selected, inside a collection "
                reason += "or beyond limits)"
            elif kind in COLLECTION_KINDS and not deep["collections"]:
                reason = "element profiling is disabled (deep.collections = false)"
            elif kind == "string" and not deep["json_paths"]:
                reason = "JSON path profiling is disabled (deep.json_paths = false)"
            elif kind not in (*COLLECTION_KINDS, "string"):
                reason = f"deep targets must be arrays, maps or JSON strings, not {kind}"
            else:
                reason = ""
            if reason or found is None:
                not_eligible.append({"display_path": text, "reason": reason})
            elif kind in COLLECTION_KINDS:
                if all(item["field_id"] != fid for item in collections):
                    collections.append(dict(found))
            elif all(item["field_id"] != fid for item in json_nodes):
                json_nodes.append(dict(found))
    targeted = {node["field_id"] for node in collections}
    element_nodes: list[dict[str, Any]] = []
    contexts: dict[str, dict[str, Any]] = {}
    omissions: list[dict[str, Any]] = []
    for node in nodes:
        positions = _dp_collection_segments(node["path"])
        if not positions:
            continue
        owner_path = node["path"][: positions[0]]
        owner = by_id.get(field_id(owner_path))
        if owner is None or owner["field_id"] not in profiled_ids:
            continue
        direct_child = len(positions) == 1 and positions[0] == len(node["path"]) - 1
        if owner["field_id"] not in targeted:
            if direct_child:
                detail = (
                    "not listed in deep.targets"
                    if requested is not None
                    else "element profiling is disabled (deep.collections = false)"
                )
                omissions.append(_dp_omission(node, "deep_not_selected", detail))
            continue
        if len(positions) > 1:
            parent = by_id.get(node["parent_field_id"] or "")
            if parent is not None and len(_dp_collection_segments(parent["path"])) == 1:
                omissions.append(
                    _dp_omission(
                        node,
                        "nested_collection",
                        "elements of collections nested inside collections are not profiled "
                        "in this release",
                    )
                )
            continue
        segment = node["path"][positions[0]]["kind"]
        inner = [s["name"] for s in node["path"][positions[0] + 1 :]]
        contexts[node["field_id"]] = {
            "collection_field_id": owner["field_id"],
            "collection_display_path": owner["display_path"],
            "collection_kind": owner["type"]["kind"],
            "segment": segment,
            "inner": inner,
            "unit": collection_unit(owner["type"]["kind"]),
        }
        element_nodes.append(node)
    return {
        "mode": "explicit" if requested is not None else DEEP_ALL_TARGETS,
        "requested": requested,
        "collections": collections,
        "json_nodes": json_nodes,
        "not_eligible": not_eligible,
        "element_nodes": element_nodes,
        "contexts": contexts,
        "omissions": omissions,
    }


# --------------------------------------------------------------------------- element specs


def _dp_catalog(kind: str) -> list[tuple[str, str, int, int]]:
    specs = list(_DP_COMMON)
    if kind == "float":
        specs += _DP_FLOAT_EXTRA
    if kind in NUMERIC_KINDS:
        specs += _DP_NUMERIC
    elif kind == "string":
        specs += _DP_STRING
    elif kind == "boolean":
        specs += _DP_BOOLEAN
    elif kind in TEMPORAL_KINDS:
        specs += _DP_TEMPORAL
    elif kind == "binary":
        specs += _DP_BINARY
    return specs


def plan_element_specs(
    element_nodes: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    *,
    scope: str,
    redacted_field_ids: set[str] | frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(candidates, static_metrics)`` for element nodes.

    Candidates use the aggregate spec shape of ``planning.plan_aggregates``
    (deep tiers, dropped before any standard metric). A node is redacted when
    it, or its collection, is redacted by the value policy.
    """
    candidates: list[dict[str, Any]] = []
    static: list[dict[str, Any]] = []
    for node in element_nodes:
        fid = node["field_id"]
        context = contexts[fid]
        kind = node["type"]["kind"]
        base = {
            "collection_field_id": context["collection_field_id"],
            "segment": context["segment"],
            "inner": list(context["inner"]),
        }
        redacted = fid in redacted_field_ids or (
            context["collection_field_id"] in redacted_field_ids
        )
        for metric, op, tier, cost in _dp_catalog(kind):
            if metric in DEEP_EXTREME_METRICS and redacted:
                static.append(
                    {
                        "field_id": fid,
                        "metric": not_measured(
                            metric,
                            "redacted",
                            "value policy redacts extremes for this collection",
                            scope=scope,
                            source="aggregate",
                        ),
                    }
                )
                continue
            candidates.append(
                {
                    "metric": metric,
                    "op": op,
                    "field_id": fid,
                    "tier": tier,
                    "cost": cost,
                    "params": dict(base),
                    "group": "element",
                }
            )
        if context["inner"]:
            candidates.append(
                {
                    "metric": "null_count_parent_present",
                    "op": "el_count_null_parent_present",
                    "field_id": fid,
                    "tier": 5,
                    "cost": 1,
                    "params": {**base, "parent_inner": list(context["inner"][:-1])},
                    "group": "element",
                }
            )
        if kind in ("date", "timestamp"):
            candidates.append(
                {
                    "metric": "after_reference_count",
                    "op": "el_count_after_reference",
                    "field_id": fid,
                    "tier": 7,
                    "cost": 1,
                    "params": dict(base),
                    "group": "element",
                }
            )
        elif kind == "timestamp_ntz":
            static.append(
                {
                    "field_id": fid,
                    "metric": not_measured(
                        "after_reference_count",
                        "not_computed",
                        "timezone-less timestamps have no unambiguous reference instant",
                        scope=scope,
                        source="aggregate",
                    ),
                }
            )
    return candidates, static


def plan_element_distinct(
    element_nodes: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Plan the single explode pass computing distinct counts of element values."""
    deep = config["deep"]
    mode = deep["element_distinct"]
    leaves = [
        {
            "field_id": node["field_id"],
            "display_path": node["display_path"],
            **{key: contexts[node["field_id"]][key] for key in ("collection_field_id", "segment")},
            "inner": list(contexts[node["field_id"]]["inner"]),
            "unit": contexts[node["field_id"]]["unit"],
        }
        for node in element_nodes
        if node["type"]["kind"] in ORDERABLE_KINDS
    ]
    plan: dict[str, Any] = {
        "enabled": False,
        "mode": mode,
        "leaves": leaves,
        "max_rows": deep["max_explode_rows"],
        "max_elements": deep["max_elements"],
        "sampling_method": config["sampling"]["method"],
        "random_fraction": config["sampling"].get("random_fraction"),
        "seed": config["sampling"].get("seed"),
        "reason": None,
    }
    if mode == "off":
        plan["reason"] = "disabled by configuration (deep.element_distinct = off)"
    elif not leaves:
        plan["reason"] = "no atomic element fields to count"
    elif mode == "sample" and config["sampling"]["method"] == "none":
        plan["reason"] = "sampling disabled by configuration (sampling.method = none)"
    else:
        plan["enabled"] = True
    return plan


# --------------------------------------------------------------------------- JSON paths


def _dp_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def json_path_text(segments: Sequence[Mapping[str, Any]]) -> str:
    """Display form of a JSON path: ``$.a.b``, ``$["a.b"]``, ``$.items[*]``, ``$.attrs.*``."""
    out = "$"
    for segment in segments:
        kind = segment["kind"]
        if kind == "key":
            name = segment["name"]
            out += f".{name}" if _DP_IDENTIFIER.match(name) else f"[{json.dumps(name)}]"
        elif kind == "items":
            out += "[*]"
        else:
            out += ".*"
    return out


def engine_json_path(segments: Sequence[Mapping[str, Any]]) -> str | None:
    """Path for ``get_json_object``/``variant_get``, or None when it cannot be expressed.

    Only object keys are supported (no wildcards); keys that are not simple
    identifiers use ``['key']`` and must not contain quotes, brackets or
    backslashes.
    """
    out = "$"
    for segment in segments:
        if segment["kind"] != "key":
            return None
        name = segment["name"]
        if _DP_IDENTIFIER.match(name):
            out += f".{name}"
        elif _DP_UNQUOTABLE.search(name):
            return None
        else:
            out += f"['{name}']"
    return out


def _dp_key_policy(
    items: Sequence[tuple[int, Any]], max_keys: int
) -> tuple[bool, str | None, set[str]]:
    """Decide which object keys at one path may be listed by name.

    Returns ``(collapse_all, reason, rare_keys)``. All keys are collapsed into
    ``*`` when the objects behave like maps (more than ``max_keys`` distinct
    keys, or keys that do not look like structural field names). Otherwise a
    key is listed only when it occurs in at least two sampled documents and in
    at least ``JSON_MIN_KEY_PRESENCE`` of the documents that contain the
    object; rarer keys, which often are data values used as keys, are
    collapsed into ``*`` too.
    """
    key_docs: Counter[str] = Counter()
    last: dict[str, int] = {}
    documents: set[int] = set()
    for doc, value in items:
        if not isinstance(value, dict):
            continue
        documents.add(doc)
        for key in value:
            if not _DP_KEY_NAME.match(key):
                return True, "some keys do not look like structural field names", set()
            if last.get(key) != doc:
                key_docs[key] += 1
                last[key] = doc
            if len(key_docs) > max_keys:
                return True, f"more than {max_keys} distinct keys", set()
    threshold = max(2.0, JSON_MIN_KEY_PRESENCE * len(documents))
    rare = {key for key, count in key_docs.items() if count < threshold}
    if key_docs and len(rare) == len(key_docs):
        return True, "every key occurs in too few sampled documents to be listed", set()
    return False, None, rare


def json_path_catalog(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    include_names: bool,
    names_omitted_reason: str | None,
    max_depth: int,
    max_paths: int,
    max_object_keys: int,
    sample_method: str,
) -> dict[str, Any]:
    """Catalogue JSON paths of sampled string values (presence, types, heterogeneity).

    Only values that parse as JSON objects or arrays are *documents*. Keys that
    look like data (objects with too many distinct keys or non-structural
    names, and keys seen in too few documents, see ``_dp_key_policy``) are
    collapsed into ``*`` and never listed. Values are never kept; key names are
    listed only when the value policy allows them.
    """
    documents: list[Any] = []
    counts = {"structured": 0, "scalar": 0, "json_null_literal": 0, "invalid": 0}
    for value in values:
        text = value.strip()
        ok, parsed = parse_json_text(text) if text else (False, None)
        if not ok:
            counts["invalid"] += 1
        elif isinstance(parsed, (dict, list)):
            documents.append(parsed)
        elif parsed is None:
            counts["json_null_literal"] += 1
        else:
            counts["scalar"] += 1
    counts["structured"] = len(documents)
    tracked_limit = max(1000, 4 * max_paths)
    stats: dict[tuple[tuple[str, ...], ...], dict[str, Any]] = {}
    map_like: dict[tuple[tuple[str, ...], ...], str] = {}
    tracking_truncated = False
    frontier: dict[tuple[tuple[str, ...], ...], list[tuple[int, Any]]] = {
        (): list(enumerate(documents))
    }
    for _depth in range(max_depth):
        following: dict[tuple[tuple[str, ...], ...], list[tuple[int, Any]]] = {}
        for parent, items in frontier.items():
            collapse, why, rare = _dp_key_policy(items, max_object_keys)
            if collapse and why:
                map_like[parent] = why
            elif rare:
                map_like[parent] = f"{len(rare)} rare key(s) collapsed into '*'"
            for doc, value in items:
                if isinstance(value, dict):
                    children = [
                        (("any_key",) if collapse or key in rare else ("key", key), child)
                        for key, child in value.items()
                    ]
                elif isinstance(value, list):
                    children = [(("items",), child) for child in value]
                else:
                    continue
                for segment, child in children:
                    path = (*parent, segment)
                    entry = stats.get(path)
                    if entry is None:
                        if len(stats) >= tracked_limit:
                            tracking_truncated = True
                            continue
                        entry = {"docs": 0, "last": -1, "occurrences": 0, "types": Counter()}
                        stats[path] = entry
                    if entry["last"] != doc:
                        entry["docs"] += 1
                        entry["last"] = doc
                    entry["occurrences"] += 1
                    entry["types"][_dp_json_type(child)] += 1
                    if isinstance(child, (dict, list)) and child:
                        following.setdefault(path, []).append((doc, child))
        frontier = following
    depth_truncated = bool(frontier)
    total = len(documents)
    records = []
    for path, entry in stats.items():
        segments = [
            {"kind": "key", "name": part[1]} if part[0] == "key" else {"kind": part[0]}
            for part in path
        ]
        types = dict(sorted(entry["types"].items()))
        non_null = [name for name in types if name != "null"]
        dominant = max(non_null, key=lambda name: (types[name], name)) if non_null else None
        records.append(
            {
                "path": json_path_text(segments),
                "segments": segments,
                "depth": len(path),
                "present_in": entry["docs"],
                "presence_ratio": entry["docs"] / total if total else None,
                "occurrences": entry["occurrences"],
                "types": types,
                "heterogeneous": len(non_null) > 1,
                "dominant_type": dominant,
                "map_like": path in map_like,
                "full_scope": None,
            }
        )
    records.sort(key=lambda item: (item["depth"], -item["present_in"], item["path"]))
    listed = records[:max_paths]
    catalog: dict[str, Any] = {
        "scope": "sample",
        "source": "sample",
        "eligible_observations": len(values),
        "excluded": {"sql_null": sql_nulls, "truncated": truncated},
        "counts": counts,
        "documents": total,
        "max_depth": max_depth,
        "depth_truncated": depth_truncated,
        "paths_observed": len(records),
        "paths_listed": len(listed) if include_names else 0,
        "paths_omitted": len(records) - len(listed) if include_names else len(records),
        "tracking_truncated": tracking_truncated,
        "map_like_paths": len(map_like),
        "heterogeneous_paths": sum(1 for item in records if item["heterogeneous"]),
        "paths": listed if include_names else None,
        "paths_omitted_reason": None,
        "full_scope": None,
        "method": (
            "strict JSON parsing of sampled values; paths walked breadth-first up to "
            "deep.max_json_depth; map-like objects collapsed into '*'"
        ),
        "limitations": [
            *sample_limitations(sample_method, len(values)),
            "Paths inferred from the sample are not a complete or guaranteed schema: paths absent "
            "from the sample may exist in the data.",
            "Values truncated by sampling.max_value_chars are excluded.",
        ],
    }
    if not include_names:
        catalog["paths_omitted_reason"] = names_omitted_reason
    elif not total:
        catalog["paths_omitted_reason"] = "no JSON objects or arrays in the sample"
    elif len(records) > len(listed):
        catalog["paths_omitted_reason"] = (
            f"{len(records) - len(listed)} path(s) beyond deep.max_json_paths = {max_paths}"
        )
    if depth_truncated:
        catalog["limitations"].append(
            f"Nesting deeper than deep.max_json_depth = {max_depth} was not catalogued."
        )
    if tracking_truncated:
        catalog["limitations"].append(
            f"More than {tracked_limit} distinct paths: further paths were not tracked."
        )
    return catalog


def plan_json_validation(
    catalogs: Mapping[str, Mapping[str, Any]], method: str | None
) -> list[dict[str, Any]]:
    """Plan full-scope presence and type checks of listed JSON paths.

    ``method`` is ``variant`` (``try_parse_json``/``try_variant_get``/
    ``is_variant_null``/``schema_of_variant``), ``get_json_object`` or None.
    Each candidate is one aggregate expression at the lowest deep tier.
    """
    candidates: list[dict[str, Any]] = []
    if method is None:
        return candidates
    for fid, catalog in catalogs.items():
        paths = [
            (item, engine_json_path(item["segments"]))
            for item in (catalog.get("paths") or [])
            if engine_json_path(item["segments"])
        ]
        if not paths:
            continue
        candidates.append(
            {
                "metric": "json_documents",
                "op": "json_count_documents",
                "field_id": fid,
                "tier": 7,
                "cost": 1,
                "params": {"method": method, "path": "$"},
                "group": "json_path",
            }
        )
        for item, engine_path in paths:
            ops = (
                [
                    ("path_present_count", "json_path_present"),
                    ("path_json_null_count", "json_path_null"),
                    ("path_type_match_count", "json_path_type_match"),
                ]
                if method == "variant"
                else [("path_non_null_count", "json_path_non_null")]
            )
            for metric, op in ops:
                if op == "json_path_type_match" and item["dominant_type"] is None:
                    continue
                candidates.append(
                    {
                        "metric": metric,
                        "op": op,
                        "field_id": fid,
                        "tier": 7,
                        "cost": 1,
                        "params": {
                            "method": method,
                            "path": engine_path,
                            "display": item["path"],
                            "expected_type": item["dominant_type"],
                        },
                        "group": "json_path",
                    }
                )
    return candidates


_DP_JSON_METHODS = {
    "variant": (
        "try_parse_json + try_variant_get/is_variant_null/schema_of_variant over the full scope",
        [
            "Distinguishes a JSON null from an absent path.",
            "Types come from schema_of_variant (numbers: integer, decimal or double).",
        ],
    ),
    "get_json_object": (
        "get_json_object over the full scope",
        [
            "get_json_object returns NULL both for a JSON null and for an absent path, so "
            "presence counts exclude JSON nulls.",
            "It cannot tell types apart (numbers and strings both come back as text), so types "
            "are not validated.",
            "Its parser is more lenient than strict JSON.",
        ],
    ),
}


def attach_json_validation(
    catalog: dict[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    dropped: Sequence[Mapping[str, Any]],
    *,
    method: str | None,
    unsupported_reason: str | None,
    scope: str,
) -> None:
    """Record full-scope validation results in a JSON path catalogue."""
    if catalog.get("paths") is None:
        return
    if method is None:
        reason = unsupported_reason or (
            "full-scope JSON path validation disabled (deep.json_full_scope_validation = false)"
        )
        catalog["full_scope"] = {
            "method": None,
            "status": "unsupported" if unsupported_reason else "not_computed",
            "reason": reason,
            "documents": None,
            "limitations": [],
        }
        for item in catalog["paths"]:
            item["full_scope"] = {"status": "not_computed", "reason": reason, "metrics": []}
        return
    description, limitations = _DP_JSON_METHODS[method]
    by_path: dict[str, list[Mapping[str, Any]]] = {}
    documents_metric = None
    for spec in specs:
        if spec["op"] == "json_count_documents":
            documents_metric = _dp_json_metric(spec, results, failed, scope, None)
        else:
            by_path.setdefault(spec["params"]["display"], []).append(spec)
    documents = (
        documents_metric["value"]
        if documents_metric and documents_metric["status"] == "measured"
        else None
    )
    dropped_paths = {
        spec["params"].get("display") for spec in dropped if spec.get("group") == "json_path"
    }
    for item in catalog["paths"]:
        engine_path = engine_json_path(item["segments"])
        if engine_path is None:
            item["full_scope"] = {
                "status": "not_computed",
                "reason": "wildcard or unquotable path; not validated",
                "metrics": [],
            }
            continue
        path_specs = by_path.get(item["path"], [])
        if not path_specs:
            reason = (
                "omitted to respect the deep expression and pass budgets"
                if item["path"] in dropped_paths or documents_metric is None
                else "not planned"
            )
            item["full_scope"] = {"status": "not_computed", "reason": reason, "metrics": []}
            continue
        metrics = [_dp_json_metric(spec, results, failed, scope, documents) for spec in path_specs]
        item["full_scope"] = {
            "status": "measured"
            if all(m["status"] == "measured" for m in metrics)
            else "incomplete",
            "reason": None,
            "metrics": metrics,
        }
    measured_documents = documents_metric is not None and documents_metric["status"] == "measured"
    summary_reason: str | None = None
    if documents_metric is None:
        summary_reason = "omitted to respect the deep expression and pass budgets"
    elif not measured_documents:
        summary_reason = documents_metric.get("reason") or "the document count was not measured"
    catalog["full_scope"] = {
        "method": method,
        "status": "measured" if measured_documents else "not_computed",
        "reason": summary_reason,
        "documents": documents_metric,
        "method_description": description,
        "limitations": list(limitations),
    }


_DP_JSON_METRIC_METHODS = {
    "json_documents": "values whose root is a JSON object or array",
    "path_present_count": "documents where the path exists (including a JSON null value)",
    "path_json_null_count": "documents where the path holds a JSON null",
    "path_type_match_count": "documents where the path holds the type most frequent in the sample",
    "path_non_null_count": "documents where get_json_object returns a value for the path "
    "(absent paths and JSON nulls are both excluded)",
}


def _dp_json_metric(
    spec: Mapping[str, Any],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    scope: str,
    documents: int | None,
) -> dict[str, Any]:
    name = spec["metric"]
    if spec["alias"] in failed:
        return not_measured(name, "error", failed[spec["alias"]], scope=scope, source="aggregate")
    raw = results.get(spec["alias"])
    if raw is None:
        return not_measured(
            name, "error", "no value returned by the engine", scope=scope, source="aggregate"
        )
    details: dict[str, Any] = {"method": spec["params"]["method"]}
    if spec["params"].get("expected_type") and name == "path_type_match_count":
        details["expected_type"] = spec["params"]["expected_type"]
    return measured(
        name,
        int(raw),
        unit="documents",
        scope=scope,
        accuracy="exact",
        source="aggregate",
        method=_DP_JSON_METRIC_METHODS[name],
        denominator=documents if name != "json_documents" else None,
        denominator_unit="documents" if name != "json_documents" else None,
        details=details,
    )

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.keys
# TableDossier 0.2.0 embedded runtime: module tabledossier.keys
# Source: src/tabledossier/keys.py (sha256:be6a343d1c43ac3e17a049917275e7f42d49b46f84eb108d2eca908e4f019e96)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Deep level, part II: exact uniqueness of keys.

Part of the embedded runtime (standard library only). Like ``planning`` and
``deep``, this module only *describes* work; the Spark adapter executes it.

A key is one or more columns of a table. Keys come from the configuration
(``deep.uniqueness.keys``), from declared PRIMARY KEY/UNIQUE constraints
(``declared_keys``) and, within the budget, from the identifier candidates of
the standard level (``identifier_candidates``). Every key is checked exactly
over the analysed scope with a grouped aggregation; several keys share one
Spark action (each row is exploded once per key it checks), so the number of
actions per table is bounded by ``deep.uniqueness.max_passes``.

Only counts leave the engine: duplicated values are never collected.
"""

from collections.abc import Mapping, Sequence
from typing import Any


KEY_ORIGINS = ("configured", "declared_primary_key", "declared_unique", "identifier_candidate")
# Atomic kinds whose values can be grouped exactly (maps, structs and variants cannot).
KEY_KINDS = ORDERABLE_KINDS
KEY_OUTCOMES = ("unique", "unique_non_null", "duplicates", "empty")
NULL_KEY_SEMANTICS = (
    "A row with NULL in any key column is counted in rows_with_null_key and excluded from the "
    "distinct and duplicate counts: NULLs are not treated as equal, as in a SQL UNIQUE "
    "constraint. A PRIMARY KEY also forbids NULLs, so it holds only when the outcome is 'unique'."
)
_KY_FLOAT_NOTE = (
    "Floating-point key columns are compared as Spark groups them: NaN equals NaN and -0.0 equals "
    "0.0."
)


def _ky_segments_of_config(columns: Sequence[Any]) -> list[list[dict[str, Any]]] | None:
    try:
        return [column_reference_segments(column) for column in columns]
    except IdentifierError:
        return None


def requested_keys(
    config: Mapping[str, Any],
    table_lookup: str,
    constraints: Sequence[Mapping[str, Any]],
    candidate_paths: Sequence[Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """Return the keys requested for one table, in priority order.

    Configured keys come first, then declared PRIMARY KEY and UNIQUE
    constraints (when ``declared_keys``), then single-column identifier
    candidates (when ``identifier_candidates``). ``candidate_paths`` are the
    typed paths of the table's identifier candidates, in schema order.
    """
    settings = config["deep"]["uniqueness"]
    out: list[dict[str, Any]] = []
    for item in settings["keys"]:
        try:
            if table_lookup_key(parse_table_identifier(item["table"])) != table_lookup:
                continue
        except IdentifierError:
            continue
        out.append(
            {
                "origin": "configured",
                "name": item.get("id"),
                "segments": _ky_segments_of_config(item["columns"]),
                "requested": [
                    column if isinstance(column, str) else ".".join(column)
                    for column in item["columns"]
                ],
            }
        )
    if settings["declared_keys"]:
        for kind, origin in (
            ("primary_key", "declared_primary_key"),
            ("unique", "declared_unique"),
        ):
            for constraint in constraints:
                if constraint.get("constraint_type") != kind:
                    continue
                columns = [str(column) for column in constraint.get("columns", [])]
                out.append(
                    {
                        "origin": origin,
                        "name": constraint.get("name"),
                        "segments": [[field_segment(column)] for column in columns] or None,
                        "requested": columns,
                    }
                )
    if settings["identifier_candidates"]:
        for path in candidate_paths:
            out.append(
                {
                    "origin": "identifier_candidate",
                    "name": None,
                    "segments": [[dict(segment) for segment in path]],
                    "requested": [display_path([dict(segment) for segment in path])],
                }
            )
    return out


def key_columns_problem(nodes: Sequence[Mapping[str, Any] | None], requested: Sequence[str]) -> str:
    """Return why these schema nodes cannot form a key ('' when they can)."""
    for node, text in zip(nodes, requested, strict=False):
        if node is None:
            return f"column {text!r} is not in the documented schema tree"
        if any(segment["kind"] != "field" for segment in node["path"]):
            return f"column {node['display_path']!r} is inside an array or map"
        if node["type"]["kind"] not in KEY_KINDS:
            return (
                f"column {node['display_path']!r} has type {node['type']['physical_type']}, which "
                "cannot be compared exactly as a key part"
            )
    ids = [node["field_id"] for node in nodes if node is not None]
    if len(ids) != len(set(ids)):
        return "a column is listed more than once"
    return ""


def plan_uniqueness(
    tree: Mapping[str, Any], requested: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> dict[str, Any]:
    """Resolve requested keys against the schema tree and apply the budget.

    Keys with the same set of columns are merged (their origins are kept).
    The first ``max_keys`` eligible keys are planned and split into at most
    ``max_passes`` passes of contiguous keys; the others are recorded as
    ``not_computed`` (budget) or ``not_eligible`` with a reason.
    """
    settings = config["deep"]["uniqueness"]
    by_id = {node["field_id"]: node for node in iter_nodes(list(tree.get("fields", [])))}
    keys: list[dict[str, Any]] = []
    index_by_set: dict[tuple[str, ...], int] = {}
    for item in requested:
        segments = item["segments"]
        if not segments:
            keys.append(
                _ky_key(item, [], [], "not_eligible", "the key lists no valid column reference")
            )
            continue
        nodes = [by_id.get(field_id(list(path))) for path in segments]
        reason = key_columns_problem(nodes, item["requested"])
        columns = [
            node["display_path"] if node is not None else text
            for node, text in zip(nodes, item["requested"], strict=False)
        ]
        ids = [node["field_id"] for node in nodes if node is not None]
        if reason:
            keys.append(_ky_key(item, columns, ids, "not_eligible", reason))
            continue
        identity = tuple(sorted(ids))
        if identity in index_by_set:
            existing = keys[index_by_set[identity]]
            if item["origin"] not in existing["origins"]:
                existing["origins"].append(item["origin"])
            if item.get("name") and item["name"] not in existing["names"]:
                existing["names"].append(item["name"])
            continue
        index_by_set[identity] = len(keys)
        keys.append(_ky_key(item, columns, ids, "planned", None))
        keys[-1]["kinds"] = [by_id[fid]["type"]["kind"] for fid in ids]
    planned = [position for position, key in enumerate(keys) if key["status"] == "planned"]
    limited: list[dict[str, str]] = []
    for position in planned[settings["max_keys"] :]:
        key = keys[position]
        key["status"] = "not_computed"
        key["reason"] = f"beyond deep.uniqueness.max_keys = {settings['max_keys']}"
        limited.append(
            {
                "item": ", ".join(key["columns"]),
                "reason": "uniqueness_budget",
                "detail": key["reason"],
            }
        )
    planned = planned[: settings["max_keys"]]
    passes: list[list[int]] = []
    if planned:
        count = min(settings["max_passes"], len(planned))
        size = -(-len(planned) // count)
        passes = [planned[start : start + size] for start in range(0, len(planned), size)]
    for number, members in enumerate(passes, start=1):
        for position in members:
            keys[position]["operation_id"] = f"op_uniqueness_{number}"
    return {"keys": keys, "passes": passes, "limited": limited}


def _ky_key(
    item: Mapping[str, Any], columns: list[str], ids: list[str], status: str, reason: str | None
) -> dict[str, Any]:
    return {
        "key_id": "k_" + short_hash(sorted(ids) or list(item["requested"])),
        "origins": [item["origin"]],
        "names": [item["name"]] if item.get("name") else [],
        "columns": columns,
        "field_ids": ids,
        "status": status,
        "reason": reason,
        "operation_id": None,
        "kinds": [],
    }


def uniqueness_metrics(raw: Mapping[str, Any], scope: str) -> tuple[list[dict[str, Any]], str]:
    """Build the metrics and the outcome of one key from the counts of its pass.

    ``raw`` holds ``rows``, ``null_rows``, ``distinct``, ``dup_groups``,
    ``dup_rows`` and ``max_n`` (largest number of rows sharing one complete key).
    """
    rows = int(raw.get("rows") or 0)
    null_rows = int(raw.get("null_rows") or 0)
    distinct = int(raw.get("distinct") or 0)
    dup_groups = int(raw.get("dup_groups") or 0)
    dup_rows = int(raw.get("dup_rows") or 0)
    complete = rows - null_rows

    def count(name: str, value: int, unit: str, method: str, **extra: Any) -> dict[str, Any]:
        source = extra.pop("source", "aggregate")
        return measured(
            name,
            value,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source=source,
            method=method,
            **extra,
        )

    metrics = [
        count("rows_in_scope", rows, "rows", "rows read by the uniqueness pass (analysed scope)"),
        count(
            "rows_with_null_key",
            null_rows,
            "rows",
            "rows with NULL in at least one key column",
            **({"denominator": rows, "denominator_unit": "rows"} if rows else {}),
        ),
        count(
            "rows_with_complete_key",
            complete,
            "rows",
            "rows_in_scope - rows_with_null_key",
            source="derived",
            **({"denominator": rows, "denominator_unit": "rows"} if rows else {}),
        ),
        count(
            "distinct_keys",
            distinct,
            "keys",
            "exact count of distinct key values among rows with a complete key (grouped "
            "aggregation)",
        ),
        count(
            "duplicate_key_groups",
            dup_groups,
            "keys",
            "key values that occur in more than one row",
            **({"denominator": distinct, "denominator_unit": "keys"} if distinct else {}),
        ),
        count(
            "rows_in_duplicate_groups",
            dup_rows,
            "rows",
            "rows whose key value occurs in more than one row",
            **({"denominator": complete, "denominator_unit": "rows"} if complete else {}),
        ),
        count(
            "surplus_duplicate_rows",
            complete - distinct,
            "rows",
            "rows_with_complete_key - distinct_keys (rows beyond the first of each key value)",
            source="derived",
            **({"denominator": complete, "denominator_unit": "rows"} if complete else {}),
        ),
    ]
    if complete and raw.get("max_n") is not None:
        metrics.append(
            count(
                "max_rows_per_key",
                int(raw["max_n"]),
                "rows",
                "largest number of rows sharing one complete key value",
            )
        )
    else:
        metrics.append(
            not_measured(
                "max_rows_per_key",
                "insufficient_data",
                "no rows with a complete key in scope",
                scope=scope,
                source="aggregate",
            )
        )
    if complete == 0:
        outcome = "empty"
    elif dup_groups:
        outcome = "duplicates"
    elif null_rows:
        outcome = "unique_non_null"
    else:
        outcome = "unique"
    return metrics, outcome


def uniqueness_record(
    plan: Mapping[str, Any],
    results: Mapping[int, Mapping[str, Any]],
    errors: Mapping[int, str],
    *,
    config: Mapping[str, Any],
    scope: str,
    requested_any: bool,
) -> dict[str, Any]:
    """Return the table's ``uniqueness`` record (contract 1.2).

    ``results`` maps key positions to raw counts; ``errors`` maps key
    positions of failed passes to a sanitized reason.
    """
    settings = config["deep"]["uniqueness"]
    keys = []
    for position, key in enumerate(plan["keys"]):
        record: dict[str, Any] = {
            "key_id": key["key_id"],
            "origins": list(key["origins"]),
            "names": list(key["names"]),
            "columns": list(key["columns"]),
            "field_ids": list(key["field_ids"]),
            "status": key["status"],
            "reason": key["reason"],
            "outcome": None,
            "scope": scope,
            "operation_id": key["operation_id"],
            "metrics": [],
            "limitations": [],
        }
        if key["status"] == "planned":
            if position in errors:
                record["status"] = "error"
                record["reason"] = errors[position]
            else:
                metrics, outcome = uniqueness_metrics(results.get(position, {}), scope)
                record.update({"status": "measured", "metrics": metrics, "outcome": outcome})
                if "float" in key["kinds"]:
                    record["limitations"].append(_KY_FLOAT_NOTE)
        keys.append(record)
    notes = []
    if not requested_any:
        notes.append(
            "No key was requested for this table (deep.uniqueness.keys, declared_keys, "
            "identifier_candidates)."
        )
    return {
        "keys": keys,
        "sources": {
            "configured": any("configured" in key["origins"] for key in plan["keys"]),
            "declared_keys": bool(settings["declared_keys"]),
            "identifier_candidates": bool(settings["identifier_candidates"]),
        },
        "budget": {"max_keys": settings["max_keys"], "max_passes": settings["max_passes"]},
        "passes": {"budget": settings["max_passes"], "planned": len(plan["passes"])},
        "limited": list(plan["limited"]),
        "null_semantics": NULL_KEY_SEMANTICS,
        "notes": notes,
    }


def measured_keys(table: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return the measured key records of a table profile (empty before contract 1.2)."""
    return [
        key
        for key in (table.get("uniqueness") or {}).get("keys", [])
        if key["status"] == "measured"
    ]

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.findings
# TableDossier 0.2.0 embedded runtime: module tabledossier.findings
# Source: src/tabledossier/findings.py (sha256:84323f45be940c19f955304d9b2f5d9a767d49e063817ae65f3eeb9216621b4a)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Heuristic findings derived from measured metrics.

Part of the embedded runtime (standard library only).

Findings are exploratory signals, never contract failures: their severity is
``info`` or ``warning`` and each one states its threshold, evidence and why the
severity was chosen. High nullity, constancy or extreme tails can all be
legitimate; no finding asserts a business error.
"""

from collections.abc import Mapping
from typing import Any



def _fd_finding(
    code: str,
    severity: str,
    field: Mapping[str, Any],
    title: str,
    message: str,
    evidence: list[dict[str, Any]],
    threshold: Mapping[str, Any] | None,
    severity_reason: str,
    limitations: str,
) -> dict[str, Any]:
    return {
        "finding_id": "fd_" + short_hash([code, field["field_id"]]),
        "code": code,
        "severity": severity,
        "field_id": field["field_id"],
        "display_path": field["display_path"],
        "title": title,
        "message": message,
        "evidence": evidence,
        "threshold": dict(threshold) if threshold else None,
        "severity_reason": severity_reason,
        "limitations": limitations,
    }


def _fd_evidence(metrics: list[Mapping[str, Any]], *names: str) -> list[dict[str, Any]]:
    out = []
    for name in names:
        value = metric_value(metrics, name)
        if value is not None:
            out.append({"metric": name, "value": value})
    return out


def field_findings(
    field: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    row_count: int | None,
    unit: str = "rows",
) -> list[dict[str, Any]]:
    """Return heuristic findings for one profiled field.

    ``row_count`` is the size of the population the field's counts refer to:
    rows in scope, or the elements/entries of a collection for element fields
    (``unit``).
    """
    metrics = field.get("metrics", [])
    kind = field["type_kind"]
    found: list[dict[str, Any]] = []
    null_count = metric_value(metrics, "null_count")
    non_null = metric_value(metrics, "non_null_count")
    null_ratio = numeric_value(metrics, "null_ratio")

    if isinstance(row_count, int) and row_count > 0 and null_count == row_count:
        found.append(
            _fd_finding(
                "all_null",
                "warning",
                field,
                "All values are null",
                f"All {row_count} row(s) in scope are null for this field."
                if unit == "rows"
                else f"All {row_count} {unit} in scope are null for this field.",
                _fd_evidence(metrics, "null_count", "null_ratio"),
                None,
                "warning: a field without any value in scope is often unused or not populated "
                "for this population, but it may be legitimate (e.g. optional or future data).",
                "Applies to the analysed scope only (filters and snapshot).",
            )
        )
    elif null_ratio is not None and null_ratio >= thresholds["high_null_ratio"]:
        found.append(
            _fd_finding(
                "high_null_ratio",
                "info",
                field,
                "High null ratio",
                f"{null_ratio:.1%} of {unit} in scope are null.",
                _fd_evidence(metrics, "null_count", "null_ratio"),
                {"name": "high_null_ratio", "value": thresholds["high_null_ratio"]},
                "info: sparse fields are frequently legitimate (optional attributes).",
                "Nullity of nested fields includes rows where a parent struct is null; "
                "see null_count_parent_present.",
            )
        )

    all_equal = metric_value(metrics, "all_values_equal")
    if (
        all_equal is True
        and isinstance(non_null, int)
        and non_null >= thresholds["constant_min_rows"]
    ):
        found.append(
            _fd_finding(
                "possible_constant",
                "info",
                field,
                "Possibly constant",
                f"All {non_null} non-null value(s) in scope are equal.",
                [
                    {"metric": "all_values_equal", "value": True},
                    *_fd_evidence(metrics, "non_null_count"),
                ],
                {"name": "constant_min_rows", "value": thresholds["constant_min_rows"]},
                "info: constancy may be a property of the analysed scope (filters, snapshot).",
                "Exact for the analysed scope; other rows or future data may differ.",
            )
        )

    roles = {
        role["role"]: role for role in (field.get("semantics") or {}).get("candidate_roles", [])
    }
    if "categorical_candidate" in roles:
        found.append(
            _fd_finding(
                "possible_categorical",
                "info",
                field,
                "Possibly categorical",
                "Few distinct values relative to rows in scope.",
                roles["categorical_candidate"]["evidence"],
                {
                    "name": "categorical_max_distinct",
                    "value": thresholds["categorical_max_distinct"],
                },
                "info: descriptive signal only.",
                roles["categorical_candidate"]["limitations"],
            )
        )
    if "identifier_candidate" in roles:
        found.append(
            _fd_finding(
                "identifier_candidate",
                "info",
                field,
                "Identifier candidate",
                "Approximate distinct count is close to the number of non-null values.",
                roles["identifier_candidate"]["evidence"],
                {
                    "name": "identifier_min_distinct_ratio",
                    "value": thresholds["identifier_min_distinct_ratio"],
                },
                "info: approximate cardinality does not prove uniqueness.",
                roles["identifier_candidate"]["limitations"],
            )
        )

    json_profile = field.get("json_profile")
    if json_profile and json_profile.get("eligible_observations"):
        counts = json_profile["counts"]
        eligible = json_profile["eligible_observations"]
        structured = (counts["object"] + counts["array"]) / eligible
        if structured >= thresholds["json_min_ratio"]:
            found.append(
                _fd_finding(
                    "probable_json",
                    "info",
                    field,
                    "Probable JSON document",
                    f"{structured:.1%} of eligible sampled values parse as JSON objects or arrays; "
                    f"{counts['invalid']} sampled value(s) are not valid JSON.",
                    [
                        {"sample_json_counts": dict(counts)},
                        *_fd_evidence(metrics, "json_invalid_count"),
                    ],
                    {"name": "json_min_ratio", "value": thresholds["json_min_ratio"]},
                    "info: storage format observation; not a quality defect.",
                    "Sample-based unless json_invalid_count was measured over the full scope.",
                )
            )

    if kind in ("integer", "float", "decimal"):
        quantiles = metric_value(metrics, "quantiles")
        maximum = numeric_value(metrics, "max")
        minimum = numeric_value(metrics, "min")
        if isinstance(quantiles, list) and maximum is not None and minimum is not None:
            points = {}
            for item in quantiles:
                try:
                    points[item["probability"]] = float(item["value"])
                except (TypeError, ValueError):
                    continue
            q1, q3 = points.get(0.25), points.get(0.75)
            if q1 is not None and q3 is not None and q3 > q1:
                iqr = q3 - q1
                multiplier = thresholds["tail_iqr_multiplier"]
                upper = (maximum - q3) / iqr
                lower = (q1 - minimum) / iqr
                if upper > multiplier or lower > multiplier:
                    found.append(
                        _fd_finding(
                            "extreme_numeric_tail",
                            "info",
                            field,
                            "Extreme numeric tail",
                            "The observed minimum or maximum lies far outside the interquartile "
                            "range "
                            f"(upper tail {upper:.1f}x IQR, lower tail {lower:.1f}x IQR).",
                            _fd_evidence(metrics, "min", "max", "quantiles"),
                            {"name": "tail_iqr_multiplier", "value": multiplier},
                            "info: heavy tails are common in amounts and counts; review if "
                            "unexpected.",
                            "Quantiles are approximate; min/max are exact over finite values in "
                            "scope.",
                        )
                    )

    if kind == "array":
        max_size = metric_value(metrics, "max_size")
        if isinstance(max_size, int) and max_size >= thresholds["large_array_size"]:
            found.append(
                _fd_finding(
                    "large_arrays",
                    "warning",
                    field,
                    "Very large arrays",
                    f"The largest array in scope has {max_size} element(s).",
                    _fd_evidence(metrics, "max_size", "mean_size"),
                    {"name": "large_array_size", "value": thresholds["large_array_size"]},
                    "warning: very large arrays increase memory and processing cost of "
                    "downstream explode/flatten operations.",
                    "Exact over the analysed scope.",
                )
            )
    return found

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.quality
# TableDossier 0.2.0 embedded runtime: module tabledossier.quality
# Source: src/tabledossier/quality.py (sha256:5f728d2906f91e8477c5a690f2bb5b9f0c5532aaa029548c03952af89c45dcd2)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Configured quality checks and proposed (never auto-applied) rules.

Part of the embedded runtime (standard library only).

Checks reuse metrics already measured by the profile; they never trigger new
reads. Their status is ``pass``, ``fail``, ``not_evaluated`` (the metric was
not measured, e.g. empty table or metadata level) or ``error`` (the check does
not fit the data, e.g. unknown column). Suggested rules are proposals derived
from observations and always require human review.
"""

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any


SUGGESTED_RULES_KIND = "tabledossier.suggested_rules"
RULE_FORMATS = ("uuid", "iso_date", "iso_timestamp", "url", "email_candidate", "numeric_string")


def _q_number(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _q_result(
    check: Mapping[str, Any],
    table_key: str,
    column: str | None,
    status: str,
    message: str,
    observed: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    parameters = {key: check[key] for key in ("min", "max") if key in check}
    return {
        "check_id": check["id"],
        "type": check["type"],
        "origin": "configured",
        "target": {"table": table_key, "column": column},
        "parameters": parameters,
        "status": status,
        "observed": dict(observed) if observed else None,
        "message": message,
        "description": check.get("description"),
    }


def _q_observed(metric: Mapping[str, Any]) -> dict[str, Any]:
    return {"metric": metric["name"], "value": metric.get("value"), "scope": metric.get("scope")}


def evaluate_checks(
    table: Mapping[str, Any], checks: list[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Evaluate configured checks against an assembled table profile."""
    table_key = table["table_key"]
    results = []
    fields = {item["field_id"]: item for item in table.get("field_profiles", [])}
    schema_ids = set()
    stack = list(table.get("schema", {}).get("fields", []))
    while stack:
        node = stack.pop()
        schema_ids.add(node["field_id"])
        stack.extend(node["children"])
    for check in checks:
        check_type = check["type"]
        if check_type in ("min_row_count", "max_row_count"):
            metric = find_metric(table.get("table_metrics", []), "row_count")
            if (
                metric is None
                or metric["status"] != "measured"
                or metric.get("source") != "aggregate"
            ):
                results.append(
                    _q_result(
                        check,
                        table_key,
                        None,
                        "not_evaluated",
                        "row count in scope was not measured",
                    )
                )
                continue
            rows = metric["value"]
            ok = rows >= check["min"] if check_type == "min_row_count" else rows <= check["max"]
            results.append(
                _q_result(
                    check,
                    table_key,
                    None,
                    "pass" if ok else "fail",
                    f"{rows} row(s) in scope",
                    _q_observed(metric),
                )
            )
            continue
        segments = column_reference_segments(check["column"])
        column = display_path(segments)
        fid = field_id(segments)
        if fid not in schema_ids:
            results.append(
                _q_result(check, table_key, column, "error", "column not found in the table schema")
            )
            continue
        field = fields.get(fid)
        if field is None or not field.get("profiled"):
            results.append(
                _q_result(
                    check, table_key, column, "not_evaluated", "column was not profiled in this run"
                )
            )
            continue
        metrics = field.get("metrics", [])
        results.append(_q_evaluate_column(check, table_key, column, field, metrics))
    return results


def _q_evaluate_column(
    check: Mapping[str, Any],
    table_key: str,
    column: str,
    field: Mapping[str, Any],
    metrics: list[Mapping[str, Any]],
) -> dict[str, Any]:
    check_type = check["type"]
    if check_type in ("max_null_ratio", "max_null_count"):
        name = "null_ratio" if check_type == "max_null_ratio" else "null_count"
        metric = find_metric(metrics, name)
        if metric is None or metric["status"] != "measured":
            reason = metric.get("reason", "not measured") if metric else "not measured"
            return _q_result(check, table_key, column, "not_evaluated", f"{name}: {reason}")
        ok = metric["value"] <= check["max"]
        return _q_result(
            check,
            table_key,
            column,
            "pass" if ok else "fail",
            f"{name} = {metric['value']}",
            _q_observed(metric),
        )
    if check_type == "max_empty_string_ratio":
        if field["type_kind"] != "string":
            return _q_result(check, table_key, column, "error", "check requires a string column")
        empty = metric_value(metrics, "empty_count")
        non_null = metric_value(metrics, "non_null_count")
        if not isinstance(empty, int) or not isinstance(non_null, int):
            return _q_result(
                check, table_key, column, "not_evaluated", "empty_count was not measured"
            )
        if non_null == 0:
            return _q_result(
                check, table_key, column, "not_evaluated", "no non-null values in scope"
            )
        value = empty / non_null
        empty_metric = find_metric(metrics, "empty_count") or {}
        return _q_result(
            check,
            table_key,
            column,
            "pass" if value <= check["max"] else "fail",
            f"empty strings / non-null values = {value}",
            {
                "metric": "empty_count/non_null_count",
                "value": value,
                "scope": empty_metric.get("scope"),
            },
        )
    # value_range
    if field["type_kind"] not in ("integer", "float", "decimal"):
        return _q_result(check, table_key, column, "error", "value_range requires a numeric column")
    observed_min = find_metric(metrics, "min")
    observed_max = find_metric(metrics, "max")
    needed = [m for m, key in ((observed_min, "min"), (observed_max, "max")) if key in check]
    if any(m is None or m["status"] != "measured" for m in needed):
        return _q_result(
            check,
            table_key,
            column,
            "not_evaluated",
            "min/max not measured (empty scope or redacted)",
        )
    failures = []
    if "min" in check and observed_min is not None:
        low = _q_number(observed_min["value"])
        bound = _q_number(check["min"])
        if low is None or bound is None or low < bound:
            failures.append(f"observed min {observed_min['value']} < {check['min']}")
    if "max" in check and observed_max is not None:
        high = _q_number(observed_max["value"])
        bound = _q_number(check["max"])
        if high is None or bound is None or high > bound:
            failures.append(f"observed max {observed_max['value']} > {check['max']}")
    return _q_result(
        check,
        table_key,
        column,
        "fail" if failures else "pass",
        "; ".join(failures) if failures else "observed finite values within bounds",
        {
            "metric": "min/max",
            "value": [
                observed_min["value"] if observed_min else None,
                observed_max["value"] if observed_max else None,
            ],
            "scope": (observed_min or observed_max or {}).get("scope"),
        },
    )


def _q_rule(
    table_key: str,
    column: str | None,
    rule_type: str,
    parameters: Mapping[str, Any],
    rationale: str,
    evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    identity: list[Any] = [table_key, column, rule_type]
    if column is None and parameters:
        identity.append(dict(parameters))
    return {
        "rule_id": "sr_" + short_hash(identity),
        "table": table_key,
        "column": column,
        "rule_type": rule_type,
        "parameters": dict(parameters),
        "status": "proposed",
        "requires_review": True,
        "rationale": rationale,
        "evidence": evidence,
    }


def _q_exact_evidence(key: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = {m["name"]: m["value"] for m in key["metrics"] if m["status"] == "measured"}
    return [
        {"check": "exact_uniqueness", "key_id": key["key_id"], "scope": key["scope"]},
        *(
            {"metric": name, "value": values[name]}
            for name in (
                "rows_with_complete_key",
                "distinct_keys",
                "duplicate_key_groups",
                "rows_with_null_key",
            )
            if name in values
        ),
    ]


def _q_exact_rationale(key: Mapping[str, Any]) -> str:
    values = {m["name"]: m["value"] for m in key["metrics"] if m["status"] == "measured"}
    text = (
        f"Exact uniqueness check: no duplicate among {values.get('rows_with_complete_key')} "
        "row(s) with a complete key in the analysed scope"
    )
    if key["outcome"] == "unique_non_null":
        text += f"; {values.get('rows_with_null_key')} row(s) have NULL in the key"
    return text + ". This describes the analysed scope, not future data."


def suggest_rules(table: Mapping[str, Any], thresholds: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Propose rules from observations of one table (never applied automatically).

    ``unique`` proposals cite the exact uniqueness check of the deep level when
    one measured the column; an exact check that found duplicates suppresses
    the proposal.
    """
    if table.get("status") == "failed":
        return []
    table_key = table["table_key"]
    rules = []
    exact = {tuple(key["field_ids"]): key for key in measured_keys(table)}
    proposed_keys: set[tuple[str, ...]] = set()
    min_rows = thresholds["identifier_min_rows"]
    for field in table.get("field_profiles", []):
        if not field.get("profiled") or field.get("element_context"):
            # Element fields are described, not turned into column rules.
            continue
        metrics = field.get("metrics", [])
        column = field["display_path"]
        null_count = metric_value(metrics, "null_count")
        non_null = metric_value(metrics, "non_null_count")
        if (
            null_count == 0
            and isinstance(non_null, int)
            and non_null >= min_rows
            and field.get("nullable") is not False
        ):
            rules.append(
                _q_rule(
                    table_key,
                    column,
                    "not_null",
                    {},
                    "No nulls observed in the analysed scope although the column is declared "
                    "nullable.",
                    [
                        {"metric": "null_count", "value": 0},
                        {"metric": "non_null_count", "value": non_null},
                    ],
                )
            )
        semantics = field.get("semantics") or {}
        for role in semantics.get("candidate_roles", []):
            if role["role"] == "identifier_candidate":
                key = exact.get((field["field_id"],))
                if key is not None:
                    proposed_keys.add((field["field_id"],))
                    if key["outcome"] in ("unique", "unique_non_null"):
                        rules.append(
                            _q_rule(
                                table_key,
                                column,
                                "unique",
                                {},
                                _q_exact_rationale(key),
                                [*_q_exact_evidence(key), *role["evidence"]],
                            )
                        )
                    continue
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "unique",
                        {},
                        "Approximate distinct count close to non-null count; confirm with an exact "
                        "uniqueness check (deep.uniqueness) before adopting.",
                        role["evidence"],
                    )
                )
            elif role["role"] == "categorical_candidate":
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "accepted_values",
                        {"values": None},
                        "Few distinct values observed; the accepted set must be defined by a data "
                        "owner (observed values are not exposed by default).",
                        role["evidence"],
                    )
                )
        fmt = semantics.get("observed_format") or {}
        if fmt.get("status") == "detected":
            if fmt["format"] in ("json_object", "json_array"):
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "valid_json",
                        {
                            "expected_top_level": "object"
                            if fmt["format"] == "json_object"
                            else "array"
                        },
                        "Sampled values parse as JSON.",
                        [
                            {
                                "observed_format": fmt["format"],
                                "eligible_observations": fmt["eligible_observations"],
                            }
                        ],
                    )
                )
            elif fmt["format"] in RULE_FORMATS:
                rules.append(
                    _q_rule(
                        table_key,
                        column,
                        "matches_format",
                        {"format": fmt["format"]},
                        "Sampled values consistently match this format.",
                        [
                            {
                                "observed_format": fmt["format"],
                                "eligible_observations": fmt["eligible_observations"],
                            }
                        ],
                    )
                )
    for ids, key in exact.items():
        if ids in proposed_keys or key["outcome"] not in ("unique", "unique_non_null"):
            continue
        single = len(key["columns"]) == 1
        rules.append(
            _q_rule(
                table_key,
                key["columns"][0] if single else None,
                "unique",
                {} if single else {"columns": list(key["columns"])},
                _q_exact_rationale(key),
                _q_exact_evidence(key),
            )
        )
    return rules


def suggested_rules_document(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Return the neutral ``suggested_rules.json`` document for a profile."""
    rules = []
    for table in profile.get("tables", []):
        rules.extend(table.get("suggested_rules", []))
    return {
        "kind": SUGGESTED_RULES_KIND,
        "schema_version": "1.0",
        "run_id": profile["run"]["run_id"],
        "profiled_at": profile["run"]["started_at"],
        "notice": "Proposals derived from observed data. They are not applied and require review "
        "by a data owner before being adopted as quality rules.",
        "rules": rules,
    }

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.relationships
# TableDossier 0.2.0 embedded runtime: module tabledossier.relationships
# Source: src/tabledossier/relationships.py (sha256:1ce3c756ef6905f9ad5f3d79841e257da7e65b4317cc986c378e405a58678ac7)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Known relationships: declared constraints and human-provided links.

Part of the embedded runtime (standard library only).

Relationships are never inferred from column names (a column ending in
``_id`` proves nothing). Each record keeps its origin, participating columns
(composite keys included), enforcement, validation state and scope.
Cardinality is recorded only when a person provided it.
"""

from collections.abc import Iterable, Mapping
from typing import Any


DECLARED_NOTE = (
    "Declared as a FOREIGN KEY constraint. Unity Catalog primary and foreign keys are "
    "informational (not enforced): the declaration alone does not prove the data."
)
PROVIDED_NOTE = "Provided by a person; the statement alone does not prove the data."


def _rel_table(text: str) -> str:
    return table_key(parse_table_identifier(text))


def _rel_columns(columns: Iterable[Any]) -> list[str]:
    return [display_path(column_reference_segments(column)) for column in columns]


def declared_relationships(tables: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build relationship records from declared FOREIGN KEY constraints."""
    out = []
    for table in tables:
        for constraint in table.get("constraints", []):
            if constraint.get("constraint_type") != "foreign_key" or not constraint.get(
                "referenced"
            ):
                continue
            referenced = constraint["referenced"]
            out.append(
                {
                    "relationship_id": f"declared:{table['table_key']}:{constraint['name']}",
                    "origin": "declared_constraint",
                    "name": constraint["name"],
                    "from": {"table": table["table_key"], "columns": list(constraint["columns"])},
                    "to": {"table": referenced["table"], "columns": list(referenced["columns"])},
                    "cardinality": None,
                    "cardinality_source": None,
                    "enforcement": constraint.get("enforcement", "unknown"),
                    "validation": "not_validated",
                    "scope": "declared_metadata",
                    "description": None,
                    "notes": [DECLARED_NOTE],
                }
            )
    return out


def provided_relationships(items: Iterable[Mapping[str, Any]], origin: str) -> list[dict[str, Any]]:
    """Build relationship records from configuration or annotation entries."""
    out = []
    for item in items:
        cardinality = item.get("cardinality")
        out.append(
            {
                "relationship_id": f"{origin}:{item['id']}",
                "origin": origin,
                "name": item["id"],
                "from": {
                    "table": _rel_table(item["from"]["table"]),
                    "columns": _rel_columns(item["from"]["columns"]),
                },
                "to": {
                    "table": _rel_table(item["to"]["table"]),
                    "columns": _rel_columns(item["to"]["columns"]),
                },
                "cardinality": dict(cardinality) if cardinality else None,
                "cardinality_source": "provided" if cardinality else None,
                "enforcement": "unknown",
                "validation": "not_validated",
                "scope": "human_provided",
                "description": item.get("description"),
                "notes": [PROVIDED_NOTE],
            }
        )
    return out


def merge_relationships(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Concatenate relationship groups, dropping exact duplicate identifiers."""
    seen: set[str] = set()
    out = []
    for group in groups:
        for item in group:
            if item["relationship_id"] in seen:
                continue
            seen.add(item["relationship_id"])
            out.append(dict(item))
    return out

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.integrity
# TableDossier 0.2.0 embedded runtime: module tabledossier.integrity
# Source: src/tabledossier/integrity.py (sha256:a5278375f083f4b3f84777b301b1f4f57655c3c1d8bfcbb3cf26e90a88ddf69f)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Deep level, part II: referential validation and relationship hypotheses.

Part of the embedded runtime (standard library only). The Spark adapter runs
one inclusion check per relationship or hypothesis pair; this module decides
what is checked (within the budgets), which column types can be compared and
how the counts become contract records.

* **Referential validation** checks known relationships (declared FOREIGN
  KEYs, relationships in the configuration): source rows with a complete key
  whose key is absent from the target are *orphans*. Both tables are read at
  the Delta versions recorded when they were profiled. A full-scope check can
  validate or violate a relationship; a sample can only violate it.
* **Relationship hypotheses** are data-driven and kept apart from known
  relationships. Only single-column keys measured exactly unique are targets;
  source columns are chosen by type compatibility and by the value (or length)
  ranges measured at the standard level, never by their names. A hypothesis
  never gets a cardinality and is never drawn as an ER edge.

Only counts leave the engine: orphan or matching values are never collected.
"""

from collections.abc import Mapping, Sequence
from typing import Any


VALIDATION_MODES = ("full_scope", "sample")
HYPOTHESIS_KINDS = ("integer", "decimal", "string", "date")
TARGET_SCOPE_NOTE = (
    "The target is read in full at its recorded version (the target table's filters are not "
    "applied): a reference is valid when its key exists anywhere in the target table."
)
_IG_EXACT_NUMERIC = ("integer", "decimal")


def _ig_scale(node: Mapping[str, Any]) -> int | None:
    scale = node["type"].get("scale")
    return int(scale) if isinstance(scale, int) else None


def compatible_kinds(left: Mapping[str, Any], right: Mapping[str, Any]) -> tuple[bool, str]:
    """Return ``(compatible, rule)`` for comparing two key columns by equality.

    Same kinds compare (except structures); integers and decimals compare as
    exact numbers. Floating point only compares with floating point, and
    timestamps with and without time zone never compare.
    """
    a, b = left["type"]["kind"], right["type"]["kind"]
    if a in ("struct", "array", "map", "variant", "interval", "null", "other") or b in (
        "struct",
        "array",
        "map",
        "variant",
        "interval",
        "null",
        "other",
    ):
        return False, f"{a} and {b} values are not compared as keys"
    if a == b:
        return True, f"same type kind ({a})"
    if a in _IG_EXACT_NUMERIC and b in _IG_EXACT_NUMERIC:
        return True, "exact numbers (integer and decimal)"
    return False, f"{a} and {b} are not compared by equality (no implicit conversion)"


def type_compatibility(
    from_nodes: Sequence[Mapping[str, Any]], to_nodes: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Return one compatibility record per pair of key columns."""
    out = []
    for left, right in zip(from_nodes, to_nodes, strict=False):
        ok, rule = compatible_kinds(left, right)
        out.append(
            {
                "from_column": left["display_path"],
                "from_type": left["type"]["physical_type"],
                "to_column": right["display_path"],
                "to_type": right["type"]["physical_type"],
                "compatible": ok,
                "rule": rule,
            }
        )
    return out


def end_segments(relationship: Mapping[str, Any], end: str) -> list[list[dict[str, Any]]]:
    """Typed paths of one end of a relationship record.

    Declared constraints list literal top-level column names; configured and
    annotated relationships list display paths.
    """
    columns = relationship[end]["columns"]
    if relationship["origin"] == "declared_constraint":
        return [[field_segment(str(column))] for column in columns]
    return [parse_display_path(column) for column in columns]


def referential_requested(relationship: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    """Return why a relationship is not requested for validation ('' when it is)."""
    if config.get("analysis_level") != "deep":
        return "referential validation runs only at the deep level"
    settings = config["deep"]["referential"]
    if relationship["origin"] == "declared_constraint" and not settings["declared"]:
        return "declared foreign keys are not validated (deep.referential.declared = false)"
    if relationship["origin"] == "configuration" and not settings["configured"]:
        return "configured relationships are not validated (deep.referential.configured = false)"
    if relationship["origin"] not in ("declared_constraint", "configuration"):
        return "only declared and configured relationships are validated"
    return ""


def not_validated_detail(reason: str, **known: Any) -> dict[str, Any]:
    """Return the validation detail of a relationship not checked against the data."""
    detail: dict[str, Any] = {
        "status": "not_validated",
        "reason": reason,
        "mode": None,
        "from": None,
        "to": None,
        "operation_id": None,
        "type_compatibility": [],
        "target_key_unique": None,
        "metrics": [],
        "limitations": [],
    }
    detail.update(known)
    return detail


def _ig_side_metrics(raw: Mapping[str, Any], scope: str, target_scope: str) -> list[dict[str, Any]]:
    rows = int(raw.get("rows") or 0)
    nulls = int(raw.get("null_rows") or 0)
    complete = rows - nulls
    t_rows = int(raw.get("t_rows") or 0)
    t_nulls = int(raw.get("t_null_rows") or 0)
    t_distinct = int(raw.get("t_distinct") or 0)

    def count(name: str, value: int, unit: str, method: str, where: str, **extra: Any) -> Any:
        source = extra.pop("source", "aggregate")
        return measured(
            name,
            value,
            unit=unit,
            scope=where,
            accuracy="exact",
            source=source,
            method=method,
            **extra,
        )

    def denominator(value: int, unit: str = "rows") -> dict[str, Any]:
        return {"denominator": value, "denominator_unit": unit} if value else {}

    return [
        count("source_rows", rows, "rows", "source rows examined", scope),
        count(
            "source_rows_with_null_key",
            nulls,
            "rows",
            "source rows with NULL in at least one key column (never orphans)",
            scope,
            **denominator(rows),
        ),
        count(
            "source_rows_with_complete_key",
            complete,
            "rows",
            "source_rows - source_rows_with_null_key",
            scope,
            source="derived",
            **denominator(rows),
        ),
        count("target_rows", t_rows, "rows", "rows of the target table read", target_scope),
        count(
            "target_rows_with_null_key",
            t_nulls,
            "rows",
            "target rows with NULL in at least one key column (never matched)",
            target_scope,
            **denominator(t_rows),
        ),
        count(
            "target_distinct_keys",
            t_distinct,
            "keys",
            "exact count of distinct complete key values in the target",
            target_scope,
        ),
        count(
            "target_duplicate_key_groups",
            int(raw.get("t_dup_groups") or 0),
            "keys",
            "target key values that occur in more than one row",
            target_scope,
            **denominator(t_distinct, "keys"),
        ),
    ]


def validation_detail(
    raw: Mapping[str, Any],
    *,
    mode: str,
    sample_rows: int | None,
    from_info: Mapping[str, Any],
    to_info: Mapping[str, Any],
    compatibility: list[dict[str, Any]],
    operation_id: str,
) -> dict[str, Any]:
    """Turn the counts of one referential check into a validation detail.

    A full-scope check with no orphan validates the relationship and any orphan
    violates it. A sample can prove a violation (an orphan of the sample is an
    orphan of the table) but never validates the whole scope.
    """
    scope = "sample" if mode == "sample" else from_info["scope"]
    metrics = _ig_side_metrics(raw, scope, to_info["scope"])
    complete = int(raw.get("rows") or 0) - int(raw.get("null_rows") or 0)
    orphans = int(raw.get("orphans") or 0)
    metrics[3:3] = [
        measured(
            "orphan_rows",
            orphans,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method="source rows with a complete key that no target row has (anti join)",
            denominator=complete or None,
            denominator_unit="rows" if complete else None,
        ),
        ratio(
            "orphan_ratio",
            orphans,
            complete,
            scope=scope,
            source="derived",
            method="orphan_rows / source_rows_with_complete_key",
            denominator_unit="rows",
        ),
    ]
    limitations = [TARGET_SCOPE_NOTE]
    reason = None
    if complete == 0:
        status = "not_validated"
        reason = "no source row with a complete key was examined"
    elif orphans:
        status = "violated"
    elif mode == "sample":
        status = "not_validated"
        reason = (
            f"no orphan in a {mode} of at most {sample_rows} source rows; a sample cannot validate "
            "the whole scope"
        )
    else:
        status = "validated"
    if mode == "sample":
        limitations.append(
            "Source rows come from a bounded sample (potentially biased for prefix samples); the "
            "counts describe the sample only."
        )
    for side in (from_info, to_info):
        if side.get("consistency_mode") != "pinned_delta_version":
            limitations.append(
                f"{side['table']} is not pinned to a Delta version; it was read in its current "
                "state."
            )
    t_dups = int(raw.get("t_dup_groups") or 0)
    return {
        "status": status,
        "reason": reason,
        "mode": mode,
        "from": dict(from_info),
        "to": dict(to_info),
        "operation_id": operation_id,
        "type_compatibility": compatibility,
        "target_key_unique": t_dups == 0 if int(raw.get("t_distinct") or 0) else None,
        "metrics": metrics,
        "limitations": limitations,
    }


def referential_summary(
    relationships: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    planned: int,
    limited: list[dict[str, str]],
) -> dict[str, Any]:
    """Run-level record of referential validation (contract 1.2)."""
    settings = config["deep"]["referential"]
    statuses = [rel["validation"] for rel in relationships]
    return {
        "requested": {"configured": settings["configured"], "declared": settings["declared"]},
        "mode": settings["mode"],
        "budget": {
            "max_relationships": settings["max_relationships"],
            "max_sample_rows": settings["max_sample_rows"],
        },
        "planned": planned,
        "validated": statuses.count("validated"),
        "violated": statuses.count("violated"),
        "not_validated": statuses.count("not_validated"),
        "limited": limited,
        "notes": [
            TARGET_SCOPE_NOTE,
            "Orphan values are never collected; only counts are recorded.",
        ],
    }


# --------------------------------------------------------------------------- hypotheses


def _ig_profile_nodes(table: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    nodes: dict[str, Mapping[str, Any]] = {}
    stack = list((table.get("schema") or {}).get("fields", []))
    while stack:
        node = stack.pop()
        nodes[node["field_id"]] = node
        stack.extend(node.get("children", []))
    return nodes


def _ig_hypothesis_kind(node: Mapping[str, Any]) -> bool:
    kind = node["type"]["kind"]
    if kind == "decimal":
        return _ig_scale(node) == 0
    return kind in HYPOTHESIS_KINDS


def _ig_range(metrics: Sequence[Mapping[str, Any]], kind: str) -> tuple[Any, Any] | None:
    if kind == "string":
        low, high = metric_value(metrics, "min_length"), metric_value(metrics, "max_length")
    elif kind == "date":
        low, high = metric_value(metrics, "min"), metric_value(metrics, "max")
    else:
        low, high = numeric_value(metrics, "min"), numeric_value(metrics, "max")
    return None if low is None or high is None else (low, high)


def _ig_compare_ranges(source: tuple[Any, Any] | None, target: tuple[Any, Any] | None) -> str:
    if source is None or target is None:
        return "not_compared"
    if source[1] < target[0] or source[0] > target[1]:
        return "disjoint"
    if target[0] <= source[0] and source[1] <= target[1]:
        return "contained"
    return "overlapping"


def plan_hypotheses(
    tables: Sequence[Mapping[str, Any]],
    known: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Choose the column pairs whose inclusion is measured, within ``max_pairs``.

    Targets are single-column keys measured exactly unique in this run.
    Sources are profiled, non-empty columns of a compatible type whose measured
    range (values for numbers and dates, lengths for strings) is not disjoint
    from the target's. Column names are never used. Pairs already known as
    relationships are skipped. Pairs whose source range lies inside the target
    range come first, then schema order.
    """
    settings = config["deep"]["relationship_hypotheses"]
    targets = []
    for t_index, table in enumerate(tables):
        nodes = _ig_profile_nodes(table)
        fields = {f["field_id"]: f for f in table.get("field_profiles", [])}
        for key in (table.get("uniqueness") or {}).get("keys", []):
            if key["status"] != "measured" or key["outcome"] not in ("unique", "unique_non_null"):
                continue
            if len(key["field_ids"]) != 1 or key["field_ids"][0] not in nodes:
                continue
            node = nodes[key["field_ids"][0]]
            if _ig_hypothesis_kind(node):
                targets.append((t_index, table, node, fields.get(node["field_id"]), key))
    counts = {"known": 0, "disjoint": 0}
    pairs = []
    for t_index, target_table, target, target_field, key in targets:
        target_range = _ig_range((target_field or {}).get("metrics", []), target["type"]["kind"])
        for s_index, table in enumerate(tables):
            nodes = _ig_profile_nodes(table)
            for field in table.get("field_profiles", []):
                if not field.get("profiled") or field.get("element_context"):
                    continue
                found = nodes.get(field["field_id"])
                if found is None or any(s["kind"] != "field" for s in found["path"]):
                    continue
                node = found
                if s_index == t_index and node["field_id"] == target["field_id"]:
                    continue
                if not _ig_hypothesis_kind(node) or not compatible_kinds(node, target)[0]:
                    continue
                if metric_value(field["metrics"], "non_null_count") == 0:
                    continue
                identity = (
                    table_lookup_key(table["identifier"]["parts"]),
                    (node["field_id"],),
                    table_lookup_key(target_table["identifier"]["parts"]),
                    (target["field_id"],),
                )
                if identity in known:
                    counts["known"] += 1
                    continue
                relation = _ig_compare_ranges(
                    _ig_range(field["metrics"], node["type"]["kind"]), target_range
                )
                if relation == "disjoint":
                    counts["disjoint"] += 1
                    continue
                pairs.append(
                    {
                        "from_table_index": s_index,
                        "from_node": node,
                        "to_table_index": t_index,
                        "to_node": target,
                        "target_key_id": key["key_id"],
                        "range_relation": relation,
                        "range_basis": "lengths" if node["type"]["kind"] == "string" else "values",
                    }
                )
    ranked = sorted(pairs, key=lambda pair: 0 if pair["range_relation"] == "contained" else 1)
    selected = ranked[: settings["max_pairs"]]
    return {
        "targets": len(targets),
        "pairs": selected,
        "considered": len(pairs),
        "not_evaluated": len(pairs) - len(selected),
        "known_excluded": counts["known"],
        "disjoint_excluded": counts["disjoint"],
    }


def hypothesis_evidence(
    raw: Mapping[str, Any],
    *,
    scope: str,
    target_scope: str,
) -> tuple[list[dict[str, Any]], float | None]:
    """Inclusion metrics of one evaluated pair and the inclusion ratio (None if undefined)."""
    metrics = _ig_side_metrics(raw, scope, target_scope)
    complete = int(raw.get("rows") or 0) - int(raw.get("null_rows") or 0)
    included = complete - int(raw.get("orphans") or 0)
    inclusion = ratio(
        "inclusion_ratio",
        included,
        complete,
        scope=scope,
        source="derived",
        method="included_rows / source_rows_with_complete_key",
        denominator_unit="rows",
    )
    metrics[3:3] = [
        measured(
            "included_rows",
            included,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method="source rows with a complete key found among the target key values",
            denominator=complete or None,
            denominator_unit="rows" if complete else None,
        ),
        inclusion,
    ]
    value = inclusion["value"] if inclusion["status"] == "measured" else None
    return metrics, value


HYPOTHESIS_LIMITATIONS = (
    "Inclusion shows that source values occur among the target key values; it does not prove "
    "that the columns mean the same thing.",
    "No cardinality is asserted and the hypothesis is never drawn in the ER diagram; declare the "
    "relationship in the configuration once a person confirms it.",
)


def hypothesis_item(
    number: int,
    pair: Mapping[str, Any],
    *,
    source_table: Mapping[str, Any],
    target_table: Mapping[str, Any],
    evidence: Mapping[str, Any],
    operation_id: str,
    sample_rows: int | None,
) -> dict[str, Any]:
    """Return one hypothesis record (always ``status: hypothesis``, never a cardinality)."""
    limitations = list(HYPOTHESIS_LIMITATIONS)
    if sample_rows is not None:
        limitations.append(
            f"Inclusion was measured on at most {sample_rows} source rows (a bounded sample); it "
            "is not extrapolated to the table."
        )
    return {
        "hypothesis_id": f"hyp_{number}",
        "status": "hypothesis",
        "from": {
            "table": source_table["table_key"],
            "columns": [pair["from_node"]["display_path"]],
        },
        "to": {"table": target_table["table_key"], "columns": [pair["to_node"]["display_path"]]},
        "cardinality": None,
        "evidence": dict(evidence),
        "operation_id": operation_id,
        "limitations": limitations,
    }


def hypotheses_record(
    config: Mapping[str, Any],
    plan: Mapping[str, Any] | None,
    hypotheses: list[dict[str, Any]],
    *,
    evaluated: int,
    rejected: Mapping[str, int],
    reason: str | None,
) -> dict[str, Any]:
    """Run-level record of relationship hypotheses (contract 1.2)."""
    settings = config["deep"]["relationship_hypotheses"]
    return {
        "enabled": bool(settings["enabled"]),
        "reason": reason,
        "budget": {
            "max_pairs": settings["max_pairs"],
            "max_sample_rows": settings["max_sample_rows"],
            "inclusion_scope": settings["inclusion_scope"],
            "min_inclusion_ratio": settings["min_inclusion_ratio"],
        },
        "targets": plan["targets"] if plan else 0,
        "pairs_considered": plan["considered"] if plan else 0,
        "pairs_evaluated": evaluated,
        "pairs_not_evaluated": plan["not_evaluated"] if plan else 0,
        "pairs_known_excluded": plan["known_excluded"] if plan else 0,
        "pairs_disjoint_excluded": plan["disjoint_excluded"] if plan else 0,
        "pairs_rejected": dict(rejected),
        "hypotheses": hypotheses,
        "method": (
            "Targets: single-column keys measured exactly unique in this run. Sources: profiled "
            "columns of a compatible type whose measured value or length range is not disjoint "
            "from the target's; column names are not used. Each pair is one inclusion check "
            "(left join against the distinct target keys)."
        ),
        "limitations": [
            "Only single-column keys measured exactly unique are targets; composite relationships "
            "are not hypothesized.",
            "Pairs beyond max_pairs and pairs whose measured ranges are disjoint are not measured: "
            "a missing hypothesis is not evidence that no relationship exists.",
        ],
    }

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.contract
# TableDossier 0.2.0 embedded runtime: module tabledossier.contract
# Source: src/tabledossier/contract.py (sha256:3bfbddb4e8b773682a366f74d6dc1e74976e1ce3d3f6c723067593c4ae0c19aa)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Profile and annotation validation: version compatibility, schema and invariants.

Part of the embedded runtime (standard library only). The schema itself is
passed in by the caller; this module adds the cross-field invariants that JSON
Schema cannot express (status consistency, denominators, references).
"""

from collections.abc import Iterator, Mapping
from typing import Any


PROFILE_KIND = "tabledossier.profile"
# The notebook writes the latest version; readers (CLI, renderer) accept every listed one.
# 1.1 only adds to 1.0 (deep level, element fields, JSON paths, deep coverage); 1.2 only adds
# to 1.1 (exact uniqueness, referential validation evidence, relationship hypotheses).
PROFILE_SCHEMA_VERSION = "1.2"
SUPPORTED_PROFILE_VERSIONS = ("1.0", "1.1", "1.2")
ANNOTATIONS_KIND = "tabledossier.annotations"
SUPPORTED_ANNOTATION_VERSIONS = ("1.0",)
COUNT_METRICS_WITH_ROW_DENOMINATOR = ("null_count", "non_null_count")


def version_error(
    document: Any, kind: str, version_key: str, supported: tuple[str, ...]
) -> str | None:
    """Return a clear message when ``document`` is not a supported version of ``kind``."""
    if not isinstance(document, Mapping):
        return f"expected a JSON object for {kind}"
    if document.get("kind") != kind:
        return f"not a {kind} document (kind = {document.get('kind')!r})"
    version = document.get(version_key)
    if version not in supported:
        return (
            f"{kind} {version_key} {version!r} is not supported by this TableDossier "
            f"version (supported: {', '.join(supported)}); use a matching tabledossier release"
        )
    return None


def _ct_nodes(nodes: list[Mapping[str, Any]]) -> Iterator[Mapping[str, Any]]:
    for node in nodes:
        yield node
        yield from _ct_nodes(node.get("children", []))


def _ct_metric_errors(metric: Mapping[str, Any], where: str, row_count: int | None) -> list[str]:
    errors: list[str] = []
    if metric.get("status") != "measured":
        return errors
    value = metric.get("value")
    if metric.get("value_type") == "ratio":
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 1:
            errors.append(f"{where}: ratio value must be a number between 0 and 1")
        if not metric.get("denominator"):
            errors.append(f"{where}: a measured ratio requires a positive denominator")
    is_count = metric.get("value_type") == "integer" and metric.get("unit") in (
        "rows",
        "values",
        "elements",
        "entries",
        "documents",
        "keys",
    )
    if is_count and isinstance(value, int) and value < 0:
        errors.append(f"{where}: counts cannot be negative")
    if (
        metric.get("name") in COUNT_METRICS_WITH_ROW_DENOMINATOR
        and metric.get("unit") == "rows"
        and metric.get("source") == "aggregate"
        and isinstance(row_count, int)
        and isinstance(value, int)
        and value > row_count
    ):
        errors.append(f"{where}: {metric['name']} exceeds the row count in scope")
    return errors


def profile_invariant_errors(profile: Mapping[str, Any]) -> list[str]:
    """Return violations of cross-field invariants of a schema-valid profile."""
    errors: list[str] = []
    run = profile["run"]
    if run["finished_at"] < run["started_at"]:
        errors.append("$.run: finished_at is earlier than started_at")
    statuses = [table["status"] for table in profile["tables"]]
    if statuses:
        if all(status == "succeeded" for status in statuses):
            expected = "succeeded"
        elif all(status == "failed" for status in statuses):
            expected = "failed"
        else:
            expected = "partial"
        if run["status"] != expected:
            errors.append(
                f"$.run.status: {run['status']!r} is inconsistent with table statuses (expected "
                f"{expected!r})"
            )
    summary = profile["summary"]
    counts = {
        "tables_total": len(statuses),
        "tables_succeeded": statuses.count("succeeded"),
        "tables_partial": statuses.count("partial"),
        "tables_failed": statuses.count("failed"),
    }
    for key, value in counts.items():
        if summary[key] != value:
            errors.append(f"$.summary.{key}: {summary[key]} does not match the tables ({value})")

    table_ids: set[str] = set()
    for index, table in enumerate(profile["tables"]):
        where = f"$.tables[{index}]"
        if table["table_id"] in table_ids:
            errors.append(f"{where}: duplicate table_id {table['table_id']}")
        table_ids.add(table["table_id"])
        if table["status"] == "failed" and not table["errors"]:
            errors.append(f"{where}: a failed table must report at least one error")
        schema_ids: set[str] = set()
        for node in _ct_nodes((table.get("schema") or {}).get("fields", [])):
            if node["field_id"] in schema_ids:
                errors.append(f"{where}.schema: duplicate field_id {node['field_id']}")
            schema_ids.add(node["field_id"])
        row_count = None
        for metric in table["table_metrics"]:
            if (
                metric["name"] == "row_count"
                and metric["status"] == "measured"
                and metric["source"] == "aggregate"
            ):
                row_count = metric["value"]
        for m_index, metric in enumerate(table["table_metrics"]):
            errors.extend(_ct_metric_errors(metric, f"{where}.table_metrics[{m_index}]", None))
        profile_ids: set[str] = set()
        for f_index, field in enumerate(table["field_profiles"]):
            f_where = f"{where}.field_profiles[{f_index}]"
            if field["field_id"] not in schema_ids:
                errors.append(f"{f_where}: field_id {field['field_id']} is not in the schema tree")
            if field["field_id"] in profile_ids:
                errors.append(f"{f_where}: duplicate field profile")
            profile_ids.add(field["field_id"])
            names = [metric["name"] for metric in field["metrics"]]
            if len(names) != len(set(names)):
                errors.append(f"{f_where}: duplicate metric names")
            for m_index, metric in enumerate(field["metrics"]):
                errors.extend(_ct_metric_errors(metric, f"{f_where}.metrics[{m_index}]", row_count))
            if field.get("element_context"):
                errors.extend(_ct_element_errors(field, f_where))
            if field.get("json_paths"):
                errors.extend(_ct_json_path_errors(field["json_paths"], f"{f_where}.json_paths"))
        for d_index, finding in enumerate(table["findings"]):
            if finding["field_id"] not in profile_ids:
                errors.append(
                    f"{where}.findings[{d_index}]: unknown field_id {finding['field_id']}"
                )
        planned_ids = {op["operation_id"] for op in table["operations"]["planned"]}
        if table.get("uniqueness"):
            errors.extend(
                _ct_uniqueness_errors(table["uniqueness"], f"{where}.uniqueness", planned_ids)
            )
    planned_by_table = {
        table["table_key"]: {op["operation_id"] for op in table["operations"]["planned"]}
        for table in profile["tables"]
    }
    known = set()
    for r_index, relationship in enumerate(profile["relationships"]):
        known.add(_ct_relationship_ends(relationship))
        detail = relationship.get("validation_detail")
        if detail:
            errors.extend(
                _ct_validation_errors(
                    relationship, detail, f"$.relationships[{r_index}]", planned_by_table
                )
            )
    hypotheses = profile.get("relationship_hypotheses")
    if hypotheses:
        errors.extend(_ct_hypothesis_errors(hypotheses, known, planned_by_table))
    return errors


def _ct_values(metrics: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {m["name"]: m["value"] for m in metrics if m.get("status") == "measured"}


def _ct_uniqueness_errors(
    record: Mapping[str, Any], where: str, planned_ids: set[str]
) -> list[str]:
    """Check that the counts of each key agree with each other and with its outcome."""
    errors: list[str] = []
    for index, key in enumerate(record["keys"]):
        k_where = f"{where}.keys[{index}]"
        if key["status"] != "measured":
            if key["outcome"] is not None or key["metrics"]:
                errors.append(f"{k_where}: only measured keys have an outcome and metrics")
            if not key["reason"]:
                errors.append(f"{k_where}: a key that was not measured needs a reason")
            continue
        if key["operation_id"] not in planned_ids:
            errors.append(f"{k_where}: operation_id is not a planned operation of the table")
        values = _ct_values(key["metrics"])
        needed = ("rows_in_scope", "rows_with_null_key", "distinct_keys", "duplicate_key_groups")
        if any(not isinstance(values.get(name), int) for name in needed) or not isinstance(
            values.get("rows_in_duplicate_groups"), int
        ):
            errors.append(f"{k_where}: a measured key needs its row, key and duplicate counts")
            continue
        rows, nulls = values["rows_in_scope"], values["rows_with_null_key"]
        distinct, groups = values["distinct_keys"], values["duplicate_key_groups"]
        duplicated = values["rows_in_duplicate_groups"]
        complete = rows - nulls
        if nulls > rows or distinct > complete or groups > distinct or duplicated > complete:
            errors.append(f"{k_where}: inconsistent uniqueness counts")
        if duplicated < 2 * groups or (groups == 0) != (duplicated == 0):
            errors.append(f"{k_where}: duplicate groups and duplicated rows disagree")
        expected = (
            "empty"
            if complete == 0
            else "duplicates"
            if groups
            else "unique_non_null"
            if nulls
            else "unique"
        )
        if key["outcome"] != expected:
            errors.append(f"{k_where}: outcome {key['outcome']!r} contradicts its counts")
    return errors


def _ct_relationship_ends(relationship: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        relationship["from"]["table"],
        *relationship["from"]["columns"],
        "->",
        relationship["to"]["table"],
        *relationship["to"]["columns"],
    )


def _ct_validation_errors(
    relationship: Mapping[str, Any],
    detail: Mapping[str, Any],
    where: str,
    planned_by_table: Mapping[str, set[str]],
) -> list[str]:
    """Check that ``validated``/``violated`` follow from the orphan count of a real check."""
    errors: list[str] = []
    status = relationship["validation"]
    if detail["status"] != status:
        errors.append(f"{where}.validation_detail: status differs from validation")
    if status == "not_validated":
        if not detail["reason"]:
            errors.append(f"{where}.validation_detail: not_validated requires a reason")
        return errors
    source_ops = planned_by_table.get(relationship["from"]["table"], set())
    if detail["operation_id"] not in source_ops:
        errors.append(f"{where}.validation_detail: operation_id is not planned by the source")
    orphans = _ct_values(detail["metrics"]).get("orphan_rows")
    if not isinstance(orphans, int):
        errors.append(f"{where}.validation_detail: {status} requires a measured orphan_rows")
    elif status == "validated" and (orphans != 0 or detail["mode"] != "full_scope"):
        errors.append(f"{where}.validation_detail: validated requires 0 orphans in full scope")
    elif status == "violated" and orphans == 0:
        errors.append(f"{where}.validation_detail: violated requires at least one orphan")
    return errors


def _ct_hypothesis_errors(
    record: Mapping[str, Any],
    known: set[tuple[str, ...]],
    planned_by_table: Mapping[str, set[str]],
) -> list[str]:
    """Check that hypotheses stay apart from known relationships and meet the threshold."""
    errors: list[str] = []
    threshold = record["budget"]["min_inclusion_ratio"]
    for index, item in enumerate(record["hypotheses"]):
        where = f"$.relationship_hypotheses.hypotheses[{index}]"
        if _ct_relationship_ends(item) in known:
            errors.append(f"{where}: a hypothesis repeats a known relationship")
        if item["operation_id"] not in planned_by_table.get(item["from"]["table"], set()):
            errors.append(f"{where}: operation_id is not planned by the source table")
        inclusion = _ct_values(item["evidence"]["metrics"]).get("inclusion_ratio")
        if not isinstance(inclusion, (int, float)) or inclusion < threshold:
            errors.append(f"{where}: inclusion_ratio is below min_inclusion_ratio")
        if not item["evidence"]["target_key_unique"]:
            errors.append(f"{where}: the target key of a hypothesis must be unique")
    return errors


def _ct_element_errors(field: Mapping[str, Any], where: str) -> list[str]:
    """Element fields count elements or entries of a collection, never rows."""
    errors: list[str] = []
    context = field["element_context"]
    kinds = [segment["kind"] for segment in field["path"]]
    if kinds.count("field") == len(kinds):
        errors.append(f"{where}: element_context on a field outside any collection")
    total = None
    for metric in field["metrics"]:
        if metric["name"] == "element_count" and metric["status"] == "measured":
            total = metric["value"]
    for m_index, metric in enumerate(field["metrics"]):
        if "rows" in (metric.get("unit"), metric.get("denominator_unit")):
            errors.append(f"{where}.metrics[{m_index}]: element metrics never count rows")
        if metric.get("denominator_unit") in ("elements", "entries") and (
            metric["denominator_unit"] != context["unit"]
        ):
            errors.append(f"{where}.metrics[{m_index}]: denominator unit differs from the element")
        if (
            metric["name"] in ("null_count", "non_null_count")
            and metric["status"] == "measured"
            and isinstance(total, int)
            and isinstance(metric["value"], int)
            and metric["value"] > total
        ):
            errors.append(f"{where}.metrics[{m_index}]: exceeds element_count")
    return errors


def _ct_json_path_errors(catalog: Mapping[str, Any], where: str) -> list[str]:
    errors: list[str] = []
    documents = catalog["documents"]
    paths = catalog.get("paths")
    if paths is not None and len(paths) != catalog["paths_listed"]:
        errors.append(f"{where}: paths_listed does not match the listed paths")
    for index, item in enumerate(paths or []):
        if item["present_in"] > documents:
            errors.append(f"{where}.paths[{index}]: present in more documents than sampled")
        ratio = item.get("presence_ratio")
        if ratio is not None and not 0 <= ratio <= 1:
            errors.append(f"{where}.paths[{index}]: presence_ratio must be between 0 and 1")
    return errors


def validate_profile(profile: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return every problem found in ``profile`` (empty list when valid)."""
    problem = version_error(profile, PROFILE_KIND, "schema_version", SUPPORTED_PROFILE_VERSIONS)
    if problem:
        return [problem]
    errors = schema_errors(profile, schema)
    if errors:
        return errors
    return profile_invariant_errors(profile)


def validate_annotations(document: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return every problem found in an annotations document."""
    problem = version_error(
        document, ANNOTATIONS_KIND, "annotations_version", SUPPORTED_ANNOTATION_VERSIONS
    )
    if problem:
        return [problem]
    errors = schema_errors(document, schema)
    if errors:
        return errors
    for key, table in document.get("tables", {}).items():
        try:
            parse_table_identifier(key)
        except IdentifierError as exc:
            errors.append(f"$.tables[{key!r}]: {exc}")
        for column in table.get("columns", {}):
            try:
                parse_display_path(column)
            except IdentifierError as exc:
                errors.append(f"$.tables[{key!r}].columns[{column!r}]: {exc}")
    for index, item in enumerate(document.get("relationships", [])):
        for end in ("from", "to"):
            try:
                parse_table_identifier(item[end]["table"])
            except IdentifierError as exc:
                errors.append(f"$.relationships[{index}].{end}.table: {exc}")
        if len(item["from"]["columns"]) != len(item["to"]["columns"]):
            errors.append(
                f"$.relationships[{index}]: 'from' and 'to' must list the same number of columns"
            )
    return errors

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.render
# TableDossier 0.2.0 embedded runtime: module tabledossier.render
# Source: src/tabledossier/render.py (sha256:31bfca26a5cd3e269508a68bf8bc50415b7f1fd17430c4d18dcc7d211e2c4661)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Markdown and Mermaid documentation derived from a validated profile.

Part of the embedded runtime (standard library only): the notebook renders the
same documents that ``tabledossier render`` regenerates locally. Rendering
never recomputes metrics and never reads the source. Output is deterministic
(no rendering timestamp); documents state when the profile was *measured*.

All text coming from sources, configuration or annotations is treated as
content: it is escaped for Markdown/Mermaid and never interpreted.
"""

import re
from collections.abc import Iterable, Mapping
from typing import Any


SCOPE_LABELS = {
    "full_snapshot": "all rows at a pinned snapshot",
    "filtered_snapshot": "filtered rows at a pinned snapshot",
    "full_table": "all rows (snapshot not pinned)",
    "filtered_table": "filtered rows (snapshot not pinned)",
    "sample": "transient sample",
    "table_metadata": "table metadata",
}
UNKNOWN_DESCRIPTION = "Unknown — no description in the source or in annotations."
_R_MD_SPECIAL = re.compile(r"([\\`*_\[\]#|~!])")
_R_CONTROL = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]+")
_R_MERMAID_UNSAFE = re.compile(r"[\"`<>{}\[\]|;#%\\]")
MAX_ERD_ATTRIBUTES = 60
ERD_LEFT = {"zero_or_one": "|o", "exactly_one": "||", "zero_or_more": "}o", "one_or_more": "}|"}
ERD_RIGHT = {"zero_or_one": "o|", "exactly_one": "||", "zero_or_more": "o{", "one_or_more": "|{"}


# --------------------------------------------------------------------------- escaping


def md_text(value: Any) -> str:
    """Escape arbitrary text for inline Markdown (safe inside table cells)."""
    text = _R_CONTROL.sub(" ", "" if value is None else str(value))
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _R_MD_SPECIAL.sub(r"\\\1", text).strip()


def md_code(value: Any) -> str:
    """Render text as an inline code span that is safe inside table cells."""
    text = _R_CONTROL.sub(" ", str(value)).replace("|", "\\|")
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * (longest + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def mermaid_text(value: Any) -> str:
    """Return single-line text with characters unsafe for Mermaid labels replaced."""
    text = _R_CONTROL.sub(" ", str(value))
    return _R_MERMAID_UNSAFE.sub("'", text).strip()


def mermaid_name(value: str, used: dict[str, str]) -> str:
    """Return a stable Mermaid-safe identifier, unique within ``used``."""
    base = re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_") or "x"
    if not base[0].isalpha():
        base = "n_" + base
    name = base
    counter = 2
    while name in used and used[name] != value:
        name = f"{base}_{counter}"
        counter += 1
    used[name] = value
    return name


def _r_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(" --- " for _ in headers) + "|"]
    lines += ["| " + " | ".join(cell if cell else " " for cell in row) + " |" for row in rows]
    return "\n".join(lines)


# --------------------------------------------------------------------------- formatting


def format_count(value: Any) -> str:
    """Format an integer count with thousands separators."""
    return f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else str(value)


def format_ratio(value: Any) -> str:
    """Format a ratio in [0, 1] as a percentage."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return str(value)
    if 0 < value < 0.0001:
        return "<0.01%"
    if 0.9999 < value < 1:
        return ">99.99%"
    return f"{value:.2%}"


def format_value(metric: Mapping[str, Any]) -> str:
    """Format a metric value for humans, or explain why there is none."""
    if metric.get("status") != "measured":
        reason = metric.get("reason") or ""
        return f"_{metric.get('status', 'unknown').replace('_', ' ')}_" + (
            f": {md_text(reason)}" if reason else ""
        )
    value = metric.get("value")
    value_type = metric.get("value_type")
    if value_type == "ratio":
        return format_ratio(value)
    if value_type == "quantiles" and isinstance(value, list):
        parts = [
            f"p{round(item['probability'] * 100):g}={_r_scalar(item['value'])}" for item in value
        ]
        return md_text(", ".join(parts))
    if value_type == "integer":
        return format_count(value)
    return md_text(_r_scalar(value))


def _r_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".6g")
    return str(value)


def _r_observed(observed: Mapping[str, Any] | None) -> str:
    if not observed:
        return "—"
    value = observed.get("value")
    name = str(observed.get("metric", ""))
    if isinstance(value, list):
        return md_text(
            ", ".join(
                f"{label} {item}"
                for label, item in zip(("min", "max"), value, strict=False)
                if item is not None
            )
        )
    if name.endswith("ratio") and isinstance(value, (int, float)) and not isinstance(value, bool):
        return format_ratio(value)
    return format_count(value) if isinstance(value, int) else md_text(value)


def _r_accuracy(metric: Mapping[str, Any]) -> str:
    if metric.get("status") != "measured":
        return ""
    return f"{metric.get('accuracy')}, {metric.get('source')}"


# --------------------------------------------------------------------------- annotations


def table_annotation(annotations: Mapping[str, Any] | None, key: str) -> dict[str, Any]:
    """Return the annotation entry for a table (matched case-insensitively)."""
    if not annotations:
        return {}
    wanted = table_lookup_key(parse_table_identifier(key))
    for name, entry in annotations.get("tables", {}).items():
        if table_lookup_key(parse_table_identifier(name)) == wanted:
            return dict(entry)
    return {}


def column_annotation(table_note: Mapping[str, Any], path: str) -> dict[str, Any]:
    """Return the annotation entry for a column display path."""
    columns = table_note.get("columns", {})
    if path in columns:
        return dict(columns[path])
    folded = path.casefold()
    for name, entry in columns.items():
        if name.casefold() == folded:
            return dict(entry)
    return {}


def all_relationships(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    """Return profile relationships plus relationships provided in annotations."""
    extra = provided_relationships((annotations or {}).get("relationships", []), "annotation")
    return merge_relationships(profile.get("relationships", []), extra)


# --------------------------------------------------------------------------- shared pieces


def provenance_header(profile: Mapping[str, Any], title: str) -> str:
    """Return the title and provenance block used by every Markdown document."""
    run = profile["run"]
    return "\n".join(
        [
            f"# {title}",
            "",
            f"> Generated by TableDossier {md_text(profile['tool']['version'])} from profile run "
            f"{md_code(run['run_id'])}.  ",
            f"> **Measured at:** {md_text(run['started_at'])} (UTC) · **Analysis level:** "
            f"`{run['analysis_level']}` · **Run status:** `{run['status']}`.  ",
            "> Re-rendering does not refresh measurements: every value below comes from that "
            "profile.",
            "",
        ]
    )


def _r_row_count(table: Mapping[str, Any]) -> str:
    metric = find_metric(table.get("table_metrics", []), "row_count")
    if metric is None:
        return "_not measured_"
    text = format_value(metric)
    if metric.get("status") == "measured" and metric.get("source") != "aggregate":
        text += f" ({md_text(metric.get('source'))}, {md_text(metric.get('accuracy'))})"
    return text


def _r_scope_text(table: Mapping[str, Any]) -> str:
    scope = table["scope"]
    consistency = table.get("consistency") or {}
    parts = [SCOPE_LABELS.get(scope["scope_label"], scope["scope_label"])]
    if consistency.get("mode") == "pinned_delta_version":
        parts.append(f"Delta version {consistency.get('delta_version')}")
    if scope.get("filters"):
        parts.append(f"{len(scope['filters'])} filter(s)")
    if scope.get("selected_columns") is not None:
        parts.append(f"{len(scope['selected_columns'])} selected column(s)")
    return md_text("; ".join(str(p) for p in parts))


def _r_filters(filters: Iterable[Mapping[str, Any]]) -> str:
    items = []
    for item in filters:
        column = item["column"] if isinstance(item["column"], str) else ".".join(item["column"])
        text = f"{column} {item['operator']}"
        if "value" in item:
            text += f" {item['value']!r}"
        items.append(md_code(text))
    return ", ".join(items) if items else "none"


def _r_anchor(table: Mapping[str, Any]) -> str:
    return f'<a id="{table["table_id"]}"></a>'


# --------------------------------------------------------------------------- overview


def render_overview(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``overview.md``: run, environment, tables, sampling and errors."""
    run = profile["run"]
    env = run["environment"]
    out = [provenance_header(profile, "Profile overview")]
    runtime = ", ".join(
        md_text(part)
        for part in (
            env.get("execution_context"),
            f"Spark {env['spark_version']}" if env.get("spark_version") else None,
            f"Databricks Runtime {env['databricks_runtime_version']}"
            if env.get("databricks_runtime_version")
            else None,
            f"Python {env.get('python_version')}",
        )
        if part
    )
    rows = [
        ["Run ID", md_code(run["run_id"])],
        ["Status", f"`{run['status']}`"],
        ["Analysis level", f"`{run['analysis_level']}`"],
        [
            "Started / finished (UTC)",
            f"{md_text(run['started_at'])} / {md_text(run['finished_at'])}",
        ],
        ["Duration", f"{run['duration_ms'] / 1000:.1f} s"],
        ["Execution environment", runtime],
        ["Session time zone", md_text(env.get("session_timezone") or "unknown")],
        ["Temporal reference instant", md_text(run["reference_time"])],
        ["Configuration fingerprint", md_code(run["config_fingerprint"])],
        [
            "Generated notebook",
            md_code(run["generation"]["generation_id"])
            if run["generation"].get("generation_id")
            else "not recorded",
        ],
        [
            "Purpose",
            md_text(run["purpose"]) + " _(configuration)_"
            if run.get("purpose")
            else "_not provided_",
        ],
    ]
    out += ["## Run", "", _r_table(["Item", "Value"], rows), ""]

    out += ["## Tables", ""]
    table_rows = []
    for table in profile["tables"]:
        summary = table.get("summary", {})
        checks = table.get("quality_checks", [])
        counts = {
            status: sum(1 for c in checks if c["status"] == status)
            for status in ("pass", "fail", "not_evaluated", "error")
        }
        consistency = (table.get("consistency") or {}).get("mode", "n/a")
        table_rows.append(
            [
                f"[{md_text(table['table_key'])}](data_dictionary.md#{table['table_id']})",
                f"`{table['status']}`",
                _r_row_count(table),
                _r_scope_text(table),
                md_text(consistency),
                format_count(summary.get("fields_profiled", 0)),
                format_count(summary.get("fields_omitted", 0)),
                format_count(len(table.get("findings", []))),
                "/".join(str(counts[s]) for s in ("pass", "fail", "not_evaluated", "error")),
            ]
        )
    if table_rows:
        out.append(
            _r_table(
                [
                    "Table",
                    "Status",
                    "Rows in scope",
                    "Scope",
                    "Consistency",
                    "Fields profiled",
                    "Fields omitted",
                    "Findings",
                    "Checks pass/fail/not evaluated/error",
                ],
                table_rows,
            )
        )
    else:
        out.append("No tables were part of this run.")
    out += [
        "",
        "Row counts are rows in the analysed scope (after filters). They are not counts of "
        "business entities: no deduplication is applied, including for historized tables.",
        "",
    ]

    exposure = profile["value_exposure"]
    out += [
        "## Sampling and value exposure",
        "",
        f"- Raw values persisted: **{'yes' if exposure['raw_values_persisted'] else 'no'}**.",
        f"- Aggregate extremes (min/max/mean/quantiles): `{exposure['aggregate_extremes']}`.",
        f"- JSON key names from samples: `{exposure['json_key_names']}`.",
    ]
    out += [f"- {md_text(note)}" for note in exposure.get("notes", [])]
    for table in profile["tables"]:
        sample = table.get("sample")
        if not sample:
            continue
        if sample["enabled"]:
            desc = (
                f"method `{sample['method']}`, {format_count(sample['rows_collected'])} row(s) "
                "collected "
                f"(limit {format_count(sample['max_rows'])}), stopped by "
                f"`{sample['stopped_reason']}`, "
                f"{format_count(sample['values_truncated'])} value(s) truncated"
            )
        else:
            desc = "not used — " + "; ".join(md_text(n) for n in sample.get("notes", [])[:1])
        out.append(f"- {md_text(table['table_key'])}: {desc}.")
    out.append("")

    capabilities = run.get("capabilities", {})
    if capabilities:
        out += ["## Engine capabilities detected", ""]
        out.append(
            _r_table(
                ["Capability", "Available", "Detail"],
                [
                    [md_code(name), "yes" if item["available"] else "no", md_text(item["detail"])]
                    for name, item in sorted(capabilities.items())
                ],
            )
        )
        out.append("")

    errors = [(table, error) for table in profile["tables"] for error in table.get("errors", [])]
    out += ["## Errors", ""]
    if errors:
        out.append(
            _r_table(
                ["Table", "Stage", "Error", "Message (sanitized)"],
                [
                    [
                        md_text(table["table_key"]),
                        md_text(error["stage"]),
                        md_code(error["condition"] or error["error_class"]),
                        md_text(error["message"]),
                    ]
                    for table, error in errors
                ],
            )
        )
    else:
        out.append("No errors were recorded.")
    out += [
        "",
        "## Files",
        "",
        "- `profile.json` — canonical profile (source of every document).",
        "- `data_dictionary.md` — tables, fields, types, descriptions and measurements.",
        "- `quality_report.md` — observed quality (DQR): checks, completeness, alerts, limits.",
        "- `relationships.md` and `erd.mmd` — known relationships and entity diagram.",
        "- `suggested_rules.json` — rule proposals (not applied).",
        "",
    ]
    return "\n".join(out)


# --------------------------------------------------------------------------- dictionary


def _r_description(node: Mapping[str, Any], note: Mapping[str, Any]) -> str:
    parts = []
    if note.get("description"):
        parts.append(f"{md_text(note['description'])} _(annotation)_")
    if node.get("comment"):
        parts.append(f"{md_text(node['comment'])} _(source comment)_")
    return "<br>".join(parts) if parts else f"_{UNKNOWN_DESCRIPTION}_"


def _r_unit_one(unit: str) -> str:
    return {"elements": "element", "entries": "entry"}.get(unit, unit)


def _r_nulls(profile_field: Mapping[str, Any] | None) -> str:
    if not profile_field or not profile_field.get("profiled"):
        return "—"
    metrics = profile_field["metrics"]
    count = find_metric(metrics, "null_count")
    ratio_metric = find_metric(metrics, "null_ratio")
    if count is None:
        return "—"
    if count.get("status") != "measured":
        return format_value(count)
    text = format_count(count["value"])
    context = profile_field.get("element_context")
    if context:
        # Element fields: the denominator is the collection's elements or entries, not rows.
        total = count.get("denominator")
        share = (
            f"{format_ratio(ratio_metric['value'])} of "
            if ratio_metric is not None and ratio_metric.get("status") == "measured"
            else ""
        )
        if isinstance(total, int):
            text += f" ({share}{format_count(total)} {context['unit']})"
        return text
    if ratio_metric is not None and ratio_metric.get("status") == "measured":
        text += f" ({format_ratio(ratio_metric['value'])})"
    return text


def _r_distinct(profile_field: Mapping[str, Any] | None) -> str:
    if not profile_field or not profile_field.get("profiled"):
        return "—"
    exact = find_metric(profile_field["metrics"], "distinct_count")
    if exact is not None:
        if exact.get("status") != "measured":
            return format_value(exact)
        label = " (sample)" if exact.get("scope") == "sample" else ""
        return format_count(exact["value"]) + label
    metric = find_metric(profile_field["metrics"], "approx_distinct_count")
    if metric is None:
        return "—"
    if metric.get("status") != "measured":
        return format_value(metric)
    return "≈ " + format_count(metric["value"])


def _r_format(profile_field: Mapping[str, Any] | None) -> str:
    semantics = (profile_field or {}).get("semantics") or {}
    fmt = semantics.get("observed_format")
    if not fmt:
        return "—"
    if fmt["status"] == "detected":
        top = fmt["candidates"][0] if fmt["candidates"] else {}
        ratio = (
            f", {format_ratio(top['match_ratio'])} of {fmt['eligible_observations']}" if top else ""
        )
        return f"{md_code(fmt['format'])} (sample{ratio})"
    return f"_{fmt['status'].replace('_', ' ')}_"


def _r_roles(profile_field: Mapping[str, Any] | None) -> str:
    semantics = (profile_field or {}).get("semantics") or {}
    roles = [role["role"].replace("_", " ") for role in semantics.get("candidate_roles", [])]
    return md_text(", ".join(roles)) if roles else "—"


def _r_node_notes(
    node: Mapping[str, Any], field: Mapping[str, Any] | None, findings: list[str]
) -> str:
    notes = []
    context = (field or {}).get("element_context")
    if context:
        notes.append(f"per {_r_unit_one(context['unit'])} of {context['collection_display_path']}")
    if field is not None and not field.get("profiled") and field.get("omission_reason"):
        notes.append(f"not profiled ({field['omission_reason']})")
    if node.get("children_omitted"):
        notes.append(
            f"{node['children_omitted']} child field(s) omitted ({node['children_omitted_reason']})"
        )
    notes.extend(findings)
    return md_text("; ".join(notes)) if notes else ""


def render_data_dictionary(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``data_dictionary.md`` for every table in the profile."""
    out = [provenance_header(profile, "Data dictionary")]
    out += [
        "Descriptions come from human annotations or source comments, as labelled. When neither "
        "exists the description is unknown: TableDossier does not invent business definitions. "
        "*Declared nullable* is the source schema declaration; *Nulls* are observed in the "
        "analysed scope.",
        "",
    ]
    for table in profile["tables"]:
        note = table_annotation(annotations, table["table_key"])
        source = table.get("source") or {}
        out += [f"## {md_text(table['table_key'])}", "", _r_anchor(table), ""]
        description = []
        if note.get("description"):
            description.append(f"{md_text(note['description'])} _(annotation)_")
        if source.get("comment"):
            description.append(f"{md_text(source['comment'])} _(source comment)_")
        purpose = []
        if table.get("purpose"):
            purpose.append(f"{md_text(table['purpose'])} _(configuration)_")
        if note.get("purpose"):
            purpose.append(f"{md_text(note['purpose'])} _(annotation)_")
        consistency = table.get("consistency") or {}
        rows = [
            ["Description", "<br>".join(description) or f"_{UNKNOWN_DESCRIPTION}_"],
            ["Purpose", "<br>".join(purpose) or "_not provided_"],
            [
                "Owner",
                f"{md_text(note['owner'])} _(annotation)_"
                if note.get("owner")
                else "_not provided_",
            ],
            ["Tags", md_text(", ".join(note.get("tags", []))) or "—"],
            [
                "Source",
                md_text(
                    ", ".join(
                        str(v)
                        for v in (
                            source.get("source_type"),
                            source.get("table_type"),
                            source.get("provider"),
                        )
                        if v
                    )
                )
                or "—",
            ],
            ["Status", f"`{table['status']}`"],
            ["Rows in scope", _r_row_count(table)],
            ["Scope", _r_scope_text(table)],
            ["Filters", _r_filters(table["scope"].get("filters", []))],
            ["Consistency", md_text(consistency.get("guarantee", "not determined"))],
        ]
        out += [_r_table(["Property", "Value"], rows), ""]
        if table.get("errors"):
            out.append(
                "**Errors:** "
                + "; ".join(md_text(f"{e['stage']}: {e['message']}") for e in table["errors"])
            )
            out.append("")
        schema = table.get("schema")
        if not schema:
            out += ["_Schema not available for this table._", ""]
            continue
        fields = {item["field_id"]: item for item in table.get("field_profiles", [])}
        finding_codes: dict[str, list[str]] = {}
        for finding in table.get("findings", []):
            finding_codes.setdefault(finding["field_id"], []).append(
                finding["code"].replace("_", " ")
            )
        out += ["### Fields", ""]
        rows = []
        for node in iter_nodes(schema["fields"]):
            field = fields.get(node["field_id"])
            column_note = column_annotation(note, node["display_path"])
            nullable = {True: "yes", False: "no", None: "—"}[node.get("nullable")]
            rows.append(
                [
                    md_code(node["display_path"]),
                    md_code(node["type"]["physical_type"]),
                    nullable,
                    _r_description(node, column_note),
                    _r_nulls(field),
                    _r_distinct(field),
                    _r_format(field),
                    _r_roles(field),
                    _r_node_notes(node, field, finding_codes.get(node["field_id"], [])),
                ]
            )
        out.append(
            _r_table(
                [
                    "Field",
                    "Type",
                    "Declared nullable",
                    "Description",
                    "Nulls",
                    "Distinct",
                    "Observed format",
                    "Candidate roles",
                    "Notes",
                ],
                rows,
            )
        )
        out.append("")
        if schema.get("top_level_omitted"):
            out += [
                f"_{schema['top_level_omitted']} top-level column(s) beyond the field limit are "
                "not listed._",
                "",
            ]
        detailed = [
            field
            for field in table.get("field_profiles", [])
            if field.get("profiled") and field["metrics"]
        ]
        if detailed:
            out += ["### Field measurements", ""]
            for field in detailed:
                out += [f"#### {md_code(field['display_path'])}", ""]
                context = field.get("element_context")
                if context:
                    out += [
                        f"Measured per {md_text(_r_unit_one(context['unit']))} of "
                        f"{md_code(context['collection_display_path'])}: denominators are "
                        f"{md_text(context['unit'])} of the collection in scope, not rows.",
                        "",
                    ]
                out.append(
                    _r_table(
                        ["Metric", "Value", "Accuracy, source", "Scope", "Denominator"],
                        [
                            [
                                md_code(metric["name"]),
                                format_value(metric),
                                md_text(_r_accuracy(metric)),
                                md_text(SCOPE_LABELS.get(metric["scope"], metric["scope"])),
                                (
                                    f"{format_count(metric['denominator'])} "
                                    f"{md_text(metric.get('denominator_unit') or '')}"
                                    if "denominator" in metric
                                    else ""
                                ),
                            ]
                            for metric in field["metrics"]
                        ],
                    )
                )
                json_profile = field.get("json_profile")
                if json_profile:
                    counts = json_profile["counts"]
                    out.append("")
                    out.append(
                        f"JSON shape (sample of {json_profile['eligible_observations']} value(s); "
                        f"{json_profile['excluded']['truncated']} truncated excluded): "
                        + ", ".join(f"{md_text(k)} {v}" for k, v in counts.items())
                        + "."
                    )
                    if json_profile.get("keys"):
                        keys = ", ".join(
                            f"{md_code(item['key'])} ({format_ratio(item['presence_ratio'])})"
                            for item in json_profile["keys"][:20]
                        )
                        out.append(f"Top-level keys: {keys}.")
                    elif json_profile.get("keys_omitted_reason"):
                        out.append(
                            f"Key names not listed: {md_text(json_profile['keys_omitted_reason'])}."
                        )
                if field.get("json_paths"):
                    out += ["", *_r_json_paths(field["json_paths"])]
                concentration = field.get("concentration")
                if concentration and concentration.get("observations"):
                    out.append("")
                    out.append(
                        "Sample concentration (labels not shown): "
                        f"{concentration['sample_distinct_count']} "
                        f"distinct in {concentration['observations']} sampled value(s); most "
                        "frequent value "
                        f"share {format_ratio(concentration['top_1_share'])}."
                    )
                examples = field.get("examples")
                if examples and examples.get("values"):
                    out.append("")
                    out.append(
                        "Allow-listed sample values: "
                        + ", ".join(
                            f"{md_code(item['value'])} ×{item['sample_count']}"
                            for item in examples["values"]
                        )
                    )
                out.append("")
    return "\n".join(out)


def _r_json_full_scope(item: Mapping[str, Any]) -> str:
    full = item.get("full_scope")
    if not full:
        return "—"
    if full["status"] == "not_computed" or not full.get("metrics"):
        return "_not computed_" + (f": {md_text(full['reason'])}" if full.get("reason") else "")
    parts = []
    for metric in full["metrics"]:
        label = {
            "path_present_count": "present",
            "path_json_null_count": "JSON null",
            "path_type_match_count": f"{(metric.get('details') or {}).get('expected_type')}",
            "path_non_null_count": "non-null",
        }.get(metric["name"], metric["name"])
        if metric["status"] != "measured":
            parts.append(f"{md_text(label)} {format_value(metric)}")
            continue
        denominator = metric.get("denominator")
        share = (
            f" ({format_ratio(metric['value'] / denominator)})"
            if isinstance(denominator, int) and denominator
            else ""
        )
        parts.append(f"{md_text(label)} {format_count(metric['value'])}{share}")
    return "; ".join(parts)


def _r_json_paths(catalog: Mapping[str, Any]) -> list[str]:
    """Render the JSON path catalogue of a string field (sample-based, not a schema)."""
    lines = [
        f"JSON paths (transient sample: {format_count(catalog['documents'])} JSON "
        f"document(s) of {format_count(catalog['eligible_observations'])} sampled value(s); "
        "not a complete or guaranteed schema).",
        "",
    ]
    full = catalog.get("full_scope") or {}
    if full.get("method"):
        documents = full.get("documents") or {}
        lines.append(
            f"Full-scope validation with `{full['method']}`"
            + (
                f" over {format_count(documents['value'])} JSON document(s) in scope"
                if documents.get("status") == "measured"
                else ""
            )
            + ". "
            + " ".join(md_text(note) for note in full.get("limitations", []))
        )
    elif full.get("status") in ("unsupported", "not_computed"):
        lines.append(
            f"Full-scope validation: _{full['status'].replace('_', ' ')}_ — "
            f"{md_text(full.get('reason') or '')}."
        )
    paths = catalog.get("paths")
    if paths is None:
        lines.append(
            f"Path names not listed: {md_text(catalog.get('paths_omitted_reason') or '')} "
            f"({format_count(catalog['paths_observed'])} path(s) observed)."
        )
        return lines
    if not paths:
        lines.append(md_text(catalog.get("paths_omitted_reason") or "No paths observed."))
        return lines
    lines += [
        "",
        _r_table(
            [
                "Path",
                "Present in (sample)",
                "Types (sample)",
                "Heterogeneous",
                "Full scope (documents)",
            ],
            [
                [
                    md_code(item["path"]) + (" _(keys collapsed)_" if item.get("map_like") else ""),
                    f"{format_count(item['present_in'])}"
                    + (
                        f" ({format_ratio(item['presence_ratio'])})"
                        if item.get("presence_ratio") is not None
                        else ""
                    ),
                    md_text(", ".join(f"{name} {count}" for name, count in item["types"].items())),
                    "yes" if item["heterogeneous"] else "no",
                    _r_json_full_scope(item),
                ]
                for item in paths
            ],
        ),
    ]
    if catalog.get("paths_omitted"):
        lines.append("")
        lines.append(
            f"{format_count(catalog['paths_omitted'])} further path(s) not listed: "
            f"{md_text(catalog.get('paths_omitted_reason') or '')}."
        )
    return lines


# --------------------------------------------------------------------------- quality report


def render_quality_report(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``quality_report.md`` (observed Data Quality Report, not a certification)."""
    out = [provenance_header(profile, "Data Quality Report (observed)")]
    out += [
        "This report describes quality **observed** in the analysed scope. It is not a "
        "certification "
        "and computes no global quality score. It keeps four things apart: configured checks that "
        "were "
        "executed, descriptive measurements, heuristic alerts and proposed rules.",
        "",
        "## Summary",
        "",
    ]
    rows = []
    for table in profile["tables"]:
        checks = table.get("quality_checks", [])
        findings = table.get("findings", [])
        rows.append(
            [
                md_text(table["table_key"]),
                f"`{table['status']}`",
                _r_row_count(table),
                *[
                    str(sum(1 for c in checks if c["status"] == s))
                    for s in ("pass", "fail", "not_evaluated", "error")
                ],
                str(sum(1 for f in findings if f["severity"] == "warning")),
                str(sum(1 for f in findings if f["severity"] == "info")),
            ]
        )
    out += [
        _r_table(
            [
                "Table",
                "Status",
                "Rows in scope",
                "Checks pass",
                "fail",
                "not evaluated",
                "error",
                "Warnings",
                "Info",
            ],
            rows,
        )
        if rows
        else "No tables.",
        "",
    ]

    out += ["## 1. Configured checks (executed)", ""]
    check_rows = []
    for table in profile["tables"]:
        for check in table.get("quality_checks", []):
            observed = check.get("observed") or {}
            check_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(check["check_id"]),
                    md_code(check["target"]["column"]) if check["target"]["column"] else "table",
                    md_code(check["type"]),
                    md_text(", ".join(f"{k}={v}" for k, v in check["parameters"].items())),
                    _r_observed(observed),
                    f"**{check['status']}**",
                    md_text(check["message"]),
                ]
            )
    if check_rows:
        out.append(
            _r_table(
                ["Table", "Check", "Target", "Type", "Parameters", "Observed", "Status", "Message"],
                check_rows,
            )
        )
    else:
        out.append(
            "No checks were configured. Add `checks` under `table_options` to evaluate explicit "
            "rules."
        )
    out += [
        "",
        "`not_evaluated` means the metric needed by the check was not measured (for example an "
        "empty "
        "scope or the metadata level); it is neither a pass nor a failure.",
        "",
        "## 2. Completeness (descriptive)",
        "",
    ]
    completeness = []
    element_completeness = []
    for table in profile["tables"]:
        for field in table.get("field_profiles", []):
            if not field.get("profiled"):
                continue
            context = field.get("element_context")
            if context:
                nulls = find_metric(field["metrics"], "null_count")
                if nulls is not None:
                    element_completeness.append(
                        [
                            md_text(table["table_key"]),
                            md_code(field["display_path"]),
                            _r_nulls(field),
                            format_value(parent)
                            if (
                                parent := find_metric(field["metrics"], "null_count_parent_present")
                            )
                            else "—",
                            md_text(context["unit"]),
                            md_text(SCOPE_LABELS.get(nulls["scope"], nulls["scope"])),
                        ]
                    )
                continue
            nulls = find_metric(field["metrics"], "null_count")
            ratio_metric = find_metric(field["metrics"], "null_ratio")
            parent = find_metric(field["metrics"], "null_count_parent_present")
            if nulls is None:
                continue
            completeness.append(
                [
                    md_text(table["table_key"]),
                    md_code(field["display_path"]),
                    format_value(nulls),
                    format_value(ratio_metric) if ratio_metric else "—",
                    format_value(parent) if parent else "—",
                    md_text(SCOPE_LABELS.get(nulls["scope"], nulls["scope"])),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Field", "Null count", "Null ratio", "Nulls while parent present", "Scope"],
            completeness,
        )
        if completeness
        else "No completeness measurements (metadata level or no profiled fields)."
    )
    out += [
        "",
        "For nested fields, *Null count* includes rows where a parent struct is null; *Nulls while "
        "parent present* counts only rows whose parent exists.",
        "",
    ]
    if element_completeness:
        out += [
            "Array elements and map entries (deep level) count nulls among the elements or entries "
            "of the collection, never among rows:",
            "",
            _r_table(
                [
                    "Table",
                    "Element field",
                    "Nulls (of elements or entries)",
                    "Nulls while parent present",
                    "Unit",
                    "Scope",
                ],
                element_completeness,
            ),
            "",
        ]
    out += ["## 3. Heuristic alerts (not failures)", ""]
    alert_rows = []
    for table in profile["tables"]:
        for finding in table.get("findings", []):
            alert_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(finding["display_path"]),
                    f"`{finding['severity']}`",
                    md_text(finding["title"]),
                    md_text(finding["message"]),
                    md_text(finding["severity_reason"]),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Field", "Severity", "Alert", "Observation", "Why this severity"], alert_rows
        )
        if alert_rows
        else "No heuristic alerts."
    )
    out += [
        "",
        "Alerts are exploratory signals with configurable thresholds. High nullity, constancy or "
        "heavy "
        "tails can be legitimate; alerts never fail a pipeline.",
        "",
        "## 4. Proposed rules (not applied)",
        "",
    ]
    rule_rows = []
    for table in profile["tables"]:
        for rule in table.get("suggested_rules", []):
            rule_rows.append(
                [
                    md_text(rule["table"]),
                    md_code(rule["column"]) if rule["column"] else "table",
                    md_code(rule["rule_type"]),
                    md_text(rule["rationale"]),
                ]
            )
    out.append(
        _r_table(["Table", "Field", "Rule", "Rationale"], rule_rows)
        if rule_rows
        else "No rules proposed."
    )
    out += [
        "",
        "Proposals require review by a data owner; see `suggested_rules.json`.",
        "",
        "## 5. Dimensions this profile does not establish",
        "",
        "- **Validity** is only evaluated through configured checks. Formats observed on samples "
        "are hypotheses.",
        _r_uniqueness_dimension(profile),
        _r_referential_dimension(profile),
        "- **Timeliness** needs a time column and an agreed SLA; `after_reference_count` is "
        "descriptive only.",
        "- **Business accuracy** cannot be inferred from distributions.",
        "",
    ]
    deep_tables = [table for table in profile["tables"] if table.get("deep")]
    deep_level = profile["run"].get("analysis_level") == "deep"
    if deep_level:
        out += _r_deep_coverage(deep_tables)
        out += _r_uniqueness(profile)
        out += _r_referential(profile)
    out += ["## 9. Limitations" if deep_level else "## 6. Limitations", ""]
    for table in profile["tables"]:
        lines = []
        consistency = table.get("consistency") or {}
        if consistency:
            lines.append(f"Consistency: {consistency.get('guarantee')}")
        sample = table.get("sample")
        if sample and sample.get("enabled"):
            lines.append(
                f"Sample: {sample['method']} "
                f"({'potentially biased' if sample.get('biased') else 'random'}), "
                f"{sample['rows_collected']} row(s), {sample['values_truncated']} truncated "
                "value(s)"
            )
        omissions = table.get("omissions", [])
        if omissions:
            lines.append(
                f"{len(omissions)} omission record(s): "
                + ", ".join(sorted({o["reason"] for o in omissions}))
            )
        for item in table.get("unsupported", []):
            lines.append(f"Unsupported: {item['capability']} — {item['detail']}")
        for error in table.get("errors", []):
            lines.append(f"Error during {error['stage']}: {error['message']}")
        lines.extend(table.get("notes", []))
        out.append(f"**{md_text(table['table_key'])}**")
        out.append("")
        out += [f"- {md_text(line)}" for line in lines] or ["- none recorded"]
        out.append("")
    return "\n".join(out)


def _r_checked(profile: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Relationships whose validation detail comes from a check of the data."""
    return [
        rel
        for rel in profile.get("relationships", [])
        if (rel.get("validation_detail") or {}).get("operation_id")
    ]


def _r_referential_dimension(profile: Mapping[str, Any]) -> str:
    checked = _r_checked(profile)
    if checked:
        return (
            f"- **Referential integrity** is established only for the {len(checked)} "
            "relationship(s) checked in section 8; other relationships were not validated."
        )
    return (
        "- **Referential integrity** is not verified: declared or provided relationships were not "
        "validated."
    )


def _r_validation_text(rel: Mapping[str, Any]) -> str:
    """Short validation status with its main evidence (orphans) or reason."""
    detail = rel.get("validation_detail") or {}
    status = rel.get("validation", "not_validated")
    orphans = find_metric(detail.get("metrics", []), "orphan_rows")
    ratio_metric = find_metric(detail.get("metrics", []), "orphan_ratio")
    if status == "violated" and orphans:
        text = f"**violated**: {format_value(orphans)} orphan row(s)"
        if ratio_metric and ratio_metric.get("status") == "measured":
            text += f" ({format_value(ratio_metric)})"
        return text
    if status == "validated":
        complete = find_metric(detail.get("metrics", []), "source_rows_with_complete_key")
        return f"validated: 0 orphans among {format_value(complete or {})} row(s)"
    reason = detail.get("reason")
    return "not validated" + (f" ({md_text(reason)})" if reason else "")


def _r_referential(profile: Mapping[str, Any]) -> list[str]:
    """Render section 8 of the DQR: referential integrity of checked relationships."""
    out = ["## 8. Referential integrity", ""]
    checked = _r_checked(profile)
    if not checked:
        return [
            *out,
            "Not established: no relationship was checked against the data in this run "
            "(`deep.referential`). See `relationships.md` for the reason of each relationship.",
            "",
        ]
    out += [
        "Orphans are source rows with a complete key that no target row has. The source keeps its "
        "analysed scope; the target is read in full at its recorded version. Only counts are "
        "recorded: orphan values are never collected.",
        "",
    ]
    rows = []
    for rel in checked:
        detail = rel["validation_detail"]
        metrics = {m["name"]: m for m in detail["metrics"]}

        def cell(name: str, metrics: Mapping[str, Any] = metrics) -> str:
            return format_value(metrics[name]) if name in metrics else "—"

        unique = detail.get("target_key_unique")
        rows.append(
            [
                md_code(rel["name"]),
                f"{md_text(rel['from']['table'])} ({md_text(', '.join(rel['from']['columns']))})",
                f"{md_text(rel['to']['table'])} ({md_text(', '.join(rel['to']['columns']))})",
                md_text(detail["mode"].replace("_", " ")),
                _r_status(rel["validation"]),
                cell("source_rows_with_complete_key"),
                cell("source_rows_with_null_key"),
                cell("orphan_rows"),
                cell("orphan_ratio"),
                "unknown" if unique is None else ("yes" if unique else "**no**"),
                _r_versions(detail),
            ]
        )
    out += [
        _r_table(
            [
                "Relationship",
                "From",
                "To",
                "Mode",
                "Validation",
                "Source rows with a complete key",
                "Source rows with NULL in the key",
                "Orphan rows",
                "Orphan ratio",
                "Target key unique",
                "Versions read",
            ],
            rows,
        ),
        "",
    ]
    notes = sorted(
        {note for rel in checked for note in rel["validation_detail"]["limitations"]}
        | {
            f"{rel['name']}: {rel['validation_detail']['reason']}"
            for rel in checked
            if rel["validation_detail"].get("reason")
        }
    )
    out += [f"- {md_text(note)}" for note in notes]
    if notes:
        out.append("")
    return out


def _r_status(status: str) -> str:
    return f"**{status}**" if status == "violated" else status.replace("_", " ")


def _r_versions(detail: Mapping[str, Any]) -> str:
    parts = []
    for side in ("from", "to"):
        info = detail.get(side) or {}
        version = info.get("delta_version")
        label = f"v{version}" if version is not None else str(info.get("consistency_mode"))
        parts.append(f"{side} {label}")
    return md_text(", ".join(parts))


def _r_uniqueness_dimension(profile: Mapping[str, Any]) -> str:
    if any(measured_keys(table) for table in profile["tables"]):
        return (
            "- **Uniqueness** is established only for the keys of section 7, exactly and over "
            "their analysed scope; other columns only have approximate (HyperLogLog-based) "
            "distinct counts."
        )
    return (
        "- **Uniqueness** is only approximated (HyperLogLog-based distinct counts); no exact "
        "uniqueness check ran."
    )


_R_OUTCOMES = {
    "unique": "unique",
    "unique_non_null": "unique among complete keys (some keys have NULL)",
    "duplicates": "**duplicates**",
    "empty": "no complete key in scope",
}


def _r_uniqueness(profile: Mapping[str, Any]) -> list[str]:
    """Render section 7 of the DQR: exact uniqueness of keys, with its evidence."""
    out = ["## 7. Uniqueness (exact)", ""]
    records = [(table, table.get("uniqueness")) for table in profile["tables"]]
    keys = [(table, key) for table, record in records if record for key in record["keys"]]
    if not keys:
        return [
            *out,
            "Not established: no key was checked for exact uniqueness in this run (configure "
            "`deep.uniqueness`: explicit keys, declared keys or identifier candidates).",
            "",
        ]
    semantics = next(record["null_semantics"] for _, record in records if record)
    out += [
        "Each key was checked with an exact grouped aggregation over the analysed scope; several "
        "keys of a table share one pass. Only counts are recorded: duplicated values are never "
        "collected. " + semantics,
        "",
    ]
    rows = []
    for table, key in keys:
        if key["status"] != "measured":
            continue
        values = {m["name"]: m for m in key["metrics"]}

        def cell(name: str, values: Mapping[str, Any] = values) -> str:
            return format_value(values[name]) if name in values else "—"

        rows.append(
            [
                md_text(table["table_key"]),
                md_code(", ".join(key["columns"])),
                md_text(", ".join(origin.replace("_", " ") for origin in key["origins"])),
                _R_OUTCOMES.get(key["outcome"], md_text(key["outcome"])),
                cell("rows_in_scope"),
                cell("rows_with_null_key"),
                cell("distinct_keys"),
                cell("duplicate_key_groups"),
                cell("rows_in_duplicate_groups"),
                cell("max_rows_per_key"),
                md_text(SCOPE_LABELS.get(key["scope"], key["scope"])),
            ]
        )
    if rows:
        out += [
            _r_table(
                [
                    "Table",
                    "Key",
                    "Origin",
                    "Outcome",
                    "Rows in scope",
                    "Rows with NULL in the key",
                    "Distinct keys",
                    "Duplicate groups",
                    "Rows in duplicate groups",
                    "Most rows per key",
                    "Scope",
                ],
                rows,
            ),
            "",
        ]
    skipped = [(table, key) for table, key in keys if key["status"] != "measured"]
    if skipped:
        out += ["**Keys not measured**", ""]
        out.append(
            _r_table(
                ["Table", "Key", "Origin", "Status", "Reason"],
                [
                    [
                        md_text(table["table_key"]),
                        md_code(", ".join(key["columns"])) if key["columns"] else "—",
                        md_text(", ".join(origin.replace("_", " ") for origin in key["origins"])),
                        md_code(key["status"]),
                        md_text(key["reason"] or ""),
                    ]
                    for table, key in skipped
                ],
            )
        )
        out.append("")
    notes = [
        md_text(note) for note in sorted({note for _, key in keys for note in key["limitations"]})
    ]
    notes += [
        f"{md_text(table['table_key'])}: {md_text(note)}"
        for table, record in records
        if record
        for note in record["notes"]
    ]
    out += [f"- {note}" for note in notes]
    if notes:
        out.append("")
    return out


def _r_deep_coverage(tables: list[Mapping[str, Any]]) -> list[str]:
    """Render what the deep level covered and what its budgets limited."""
    out = [
        "## 6. Deep analysis: coverage and budget",
        "",
        "Element metrics are exact over the analysed scope (higher-order functions inside the "
        "shared passes). Element distinct counts come from one explode pass, usually over a "
        "bounded sample. JSON paths are catalogued from the transient sample and are not a "
        "complete schema.",
        "",
    ]
    if not tables:
        return [*out, "No table reached the deep analysis (see errors).", ""]
    rows = []
    for table in tables:
        deep = table["deep"]
        passes = deep["extra_passes"]
        distinct = deep.get("element_distinct") or {}
        distinct_text = "—"
        if distinct:
            distinct_text = md_text(distinct["status"].replace("_", " "))
            if distinct.get("elements_examined") is not None:
                distinct_text += (
                    f", {format_count(distinct['elements_examined'])} element(s) examined"
                    + (f" ({distinct['method']} sample)" if distinct["mode"] == "sample" else "")
                )
        rows.append(
            [
                md_text(table["table_key"]),
                md_text(deep["targets"].replace("_", " ")),
                format_count(sum(c["element_fields_profiled"] for c in deep["collections"]))
                + f" in {len(deep['collections'])} collection(s)",
                format_count(sum(j["paths_listed"] for j in deep["json_fields"]))
                + f" in {len(deep['json_fields'])} field(s)",
                format_count(sum(j["paths_validated"] for j in deep["json_fields"])),
                distinct_text,
                f"{passes['planned']} of {passes['budget']}",
                format_count(deep["expressions"]["omitted"]),
            ]
        )
    out.append(
        _r_table(
            [
                "Table",
                "Targets",
                "Element fields",
                "JSON paths listed",
                "Paths validated (full scope)",
                "Element distinct counts",
                "Extra passes used",
                "Deep expressions omitted",
            ],
            rows,
        )
    )
    out.append("")
    limited = [(table, item) for table in tables for item in table["deep"]["limited"]]
    ineligible = [(table, item) for table in tables for item in table["deep"]["not_eligible"]]
    if limited:
        out += ["**Limited by the deep budgets**", ""]
        out.append(
            _r_table(
                ["Table", "Item", "Budget", "Detail"],
                [
                    [
                        md_text(table["table_key"]),
                        md_code(item["item"]),
                        md_code(item["reason"]),
                        md_text(item["detail"]),
                    ]
                    for table, item in limited
                ],
            )
        )
        out.append("")
    else:
        out += ["Nothing was limited by the deep budgets.", ""]
    if ineligible:
        out += ["**Requested deep targets that were not eligible**", ""]
        out.append(
            _r_table(
                ["Table", "Target", "Reason"],
                [
                    [
                        md_text(table["table_key"]),
                        md_code(item["display_path"]),
                        md_text(item["reason"]),
                    ]
                    for table, item in ineligible
                ],
            )
        )
        out.append("")
    return out


# --------------------------------------------------------------------------- relationships / ERD


def _r_cardinality(rel: Mapping[str, Any]) -> str:
    card = rel.get("cardinality")
    if not card:
        return "_not asserted_"
    return md_text(f"{card['from']} → {card['to']} (provided)")


def render_relationships(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``relationships.md``: known relationships, declared keys and hypotheses."""
    relationships = all_relationships(profile, annotations)
    checked = _r_checked(profile)
    out = [provenance_header(profile, "Relationships")]
    out += [
        "Relationships come only from declared constraints or from people (configuration or "
        "annotations). TableDossier never infers a relationship from column names"
        + (
            f"; {len(checked)} relationship(s) were checked against the data (see Referential "
            "validation)."
            if checked
            else ", and it did not validate any relationship against the data."
        ),
        "",
        "## Known relationships",
        "",
    ]
    if relationships:
        out.append(
            _r_table(
                [
                    "Relationship",
                    "Origin",
                    "From",
                    "To",
                    "Cardinality",
                    "Enforcement",
                    "Validation",
                    "Scope",
                ],
                [
                    [
                        md_code(rel["name"]),
                        md_text(rel["origin"]),
                        f"{md_text(rel['from']['table'])} "
                        f"({md_text(', '.join(rel['from']['columns']))})",
                        f"{md_text(rel['to']['table'])} "
                        f"({md_text(', '.join(rel['to']['columns']))})",
                        _r_cardinality(rel),
                        md_text(rel["enforcement"]),
                        _r_validation_text(rel),
                        md_text(rel["scope"]),
                    ]
                    for rel in relationships
                ],
            )
        )
        uncharted = [rel for rel in relationships if not rel.get("cardinality")]
        if uncharted:
            used: dict[str, str] = {}
            out += [
                "",
                "### References without asserted cardinality",
                "",
                "These references are not drawn in `erd.mmd` because an ER edge requires a "
                "cardinality "
                "that the evidence does not determine. This auxiliary diagram only shows "
                "direction.",
                "",
                "```mermaid",
                "flowchart LR",
            ]
            for rel in uncharted:
                left = mermaid_name(rel["from"]["table"], used)
                right = mermaid_name(rel["to"]["table"], used)
                label = mermaid_text(
                    f"{rel['origin'].replace('_', ' ')}: {', '.join(rel['from']['columns'])} to "
                    f"{', '.join(rel['to']['columns'])}"
                )
                out.append(
                    f'    {left}["{mermaid_text(rel["from"]["table"])}"] -. "{label}" .-> '
                    f'{right}["{mermaid_text(rel["to"]["table"])}"]'
                )
            out.append("```")
    else:
        out.append(
            "No relationships are known for these tables: no FOREIGN KEY constraints were visible "
            "to "
            "the run and none were provided in configuration or annotations."
        )
    out += ["", "## Referential validation", ""]
    if checked:
        out += _r_referential(profile)[2:]
    else:
        out += [
            "No relationship was checked against the data in this run. Referential validation "
            "runs at the deep level for the relationships selected by `deep.referential`; the "
            "Validation column above gives the reason for each relationship.",
            "",
        ]
    out += ["## Declared keys and constraints", ""]
    constraint_rows = []
    for table in profile["tables"]:
        for constraint in table.get("constraints", []):
            constraint_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(constraint["name"]),
                    md_text(constraint["constraint_type"]),
                    md_text(", ".join(constraint["columns"])) or "—",
                    md_code(constraint["expression"]) if constraint.get("expression") else "—",
                    md_text(constraint["enforcement"]),
                    md_text(constraint["source"]),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Constraint", "Type", "Columns", "Expression", "Enforcement", "Source"],
            constraint_rows,
        )
        if constraint_rows
        else "No declared constraints were visible."
    )
    out += [
        "",
        "Declared PRIMARY KEY/FOREIGN KEY constraints in Unity Catalog are informational: they "
        "document "
        "intent and are not enforced, so they do not prove integrity of the data.",
        "",
        *_r_hypotheses(profile),
    ]
    return "\n".join(out)


def _r_hypotheses(profile: Mapping[str, Any]) -> list[str]:
    """Render the hypotheses section of relationships.md (never known relationships)."""
    out = [
        "## Hypotheses (data-driven, not relationships)",
        "",
        "Hypotheses are observations about the data, kept apart from the known relationships "
        "above: they are never inferred from column names, have no cardinality and are never "
        "drawn in `erd.mmd`. A person must confirm one before declaring it.",
        "",
    ]
    record = profile.get("relationship_hypotheses")
    if not record:
        return [
            *out,
            "No data-driven relationship hypothesis was evaluated (deep level with "
            "`deep.relationship_hypotheses.enabled`). Candidate identifiers in the data dictionary "
            "are not keys unless an exact uniqueness check says so (quality report).",
            "",
        ]
    if not record["enabled"] or (record["reason"] and not record["pairs_evaluated"]):
        return [*out, f"Not evaluated: {md_text(record['reason'] or 'disabled')}.", ""]
    budget = record["budget"]
    rejected = ", ".join(
        f"{count} {reason.replace('_', ' ')}"
        for reason, count in sorted(record["pairs_rejected"].items())
    )
    out += [
        f"Targets (single-column keys measured exactly unique): {format_count(record['targets'])}. "
        f"Candidate pairs after the type and range filters: "
        f"{format_count(record['pairs_considered'])}; evaluated: "
        f"{format_count(record['pairs_evaluated'])}; left out by `max_pairs` = "
        f"{budget['max_pairs']}: {format_count(record['pairs_not_evaluated'])}. Skipped: "
        f"{format_count(record['pairs_known_excluded'])} known relationship(s), "
        f"{format_count(record['pairs_disjoint_excluded'])} disjoint range(s). Rejected: "
        f"{md_text(rejected) or 'none'}.",
        "",
    ]
    rows = []
    for item in record["hypotheses"]:
        evidence = item["evidence"]
        metrics = {m["name"]: m for m in evidence["metrics"]}
        rows.append(
            [
                md_code(item["hypothesis_id"]),
                f"{md_text(item['from']['table'])} ({md_text(', '.join(item['from']['columns']))})",
                f"{md_text(item['to']['table'])} ({md_text(', '.join(item['to']['columns']))})",
                format_value(metrics["inclusion_ratio"])
                + f" ({md_text(evidence['inclusion_scope'].replace('_', ' '))})",
                format_value(metrics["included_rows"])
                + " of "
                + format_value(metrics["source_rows_with_complete_key"]),
                "yes" if evidence["target_key_unique"] else "no",
                md_text(evidence["type_compatibility"][0]["rule"]),
                md_text(
                    f"{evidence['range_relation'].replace('_', ' ')} ({evidence['range_basis']})"
                ),
                _r_versions(evidence),
            ]
        )
    if rows:
        out += [
            _r_table(
                [
                    "Hypothesis",
                    "From",
                    "To",
                    "Inclusion",
                    "Included rows",
                    "Target key unique",
                    "Types",
                    "Measured range",
                    "Versions read",
                ],
                rows,
            ),
            "",
        ]
    else:
        out += [
            f"No evaluated pair reached `min_inclusion_ratio` = {budget['min_inclusion_ratio']}.",
            "",
        ]
    notes = sorted({note for item in record["hypotheses"] for note in item["limitations"]})
    out += [f"- {md_text(note)}" for note in [*notes, *record["limitations"]]]
    out.append("")
    return out


def _r_erd_type(node: Mapping[str, Any]) -> str:
    kind = node["type"]["kind"]
    physical = node["type"]["physical_type"]
    head = (physical.split("(")[0].split("<")[0].split() or [kind])[0]
    simple = re.sub(r"[^A-Za-z0-9_]", "_", head) or kind
    return simple if simple[0].isalpha() else kind


def render_erd(profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None) -> str:
    """Render ``erd.mmd`` (Mermaid erDiagram).

    Entities are the profiled tables with their top-level columns. Edges are
    drawn only when a person provided the cardinality; other known references
    are listed as comments and in ``relationships.md``.
    """
    run = profile["run"]
    relationships = all_relationships(profile, annotations)
    used: dict[str, str] = {}
    lines = [
        f"%% Generated by TableDossier {mermaid_text(profile['tool']['version'])} from profile run "
        f"{mermaid_text(run['run_id'])}, measured at {mermaid_text(run['started_at'])} UTC.",
        "%% Edges appear only for relationships whose cardinality was provided by a person.",
        "%% Line style is dashed (non-identifying); TableDossier does not assert identifying "
        "relationships.",
        "%% Entity names are sanitized; the original table names are:",
    ]
    names: dict[str, str] = {}
    for table in profile["tables"]:
        names[table["table_key"]] = mermaid_name(table["table_key"], used)
    for rel in relationships:
        for end in ("from", "to"):
            key = rel[end]["table"]
            if key not in names:
                names[key] = mermaid_name(key, used)
    lines += [f"%%   {name} = {mermaid_text(key)}" for key, name in names.items()]
    lines.append("erDiagram")
    pk_columns: dict[str, set[str]] = {}
    fk_columns: dict[str, set[str]] = {}
    uk_columns: dict[str, set[str]] = {}
    for table in profile["tables"]:
        for constraint in table.get("constraints", []):
            target = {
                "primary_key": pk_columns,
                "foreign_key": fk_columns,
                "unique": uk_columns,
            }.get(constraint["constraint_type"])
            if target is not None:
                target.setdefault(table["table_key"], set()).update(constraint["columns"])
    for table in profile["tables"]:
        entity = names[table["table_key"]]
        schema = table.get("schema") or {}
        top_nodes = schema.get("fields", [])
        if not top_nodes:
            lines.append(f"    {entity}")
            continue
        lines.append(f"    {entity} {{")
        attr_used: dict[str, str] = {}
        for node in top_nodes[:MAX_ERD_ATTRIBUTES]:
            path = node["display_path"]
            attr = mermaid_name(path, attr_used)
            keys = [
                label
                for label, source in (("PK", pk_columns), ("FK", fk_columns), ("UK", uk_columns))
                if path in source.get(table["table_key"], set())
            ]
            nullable = {True: "nullable", False: "not null", None: ""}[node.get("nullable")]
            comment = mermaid_text(
                " ".join(part for part in (path, node["type"]["physical_type"], nullable) if part)
            )
            key_text = f" {','.join(keys)}" if keys else ""
            lines.append(f'        {_r_erd_type(node)} {attr}{key_text} "{comment}"')
        lines.append("    }")
        if len(top_nodes) > MAX_ERD_ATTRIBUTES:
            lines.append(
                f"    %% {len(top_nodes) - MAX_ERD_ATTRIBUTES} more column(s) of {entity} not shown"
            )
    for key, name in names.items():
        if key not in {table["table_key"] for table in profile["tables"]}:
            lines.append(f"    {name}")
            lines.append(f"    %% {name} is referenced but was not profiled in this run")
    if not relationships:
        lines.append(
            "    %% No relationships are known (no visible FOREIGN KEY constraints and none "
            "provided); entities are shown without edges."
        )
    for rel in relationships:
        left = names[rel["from"]["table"]]
        right = names[rel["to"]["table"]]
        label = mermaid_text(", ".join(rel["from"]["columns"]))
        card = rel.get("cardinality")
        if card:
            lines.append(
                f'    {left} {ERD_LEFT[card["from"]]}..{ERD_RIGHT[card["to"]]} {right} : "{label}"'
            )
        else:
            lines.append(
                f"    %% not drawn ({mermaid_text(rel['origin'])}, cardinality not asserted): "
                f"{left} ({label}) references {right}"
            )
    return "\n".join(lines) + "\n"


def render_all(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Render every Markdown/Mermaid document, keyed by file name."""
    return {
        "overview.md": render_overview(profile, annotations),
        "data_dictionary.md": render_data_dictionary(profile, annotations),
        "quality_report.md": render_quality_report(profile, annotations),
        "relationships.md": render_relationships(profile, annotations),
        "erd.mmd": render_erd(profile, annotations),
    }

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.package
# TableDossier 0.2.0 embedded runtime: module tabledossier.package
# Source: src/tabledossier/package.py (sha256:b06849312bb7b1d7176c4399eac9cc30c035afaee111c9cf488434e85391e4df)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Result package: documents derived from a profile, manifests and safe writing.

Part of the embedded runtime (standard library only). The notebook writes the
full package after a run; ``tabledossier render`` regenerates the documents
from a transferred ``profile.json`` with exactly the same functions.
"""

import hashlib
import os
from collections.abc import Mapping
from typing import Any


DOCUMENT_FILES = (
    "overview.md",
    "data_dictionary.md",
    "quality_report.md",
    "relationships.md",
    "erd.mmd",
    "suggested_rules.json",
)
PACKAGE_FILES = ("manifest.json", "profile.json", *DOCUMENT_FILES)


class OutputExistsError(FileExistsError):
    """Raised when writing would replace existing files without explicit consent."""


def build_documents(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Return every derived document (Markdown, Mermaid, suggested rules)."""
    documents = render_all(profile, annotations)
    documents["suggested_rules.json"] = pretty_json(suggested_rules_document(profile))
    return documents


def file_digest(text: str) -> tuple[str, int]:
    """Return ``(sha256:<hex>, byte_length)`` of UTF-8 ``text``."""
    data = text.encode("utf-8")
    return "sha256:" + hashlib.sha256(data).hexdigest(), len(data)


def run_manifest(
    profile: Mapping[str, Any],
    files: Mapping[str, str],
    *,
    status: str,
    validation_errors: list[str] | None,
    generation_id: str | None,
) -> dict[str, Any]:
    """Return the run manifest describing a result package."""
    entries = []
    for name in sorted(files):
        digest, size = file_digest(files[name])
        entries.append({"path": name, "sha256": digest, "bytes": size})
    run = profile["run"]
    return {
        "kind": "tabledossier.run_manifest",
        "manifest_version": "1.0",
        "run_id": run["run_id"],
        "status": status,
        "tool_version": __version__,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "started_at": run["started_at"],
        "finished_at": run.get("finished_at"),
        "generation_id": generation_id,
        "files": entries,
        "profile_validation": {
            "checked": validation_errors is not None,
            "valid": None if validation_errors is None else not validation_errors,
            "errors": list(validation_errors or [])[:50],
        },
        "notes": [
            "profile.json is the source of every other file in this package.",
            "Documents can be regenerated offline with: tabledossier render --input profile.json "
            "--output <dir>",
        ],
    }


def running_manifest(run_id: str, started_at: str, generation_id: str | None) -> dict[str, Any]:
    """Return the manifest written before analysis starts (destination probe)."""
    return {
        "kind": "tabledossier.run_manifest",
        "manifest_version": "1.0",
        "run_id": run_id,
        "status": "running",
        "tool_version": __version__,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "started_at": started_at,
        "finished_at": None,
        "generation_id": generation_id,
        "files": [],
        "profile_validation": {"checked": False, "valid": None, "errors": []},
        "notes": ["Run in progress or interrupted before export; no profile has been written yet."],
    }


def write_files(
    directory: str,
    files: Mapping[str, str],
    *,
    overwrite: bool = False,
    replaceable: tuple[str, ...] = (),
) -> list[str]:
    """Write ``files`` into ``directory`` and return the written paths.

    Existing files are never replaced unless ``overwrite`` is true (or the
    name is listed in ``replaceable``, used for the run's own manifest).
    Nothing is written when a conflict is found.
    """
    os.makedirs(directory, exist_ok=True)
    targets = {name: os.path.join(directory, name) for name in files}
    for name, name_path in targets.items():
        if os.sep in name or name.startswith(".."):
            raise ValueError(f"invalid output file name: {name!r}")
        if os.path.exists(name_path) and not overwrite and name not in replaceable:
            raise OutputExistsError(
                f"{name_path} already exists; choose another output directory or allow "
                "overwriting explicitly"
            )
    written = []
    for name, name_path in targets.items():
        with open(name_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(files[name])
        written.append(name_path)
    return written

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.assemble
# TableDossier 0.2.0 embedded runtime: module tabledossier.assemble
# Source: src/tabledossier/assemble.py (sha256:b868890b99e2be458e6d45f6fd6f0cae5f8a63a76906122de9d794e5647baf96)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Engine-neutral assembly of table and run profiles.

Part of the embedded runtime (standard library only). Engine adapters return
raw Python values keyed by plan alias; this module turns them into contract
metrics (units, accuracy, method, denominators), derives ratios, attaches
findings, checks and proposals, and builds the final profile document.
"""

from collections.abc import Mapping, Sequence
from typing import Any


# metric name -> (unit, accuracy, method description)
METRIC_INFO: dict[str, tuple[str | None, str, str]] = {
    "row_count": ("rows", "exact", "count(*) over the analysed scope"),
    "null_count": (
        "rows",
        "exact",
        "rows where the field is null (includes rows whose parent struct is null)",
    ),
    "non_null_count": ("rows", "exact", "row_count - null_count"),
    "null_ratio": ("ratio", "exact", "null_count / row_count"),
    "null_count_parent_present": (
        "rows",
        "exact",
        "rows whose parent struct is not null and the field is null",
    ),
    "null_ratio_given_parent_present": (
        "ratio",
        "exact",
        "null_count_parent_present / rows with non-null parent",
    ),
    "approx_distinct_count": ("values", "approximate", "approx_count_distinct (HyperLogLog++)"),
    "all_values_equal": (None, "exact", "min(field) = max(field) over non-null values"),
    "finite_count": ("values", "exact", "non-null values that are neither NaN nor infinite"),
    "nan_count": ("values", "exact", "NaN values (distinct from SQL NULL)"),
    "positive_infinity_count": ("values", "exact", "+Infinity values"),
    "negative_infinity_count": ("values", "exact", "-Infinity values"),
    "min": (None, "exact", "minimum over non-null values (finite values only for floating point)"),
    "max": (None, "exact", "maximum over non-null values (finite values only for floating point)"),
    "mean": (None, "exact", "average over non-null values (finite values only for floating point)"),
    "stddev": (
        None,
        "exact",
        "sample standard deviation (stddev_samp), computed in double precision",
    ),
    "quantiles": (None, "approximate", "percentile_approx over non-null (finite) values"),
    "zero_count": ("values", "exact", "values equal to zero"),
    "negative_count": ("values", "exact", "values below zero"),
    "positive_count": ("values", "exact", "values above zero"),
    "empty_count": ("values", "exact", "empty strings or empty collections"),
    "whitespace_only_count": (
        "values",
        "exact",
        "non-empty strings made only of whitespace (regex ^\\s+$)",
    ),
    "min_length": ("characters", "exact", "minimum length over non-null values"),
    "max_length": ("characters", "exact", "maximum length over non-null values"),
    "mean_length": ("characters", "exact", "average length over non-null values"),
    "length_quantiles": (
        "characters",
        "approximate",
        "percentile_approx of length over non-null values",
    ),
    "true_count": ("values", "exact", "values equal to true"),
    "false_count": ("values", "exact", "values equal to false"),
    "after_reference_count": (
        "values",
        "exact",
        "values after the run reference instant (run.reference_time)",
    ),
    "min_size": ("elements", "exact", "minimum size over non-null collections"),
    "max_size": ("elements", "exact", "maximum size over non-null collections"),
    "mean_size": ("elements", "exact", "average size over non-null collections"),
    "total_element_count": ("elements", "exact", "sum of array sizes over non-null arrays"),
    "total_entry_count": ("entries", "exact", "sum of map sizes over non-null maps"),
    "null_element_count": ("elements", "exact", "null elements inside non-null arrays"),
    "null_value_count": ("entries", "exact", "null values inside non-null maps"),
    "json_invalid_count": (
        "values",
        "exact",
        "non-null strings for which try_parse_json returns NULL",
    ),
}
METRIC_ORDER = list(METRIC_INFO)
_AS_FINITE_DENOMINATOR = ("zero_count", "negative_count", "positive_count")
_AS_NON_NULL_DENOMINATOR = (
    "finite_count",
    "nan_count",
    "positive_infinity_count",
    "negative_infinity_count",
    "empty_count",
    "whitespace_only_count",
    "true_count",
    "false_count",
    "after_reference_count",
    "json_invalid_count",
)
_AS_SUM_DEFAULT_ZERO = (
    "total_element_count",
    "total_entry_count",
    "null_element_count",
    "null_value_count",
)


def _as_value_type(metric: str, kind: str, raw: Any) -> str | None:
    if metric in ("min", "max") and kind in ("timestamp", "timestamp_ntz"):
        return kind
    return None


def _as_insufficient(metric: str, kind: str, scope: str) -> dict[str, Any]:
    if metric == "stddev":
        reason = "fewer than two (finite) non-null values in scope"
    elif kind == "float" and metric in ("min", "max", "mean", "quantiles"):
        reason = "no finite non-null values in scope"
    else:
        reason = "no non-null values in scope"
    return not_measured(metric, "insufficient_data", reason, scope=scope, source="aggregate")


def aggregate_metric(
    spec: Mapping[str, Any],
    raw: Any,
    kind: str,
    *,
    scope: str,
    denominators: Mapping[str, int | None],
) -> dict[str, Any]:
    """Convert one raw aggregate result into a contract metric."""
    name = spec["metric"]
    unit, accuracy, method = METRIC_INFO[name]
    if kind == "binary" and name in ("min_length", "max_length"):
        unit = "bytes"
    if kind == "map" and unit == "elements":
        unit = "entries"
    details: dict[str, Any] = {}
    if spec["op"] == "approx_distinct":
        details["relative_standard_deviation"] = spec["params"]["rsd"]
    if name == "after_reference_count":
        details["reference"] = "run.reference_time"
        details["comparison"] = (
            "dates are compared with the UTC calendar date of run.reference_time; timestamps are "
            "compared as instants (session time zone recorded in run.environment)"
        )
    if name in _AS_SUM_DEFAULT_ZERO and raw is None:
        raw = 0
    if raw is None:
        return _as_insufficient(name, kind, scope)
    if name in ("quantiles", "length_quantiles"):
        encoded = []
        element_type = None
        for probability, item in zip(spec["params"]["probabilities"], raw, strict=False):
            probe = measured(
                "q",
                item,
                unit=None,
                scope=scope,
                accuracy="approximate",
                source="aggregate",
                method="-",
            )
            element_type = probe["value_type"]
            encoded.append({"probability": probability, "value": probe["value"]})
        details.update(
            {"element_type": element_type, "accuracy_parameter": spec["params"]["accuracy"]}
        )
        return measured(
            name,
            encoded,
            value_type="quantiles",
            unit=unit,
            scope=scope,
            accuracy=accuracy,
            source="aggregate",
            method=method,
            details=details,
        )
    denominator = None
    denominator_unit = None
    if name in _AS_FINITE_DENOMINATOR:
        key = "finite" if kind == "float" else "non_null"
        denominator, denominator_unit = denominators.get(key), "values"
    elif name in _AS_NON_NULL_DENOMINATOR:
        denominator, denominator_unit = denominators.get("non_null"), "values"
    elif name == "null_element_count":
        denominator, denominator_unit = denominators.get("total_element_count"), "elements"
    elif name == "null_value_count":
        denominator, denominator_unit = denominators.get("total_entry_count"), "entries"
    return measured(
        name,
        raw,
        value_type=_as_value_type(name, kind, raw),
        unit=unit,
        scope=scope,
        accuracy=accuracy,
        source="aggregate",
        method=method,
        denominator=denominator,
        denominator_unit=denominator_unit if denominator is not None else None,
        details=details or None,
    )


def field_metrics(
    node: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed_aliases: Mapping[str, str],
    *,
    scope: str,
    row_count: int | None,
    parent_null_count: int | None,
    static: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build the ordered metric list of one field from raw aggregate results."""
    kind = node["type"]["kind"]
    raw = {
        spec["metric"]: results.get(spec["alias"])
        for spec in specs
        if spec["alias"] not in failed_aliases
    }
    null_count = raw.get("null_count")
    non_null = (
        row_count - null_count
        if isinstance(row_count, int) and isinstance(null_count, int)
        else None
    )
    finite = raw.get("finite_count") if kind == "float" else non_null
    total_elements = raw.get("total_element_count")
    total_entries = raw.get("total_entry_count")
    denominators = {
        "non_null": non_null,
        "finite": finite if isinstance(finite, int) else None,
        "total_element_count": total_elements
        if total_elements is not None
        else (0 if "total_element_count" in raw else None),
        "total_entry_count": total_entries
        if total_entries is not None
        else (0 if "total_entry_count" in raw else None),
    }
    out: dict[str, dict[str, Any]] = {}
    for spec in specs:
        name = spec["metric"]
        if spec["alias"] in failed_aliases:
            out[name] = not_measured(
                name, "error", failed_aliases[spec["alias"]], scope=scope, source="aggregate"
            )
            continue
        metric = aggregate_metric(spec, raw.get(name), kind, scope=scope, denominators=denominators)
        if name == "null_count" and metric["status"] == "measured":
            metric["denominator"] = row_count
            metric["denominator_unit"] = "rows"
        out[name] = metric
    if isinstance(non_null, int):
        out["non_null_count"] = measured(
            "non_null_count",
            non_null,
            unit="rows",
            scope=scope,
            accuracy="exact",
            source="derived",
            method=METRIC_INFO["non_null_count"][2],
            denominator=row_count,
            denominator_unit="rows",
        )
        out["null_ratio"] = ratio(
            "null_ratio",
            null_count,
            row_count,
            scope=scope,
            source="derived",
            method=METRIC_INFO["null_ratio"][2],
            denominator_unit="rows",
        )
    if (
        "null_count_parent_present" in out
        and out["null_count_parent_present"]["status"] == "measured"
    ):
        parent_non_null = (
            row_count - parent_null_count
            if isinstance(row_count, int) and isinstance(parent_null_count, int)
            else None
        )
        metric = out["null_count_parent_present"]
        if parent_non_null is not None:
            metric["denominator"] = parent_non_null
            metric["denominator_unit"] = "rows"
        out["null_ratio_given_parent_present"] = ratio(
            "null_ratio_given_parent_present",
            metric["value"],
            parent_non_null,
            scope=scope,
            source="derived",
            method=METRIC_INFO["null_ratio_given_parent_present"][2],
            denominator_unit="rows with a non-null parent",
        )
    if "all_values_equal" in out and non_null == 0:
        out["all_values_equal"] = _as_insufficient("all_values_equal", kind, scope)
    for item in static:
        out.setdefault(item["metric"]["name"], dict(item["metric"]))
    order = {name: index for index, name in enumerate(METRIC_ORDER)}
    return [out[name] for name in sorted(out, key=lambda n: (order.get(n, len(order)), n))]


def field_profile(
    node: Mapping[str, Any], *, profiled: bool, omission_reason: str | None
) -> dict[str, Any]:
    """Return an empty field profile record for a schema node."""
    return {
        "field_id": node["field_id"],
        "display_path": node["display_path"],
        "path": node["path"],
        "type_kind": node["type"]["kind"],
        "physical_type": node["type"]["physical_type"],
        "nullable": node.get("nullable"),
        "comment": node.get("comment"),
        "profiled": profiled,
        "omission_reason": omission_reason,
        "metrics": [],
        "semantics": None,
        "json_profile": None,
        "concentration": None,
        "examples": None,
        "element_context": None,
        "json_paths": None,
    }


# metric name -> (unit or None for the element unit, method); element metrics are exact.
ELEMENT_METRIC_INFO: dict[str, tuple[str | None, str]] = {
    "element_count": (None, "size of the collection summed over rows (denominator of the others)"),
    "null_count": (
        None,
        "elements whose value is null (includes elements whose parent struct is null); "
        "higher-order functions per row, summed",
    ),
    "non_null_count": (None, "element_count - null_count"),
    "null_ratio": ("ratio", "null_count / element_count"),
    "null_count_parent_present": (
        None,
        "elements whose parent struct is not null and the field is null",
    ),
    "null_ratio_given_parent_present": (
        "ratio",
        "null_count_parent_present / elements with a non-null parent",
    ),
    "finite_count": (None, "non-null values that are neither NaN nor infinite"),
    "nan_count": (None, "NaN values (distinct from SQL NULL)"),
    "positive_infinity_count": (None, "+Infinity values"),
    "negative_infinity_count": (None, "-Infinity values"),
    "min": (None, "array_min per row, then min over rows (finite values only for floats)"),
    "max": (None, "array_max per row, then max over rows (finite values only for floats)"),
    "zero_count": (None, "values equal to zero"),
    "negative_count": (None, "values below zero"),
    "positive_count": (None, "values above zero"),
    "empty_count": (None, "empty strings"),
    "whitespace_only_count": (None, "non-empty strings made only of whitespace (regex ^\\s+$)"),
    "min_length": ("characters", "minimum length over non-null values"),
    "max_length": ("characters", "maximum length over non-null values"),
    "true_count": (None, "values equal to true"),
    "false_count": (None, "values equal to false"),
    "after_reference_count": (None, "values after the run reference instant (run.reference_time)"),
    "distinct_count": (None, "exact count of distinct non-null values among the elements examined"),
}
ELEMENT_METRIC_ORDER = list(ELEMENT_METRIC_INFO)
_AS_ELEMENT_NON_NULL_DENOMINATOR = (
    "finite_count",
    "nan_count",
    "positive_infinity_count",
    "negative_infinity_count",
    "empty_count",
    "whitespace_only_count",
    "true_count",
    "false_count",
    "after_reference_count",
)


def element_field_metrics(
    node: Mapping[str, Any],
    context: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    results: Mapping[str, Any],
    failed_aliases: Mapping[str, str],
    *,
    scope: str,
    element_total: int | None,
    parent_null_count: int | None,
    static: Sequence[Mapping[str, Any]],
    extra: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Build the metrics of an array element or map entry field.

    Denominators are the elements (arrays) or entries (maps) of the collection
    in scope, never rows. ``element_total`` is the collection's measured
    ``total_element_count``/``total_entry_count``; ``parent_null_count`` is
    the null count of the enclosing element struct (for struct leaves).
    """
    kind = node["type"]["kind"]
    unit = context["unit"]
    collection = context["collection_display_path"]
    out: dict[str, dict[str, Any]] = {}
    raw: dict[str, Any] = {}
    for spec in specs:
        name = spec["metric"]
        if spec["alias"] in failed_aliases:
            out[name] = not_measured(
                name, "error", failed_aliases[spec["alias"]], scope=scope, source="aggregate"
            )
        else:
            raw[name] = results.get(spec["alias"])
    if isinstance(element_total, int):
        out["element_count"] = measured(
            "element_count",
            element_total,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source="derived",
            method=f"{unit} of {collection} in scope ({ELEMENT_METRIC_INFO['element_count'][1]})",
        )
    null_count = raw.get("null_count")
    null_count = 0 if null_count is None and "null_count" in raw else null_count
    non_null = (
        element_total - null_count
        if isinstance(element_total, int) and isinstance(null_count, int)
        else None
    )
    finite = raw.get("finite_count") if kind == "float" else non_null
    if kind == "float" and finite is None and "finite_count" in raw:
        finite = 0
    for name, value in raw.items():
        info_unit, method = ELEMENT_METRIC_INFO[name]
        metric_unit = info_unit or unit
        if kind == "binary" and name in ("min_length", "max_length"):
            metric_unit = "bytes"
        details = None
        if name == "after_reference_count":
            details = {"reference": "run.reference_time"}
        if name in ("min", "max", "min_length", "max_length"):
            if value is None:
                out[name] = not_measured(
                    name,
                    "insufficient_data",
                    f"no non-null {unit} in scope"
                    if kind != "float"
                    else f"no finite non-null {unit} in scope",
                    scope=scope,
                    source="aggregate",
                )
                continue
            value_type = kind if name in ("min", "max") and kind.startswith("timestamp") else None
            out[name] = measured(
                name,
                value,
                value_type=value_type,
                unit=None if name in ("min", "max") else metric_unit,
                scope=scope,
                accuracy="exact",
                source="aggregate",
                method=method,
            )
            continue
        count = 0 if value is None else int(value)
        denominator: int | None = None
        if name == "null_count":
            denominator = element_total
        elif name in ("zero_count", "negative_count", "positive_count"):
            denominator = finite if isinstance(finite, int) else None
        elif name in _AS_ELEMENT_NON_NULL_DENOMINATOR:
            denominator = non_null
        elif name == "null_count_parent_present":
            denominator = (
                element_total - parent_null_count
                if isinstance(element_total, int) and isinstance(parent_null_count, int)
                else None
            )
        out[name] = measured(
            name,
            count,
            unit=metric_unit,
            scope=scope,
            accuracy="exact",
            source="aggregate",
            method=method,
            denominator=denominator,
            denominator_unit=unit if denominator is not None else None,
            details=details,
        )
    if isinstance(non_null, int) and "null_count" in out:
        out["non_null_count"] = measured(
            "non_null_count",
            non_null,
            unit=unit,
            scope=scope,
            accuracy="exact",
            source="derived",
            method=ELEMENT_METRIC_INFO["non_null_count"][1],
            denominator=element_total,
            denominator_unit=unit,
        )
        out["null_ratio"] = ratio(
            "null_ratio",
            null_count,
            element_total,
            scope=scope,
            source="derived",
            method=ELEMENT_METRIC_INFO["null_ratio"][1],
            denominator_unit=unit,
        )
    parent_metric = out.get("null_count_parent_present")
    if parent_metric is not None and parent_metric["status"] == "measured":
        out["null_ratio_given_parent_present"] = ratio(
            "null_ratio_given_parent_present",
            parent_metric["value"],
            parent_metric.get("denominator"),
            scope=scope,
            source="derived",
            method=ELEMENT_METRIC_INFO["null_ratio_given_parent_present"][1],
            denominator_unit=f"{unit} with a non-null parent",
        )
    for metric in extra:
        out.setdefault(metric["name"], dict(metric))
    for item in static:
        out.setdefault(item["metric"]["name"], dict(item["metric"]))
    order = {name: index for index, name in enumerate(ELEMENT_METRIC_ORDER)}
    return [out[name] for name in sorted(out, key=lambda n: (order.get(n, len(order)), n))]


def policy_field_ids(config: Mapping[str, Any], table_lookup: str, list_name: str) -> set[str]:
    """Return field ids listed for a table in ``value_policy.<list_name>``."""
    ids = set()
    for item in config["value_policy"].get(list_name, []):
        if table_lookup_key(parse_table_identifier(item["table"])) == table_lookup:
            ids.add(field_id(parse_display_path(item["column"])))
    return ids


def new_table(
    input_text: str, parts: list[str], table_key_text: str, table_id_text: str, quoted: str
) -> dict[str, Any]:
    """Return a table profile skeleton (filled in by an engine adapter)."""
    return {
        "table_id": table_id_text,
        "table_key": table_key_text,
        "identifier": {"input": input_text, "parts": list(parts), "quoted": quoted},
        "status": "succeeded",
        "errors": [],
        "purpose": None,
        "source": None,
        "consistency": None,
        "scope": {
            "population": "full",
            "scope_label": "table_metadata",
            "filters": [],
            "selected_columns": None,
            "row_semantics": (
                "Rows in the analysed scope after filters. No deduplication is applied; "
                "row counts are not counts of business entities (e.g. in historized tables)."
            ),
        },
        "sample": None,
        "summary": {},
        "table_metrics": [],
        "schema": None,
        "field_profiles": [],
        "constraints": [],
        "findings": [],
        "quality_checks": [],
        "suggested_rules": [],
        "omissions": [],
        "unsupported": [],
        "operations": {
            "planned": [],
            "observed": [],
            "physical_scans": "unknown",
            "bytes_read": "unknown",
            "monetary_cost": "unknown",
        },
        "timings_ms": {},
        "notes": [],
        "deep": None,
        "uniqueness": None,
    }


def finalize_table(table: dict[str, Any], config: Mapping[str, Any]) -> None:
    """Attach roles, findings, checks, proposals, summary and status to a table."""
    thresholds = config["thresholds"]
    row_count = None
    for metric in table["table_metrics"]:
        if (
            metric["name"] == "row_count"
            and metric["status"] == "measured"
            and metric["source"] == "aggregate"
        ):
            row_count = metric["value"]
    findings: list[dict[str, Any]] = []
    for field in table["field_profiles"]:
        if not field["profiled"]:
            continue
        context = field.get("element_context")
        if context:
            # Element fields: counts are elements or entries of a collection, never rows.
            total = metric_value(field["metrics"], "element_count")
            findings.extend(
                field_findings(
                    field,
                    thresholds,
                    total if isinstance(total, int) else None,
                    unit=context["unit"],
                )
            )
            continue
        if field["metrics"] or field["semantics"] is not None:
            semantics = field["semantics"] or {"observed_format": None, "candidate_roles": []}
            semantics["candidate_roles"] = candidate_roles(
                field["type_kind"],
                field["metrics"],
                semantics.get("observed_format"),
                thresholds,
                field["physical_type"],
            )
            field["semantics"] = semantics
        findings.extend(field_findings(field, thresholds, row_count))
    table["findings"] = findings
    options = table_options_for(config, table_lookup_key(table["identifier"]["parts"]))
    checks = options.get("checks", [])
    if table["schema"] is None:
        table["quality_checks"] = [
            {
                "check_id": check["id"],
                "type": check["type"],
                "origin": "configured",
                "target": {"table": table["table_key"], "column": None},
                "parameters": {key: check[key] for key in ("min", "max") if key in check},
                "status": "not_evaluated",
                "observed": None,
                "message": "table could not be profiled",
                "description": check.get("description"),
            }
            for check in checks
        ]
    else:
        table["quality_checks"] = evaluate_checks(table, checks)
    table["suggested_rules"] = suggest_rules(table, thresholds)
    schema = table["schema"] or {}
    nodes = list(iter_nodes(schema.get("fields", [])))
    profiled = [field for field in table["field_profiles"] if field["profiled"]]
    table["summary"] = {
        "top_level_columns": len(schema.get("fields", [])) + schema.get("top_level_omitted", 0)
        if schema
        else None,
        "columns_selected": len(table["scope"]["selected_columns"])
        if table["scope"]["selected_columns"] is not None
        else None,
        "schema_nodes": len(nodes) if schema else None,
        "fields_profiled": len(profiled),
        "fields_omitted": len(
            {item["field_id"] for item in table["omissions"] if item["field_id"]}
        ),
        "findings": len(findings),
        "quality_checks": len(table["quality_checks"]),
        "suggested_rules": len(table["suggested_rules"]),
    }
    stages = {error["stage"] for error in table["errors"]}
    if table["schema"] is None or "resolve" in stages:
        table["status"] = "failed"
    elif stages:
        table["status"] = "partial"
    else:
        table["status"] = "succeeded"


def value_exposure(config: Mapping[str, Any]) -> dict[str, Any]:
    """Describe what kinds of values a profile can contain under the policy."""
    policy = config["value_policy"]
    examples = list(policy.get("example_columns", [])) if policy.get("persist_examples") else []
    notes = [
        "Profiles reveal schema, comments and aggregate statistics; they are not anonymized.",
        "Sampled values are inspected transiently in the execution environment for format and JSON "
        "inference and are not persisted unless a column is allow-listed.",
        "Filter values from the configuration are recorded because they define the analysed "
        "population.",
    ]
    if policy.get("aggregate_extremes") == "include":
        notes.append(
            "Numeric and temporal min, max, mean and quantiles are included; set "
            "value_policy.aggregate_extremes = redact (or list redact_columns) to omit them."
        )
    return {
        "raw_values_persisted": bool(examples),
        "example_columns": examples,
        "aggregate_extremes": policy.get("aggregate_extremes", "include"),
        "json_key_names": policy.get("json_key_names", "include"),
        "redacted_columns": list(policy.get("redact_columns", [])),
        "notes": notes,
    }


def known_relationships(
    tables: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Return the declared FOREIGN KEYs of the tables plus the configured relationships."""
    return merge_relationships(
        declared_relationships(tables),
        provided_relationships(config.get("relationships", []), "configuration"),
    )


def _as_summary_counts(
    tables: Sequence[Mapping[str, Any]], relationships: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    keys = [
        key
        for table in tables
        for key in (table.get("uniqueness") or {}).get("keys", [])
        if key["status"] == "measured"
    ]
    statuses = [rel["validation"] for rel in relationships]
    return {
        "uniqueness": {
            "keys_measured": len(keys),
            **{
                outcome: sum(1 for key in keys if key["outcome"] == outcome)
                for outcome in ("unique", "unique_non_null", "duplicates", "empty")
            },
        },
        "relationships": {
            name: statuses.count(name) for name in ("validated", "violated", "not_validated")
        },
    }


def build_profile(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    reference_time: str,
    environment: Mapping[str, Any],
    config: Mapping[str, Any],
    parameter_sources: Mapping[str, Any],
    generation: Mapping[str, Any],
    capabilities: Mapping[str, Any],
    tables: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None = None,
    referential_validation: dict[str, Any] | None = None,
    relationship_hypotheses: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the canonical profile document for a run.

    ``relationships`` are the known relationships, with their validation when
    the deep level checked them; without it they are built here and recorded
    as ``not_validated`` with the reason.
    """
    statuses = [table["status"] for table in tables]
    if statuses and all(status == "succeeded" for status in statuses):
        status = "succeeded"
    elif not statuses or all(status == "failed" for status in statuses):
        status = "failed"
    else:
        status = "partial"
    effective = sanitized_config(config)
    if relationships is None:
        relationships = known_relationships(tables, config)
    for relationship in relationships:
        if relationship.get("validation_detail") is None:
            relationship["validation_detail"] = not_validated_detail(
                referential_requested(relationship, config) or "not checked in this run"
            )
    deep = config["analysis_level"] == "deep"
    if deep and referential_validation is None:
        referential_validation = referential_summary(relationships, config, planned=0, limited=[])
    if deep and relationship_hypotheses is None:
        relationship_hypotheses = hypotheses_record(
            config,
            None,
            [],
            evaluated=0,
            rejected={},
            reason="disabled by configuration (deep.relationship_hypotheses.enabled = false)",
        )
    checks = [check for table in tables for check in table["quality_checks"]]
    findings = [finding for table in tables for finding in table["findings"]]
    return {
        "kind": PROFILE_KIND,
        "schema_version": PROFILE_SCHEMA_VERSION,
        "tool": {"name": "tabledossier", "version": __version__},
        "run": {
            "run_id": run_id,
            "status": status,
            "analysis_level": config["analysis_level"],
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "reference_time": reference_time,
            "environment": dict(environment),
            "effective_config": effective,
            "config_fingerprint": fingerprint(effective),
            "parameter_sources": dict(parameter_sources),
            "generation": dict(generation),
            "purpose": config.get("purpose"),
            "capabilities": dict(capabilities),
        },
        "value_exposure": value_exposure(config),
        "tables": tables,
        "relationships": relationships,
        "summary": {
            "tables_total": len(tables),
            "tables_succeeded": statuses.count("succeeded"),
            "tables_partial": statuses.count("partial"),
            "tables_failed": statuses.count("failed"),
            "fields_profiled": sum(
                table["summary"].get("fields_profiled") or 0 for table in tables
            ),
            "checks": {
                s: sum(1 for c in checks if c["status"] == s)
                for s in ("pass", "fail", "not_evaluated", "error")
            },
            "findings": {
                s: sum(1 for f in findings if f["severity"] == s) for s in ("info", "warning")
            },
            "suggested_rules": sum(len(table["suggested_rules"]) for table in tables),
            **_as_summary_counts(tables, relationships),
            "relationship_hypotheses": len((relationship_hypotheses or {}).get("hypotheses", [])),
        },
        "referential_validation": referential_validation,
        "relationship_hypotheses": relationship_hypotheses,
    }

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.runtime.spark
# TableDossier 0.2.0 embedded runtime: module tabledossier.runtime.spark
# Source: src/tabledossier/runtime/spark.py (sha256:c99fa30e180d408f8b8f1430a8f69de3716f580487474031bf1b24664c91d943)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Spark adapter: metadata capture, sampling and shared aggregations.

Part of the embedded runtime. It only needs the PySpark already present in the
execution environment (classic or Spark Connect sessions) and public APIs:

* catalog metadata through ``DESCRIBE TABLE EXTENDED``/``DESCRIBE DETAIL``/
  ``DESCRIBE HISTORY`` and Unity Catalog ``information_schema`` (never by
  reading ``_delta_log`` or data files directly);
* Delta time travel (``VERSION AS OF``) to pin every row read of a table to
  one snapshot when possible;
* one bounded, projected sample and a bounded number of ``agg`` passes per
  table. No Python UDFs, no automatic caching, no per-column jobs, no
  ``toPandas()`` on sources, and no writes to sources.
"""

import os
import platform
import re
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pyspark.sql import functions as F


TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
TIMESTAMP_NTZ_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
_SP_STATS = re.compile(r"(\d+)\s+bytes(?:,\s*(\d+)\s+rows)?")
_SP_UNITY_EXCLUDED = ("spark_catalog", "hive_metastore")
_SP_VARIANT_FUNCTIONS = (
    "try_parse_json",
    "try_variant_get",
    "is_variant_null",
    "schema_of_variant",
)
_SP_INF = float("inf")
_SP_CAST_TYPES = {
    "string": "string",
    "integer": "bigint",
    "double": "double",
    "decimal": "decimal(38,18)",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


# --------------------------------------------------------------------------- timing


def _sp_ms(start: float) -> int:
    return max(0, int((time.perf_counter() - start) * 1000))


def _sp_observed(
    op_id: str, status: str, start: float | None, rows: int | None = None, detail: str | None = None
) -> dict[str, Any]:
    return {
        "operation_id": op_id,
        "status": status,
        "duration_ms": _sp_ms(start) if start is not None else None,
        "rows_returned": rows,
        "detail": detail,
    }


# --------------------------------------------------------------------------- environment


def _sp_conf(spark: Any, key: str) -> str | None:
    try:
        value = spark.conf.get(key)
    except Exception:  # noqa: BLE001 - configuration may be hidden (e.g. serverless)
        return None
    return None if value is None else str(value)


def spark_environment(spark: Any) -> dict[str, Any]:
    """Describe the actual execution environment (no user or host names)."""
    runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    ansi = _sp_conf(spark, "spark.sql.ansi.enabled")
    return {
        "engine": "spark",
        "execution_context": "databricks" if runtime else "spark",
        "python_version": platform.python_version(),
        "spark_version": str(spark.version),
        "databricks_runtime_version": runtime,
        "session_timezone": _sp_conf(spark, "spark.sql.session.timeZone"),
        "ansi_mode": None if ansi is None else ansi.lower() == "true",
        "spark_connect": type(spark).__module__.startswith("pyspark.sql.connect"),
    }


def _sp_version_tuple(version: str) -> tuple[int, int]:
    match = re.match(r"(\d+)\.(\d+)", version)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)


def detect_capabilities(spark: Any) -> dict[str, dict[str, Any]]:
    """Detect optional engine features instead of assuming them."""
    capabilities: dict[str, dict[str, Any]] = {}
    has_call = hasattr(F, "call_function")
    exists = None
    try:
        exists = bool(spark.catalog.functionExists("try_parse_json"))
    except Exception:  # noqa: BLE001
        exists = None
    capabilities["try_parse_json"] = {
        "available": bool(exists) and has_call,
        "detail": (
            "catalog.functionExists('try_parse_json') = "
            + ("unknown" if exists is None else str(exists).lower())
            + f"; pyspark.sql.functions.call_function available = {str(has_call).lower()}"
        ),
    }
    version = _sp_version_tuple(str(spark.version))
    capabilities["parameterized_sql"] = {
        "available": version >= (3, 4),
        "detail": f"spark.sql(query, args=...) requires Spark 3.4+ (running {spark.version})",
    }
    capabilities["higher_order_functions"] = {
        "available": hasattr(F, "filter"),
        "detail": "pyspark.sql.functions.filter used for null elements inside arrays and maps",
    }
    found: dict[str, bool | None] = {}
    for name in (*_SP_VARIANT_FUNCTIONS, "get_json_object"):
        try:
            found[name] = bool(spark.catalog.functionExists(name))
        except Exception:  # noqa: BLE001
            found[name] = None
    capabilities["variant_functions"] = {
        "available": has_call and all(found[name] for name in _SP_VARIANT_FUNCTIONS),
        "detail": "; ".join(
            f"functionExists('{name}') = {_sp_flag(found[name])}" for name in _SP_VARIANT_FUNCTIONS
        )
        + "; used by the deep level for full-scope JSON path presence and type checks",
    }
    capabilities["get_json_object"] = {
        "available": bool(found["get_json_object"]) and hasattr(F, "get_json_object"),
        "detail": f"functionExists('get_json_object') = {_sp_flag(found['get_json_object'])}; "
        "deep-level fallback for full-scope JSON path presence (cannot tell a JSON null from an "
        "absent path, nor types)",
    }
    return capabilities


def _sp_flag(value: bool | None) -> str:
    return "unknown" if value is None else str(value).lower()


# --------------------------------------------------------------------------- types


def neutral_type(data_type: Any) -> dict[str, Any]:
    """Convert a Spark ``DataType`` into the engine-neutral type descriptor."""
    name = type(data_type).__name__
    simple = data_type.simpleString()
    if name in ("ByteType", "ShortType", "IntegerType", "LongType"):
        return {"kind": "integer", "physical_type": simple}
    if name in ("FloatType", "DoubleType"):
        return {"kind": "float", "physical_type": simple}
    if name == "DecimalType":
        return {
            "kind": "decimal",
            "physical_type": simple,
            "precision": data_type.precision,
            "scale": data_type.scale,
        }
    if name in ("StringType", "VarcharType", "CharType"):
        return {"kind": "string", "physical_type": simple}
    if name == "BooleanType":
        return {"kind": "boolean", "physical_type": simple}
    if name == "DateType":
        return {"kind": "date", "physical_type": simple}
    if name == "TimestampType":
        return {"kind": "timestamp", "physical_type": simple}
    if name == "TimestampNTZType":
        return {"kind": "timestamp_ntz", "physical_type": simple}
    if name == "BinaryType":
        return {"kind": "binary", "physical_type": simple}
    if name == "StructType":
        return {"kind": "struct", "physical_type": "struct", "fields": neutral_fields(data_type)}
    if name == "ArrayType":
        element = neutral_type(data_type.elementType)
        return {
            "kind": "array",
            "physical_type": f"array<{_sp_short(data_type.elementType)}>",
            "element_type": element,
            "contains_null": bool(data_type.containsNull),
        }
    if name == "MapType":
        return {
            "kind": "map",
            "physical_type": (
                f"map<{_sp_short(data_type.keyType)},{_sp_short(data_type.valueType)}>"
            ),
            "key_type": neutral_type(data_type.keyType),
            "value_type": neutral_type(data_type.valueType),
            "value_contains_null": bool(data_type.valueContainsNull),
        }
    if name == "VariantType":
        return {"kind": "variant", "physical_type": simple}
    if name in ("DayTimeIntervalType", "YearMonthIntervalType", "CalendarIntervalType"):
        return {"kind": "interval", "physical_type": simple}
    if name == "NullType":
        return {"kind": "null", "physical_type": simple}
    return {"kind": "other", "physical_type": simple}


def _sp_short(data_type: Any) -> str:
    name = type(data_type).__name__
    if name == "StructType":
        return "struct"
    if name in ("ArrayType", "MapType"):
        return name[:-4].lower()
    text = str(data_type.simpleString())
    return text if len(text) <= 60 else text[:57] + "..."


def neutral_fields(struct_type: Any) -> list[dict[str, Any]]:
    """Convert a Spark ``StructType`` into neutral top-level field descriptors."""
    fields = []
    for field in struct_type.fields:
        ntype = neutral_type(field.dataType)
        metadata = dict(field.metadata or {})
        declared = metadata.get("__CHAR_VARCHAR_TYPE_STRING")
        if declared and ntype["kind"] == "string":
            ntype["physical_type"] = str(declared)
        comment = metadata.get("comment")
        fields.append(
            {
                "name": field.name,
                "type": ntype,
                "nullable": bool(field.nullable),
                "comment": str(comment) if comment else None,
            }
        )
    return fields


# --------------------------------------------------------------------------- expressions


def column_for(path: Sequence[Mapping[str, Any]]) -> Any:
    """Return a Column for a path of struct field segments (names never parsed)."""
    if not path or any(segment["kind"] != "field" for segment in path):
        raise ValueError("only struct field paths can be resolved to columns")
    column = F.col(quote_name(path[0]["name"]))
    for segment in path[1:]:
        column = column.getField(segment["name"])
    return column


def _sp_literal(value: Any, value_type: str | None) -> Any:
    literal = F.lit(value)
    return literal.cast(_SP_CAST_TYPES[value_type]) if value_type else literal


def filter_condition(filters: list[Mapping[str, Any]]) -> Any:
    """Build a filter Column from structured filters (values are literals, never SQL)."""
    condition = None
    for item in filters:
        column = column_for(column_reference_segments(item["column"]))
        operator = item["operator"]
        value_type = item.get("value_type")
        value: Any = item.get("value")
        if operator == "is_null":
            expr = column.isNull()
        elif operator == "is_not_null":
            expr = column.isNotNull()
        elif operator in ("in", "not_in"):
            expr = column.isin(*[_sp_literal(v, value_type) for v in value])
            if operator == "not_in":
                expr = ~expr
        elif operator == "between":
            expr = column.between(
                _sp_literal(value[0], value_type), _sp_literal(value[1], value_type)
            )
        elif operator == "like":
            expr = column.like(str(value))
        else:
            literal = _sp_literal(value, value_type)
            expr = {
                "eq": column == literal,
                "ne": column != literal,
                "lt": column < literal,
                "le": column <= literal,
                "gt": column > literal,
                "ge": column >= literal,
            }[operator]
        condition = expr if condition is None else condition & expr
    return condition


def _sp_finite(column: Any) -> Any:
    return (
        column.isNotNull()
        & ~F.isnan(column)
        & (column != F.lit(float("inf")))
        & (column != F.lit(float("-inf")))
    )


def compile_spec(
    spec: Mapping[str, Any],
    node: Mapping[str, Any] | None,
    parent: Mapping[str, Any] | None,
    ctx: Mapping[str, Any],
) -> Any:
    """Compile a planned metric spec into an aggregate Column expression."""
    op = spec["op"]
    if op == "count_all":
        return F.count(F.lit(1))
    assert node is not None
    if op.startswith("el_"):
        return compile_element_spec(spec, node, ctx)
    if op.startswith("json_"):
        return compile_json_spec(spec, node)
    column = column_for(node["path"])
    kind = node["type"]["kind"]
    present = _sp_finite(column) if kind == "float" else column.isNotNull()
    values = F.when(present, column) if kind == "float" else column
    params = spec["params"]
    if op == "count_null":
        return F.count(F.when(column.isNull(), 1))
    if op == "count_null_parent_present":
        assert parent is not None
        return F.count(F.when(column_for(parent["path"]).isNotNull() & column.isNull(), 1))
    if op == "approx_distinct":
        return F.approx_count_distinct(column, params["rsd"])
    if op == "all_values_equal":
        return F.min(column) == F.max(column)
    if op == "count_nan":
        return F.count(F.when(F.isnan(column), 1))
    if op == "count_pos_inf":
        return F.count(F.when(column == F.lit(float("inf")), 1))
    if op == "count_neg_inf":
        return F.count(F.when(column == F.lit(float("-inf")), 1))
    if op == "count_finite":
        return F.count(F.when(present, 1))
    if op in ("min_value", "max_value"):
        agg = F.min(values) if op == "min_value" else F.max(values)
        if kind == "timestamp":
            return F.date_format(agg, TIMESTAMP_PATTERN)
        if kind == "timestamp_ntz":
            return F.date_format(agg, TIMESTAMP_NTZ_PATTERN)
        return agg
    if op == "mean_value":
        return F.avg(values)
    if op == "stddev_value":
        return F.stddev_samp(values)
    if op == "count_zero":
        return F.count(F.when(present & (column == F.lit(0)), 1))
    if op == "count_negative":
        return F.count(F.when(present & (column < F.lit(0)), 1))
    if op == "count_positive":
        return F.count(F.when(present & (column > F.lit(0)), 1))
    if op == "quantiles":
        return F.percentile_approx(values, list(params["probabilities"]), params["accuracy"])
    if op == "count_empty_string":
        return F.count(F.when(column == F.lit(""), 1))
    if op == "count_whitespace_only":
        return F.count(F.when((column != F.lit("")) & column.rlike("^\\s+$"), 1))
    if op == "min_length":
        return F.min(F.length(column))
    if op == "max_length":
        return F.max(F.length(column))
    if op == "mean_length":
        return F.avg(F.length(column))
    if op == "length_quantiles":
        return F.percentile_approx(
            F.length(column), list(params["probabilities"]), params["accuracy"]
        )
    if op == "count_true":
        return F.count(F.when(column == F.lit(True), 1))
    if op == "count_false":
        return F.count(F.when(column == F.lit(False), 1))
    if op == "count_after_reference":
        reference = ctx["reference_date"] if kind == "date" else ctx["reference_timestamp"]
        return F.count(F.when(column > reference, 1))
    if op == "count_empty_collection":
        return F.count(F.when(column.isNotNull() & (F.size(column) == F.lit(0)), 1))
    if op == "min_size":
        return F.min(F.when(column.isNotNull(), F.size(column)))
    if op == "max_size":
        return F.max(F.when(column.isNotNull(), F.size(column)))
    if op == "mean_size":
        return F.avg(F.when(column.isNotNull(), F.size(column)))
    if op == "sum_size":
        return F.sum(F.when(column.isNotNull(), F.size(column)))
    if op == "count_null_elements":
        return F.sum(
            F.when(column.isNotNull(), F.size(F.filter(column, lambda item: item.isNull())))
        )
    if op == "count_null_map_values":
        return F.sum(
            F.when(
                column.isNotNull(),
                F.size(F.filter(F.map_values(column), lambda item: item.isNull())),
            )
        )
    if op == "count_json_invalid":
        parsed = F.call_function("try_parse_json", column)
        return F.count(F.when(column.isNotNull() & parsed.isNull(), 1))
    raise ValueError(f"unknown aggregate op: {op}")


# --------------------------------------------------------------------------- deep expressions


def _sp_inner(value: Any, names: Sequence[str]) -> Any:
    for name in names:
        value = value.getField(name)
    return value


def _sp_elements(params: Mapping[str, Any], nodes_by_id: Mapping[str, Any]) -> tuple[Any, Any]:
    """Return ``(collection column, per-row array of its elements, keys or values)``."""
    collection = column_for(nodes_by_id[params["collection_field_id"]]["path"])
    if params["segment"] == "map_key":
        return collection, F.map_keys(collection)
    if params["segment"] == "map_value":
        return collection, F.map_values(collection)
    return collection, collection


def _sp_leaf_values(elements: Any, inner: Sequence[str]) -> Any:
    names = list(inner)
    return F.transform(elements, lambda item: _sp_inner(item, names)) if names else elements


def _sp_finite_value(value: Any) -> Any:
    return (
        value.isNotNull() & ~F.isnan(value) & (value != F.lit(_SP_INF)) & (value != F.lit(-_SP_INF))
    )


def compile_element_spec(
    spec: Mapping[str, Any], node: Mapping[str, Any], ctx: Mapping[str, Any]
) -> Any:
    """Compile an element metric: per-row higher-order functions aggregated over rows.

    No ``explode``: every expression reads each row's collection once, inside
    the shared aggregation pass, and is exact over the analysed scope.
    """
    op = spec["op"]
    params = spec["params"]
    kind = node["type"]["kind"]
    collection, elements = _sp_elements(params, ctx["nodes_by_id"])
    inner = list(params["inner"])
    values = _sp_leaf_values(elements, inner)

    def present(value: Any) -> Any:
        return _sp_finite_value(value) if kind == "float" else value.isNotNull()

    def count(predicate: Callable[[Any], Any]) -> Any:
        return F.sum(F.when(collection.isNotNull(), F.size(F.filter(values, predicate))))

    if op == "el_count_null":
        return count(lambda value: value.isNull())
    if op == "el_count_null_parent_present":
        parent = list(params["parent_inner"])
        return F.sum(
            F.when(
                collection.isNotNull(),
                F.size(
                    F.filter(
                        elements,
                        lambda item: (
                            _sp_inner(item, parent).isNotNull() & _sp_inner(item, inner).isNull()
                        ),
                    )
                ),
            )
        )
    if op in ("el_min", "el_max"):
        kept = F.filter(values, present) if kind == "float" else values
        agg = F.min(F.array_min(kept)) if op == "el_min" else F.max(F.array_max(kept))
        if kind == "timestamp":
            return F.date_format(agg, TIMESTAMP_PATTERN)
        if kind == "timestamp_ntz":
            return F.date_format(agg, TIMESTAMP_NTZ_PATTERN)
        return agg
    if op == "el_count_zero":
        return count(lambda value: present(value) & (value == F.lit(0)))
    if op == "el_count_negative":
        return count(lambda value: present(value) & (value < F.lit(0)))
    if op == "el_count_positive":
        return count(lambda value: present(value) & (value > F.lit(0)))
    if op == "el_count_nan":
        return count(lambda value: F.isnan(value))
    if op == "el_count_pos_inf":
        return count(lambda value: value == F.lit(_SP_INF))
    if op == "el_count_neg_inf":
        return count(lambda value: value == F.lit(-_SP_INF))
    if op == "el_count_finite":
        return count(_sp_finite_value)
    if op == "el_count_empty":
        return count(lambda value: value == F.lit(""))
    if op == "el_count_whitespace_only":
        return count(lambda value: (value != F.lit("")) & value.rlike("^\\s+$"))
    if op in ("el_min_length", "el_max_length"):
        lengths = F.transform(values, lambda value: F.length(value))
        if op == "el_min_length":
            return F.min(F.array_min(lengths))
        return F.max(F.array_max(lengths))
    if op == "el_count_true":
        return count(lambda value: value == F.lit(True))
    if op == "el_count_false":
        return count(lambda value: value == F.lit(False))
    if op == "el_count_after_reference":
        reference = ctx["reference_date"] if kind == "date" else ctx["reference_timestamp"]
        return count(lambda value: value > reference)
    raise ValueError(f"unknown element op: {op}")


def compile_json_spec(spec: Mapping[str, Any], node: Mapping[str, Any]) -> Any:
    """Compile a full-scope JSON path check (the path is a literal argument, never SQL)."""
    op = spec["op"]
    params = spec["params"]
    column = column_for(node["path"])
    if params["method"] == "variant":
        parsed = F.call_function("try_parse_json", column)
        if op == "json_count_documents":
            root = F.call_function("schema_of_variant", parsed)
            return F.count(F.when(root.rlike("^(OBJECT|ARRAY)"), 1))
        value = F.call_function("try_variant_get", parsed, F.lit(params["path"]), F.lit("variant"))
        if op == "json_path_present":
            return F.count(F.when(value.isNotNull(), 1))
        if op == "json_path_null":
            return F.count(F.when(F.call_function("is_variant_null", value), 1))
        if op == "json_path_type_match":
            pattern = JSON_TYPE_PATTERNS[params["expected_type"]]
            return F.count(F.when(F.call_function("schema_of_variant", value).rlike(pattern), 1))
    elif params["method"] == "get_json_object":
        if op == "json_count_documents":
            root = F.ltrim(F.get_json_object(column, "$"))
            return F.count(F.when(F.substring(root, 1, 1).isin("{", "["), 1))
        if op == "json_path_non_null":
            return F.count(F.when(F.get_json_object(column, params["path"]).isNotNull(), 1))
    raise ValueError(f"unknown JSON op or method: {op} ({params.get('method')})")


def _sp_data_type(schema: Any, path: Sequence[Mapping[str, Any]]) -> Any:
    """Return the Spark DataType at a typed path of a DataFrame schema."""
    data_type = schema
    for segment in path:
        kind = segment["kind"]
        if kind == "field":
            data_type = data_type[segment["name"]].dataType
        elif kind == "array_element":
            data_type = data_type.elementType
        elif kind == "map_key":
            data_type = data_type.keyType
        else:
            data_type = data_type.valueType
    return data_type


def _sp_slot_builder(index: int, types: Sequence[Any]) -> Callable[[Any], Any]:
    """Wrap one element value in a struct with one typed slot per leaf (only its own is set)."""

    def build(value: Any) -> Any:
        slots = [
            (value if slot == index else F.lit(None).cast(types[slot])).alias(f"s{slot}")
            for slot in range(len(types))
        ]
        return F.struct(F.lit(index).alias("l"), *slots)

    return build


def run_element_distinct(
    frame: Any, plan: Mapping[str, Any], nodes_by_id: Mapping[str, Any]
) -> dict[str, Any]:
    """Run the single explode pass of the deep level and return raw counts.

    All element fields are exploded together in one action: each row's
    elements become structs tagged with their leaf, concatenated and exploded
    once. In ``sample`` mode only a bounded prefix (or random) sample of rows is
    read; in every mode at most ``max_elements`` exploded elements are
    aggregated. Only counts come back to the driver, never values.
    """
    leaves = plan["leaves"]
    schema = frame.schema
    types = [_sp_data_type(schema, nodes_by_id[leaf["field_id"]]["path"]) for leaf in leaves]
    arrays = []
    for index, leaf in enumerate(leaves):
        _, elements = _sp_elements(leaf, nodes_by_id)
        values = _sp_leaf_values(elements, leaf["inner"])
        empty = F.array_repeat(F.lit(None).cast(types[index]), 0)
        arrays.append(F.transform(F.coalesce(values, empty), _sp_slot_builder(index, types)))
    combined = arrays[0] if len(arrays) == 1 else F.concat(*arrays)
    source = frame
    if plan["mode"] == "sample":
        if plan["sampling_method"] == "random":
            source = source.sample(
                withReplacement=False, fraction=float(plan["random_fraction"]), seed=plan["seed"]
            )
        source = source.limit(plan["max_rows"])
    exploded = source.select(F.posexplode(combined).alias("pos", "e")).limit(plan["max_elements"])
    element = F.col("e")
    aggregates = [
        F.count(F.lit(1)).alias("n"),
        F.count(F.when(F.col("pos") == F.lit(0), 1)).alias("rows"),
    ]
    for index in range(len(leaves)):
        slot = element.getField(f"s{index}")
        aggregates += [
            F.count(F.when(element.getField("l") == F.lit(index), 1)).alias(f"n{index}"),
            F.count(slot).alias(f"nn{index}"),
            F.countDistinct(slot).alias(f"d{index}"),
        ]
    row = exploded.agg(*aggregates).collect()[0]
    return {key: (0 if value is None else int(value)) for key, value in row.asDict().items()}


def _sp_any_null(columns: Sequence[Any]) -> Any:
    condition = columns[0].isNull()
    for column in columns[1:]:
        condition = condition | column.isNull()
    return condition


def run_uniqueness_pass(
    frame: Any, keys: Sequence[Mapping[str, Any]], nodes_by_id: Mapping[str, Any]
) -> dict[int, dict[str, Any]]:
    """Check several keys exactly in one Spark action and return counts per key position.

    Each row becomes one entry per key: a struct with the key position, a flag
    for NULL in any key column and one typed slot per key column of every key
    (only the entry's own slots are set). The entries are exploded once,
    grouped by position, flag and slots, and the group sizes are aggregated per
    key. Only counts come back to the driver, never key values.
    """
    schema = frame.schema
    slots: list[tuple[int, Any, Any]] = []
    for position, key in enumerate(keys):
        for fid in key["field_ids"]:
            path = nodes_by_id[fid]["path"]
            slots.append((position, column_for(path), _sp_data_type(schema, path)))
    names = [f"s{index}" for index in range(len(slots))]

    def entry(position: int) -> Any:
        own = [column for owner, column, _ in slots if owner == position]
        values = [
            (column if owner == position else F.lit(None).cast(data_type)).alias(names[index])
            for index, (owner, column, data_type) in enumerate(slots)
        ]
        return F.struct(F.lit(position).alias("l"), _sp_any_null(own).alias("z"), *values)

    if len(keys) == 1:
        exploded = frame.select(entry(0).alias("e"))
    else:
        entries = F.array(*[entry(position) for position in range(len(keys))])
        exploded = frame.select(F.explode(entries).alias("e"))
    element = F.col("e")
    flat = exploded.select(*[element.getField(name).alias(name) for name in ("l", "z", *names)])
    groups = flat.groupBy("l", "z", *names).agg(F.count(F.lit(1)).alias("n"))
    complete = ~F.col("z")
    repeated = complete & (F.col("n") > F.lit(1))
    stats = groups.groupBy("l").agg(
        F.sum("n").alias("rows"),
        F.sum(F.when(F.col("z"), F.col("n"))).alias("null_rows"),
        F.count(F.when(complete, 1)).alias("distinct"),
        F.count(F.when(repeated, 1)).alias("dup_groups"),
        F.sum(F.when(repeated, F.col("n"))).alias("dup_rows"),
        F.max(F.when(complete, F.col("n"))).alias("max_n"),
    )
    out: dict[int, dict[str, Any]] = {}
    for row in stats.collect():
        values = row.asDict()
        out[int(values.pop("l"))] = {
            key: (None if value is None else int(value)) for key, value in values.items()
        }
    return out


def run_inclusion_check(
    source: Any,
    source_columns: Sequence[Any],
    target: Any,
    target_columns: Sequence[Any],
    *,
    sample: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Count source rows whose complete key is absent from the target, in one Spark action.

    The target is grouped by its key (distinct values and their multiplicity),
    the source is left-joined to it and both sides are aggregated; the two
    single-row aggregates are cross-joined and collected once. With ``sample``
    the source is limited to a bounded prefix (or random) sample first. Only
    counts come back to the driver, never key values.
    """
    keys = [column.alias(f"k{index}") for index, column in enumerate(source_columns)]
    rows = source.select(*keys, _sp_any_null(list(source_columns)).alias("z"))
    if sample is not None:
        if sample["method"] == "random":
            rows = rows.sample(
                withReplacement=False, fraction=float(sample["fraction"]), seed=sample["seed"]
            )
        rows = rows.limit(sample["max_rows"])
    names = [f"t{index}" for index in range(len(target_columns))]
    aliased = [column.alias(name) for column, name in zip(target_columns, names, strict=True)]
    groups = (
        target.select(*aliased, _sp_any_null(list(target_columns)).alias("tz"))
        .groupBy(*names, "tz")
        .agg(F.count(F.lit(1)).alias("tn"))
    )
    distinct = groups.where(~F.col("tz")).select(*names, "tn")
    condition = None
    for index, name in enumerate(names):
        equal = F.col(f"k{index}") == F.col(name)
        condition = equal if condition is None else condition & equal
    joined = rows.join(distinct, on=condition, how="left")
    source_stats = joined.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("z"), 1)).alias("null_rows"),
        F.count(F.when(~F.col("z") & F.col("tn").isNull(), 1)).alias("orphans"),
    )
    target_stats = groups.agg(
        F.sum("tn").alias("t_rows"),
        F.sum(F.when(F.col("tz"), F.col("tn"))).alias("t_null_rows"),
        F.count(F.when(~F.col("tz"), 1)).alias("t_distinct"),
        F.count(F.when(~F.col("tz") & (F.col("tn") > F.lit(1)), 1)).alias("t_dup_groups"),
    )
    row = source_stats.crossJoin(target_stats).collect()[0]
    return {key: (0 if value is None else int(value)) for key, value in row.asDict().items()}


# --------------------------------------------------------------------------- metadata


def _sp_formatted_rows(frame: Any) -> list[dict[str, Any]]:
    """Collect a small metadata result, formatting timestamps server-side as ISO 8601."""
    columns = []
    for field in frame.schema.fields:
        type_name = type(field.dataType).__name__
        if type_name == "TimestampType":
            columns.append(
                F.date_format(F.col(quote_name(field.name)), TIMESTAMP_PATTERN).alias(field.name)
            )
        else:
            columns.append(F.col(quote_name(field.name)))
    return [row.asDict(recursive=True) for row in frame.select(*columns).collect()]


def read_describe_extended(spark: Any, quoted: str) -> dict[str, str]:
    """Return the ``# Detailed Table Information`` section of DESCRIBE TABLE EXTENDED."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {quoted}").collect()
    info: dict[str, str] = {}
    in_detail = False
    for row in rows:
        name = (row[0] or "").strip()
        value = "" if row[1] is None else str(row[1])
        if name.startswith("# Detailed Table Information"):
            in_detail = True
            continue
        if in_detail and name and not name.startswith("#"):
            info[name] = value
    return info


def read_describe_detail(spark: Any, quoted: str) -> dict[str, Any]:
    """Return DESCRIBE DETAIL (Delta) without location, owner or identifiers."""
    rows = _sp_formatted_rows(spark.sql(f"DESCRIBE DETAIL {quoted}"))
    if not rows:
        return {}
    row = rows[0]
    keep = (
        "format",
        "sizeInBytes",
        "numFiles",
        "partitionColumns",
        "clusteringColumns",
        "createdAt",
        "lastModified",
    )
    detail = {key: row.get(key) for key in keep if key in row}
    properties = row.get("properties") or {}
    detail["check_constraints"] = {
        key[len("delta.constraints.") :]: str(value)
        for key, value in sorted(properties.items())
        if key.startswith("delta.constraints.")
    }
    return detail


def read_latest_version(spark: Any, quoted: str) -> tuple[int, str | None]:
    """Return the latest Delta version and its commit timestamp (DESCRIBE HISTORY LIMIT 1)."""
    rows = _sp_formatted_rows(spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT 1"))
    if not rows:
        raise ValueError("DESCRIBE HISTORY returned no rows")
    return int(rows[0]["version"]), rows[0].get("timestamp")


def _sp_full_name(spark: Any, parts: list[str]) -> list[str] | None:
    if len(parts) == 3:
        return list(parts)
    try:
        catalog = spark.catalog.currentCatalog()
        if len(parts) == 2:
            return [catalog, *parts]
        return [catalog, spark.catalog.currentDatabase(), parts[0]]
    except Exception:  # noqa: BLE001
        return None


def read_unity_constraints(spark: Any, parts: list[str]) -> tuple[list[dict[str, Any]], str | None]:
    """Read PRIMARY/FOREIGN KEY/UNIQUE constraints from Unity Catalog information_schema.

    Returns ``(constraints, note)``; ``note`` explains when nothing could be read.
    """
    full = _sp_full_name(spark, parts)
    if full is None or full[0].casefold() in _SP_UNITY_EXCLUDED:
        return [], "declared key constraints are read from Unity Catalog information_schema only"
    catalog, schema, table = full
    return (
        query_key_constraints(
            spark, catalog, schema, table, lambda name: quote_name(name) + ".information_schema"
        ),
        None,
    )


def query_key_constraints(
    spark: Any,
    catalog: str,
    schema: str,
    table: str,
    information_schema: Callable[[str], str],
) -> list[dict[str, Any]]:
    """Query key constraints of one table from ``information_schema``-shaped views.

    ``information_schema`` maps a catalog name to the quoted prefix of its
    ``information_schema`` (Unity Catalog: ``<catalog>.information_schema``).
    Table and schema names are bound parameters, never interpolated.
    """
    info = information_schema(catalog)
    query = (
        "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position, "
        "kcu.position_in_unique_constraint, rc.unique_constraint_catalog, "
        "rc.unique_constraint_schema, "
        "rc.unique_constraint_name "
        f"FROM {info}.table_constraints tc "
        f"JOIN {info}.key_column_usage kcu ON tc.constraint_catalog = kcu.constraint_catalog "
        "AND tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = "
        "kcu.constraint_name "
        f"LEFT JOIN {info}.referential_constraints rc ON tc.constraint_catalog = "
        "rc.constraint_catalog "
        "AND tc.constraint_schema = rc.constraint_schema AND tc.constraint_name = "
        "rc.constraint_name "
        "WHERE lower(tc.table_schema) = lower(:schema_name) AND lower(tc.table_name) = "
        "lower(:table_name) "
        "ORDER BY tc.constraint_name, kcu.ordinal_position"
    )
    rows = [
        row.asDict()
        for row in spark.sql(query, args={"schema_name": schema, "table_name": table}).collect()
    ]
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = grouped.setdefault(
            row["constraint_name"],
            {
                "type": str(row["constraint_type"]).upper(),
                "columns": [],
                "positions": [],
                "ref": None,
            },
        )
        entry["columns"].append(row["column_name"])
        entry["positions"].append(row.get("position_in_unique_constraint"))
        if row.get("unique_constraint_name"):
            entry["ref"] = (
                row["unique_constraint_catalog"],
                row["unique_constraint_schema"],
                row["unique_constraint_name"],
            )
    constraints = []
    kinds = {"PRIMARY KEY": "primary_key", "FOREIGN KEY": "foreign_key", "UNIQUE": "unique"}
    for name, entry in grouped.items():
        kind = kinds.get(entry["type"])
        if kind is None:
            continue
        referenced = None
        if kind == "foreign_key" and entry["ref"]:
            ref_catalog, ref_schema, ref_name = entry["ref"]
            ref_rows = spark.sql(
                "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position "
                f"FROM {information_schema(ref_catalog)}.key_column_usage "
                "WHERE lower(constraint_schema) = lower(:schema_name) AND constraint_name = "
                ":constraint_name "
                "ORDER BY ordinal_position",
                args={"schema_name": ref_schema, "constraint_name": ref_name},
            ).collect()
            if ref_rows:
                by_position = {int(r["ordinal_position"]): r["column_name"] for r in ref_rows}
                columns = [
                    by_position.get(int(position) if position is not None else index + 1, "?")
                    for index, position in enumerate(entry["positions"])
                ]
                first = ref_rows[0]
                referenced = {
                    "table": table_key(
                        [first["table_catalog"], first["table_schema"], first["table_name"]]
                    ),
                    "columns": columns,
                }
        constraints.append(
            {
                "name": name,
                "constraint_type": kind,
                "columns": list(entry["columns"]),
                "expression": None,
                "referenced": referenced,
                "enforcement": "not_enforced",
                "source": "information_schema",
            }
        )
    return constraints


# --------------------------------------------------------------------------- sample


def collect_sample(
    frame: Any, nodes: Sequence[Mapping[str, Any]], plan: Mapping[str, Any]
) -> dict[str, Any]:
    """Collect a projected, bounded sample of string fields.

    Values are cut server-side to ``max_value_chars`` (truncated values are
    flagged and excluded from inference). Rows are retained until the payload
    budget ``max_bytes`` would be exceeded; iteration then stops. The payload
    budget accounts for retained values, not total process memory.
    """
    max_chars = plan["max_value_chars"]
    expressions = []
    for index, node in enumerate(nodes):
        column = column_for(node["path"])
        expressions.append(F.substring(column, 1, max_chars).alias(f"s{index}"))
        expressions.append((F.length(column) > F.lit(max_chars)).alias(f"t{index}"))
    source = frame
    if plan["method"] == "random":
        source = source.sample(
            withReplacement=False, fraction=float(plan["random_fraction"]), seed=plan["seed"]
        )
    limited = source.select(*expressions).limit(plan["max_rows"])
    try:
        rows_iter: Any = limited.toLocalIterator()
    except (AttributeError, NotImplementedError):
        rows_iter = iter(limited.collect())
    fields: dict[str, dict[str, Any]] = {
        node["field_id"]: {"values": [], "nulls": 0, "truncated": 0} for node in nodes
    }
    rows = 0
    used = 0
    stopped = "exhausted"
    for row in rows_iter:
        pending = []
        row_bytes = 0
        for index, node in enumerate(nodes):
            value = row[f"s{index}"]
            if value is None:
                pending.append((node["field_id"], "null", None))
            elif row[f"t{index}"]:
                pending.append((node["field_id"], "truncated", None))
            else:
                row_bytes += len(value.encode("utf-8"))
                pending.append((node["field_id"], "value", value))
        if used + row_bytes > plan["max_bytes"]:
            stopped = "byte_budget"
            break
        used += row_bytes
        rows += 1
        for fid, status, value in pending:
            if status == "null":
                fields[fid]["nulls"] += 1
            elif status == "truncated":
                fields[fid]["truncated"] += 1
            else:
                fields[fid]["values"].append(value)
    if stopped == "exhausted" and rows >= plan["max_rows"]:
        stopped = "row_limit"
    return {"rows": rows, "bytes": used, "stopped_reason": stopped, "fields": fields}


# --------------------------------------------------------------------------- aggregates


def run_aggregates(
    frame: Any,
    plan: Mapping[str, Any],
    nodes_by_id: Mapping[str, Mapping[str, Any]],
    ctx: Mapping[str, Any],
    observed: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]:
    """Execute the planned aggregation passes.

    Returns ``(results by alias, failed aliases with reasons, errors)``. An
    expression rejected at analysis time (no data read) is isolated so the
    rest of its pass still runs; a failure during execution fails the pass.
    """
    compiled: dict[str, Any] = {}
    failed: dict[str, str] = {}
    errors: list[dict[str, Any]] = []
    for spec in plan["specs"]:
        node = nodes_by_id.get(spec["field_id"]) if spec["field_id"] else None
        parent = nodes_by_id.get(spec["params"].get("parent_field_id", ""))
        try:
            compiled[spec["alias"]] = compile_spec(spec, node, parent, ctx).alias(spec["alias"])
        except Exception as exc:  # noqa: BLE001
            failed[spec["alias"]] = "expression could not be built: " + sanitize_message(
                str(exc), 200
            )
    results: dict[str, Any] = {}
    for index, aliases in enumerate(plan["passes"]):
        op_id = f"op_aggregate_{index + 1}"
        active = [alias for alias in aliases if alias in compiled]
        if not active:
            observed.append(_sp_observed(op_id, "skipped", None, detail="no valid expressions"))
            continue
        start = time.perf_counter()
        try:
            try:
                aggregated = frame.agg(*[compiled[alias] for alias in active])
                _ = aggregated.schema
            except Exception:  # noqa: BLE001 - isolate expressions rejected by the analyzer
                valid = []
                for alias in active:
                    try:
                        _ = frame.agg(compiled[alias]).schema
                        valid.append(alias)
                    except Exception as exc:  # noqa: BLE001
                        record = error_record(exc, "aggregate")
                        failed[alias] = "expression rejected by the engine: " + (
                            record["condition"] or record["error_class"]
                        )
                active = valid
                if not active:
                    observed.append(
                        _sp_observed(op_id, "skipped", start, detail="all expressions rejected")
                    )
                    continue
                aggregated = frame.agg(*[compiled[alias] for alias in active])
            row = aggregated.collect()[0]
            results.update(row.asDict())
            observed.append(_sp_observed(op_id, "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "aggregate")
            errors.append(record)
            for alias in active:
                failed.setdefault(
                    alias,
                    "aggregation pass failed: " + (record["condition"] or record["error_class"]),
                )
            observed.append(
                _sp_observed(
                    op_id, "failed", start, detail=record["condition"] or record["error_class"]
                )
            )
    return results, failed, errors


# --------------------------------------------------------------------------- table profile


def _sp_source(
    extended: Mapping[str, str], detail: Mapping[str, Any] | None, captured_at: str
) -> dict[str, Any]:
    table_type = extended.get("Type") or None
    source_type = "unknown"
    if table_type:
        source_type = "view" if "VIEW" in table_type.upper() else "table"
    provider = extended.get("Provider") or None
    notes = [
        "Catalog metadata reflects the table state when it was captured, not necessarily the "
        "pinned snapshot."
    ]
    size_metric: dict[str, Any]
    files_metric: dict[str, Any]
    if detail and detail.get("sizeInBytes") is not None:
        size_metric = measured(
            "size_in_bytes",
            int(detail["sizeInBytes"]),
            unit="bytes",
            scope="table_metadata",
            accuracy="as_recorded",
            source="catalog_metadata",
            method="DESCRIBE DETAIL sizeInBytes (latest table state)",
        )
    else:
        size_metric = not_measured(
            "size_in_bytes",
            "unavailable",
            "not provided by catalog metadata for this source",
            scope="table_metadata",
            source="catalog_metadata",
        )
    if detail and detail.get("numFiles") is not None:
        files_metric = measured(
            "file_count",
            int(detail["numFiles"]),
            unit="files",
            scope="table_metadata",
            accuracy="as_recorded",
            source="catalog_metadata",
            method="DESCRIBE DETAIL numFiles (latest table state)",
        )
    else:
        files_metric = not_measured(
            "file_count",
            "unavailable",
            "not provided by catalog metadata for this source",
            scope="table_metadata",
            source="catalog_metadata",
        )
    return {
        "source_type": source_type,
        "table_type": table_type,
        "provider": provider,
        "comment": extended.get("Comment") or None,
        "created_at": (detail or {}).get("createdAt") or extended.get("Created Time") or None,
        "last_modified": (detail or {}).get("lastModified"),
        "partition_columns": [str(c) for c in (detail or {}).get("partitionColumns") or []],
        "clustering_columns": [str(c) for c in (detail or {}).get("clusteringColumns") or []],
        "size_in_bytes": size_metric,
        "file_count": files_metric,
        "metadata_state": "current_at_capture",
        "captured_at": captured_at,
        "notes": notes,
    }


def _sp_statistics_row_count(extended: Mapping[str, str]) -> dict[str, Any]:
    match = _SP_STATS.search(extended.get("Statistics", ""))
    if match and match.group(2) is not None:
        return measured(
            "row_count",
            int(match.group(2)),
            unit="rows",
            scope="table_metadata",
            accuracy="as_recorded",
            source="table_statistics",
            method="row count recorded in catalog statistics (e.g. by an earlier ANALYZE TABLE)",
            details={
                "freshness": "unknown",
                "limitations": "Pre-existing statistics may be stale and describe the whole table, "
                "never a filtered scope.",
            },
        )
    return not_measured(
        "row_count",
        "unavailable",
        "no row count recorded in catalog statistics; the metadata level does not scan rows "
        "and never runs ANALYZE TABLE",
        scope="table_metadata",
        source="table_statistics",
    )


def _sp_omission_reason(
    fid: str,
    top_name: str,
    reasons: Mapping[str, str],
    parents: Mapping[str, Mapping[str, Any]],
    selected: set[str] | None,
) -> str:
    current: str | None = fid
    while current is not None:
        if current in reasons:
            return reasons[current]
        parent = parents.get(current)
        current = parent["field_id"] if parent else None
    if selected is not None and top_name not in selected:
        return "not_selected"
    return "inside_collection"


def profile_table(
    spark: Any,
    input_text: str,
    config: Mapping[str, Any],
    *,
    capabilities: Mapping[str, Mapping[str, Any]],
    reference_time: str,
    now: Callable[[], str],
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Profile one table and return its contract record (never raises for data errors)."""
    total_start = time.perf_counter()
    parts = parse_table_identifier(input_text)
    key = table_key(parts)
    quoted = quote_table_identifier(parts)
    table = new_table(input_text, parts, key, table_id(parts), quoted)
    options = table_options_for(config, table_lookup_key(parts))
    level = config["analysis_level"]
    filters = list(options.get("filters", []))
    selected = options.get("columns")
    table["purpose"] = options.get("purpose")
    table["scope"]["filters"] = filters
    table["scope"]["selected_columns"] = list(selected) if selected is not None else None
    table["scope"]["population"] = "filtered" if filters else "full"
    planned = table["operations"]["planned"]
    observed = table["operations"]["observed"]
    timings = table["timings_ms"]

    # 1. catalog metadata -------------------------------------------------------
    metadata_start = time.perf_counter()
    planned.append(
        operation(
            "op_describe",
            "catalog_metadata",
            "DESCRIBE TABLE EXTENDED (catalog metadata)",
            reads_user_data=False,
        )
    )
    try:
        extended = read_describe_extended(spark, quoted)
        observed.append(_sp_observed("op_describe", "succeeded", metadata_start))
    except Exception as exc:  # noqa: BLE001
        observed.append(
            _sp_observed(
                "op_describe", "failed", metadata_start, detail="table could not be resolved"
            )
        )
        table["errors"].append(error_record(exc, "resolve"))
        log(
            f"[tabledossier] {key}: could not be resolved "
            f"({table['errors'][-1]['condition'] or table['errors'][-1]['error_class']})"
        )
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    is_view = "VIEW" in (extended.get("Type") or "").upper()
    detail = None
    if not is_view:
        planned.append(
            operation(
                "op_detail",
                "table_detail",
                "DESCRIBE DETAIL (Delta format, size and files)",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            detail = read_describe_detail(spark, quoted)
            observed.append(_sp_observed("op_detail", "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "metadata")
            cause = record["condition"] or record["error_class"]
            provider = (extended.get("Provider") or "").strip().lower()
            if provider and provider != "delta":
                # Expected: engines without Delta (or Delta itself) may reject DESCRIBE DETAIL
                # for other formats. Size and file count are then simply unavailable.
                observed.append(
                    _sp_observed(
                        "op_detail",
                        "skipped",
                        start,
                        detail=f"not available for this non-Delta source (provider {provider}; "
                        f"{cause})",
                    )
                )
                table["notes"].append(
                    f"DESCRIBE DETAIL is not available for this non-Delta source (provider "
                    f"{provider}); size and file count are unavailable."
                )
            else:
                observed.append(_sp_observed("op_detail", "failed", start, detail=cause))
                table["notes"].append(
                    "DESCRIBE DETAIL failed for this source (not permitted or not supported)."
                )
    table["source"] = _sp_source(extended, detail, now())
    is_delta = (detail or {}).get("format") == "delta" or (
        extended.get("Provider") or ""
    ).lower() == "delta"

    version = None
    version_timestamp = None
    if level in ROW_READING_LEVELS and is_delta and config["consistency"]["pin_delta_version"]:
        planned.append(
            operation(
                "op_history",
                "table_history",
                "DESCRIBE HISTORY LIMIT 1 (Delta version to pin)",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            version, version_timestamp = read_latest_version(spark, quoted)
            observed.append(_sp_observed("op_history", "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "snapshot")
            observed.append(
                _sp_observed(
                    "op_history",
                    "failed",
                    start,
                    detail=record["condition"] or record["error_class"],
                )
            )
            table["notes"].append(
                "The Delta version could not be resolved; reads are not pinned to one snapshot."
            )

    constraints: list[dict[str, Any]] = []
    for name, expression in sorted(((detail or {}).get("check_constraints") or {}).items()):
        constraints.append(
            {
                "name": name,
                "constraint_type": "check",
                "columns": [],
                "expression": expression,
                "referenced": None,
                "enforcement": "enforced",
                "source": "delta_table_property",
            }
        )
    if capabilities.get("parameterized_sql", {}).get("available"):
        planned.append(
            operation(
                "op_constraints",
                "information_schema",
                "Unity Catalog information_schema key constraints",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            declared, note = read_unity_constraints(spark, parts)
            constraints.extend(declared)
            observed.append(
                _sp_observed(
                    "op_constraints", "skipped" if note else "succeeded", start, detail=note
                )
            )
            if note:
                table["notes"].append(note[0].upper() + note[1:] + ".")
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "metadata")
            observed.append(
                _sp_observed(
                    "op_constraints",
                    "failed",
                    start,
                    detail=record["condition"] or record["error_class"],
                )
            )
            table["notes"].append(
                "Declared key constraints could not be read from information_schema."
            )
    table["constraints"] = constraints

    try:
        if version is not None:
            base = spark.sql(f"SELECT * FROM {quoted} VERSION AS OF {int(version)}")
        else:
            base = spark.table(quoted)
        schema = base.schema
    except Exception as exc:  # noqa: BLE001
        table["errors"].append(error_record(exc, "resolve"))
        timings["metadata"] = _sp_ms(metadata_start)
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    limits = config["limits"]
    tree = build_schema_tree(
        neutral_fields(schema), max_depth=limits["max_depth"], max_fields=limits["max_fields"]
    )
    tree["captured_from"] = "pinned_snapshot" if version is not None else "current_table"
    table["schema"] = tree
    timings["metadata"] = _sp_ms(metadata_start)

    pinned = version is not None
    if level == "metadata":
        table["consistency"] = {
            "mode": "metadata_only",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                "Metadata only: no table rows were read. The schema reflects the current "
                "table definition and catalog metadata reflects the state at capture time."
            ),
            "notes": [],
        }
        table["scope"]["scope_label"] = "table_metadata"
    elif pinned:
        table["consistency"] = {
            "mode": "pinned_delta_version",
            "delta_version": int(version) if version is not None else None,
            "version_timestamp": version_timestamp,
            "guarantee": f"All row reads (sample and aggregations) used Delta version {version}, "
            "resolved at the start of this table's profile.",
            "notes": [
                "Catalog metadata (size, files, comments) describes the latest state at capture "
                "time."
            ],
        }
    else:
        unpinned_reason = (
            "the source is a view"
            if is_view
            else "the source is not a Delta table"
            if not is_delta
            else "pinning is disabled or the version could not be resolved"
        )
        table["consistency"] = {
            "mode": "unpinned",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                f"No snapshot was pinned ({unpinned_reason}). The sample and each aggregation pass "
                "read the source independently and may observe different data if it "
                "changes during the run."
            ),
            "notes": [],
        }

    top_names = [field.name for field in schema.fields]
    if selected is not None:
        unknown = [name for name in selected if name not in top_names]
        if unknown:
            table["errors"].append(
                {
                    "stage": "resolve",
                    "error_class": "ConfigurationError",
                    "condition": "SELECTED_COLUMN_NOT_FOUND",
                    "message": f"{len(unknown)} selected column(s) not found in the table schema",
                }
            )
    schema_ids = {node["field_id"] for node in iter_nodes(tree["fields"])}
    for item in filters:
        if field_id(column_reference_segments(item["column"])) not in schema_ids:
            table["errors"].append(
                {
                    "stage": "resolve",
                    "error_class": "ConfigurationError",
                    "condition": "FILTER_COLUMN_NOT_FOUND",
                    "message": "a filter references a column that is not in the table schema",
                }
            )

    profiled, omissions = select_profile_fields(tree, selected)
    deep_plan = None
    if level == "deep":
        deep_plan = plan_deep_elements(tree, profiled, config, table_lookup_key(parts))
        replaced = set(deep_plan["contexts"]) | {o["field_id"] for o in deep_plan["omissions"]}
        omissions = [
            item
            for item in omissions
            if not (item["reason"] == "inside_collection" and item["field_id"] in replaced)
        ]
        omissions.extend(deep_plan["omissions"])
    element_contexts: dict[str, dict[str, Any]] = deep_plan["contexts"] if deep_plan else {}
    table["omissions"].extend(omissions)
    profiled_ids = {node["field_id"] for node in profiled}
    reasons = {item["field_id"]: item["reason"] for item in omissions if item["field_id"]}
    parents: dict[str, Any] = {}
    for node in iter_nodes(tree["fields"]):
        for child in node["children"]:
            parents[child["field_id"]] = node
    nodes_by_id = {node["field_id"]: node for node in iter_nodes(tree["fields"])}
    wanted = set(selected) if selected is not None else None
    for node in iter_nodes(tree["fields"]):
        fid = node["field_id"]
        reason: str | None = None
        is_profiled = fid in profiled_ids or fid in element_contexts
        if not is_profiled:
            reason = _sp_omission_reason(fid, node["path"][0]["name"], reasons, parents, wanted)
        record = field_profile(node, profiled=is_profiled, omission_reason=reason)
        if fid in element_contexts:
            record["element_context"] = dict(element_contexts[fid])
        table["field_profiles"].append(record)
    if level == "metadata":
        for field in table["field_profiles"]:
            field["profiled"] = False
            field["omission_reason"] = "metadata_level"
        table["table_metrics"].append(_sp_statistics_row_count(extended))
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    if any(error["stage"] == "resolve" for error in table["errors"]):
        timings["total"] = _sp_ms(total_start)
        table["schema"] = tree
        finalize_table(table, config)
        return table

    scope = scope_label(pinned, bool(filters))
    table["scope"]["scope_label"] = scope
    scoped = base.filter(filter_condition(filters)) if filters else base
    frame = scoped
    if selected is not None:
        frame = frame.select(*[F.col(quote_name(name)) for name in selected])

    # 2. transient sample ---------------------------------------------------------
    sample_plan = plan_sample(profiled, config)
    sample_nodes = [nodes_by_id[fid] for fid in sample_plan["field_ids"]]
    sample_record: dict[str, Any] = {
        "enabled": sample_plan["enabled"],
        "method": sample_plan["method"]
        if sample_plan["enabled"]
        else ("none" if config["sampling"]["method"] == "none" else config["sampling"]["method"]),
        "biased": True
        if sample_plan["method"] == "prefix"
        else (False if sample_plan["method"] == "random" else None),
        "reads_full_source": True if sample_plan["method"] == "random" else None,
        "max_rows": sample_plan["max_rows"],
        "max_bytes": sample_plan["max_bytes"],
        "max_value_chars": sample_plan["max_value_chars"],
        "random_fraction": sample_plan["random_fraction"],
        "seed": sample_plan["seed"],
        "rows_collected": None,
        "bytes_retained": None,
        "values_truncated": None,
        "stopped_reason": "disabled"
        if config["sampling"]["method"] == "none"
        else "not_applicable",
        "fields_sampled": len(sample_nodes),
        "fields_omitted": len(sample_plan["omitted_field_ids"]),
        "notes": [sample_plan["reason"]] if sample_plan["reason"] else [],
    }
    table["sample"] = sample_record
    json_ids: set[str] = set()
    semantic_by_id: dict[str, dict[str, Any]] = {}
    json_catalogs: dict[str, dict[str, Any]] = {}
    redacted_ids = policy_field_ids(config, table_lookup_key(parts), "redact_columns")
    sample_start = time.perf_counter()
    if sample_plan["enabled"]:
        sample_record["notes"] = [
            "Values are inspected transiently for format and JSON inference; they are not "
            "persisted.",
            "prefix: the first rows produced by the engine; potentially biased, not random."
            if sample_plan["method"] == "prefix"
            else (
                "random: Bernoulli sampling may evaluate every row in scope; the fraction "
                "does not reduce I/O proportionally."
            ),
        ]
        planned.append(
            operation(
                "op_sample",
                "sample_collect",
                f"projected {sample_plan['method']} sample of string fields, values cut to "
                f"{sample_plan['max_value_chars']} characters",
                reads_user_data=True,
                max_rows=sample_plan["max_rows"],
                max_bytes=sample_plan["max_bytes"],
                columns=len(sample_nodes),
            )
        )
        try:
            sample = collect_sample(frame, sample_nodes, sample_plan)
            observed.append(
                _sp_observed("op_sample", "succeeded", sample_start, rows=sample["rows"])
            )
            sample_record.update(
                {
                    "rows_collected": sample["rows"],
                    "bytes_retained": sample["bytes"],
                    "values_truncated": sum(
                        item["truncated"] for item in sample["fields"].values()
                    ),
                    "stopped_reason": sample["stopped_reason"],
                }
            )
            example_ids = (
                policy_field_ids(config, table_lookup_key(parts), "example_columns")
                if config["value_policy"]["persist_examples"]
                else set()
            )
            json_keys = config["value_policy"]["json_key_names"] == "include"
            for node in sample_nodes:
                data = sample["fields"][node["field_id"]]
                fmt = infer_format(
                    data["values"],
                    sql_nulls=data["nulls"],
                    truncated=data["truncated"],
                    sample_method=sample_plan["method"],
                    semantic_config=config["semantic"],
                )
                entry: dict[str, Any] = {
                    "observed_format": fmt,
                    "json_profile": None,
                    "concentration": None,
                    "examples": None,
                }
                if is_json_like(data["values"]) or fmt.get("format") in (
                    "json_object",
                    "json_array",
                ):
                    shape = json_shape(
                        data["values"],
                        sql_nulls=data["nulls"],
                        truncated=data["truncated"],
                        include_key_names=json_keys and node["field_id"] not in redacted_ids,
                        sample_method=sample_plan["method"],
                    )
                    entry["json_profile"] = shape
                    eligible = shape["eligible_observations"]
                    structured = shape["counts"]["object"] + shape["counts"]["array"]
                    if eligible and structured / eligible >= config["thresholds"]["json_min_ratio"]:
                        json_ids.add(node["field_id"])
                entry["concentration"] = value_concentration(data["values"])
                if node["field_id"] in example_ids and node["field_id"] not in redacted_ids:
                    entry["examples"] = value_examples(
                        data["values"],
                        max_examples=config["value_policy"]["max_examples_per_column"],
                        max_chars=config["value_policy"]["max_example_chars"],
                    )
                semantic_by_id[node["field_id"]] = entry
            if deep_plan is not None:
                json_catalogs = _sp_json_catalogs(
                    deep_plan, sample, json_ids, config, redacted_ids, sample_plan["method"]
                )
            del sample
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "sample")
            table["errors"].append(record)
            sample_record["stopped_reason"] = "error"
            observed.append(
                _sp_observed(
                    "op_sample",
                    "failed",
                    sample_start,
                    detail=record["condition"] or record["error_class"],
                )
            )
    timings["sample"] = _sp_ms(sample_start)

    # 3. shared aggregation passes (+ deep expressions) --------------------------
    redacted = set(redacted_ids)
    if config["value_policy"]["aggregate_extremes"] == "redact":
        redacted = set(profiled_ids) | set(element_contexts)
    caps = {name: bool(item.get("available")) for name, item in capabilities.items()}
    deep_candidates: list[dict[str, Any]] = []
    deep_static: list[dict[str, Any]] = []
    json_method, json_unsupported = _sp_json_method(config, caps) if deep_plan else (None, None)
    if deep_plan is not None:
        deep_candidates, deep_static = plan_element_specs(
            deep_plan["element_nodes"], element_contexts, scope=scope, redacted_field_ids=redacted
        )
        deep_candidates.extend(plan_json_validation(json_catalogs, json_method))
    agg_plan = plan_aggregates(
        profiled,
        config=config,
        capabilities=caps,
        scope=scope,
        json_field_ids=json_ids,
        redacted_field_ids=redacted,
        deep_candidates=deep_candidates,
        max_extra_passes=config["deep"]["max_extra_passes"] if deep_plan else 0,
    )
    for omission in agg_plan["omissions"]:
        table["omissions"].append(omission)
    for index, aliases in enumerate(agg_plan["passes"]):
        specs = [spec for spec in agg_plan["specs"] if spec["alias"] in set(aliases)]
        extra_pass = index >= agg_plan["standard_passes"]
        details: dict[str, Any] = {
            "expressions": sum(spec["cost"] for spec in specs),
            "fields": len({spec["field_id"] for spec in specs if spec["field_id"]}),
        }
        if deep_plan is not None:
            details["deep_expressions"] = sum(spec["cost"] for spec in specs if spec.get("group"))
        planned.append(
            operation(
                f"op_aggregate_{index + 1}",
                "deep_aggregate_pass" if extra_pass else "aggregate_pass",
                (
                    "extra aggregation for deep expressions that did not fit the standard passes "
                    "(counts against deep.max_extra_passes; single agg call)"
                    if extra_pass
                    else "one shared aggregation over the analysed scope (single agg call; not a "
                    "guarantee of a single physical scan)"
                ),
                reads_user_data=True,
                **details,
            )
        )
    distinct_plan = None
    if deep_plan is not None:
        distinct_plan = plan_element_distinct(deep_plan["element_nodes"], element_contexts, config)
        left = config["deep"]["max_extra_passes"] - agg_plan["extra_passes"]
        if distinct_plan["enabled"] and left < 1:
            distinct_plan["enabled"] = False
            distinct_plan["reason"] = (
                "no extra pass left: deep.max_extra_passes = "
                f"{config['deep']['max_extra_passes']} used by {agg_plan['extra_passes']} "
                "aggregation overflow pass(es)"
            )
            distinct_plan["budget_limited"] = True
        if distinct_plan["enabled"]:
            planned.append(
                operation(
                    "op_deep_elements",
                    "element_explode_pass",
                    (
                        f"one explode of the element fields over a {config['sampling']['method']} "
                        f"sample of at most {distinct_plan['max_rows']} rows, stopped at "
                        f"{distinct_plan['max_elements']} elements (distinct counts only)"
                        if distinct_plan["mode"] == "sample"
                        else "one explode of the element fields over the full scope when its "
                        f"measured size is at most {distinct_plan['max_elements']} elements "
                        "(distinct counts only)"
                    ),
                    reads_user_data=True,
                    mode=distinct_plan["mode"],
                    max_rows=distinct_plan["max_rows"]
                    if distinct_plan["mode"] == "sample"
                    else None,
                    max_elements=distinct_plan["max_elements"],
                    fields=len(distinct_plan["leaves"]),
                )
            )
    ctx = {
        "reference_timestamp": F.lit(reference_time).cast("timestamp"),
        "reference_date": F.lit(reference_time[:10]).cast("date"),
        "nodes_by_id": nodes_by_id,
    }
    aggregate_start = time.perf_counter()
    results, failed, agg_errors = run_aggregates(frame, agg_plan, nodes_by_id, ctx, observed)
    table["errors"].extend(agg_errors)
    timings["aggregate"] = _sp_ms(aggregate_start)

    specs_by_field: dict[str | None, list[dict[str, Any]]] = {}
    for spec in agg_plan["specs"]:
        if spec.get("group") != "json_path":
            specs_by_field.setdefault(spec["field_id"], []).append(spec)
    static_by_field: dict[str, list[dict[str, Any]]] = {}
    for item in [*agg_plan["static_metrics"], *deep_static]:
        static_by_field.setdefault(item["field_id"], []).append(item)
    row_spec = specs_by_field[None][0]
    if row_spec["alias"] in failed or results.get(row_spec["alias"]) is None:
        row_count = None
        table["table_metrics"].append(
            not_measured(
                "row_count",
                "error",
                failed.get(row_spec["alias"], "row count not returned"),
                scope=scope,
                source="aggregate",
            )
        )
    else:
        row_count = int(results[row_spec["alias"]])
        table["table_metrics"].append(
            measured(
                "row_count",
                row_count,
                unit="rows",
                scope=scope,
                accuracy="exact",
                source="aggregate",
                method="count(*) over the analysed scope",
            )
        )
    null_counts: dict[str, int | None] = {}
    for spec in agg_plan["specs"]:
        if spec["metric"] == "null_count" and spec["alias"] not in failed:
            value = results.get(spec["alias"])
            null_counts[spec["field_id"]] = int(value) if value is not None else None
    distinct_metrics: dict[str, list[dict[str, Any]]] = {}
    if distinct_plan is not None:
        distinct_metrics = _sp_run_element_distinct(
            frame,
            distinct_plan,
            nodes_by_id,
            _sp_collection_totals(element_contexts, specs_by_field, results, failed),
            scope=scope,
            table=table,
            timings=timings,
        )
    for field in table["field_profiles"]:
        if not field["profiled"]:
            continue
        node = nodes_by_id[field["field_id"]]
        parent_id = node.get("parent_field_id")
        context = element_contexts.get(field["field_id"])
        if context is not None:
            totals = _sp_collection_totals(
                {field["field_id"]: context}, specs_by_field, results, failed
            )
            field["metrics"] = element_field_metrics(
                node,
                context,
                specs_by_field.get(field["field_id"], []),
                results,
                failed,
                scope=scope,
                element_total=totals.get(context["collection_field_id"]),
                parent_null_count=null_counts.get(parent_id)
                if parent_id and parent_id in element_contexts
                else None,
                static=static_by_field.get(field["field_id"], []),
                extra=distinct_metrics.get(field["field_id"], []),
            )
            continue
        field["metrics"] = field_metrics(
            node,
            specs_by_field.get(field["field_id"], []),
            results,
            failed,
            scope=scope,
            row_count=row_count,
            parent_null_count=null_counts.get(parent_id) if parent_id else None,
            static=static_by_field.get(field["field_id"], []),
        )
        inferred = semantic_by_id.get(field["field_id"])
        if inferred is not None:
            field["semantics"] = {
                "observed_format": inferred["observed_format"],
                "candidate_roles": [],
            }
            field["json_profile"] = inferred["json_profile"]
            field["concentration"] = inferred["concentration"]
            field["examples"] = inferred["examples"]
    if not capabilities.get("try_parse_json", {}).get("available") and json_ids:
        table["unsupported"].append(
            {
                "capability": "try_parse_json",
                "detail": (
                    "full-scope JSON validation is unavailable in this runtime; "
                    "JSON validity is sample-based"
                ),
            }
        )
    if deep_plan is not None:
        _sp_finish_deep(
            table,
            deep_plan,
            json_catalogs,
            agg_plan,
            distinct_plan,
            results,
            failed,
            config=config,
            json_method=json_method,
            json_unsupported=json_unsupported,
            scope=scope,
        )
        try:
            _sp_uniqueness(table, scoped, tree, nodes_by_id, config, scope=scope, log=log)
        except Exception as exc:  # noqa: BLE001 - uniqueness never costs the table its profile
            table["errors"].append(error_record(exc, "assemble"))
            table["uniqueness"] = None
            log(f"[tabledossier] {key}: uniqueness failed unexpectedly ({type(exc).__name__})")
    timings["total"] = _sp_ms(total_start)
    finalize_table(table, config)
    extra = (
        f" (+{(table['deep'] or {}).get('extra_passes', {}).get('planned', 0)} deep extra pass(es))"
        if deep_plan is not None
        else ""
    )
    log(
        f"[tabledossier] {key}: {table['status']} — {len(profiled) + len(element_contexts)} "
        f"field(s) profiled in {len(agg_plan['passes'])} aggregation pass(es){extra}"
    )
    return table


# --------------------------------------------------------------------------- deep helpers


def _sp_identifier_candidates(
    table: Mapping[str, Any], config: Mapping[str, Any]
) -> list[list[dict[str, Any]]]:
    """Paths of the fields the standard metrics mark as identifier candidates (schema order)."""
    out = []
    for field in table["field_profiles"]:
        if not field["profiled"] or field.get("element_context") or not field["metrics"]:
            continue
        roles = candidate_roles(
            field["type_kind"],
            field["metrics"],
            (field.get("semantics") or {}).get("observed_format"),
            config["thresholds"],
            field["physical_type"],
        )
        if any(role["role"] == "identifier_candidate" for role in roles):
            out.append([dict(segment) for segment in field["path"]])
    return out


def _sp_uniqueness(
    table: dict[str, Any],
    frame: Any,
    tree: Mapping[str, Any],
    nodes_by_id: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    scope: str,
    log: Callable[[str], None],
) -> None:
    """Plan, run and record the exact uniqueness checks of one table (deep level)."""
    lookup = table_lookup_key(table["identifier"]["parts"])
    requested = requested_keys(
        config, lookup, table["constraints"], _sp_identifier_candidates(table, config)
    )
    plan = plan_uniqueness(tree, requested, config)
    planned = table["operations"]["planned"]
    observed = table["operations"]["observed"]
    for number, members in enumerate(plan["passes"], start=1):
        planned.append(
            operation(
                f"op_uniqueness_{number}",
                "uniqueness_pass",
                "exact uniqueness of "
                f"{len(members)} key(s) in one grouped aggregation over the analysed scope (each "
                "row is exploded once per key; only counts are collected)",
                reads_user_data=True,
                keys=len(members),
                columns=sum(len(plan["keys"][m]["field_ids"]) for m in members),
            )
        )
    results: dict[int, dict[str, Any]] = {}
    errors: dict[int, str] = {}
    start = time.perf_counter()
    for number, members in enumerate(plan["passes"], start=1):
        op_id = f"op_uniqueness_{number}"
        pass_start = time.perf_counter()
        try:
            raw = run_uniqueness_pass(frame, [plan["keys"][m] for m in members], nodes_by_id)
        except Exception as exc:  # noqa: BLE001 - a failed pass leaves its keys unmeasured
            record = error_record(exc, "aggregate")
            table["errors"].append(record)
            cause = record["condition"] or record["error_class"]
            for member in members:
                errors[member] = "uniqueness pass failed: " + cause
            observed.append(_sp_observed(op_id, "failed", pass_start, detail=cause))
            continue
        for index, member in enumerate(members):
            results[member] = raw.get(index, {})
        observed.append(_sp_observed(op_id, "succeeded", pass_start, rows=len(raw)))
    if plan["passes"]:
        table["timings_ms"]["uniqueness"] = _sp_ms(start)
        log(
            f"[tabledossier] {table['table_key']}: {sum(len(m) for m in plan['passes'])} key(s) "
            f"checked for exact uniqueness in {len(plan['passes'])} pass(es)"
        )
    table["uniqueness"] = uniqueness_record(
        plan, results, errors, config=config, scope=scope, requested_any=bool(requested)
    )


def _sp_json_method(
    config: Mapping[str, Any], caps: Mapping[str, bool]
) -> tuple[str | None, str | None]:
    """Choose the full-scope JSON path method: ``(method, unsupported reason)``."""
    if not config["deep"]["json_full_scope_validation"]:
        return None, None
    if caps.get("variant_functions"):
        return "variant", None
    if caps.get("get_json_object"):
        return "get_json_object", None
    return None, "neither variant functions nor get_json_object are available in this runtime"


def _sp_json_catalogs(
    deep_plan: Mapping[str, Any],
    sample: Mapping[str, Any],
    json_ids: set[str],
    config: Mapping[str, Any],
    redacted_ids: set[str],
    sample_method: str,
) -> dict[str, dict[str, Any]]:
    """Build JSON path catalogues from the transient sample (values are not kept)."""
    deep = config["deep"]
    if deep_plan["json_nodes"] is None:
        targets = [fid for fid in sample["fields"] if fid in json_ids] if deep["json_paths"] else []
    else:
        targets = [node["field_id"] for node in deep_plan["json_nodes"]]
    catalogs = {}
    policy_names = config["value_policy"]["json_key_names"] == "include"
    for fid in targets:
        data = sample["fields"].get(fid)
        if data is None:
            continue
        include = policy_names and fid not in redacted_ids
        catalogs[fid] = json_path_catalog(
            data["values"],
            sql_nulls=data["nulls"],
            truncated=data["truncated"],
            include_names=include,
            names_omitted_reason=None
            if include
            else (
                "value policy (json_key_names = redact)"
                if not policy_names
                else "value policy (redact_columns)"
            ),
            max_depth=deep["max_json_depth"],
            max_paths=deep["max_json_paths"],
            max_object_keys=deep["max_json_object_keys"],
            sample_method=sample_method,
        )
    return catalogs


def _sp_collection_totals(
    contexts: Mapping[str, Mapping[str, Any]],
    specs_by_field: Mapping[str | None, Sequence[Mapping[str, Any]]],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
) -> dict[str, int | None]:
    """Measured element/entry totals of the collections behind element fields."""
    totals: dict[str, int | None] = {}
    for context in contexts.values():
        fid = context["collection_field_id"]
        if fid in totals:
            continue
        totals[fid] = None
        for spec in specs_by_field.get(fid, []):
            if (
                spec["metric"] in ("total_element_count", "total_entry_count")
                and spec["alias"] not in failed
            ):
                value = results.get(spec["alias"])
                totals[fid] = 0 if value is None else int(value)
    return totals


def _sp_run_element_distinct(
    frame: Any,
    plan: dict[str, Any],
    nodes_by_id: Mapping[str, Any],
    totals: Mapping[str, int | None],
    *,
    scope: str,
    table: dict[str, Any],
    timings: dict[str, int],
) -> dict[str, list[dict[str, Any]]]:
    """Run (or explain) the element explode pass and return ``distinct_count`` metrics."""
    observed = table["operations"]["observed"]
    sample_mode = plan["mode"] == "sample"
    metric_scope = "sample" if sample_mode else scope
    source = "sample" if sample_mode else "aggregate"
    out: dict[str, list[dict[str, Any]]] = {}
    plan["result"] = None

    def unmeasured(status: str, reason: str) -> None:
        for leaf in plan["leaves"]:
            out[leaf["field_id"]] = [
                {
                    "field_id": leaf["field_id"],
                    "metric": not_measured(
                        "distinct_count", status, reason, scope=metric_scope, source=source
                    ),
                }
            ]

    if not plan["enabled"]:
        if plan["mode"] != "off" and plan["leaves"]:
            unmeasured("not_computed", plan["reason"] or "not planned")
        return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    start = time.perf_counter()
    if not sample_mode:
        wanted = {leaf["collection_field_id"] for leaf in plan["leaves"]}
        sizes = [totals.get(fid) for fid in wanted]
        if any(size is None for size in sizes) or sum(s or 0 for s in sizes) > plan["max_elements"]:
            reason = (
                "the measured elements in scope exceed deep.max_elements = "
                f"{plan['max_elements']} (or were not measured)"
            )
            plan["skip_reason"] = reason
            plan["budget_limited"] = True
            observed.append(_sp_observed("op_deep_elements", "skipped", start, detail=reason))
            unmeasured("not_computed", reason)
            return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    try:
        raw = run_element_distinct(frame, plan, nodes_by_id)
    except Exception as exc:  # noqa: BLE001
        record = error_record(exc, "aggregate")
        table["errors"].append(record)
        cause = record["condition"] or record["error_class"]
        plan["error"] = cause
        observed.append(_sp_observed("op_deep_elements", "failed", start, detail=cause))
        unmeasured("error", "element distinct pass failed: " + cause)
        timings["deep_elements"] = _sp_ms(start)
        return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    observed.append(_sp_observed("op_deep_elements", "succeeded", start, rows=1))
    timings["deep_elements"] = _sp_ms(start)
    plan["result"] = raw
    stopped = raw["n"] >= plan["max_elements"]
    method = (
        "count(DISTINCT) over the elements of one explode of a bounded "
        f"{plan['sampling_method']} sample (at most {plan['max_rows']} rows, stopped at "
        f"{plan['max_elements']} elements); exact for the elements examined only"
        if sample_mode
        else "count(DISTINCT) over every element in scope (one explode; measured size within "
        "deep.max_elements)"
    )
    for index, leaf in enumerate(plan["leaves"]):
        examined, non_null, distinct = raw[f"n{index}"], raw[f"nn{index}"], raw[f"d{index}"]
        details = {
            "elements_examined": examined,
            "rows_with_elements": raw["rows"],
            "max_rows": plan["max_rows"] if sample_mode else None,
            "max_elements": plan["max_elements"],
            "stopped_by_element_limit": stopped,
        }
        if non_null == 0:
            metric = not_measured(
                "distinct_count",
                "insufficient_data",
                f"no non-null {leaf['unit']} among the elements examined",
                scope=metric_scope,
                source=source,
            )
        else:
            metric = measured(
                "distinct_count",
                distinct,
                unit="values",
                scope=metric_scope,
                accuracy="exact",
                source=source,
                method=method,
                denominator=non_null,
                denominator_unit=leaf["unit"],
                details=details,
            )
        out[leaf["field_id"]] = [{"field_id": leaf["field_id"], "metric": metric}]
    return {fid: [item["metric"] for item in items] for fid, items in out.items()}


def _sp_finish_deep(
    table: dict[str, Any],
    deep_plan: Mapping[str, Any],
    catalogs: Mapping[str, dict[str, Any]],
    agg_plan: Mapping[str, Any],
    distinct_plan: Mapping[str, Any] | None,
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    *,
    config: Mapping[str, Any],
    json_method: str | None,
    json_unsupported: str | None,
    scope: str,
) -> None:
    """Attach JSON path catalogues and the deep coverage record to a table."""
    deep = config["deep"]
    fields = {field["field_id"]: field for field in table["field_profiles"]}
    json_specs = [spec for spec in agg_plan["specs"] if spec.get("group") == "json_path"]
    dropped = list(agg_plan["deep_dropped"])
    limited: list[dict[str, str]] = []
    json_fields = []
    for fid, catalog in catalogs.items():
        attach_json_validation(
            catalog,
            [spec for spec in json_specs if spec["field_id"] == fid],
            results,
            failed,
            [spec for spec in dropped if spec["field_id"] == fid],
            method=json_method,
            unsupported_reason=json_unsupported,
            scope=scope,
        )
        fields[fid]["json_paths"] = catalog
        validated = sum(
            1
            for item in catalog.get("paths") or []
            if (item.get("full_scope") or {}).get("status") == "measured"
        )
        json_fields.append(
            {
                "field_id": fid,
                "display_path": fields[fid]["display_path"],
                "status": "catalogued",
                "reason": catalog["paths_omitted_reason"],
                "paths_listed": catalog["paths_listed"],
                "paths_validated": validated,
                "full_scope_method": json_method if catalog.get("paths") else None,
            }
        )
        if catalog["paths_observed"] > catalog["paths_listed"] and catalog.get("paths") is not None:
            limited.append(
                {
                    "item": fields[fid]["display_path"],
                    "reason": "json_path_budget",
                    "detail": f"{catalog['paths_observed'] - catalog['paths_listed']} JSON path(s) "
                    f"beyond deep.max_json_paths = {deep['max_json_paths']}",
                }
            )
        if catalog["depth_truncated"]:
            limited.append(
                {
                    "item": fields[fid]["display_path"],
                    "reason": "json_depth_budget",
                    "detail": f"nesting beyond deep.max_json_depth = {deep['max_json_depth']}",
                }
            )
    if deep_plan["json_nodes"] is not None:
        for node in deep_plan["json_nodes"]:
            if node["field_id"] not in catalogs:
                json_fields.append(
                    {
                        "field_id": node["field_id"],
                        "display_path": node["display_path"],
                        "status": "not_catalogued",
                        "reason": "not in the transient sample (sampling disabled or failed, or "
                        "beyond sampling.max_columns)",
                        "paths_listed": 0,
                        "paths_validated": 0,
                        "full_scope_method": None,
                    }
                )
    if json_unsupported and any(catalog.get("paths") for catalog in catalogs.values()):
        table["unsupported"].append(
            {"capability": "json_path_functions", "detail": json_unsupported}
        )
    for omission in agg_plan["omissions"]:
        if omission["reason"] == "deep_budget":
            limited.append(
                {
                    "item": omission["display_path"] or "table",
                    "reason": "deep_budget",
                    "detail": f"{', '.join(omission['metrics'])}: {omission['detail']}",
                }
            )
    element_distinct = None
    explode = 0
    if distinct_plan is not None and (distinct_plan["leaves"] or distinct_plan["mode"] == "off"):
        raw = distinct_plan.get("result")
        explode = 1 if distinct_plan["enabled"] else 0
        status = "measured" if raw else ("error" if distinct_plan.get("error") else "not_computed")
        stopped = "not_applicable"
        if raw:
            stopped = "element_limit" if raw["n"] >= distinct_plan["max_elements"] else "exhausted"
        elif distinct_plan.get("error"):
            stopped = "error"
        element_distinct = {
            "mode": distinct_plan["mode"],
            "status": status,
            "reason": distinct_plan.get("skip_reason")
            or distinct_plan.get("reason")
            or (f"failed: {distinct_plan['error']}" if distinct_plan.get("error") else None),
            "fields": len(distinct_plan["leaves"]),
            "method": distinct_plan["sampling_method"]
            if distinct_plan["mode"] == "sample"
            else "none",
            "max_rows": distinct_plan["max_rows"] if distinct_plan["mode"] == "sample" else None,
            "max_elements": distinct_plan["max_elements"],
            "rows_with_elements": raw["rows"] if raw else None,
            "elements_examined": raw["n"] if raw else None,
            "stopped_reason": stopped,
        }
        if distinct_plan.get("budget_limited"):
            limited.append(
                {
                    "item": "element distinct counts",
                    "reason": "deep_pass_budget"
                    if distinct_plan["mode"] == "sample"
                    else "element_budget",
                    "detail": element_distinct["reason"] or "",
                }
            )
        elif stopped == "element_limit":
            limited.append(
                {
                    "item": "element distinct counts",
                    "reason": "element_budget",
                    "detail": f"stopped at deep.max_elements = {distinct_plan['max_elements']} "
                    "elements; later elements were not examined",
                }
            )
    collections = []
    for node in deep_plan["collections"]:
        fid = node["field_id"]
        profiled = sum(1 for c in deep_plan["contexts"].values() if c["collection_field_id"] == fid)
        omitted = sum(
            1
            for item in deep_plan["omissions"]
            if item["reason"] == "nested_collection"
            and (item["display_path"] or "").startswith(node["display_path"])
        )
        collections.append(
            {
                "field_id": fid,
                "display_path": node["display_path"],
                "kind": node["type"]["kind"],
                "element_fields_profiled": profiled,
                "element_fields_omitted": omitted,
            }
        )
    notes = [
        "Element metrics are exact over the analysed scope and computed per row with "
        "higher-order functions inside the shared aggregation passes (no explode); their "
        "denominators are elements or entries, never rows.",
        "JSON path catalogues come from the transient sample; they are not a complete schema.",
    ]
    if deep_plan["mode"] == "explicit" and not deep_plan["requested"]:
        notes.append("deep.targets lists no field of this table; nothing was profiled in depth.")
    extra_aggregate = agg_plan["extra_passes"]
    table["deep"] = {
        "targets": deep_plan["mode"],
        "requested_targets": deep_plan["requested"],
        "not_eligible": list(deep_plan["not_eligible"]),
        "collections": collections,
        "json_fields": json_fields,
        "budget": {
            key: deep[key]
            for key in (
                "max_extra_passes",
                "max_explode_rows",
                "max_elements",
                "max_json_paths",
                "max_json_depth",
                "max_json_object_keys",
            )
        },
        "extra_passes": {
            "budget": deep["max_extra_passes"],
            "planned": extra_aggregate + explode,
            "aggregate": extra_aggregate,
            "element_explode": explode,
        },
        "expressions": {
            "planned": agg_plan["deep_expressions"],
            "omitted": sum(spec["cost"] for spec in dropped),
        },
        "element_distinct": element_distinct,
        "limited": limited,
        "notes": notes,
    }


# --------------------------------------------------------------------------- relationships


def _sp_table_frame(spark: Any, table: Mapping[str, Any], *, filtered: bool) -> Any:
    """Re-read a profiled table at the Delta version recorded in its profile."""
    quoted = table["identifier"]["quoted"]
    consistency = table.get("consistency") or {}
    version = consistency.get("delta_version")
    if consistency.get("mode") == "pinned_delta_version" and version is not None:
        frame = spark.sql(f"SELECT * FROM {quoted} VERSION AS OF {int(version)}")
    else:
        frame = spark.table(quoted)
    filters = table["scope"]["filters"]
    return frame.filter(filter_condition(filters)) if filtered and filters else frame


def _sp_side(table: Mapping[str, Any], scope: str) -> dict[str, Any]:
    consistency = table.get("consistency") or {}
    return {
        "table": table["table_key"],
        "scope": scope,
        "consistency_mode": consistency.get("mode", "unpinned"),
        "delta_version": consistency.get("delta_version"),
    }


def _sp_full_scope(table: Mapping[str, Any]) -> str:
    consistency = table.get("consistency") or {}
    return "full_snapshot" if consistency.get("mode") == "pinned_delta_version" else "full_table"


def _sp_run_tables(spark: Any, tables: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Index profiled tables by their name as given and, when known, by their full name."""
    index: dict[str, Any] = {}
    for table in tables:
        parts = table["identifier"]["parts"]
        index.setdefault(table_lookup_key(parts), table)
        full = _sp_full_name(spark, list(parts))
        if full is not None:
            index.setdefault(table_lookup_key(full), table)
    return index


def _sp_find_table(index: Mapping[str, Any], text: str) -> Any:
    try:
        return index.get(table_lookup_key(parse_table_identifier(text)))
    except IdentifierError:
        return None


def _sp_end_nodes(
    relationship: Mapping[str, Any], end: str, table: Mapping[str, Any]
) -> tuple[list[Any], str]:
    by_id = {
        node["field_id"]: node for node in iter_nodes((table["schema"] or {}).get("fields", []))
    }
    try:
        paths = end_segments(relationship, end)
    except IdentifierError as exc:
        return [], f"{end} columns: {exc}"
    nodes = [by_id.get(field_id(path)) for path in paths]
    problem = key_columns_problem(nodes, [str(c) for c in relationship[end]["columns"]])
    return nodes, (f"{end} key: {problem}" if problem else "")


def _sp_referential_problem(
    relationship: Mapping[str, Any], index: Mapping[str, Any], config: Mapping[str, Any]
) -> tuple[str, Any, Any, list[Any], list[Any], list[dict[str, Any]]]:
    """Return ``(reason, source, target, source nodes, target nodes, compatibility)``."""
    source = _sp_find_table(index, relationship["from"]["table"])
    target = _sp_find_table(index, relationship["to"]["table"])
    empty: list[Any] = []
    if source is None:
        return "the source table was not profiled in this run", None, None, empty, empty, []
    if target is None:
        return (
            "the target table was not profiled in this run (tables outside the run are never read)",
            source,
            None,
            empty,
            empty,
            [],
        )
    for side, table in (("source", source), ("target", target)):
        if table["status"] == "failed" or table["schema"] is None:
            return f"the {side} table could not be profiled", source, target, empty, empty, []
    from_nodes, problem = _sp_end_nodes(relationship, "from", source)
    to_nodes, other = _sp_end_nodes(relationship, "to", target)
    if problem or other:
        return problem or other, source, target, empty, empty, []
    compatibility = type_compatibility(from_nodes, to_nodes)
    incompatible = [item for item in compatibility if not item["compatible"]]
    if incompatible:
        pairs = "; ".join(
            f"{item['from_column']} ({item['from_type']}) vs {item['to_column']} "
            f"({item['to_type']})"
            for item in incompatible
        )
        return f"incompatible column types: {pairs}", source, target, [], [], compatibility
    settings = config["deep"]["referential"]
    if settings["mode"] == "sample" and config["sampling"]["method"] == "none":
        return (
            "sample mode needs a sampling method (sampling.method = none)",
            source,
            target,
            [],
            [],
            compatibility,
        )
    return "", source, target, from_nodes, to_nodes, compatibility


def validate_relationships(
    spark: Any,
    tables: Sequence[dict[str, Any]],
    config: Mapping[str, Any],
    *,
    log: Callable[[str], None] = print,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Validate the known relationships requested by ``deep.referential``.

    Returns ``(relationships, referential_validation)``. Each check is one
    Spark action, recorded as a ``referential_check`` operation of the source
    table and bounded by ``max_relationships`` per run. Both tables are read at
    the Delta versions recorded when they were profiled; the source keeps its
    analysed scope, the target is read in full.
    """
    relationships = known_relationships(tables, config)
    if config["analysis_level"] != "deep":
        return relationships, None
    settings = config["deep"]["referential"]
    sampling = config["sampling"]
    index = _sp_run_tables(spark, tables)
    planned = 0
    limited: list[dict[str, str]] = []
    for relationship in relationships:
        reason = referential_requested(relationship, config)
        if reason:
            relationship["validation_detail"] = not_validated_detail(reason)
            continue
        reason, source, target, from_nodes, to_nodes, compatibility = _sp_referential_problem(
            relationship, index, config
        )
        if not reason and planned >= settings["max_relationships"]:
            reason = f"beyond deep.referential.max_relationships = {settings['max_relationships']}"
            limited.append(
                {"item": relationship["name"], "reason": "referential_budget", "detail": reason}
            )
        if reason:
            relationship["validation_detail"] = not_validated_detail(
                reason, type_compatibility=compatibility
            )
            continue
        planned += 1
        op_id = f"op_referential_{planned}"
        sample = (
            {
                "method": sampling["method"],
                "max_rows": settings["max_sample_rows"],
                "fraction": sampling.get("random_fraction"),
                "seed": sampling.get("seed"),
            }
            if settings["mode"] == "sample"
            else None
        )
        source["operations"]["planned"].append(
            operation(
                op_id,
                "referential_check",
                f"orphan count of {relationship['name']} against {target['table_key']} (one "
                "anti join; the target is read in full; only counts are collected)",
                reads_user_data=True,
                relationship_id=relationship["relationship_id"],
                target_table=target["table_key"],
                mode=settings["mode"],
                max_sample_rows=settings["max_sample_rows"] if sample else None,
            )
        )
        start = time.perf_counter()
        from_info = _sp_side(source, "sample" if sample else source["scope"]["scope_label"])
        to_info = _sp_side(target, _sp_full_scope(target))
        try:
            raw = run_inclusion_check(
                _sp_table_frame(spark, source, filtered=True),
                [column_for(node["path"]) for node in from_nodes],
                _sp_table_frame(spark, target, filtered=False),
                [column_for(node["path"]) for node in to_nodes],
                sample=sample,
            )
        except Exception as exc:  # noqa: BLE001 - one failed check never stops the run
            record = error_record(exc, "aggregate")
            cause = record["condition"] or record["error_class"]
            source["operations"]["observed"].append(
                _sp_observed(op_id, "failed", start, detail=cause)
            )
            relationship["validation_detail"] = not_validated_detail(
                f"the check could not read the data ({cause})",
                mode=settings["mode"],
                **{"from": from_info, "to": to_info},
                type_compatibility=compatibility,
            )
            continue
        source["operations"]["observed"].append(_sp_observed(op_id, "succeeded", start, rows=1))
        detail = validation_detail(
            raw,
            mode=settings["mode"],
            sample_rows=settings["max_sample_rows"] if sample else None,
            from_info=from_info,
            to_info=to_info,
            compatibility=compatibility,
            operation_id=op_id,
        )
        relationship["validation"] = detail["status"]
        relationship["validation_detail"] = detail
        log(f"[tabledossier] relationship {relationship['name']}: {detail['status']}")
    return relationships, referential_summary(
        relationships, config, planned=planned, limited=limited
    )


def evaluate_hypotheses(
    spark: Any,
    tables: Sequence[dict[str, Any]],
    relationships: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    log: Callable[[str], None] = print,
) -> dict[str, Any] | None:
    """Measure data-driven relationship hypotheses (``deep.relationship_hypotheses``).

    Candidate pairs come from ``plan_hypotheses`` (types, measured ranges and
    exact unique keys, never names); each evaluated pair is one Spark action
    recorded as a ``relationship_hypothesis_check`` of the source table. Pairs
    that reach ``min_inclusion_ratio`` against a target key that is unique over
    its whole snapshot are listed as hypotheses; the others are counted by
    reason. Known relationships are never repeated.
    """
    if config["analysis_level"] != "deep":
        return None
    settings = config["deep"]["relationship_hypotheses"]
    sampling = config["sampling"]
    if not settings["enabled"]:
        return hypotheses_record(
            config,
            None,
            [],
            evaluated=0,
            rejected={},
            reason="disabled by configuration (deep.relationship_hypotheses.enabled = false)",
        )
    sample_mode = settings["inclusion_scope"] == "sample"
    if sample_mode and sampling["method"] == "none":
        return hypotheses_record(
            config,
            None,
            [],
            evaluated=0,
            rejected={},
            reason="sample inclusion needs a sampling method (sampling.method = none)",
        )
    usable = [table for table in tables if table["status"] != "failed" and table["schema"]]
    index = _sp_run_tables(spark, usable)
    known: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = set()
    for relationship in relationships:
        source = _sp_find_table(index, relationship["from"]["table"])
        target = _sp_find_table(index, relationship["to"]["table"])
        if source is None or target is None:
            continue
        from_nodes, problem = _sp_end_nodes(relationship, "from", source)
        to_nodes, other = _sp_end_nodes(relationship, "to", target)
        if not problem and not other:
            known.add(
                (
                    table_lookup_key(source["identifier"]["parts"]),
                    tuple(node["field_id"] for node in from_nodes),
                    table_lookup_key(target["identifier"]["parts"]),
                    tuple(node["field_id"] for node in to_nodes),
                )
            )
    plan = plan_hypotheses(usable, known, config)
    reason = (
        None
        if plan["targets"]
        else "no single-column key was measured exactly unique in this run (deep.uniqueness)"
    )
    sample = (
        {
            "method": sampling["method"],
            "max_rows": settings["max_sample_rows"],
            "fraction": sampling.get("random_fraction"),
            "seed": sampling.get("seed"),
        }
        if sample_mode
        else None
    )
    hypotheses: list[dict[str, Any]] = []
    rejected: Counter[str] = Counter()
    evaluated = 0
    for number, pair in enumerate(plan["pairs"], start=1):
        source = usable[pair["from_table_index"]]
        target = usable[pair["to_table_index"]]
        op_id = f"op_hypothesis_{number}"
        source["operations"]["planned"].append(
            operation(
                op_id,
                "relationship_hypothesis_check",
                f"inclusion of {pair['from_node']['display_path']} in the unique key "
                f"{target['table_key']}.{pair['to_node']['display_path']} (one left join; only "
                "counts are collected)",
                reads_user_data=True,
                target_table=target["table_key"],
                inclusion_scope=settings["inclusion_scope"],
                max_sample_rows=settings["max_sample_rows"] if sample else None,
            )
        )
        start = time.perf_counter()
        try:
            raw = run_inclusion_check(
                _sp_table_frame(spark, source, filtered=True),
                [column_for(pair["from_node"]["path"])],
                _sp_table_frame(spark, target, filtered=False),
                [column_for(pair["to_node"]["path"])],
                sample=sample,
            )
        except Exception as exc:  # noqa: BLE001 - a failed pair never stops the run
            record = error_record(exc, "aggregate")
            cause = record["condition"] or record["error_class"]
            source["operations"]["observed"].append(
                _sp_observed(op_id, "failed", start, detail=cause)
            )
            rejected["check_failed"] += 1
            continue
        source["operations"]["observed"].append(_sp_observed(op_id, "succeeded", start, rows=1))
        evaluated += 1
        scope = "sample" if sample else source["scope"]["scope_label"]
        metrics, inclusion = hypothesis_evidence(
            raw, scope=scope, target_scope=_sp_full_scope(target)
        )
        if inclusion is None:
            rejected["no_source_values"] += 1
            continue
        if raw.get("t_dup_groups"):
            rejected["target_not_unique"] += 1
            continue
        if inclusion < settings["min_inclusion_ratio"]:
            rejected["below_threshold"] += 1
            continue
        hypotheses.append(
            hypothesis_item(
                len(hypotheses) + 1,
                pair,
                source_table=source,
                target_table=target,
                evidence={
                    "inclusion_scope": settings["inclusion_scope"],
                    "from": _sp_side(source, scope),
                    "to": _sp_side(target, _sp_full_scope(target)),
                    "metrics": metrics,
                    "target_key_id": pair["target_key_id"],
                    "target_key_unique": True,
                    "type_compatibility": type_compatibility(
                        [pair["from_node"]], [pair["to_node"]]
                    ),
                    "range_relation": pair["range_relation"],
                    "range_basis": pair["range_basis"],
                },
                operation_id=op_id,
                sample_rows=settings["max_sample_rows"] if sample else None,
            )
        )
    log(
        f"[tabledossier] relationship hypotheses: {evaluated} pair(s) evaluated, "
        f"{len(hypotheses)} listed"
    )
    return hypotheses_record(
        config, plan, hypotheses, evaluated=evaluated, rejected=dict(rejected), reason=reason
    )

# COMMAND ----------

# DBTITLE 1,Runtime: tabledossier.runtime.databricks
# TableDossier 0.2.0 embedded runtime: module tabledossier.runtime.databricks
# Source: src/tabledossier/runtime/databricks.py (sha256:75794e21f2aaec6b18e41e019b5ca03e7eeb8bd7beae90bf8ad3633f242b5c97)
# Copyright 2026 ruanpato and TableDossier contributors.
# Licensed under the Apache License, Version 2.0; see https://www.apache.org/licenses/LICENSE-2.0
# Intra-package imports were removed at generation time; the names they
# provided are defined by earlier cells. Do not edit: regenerate instead.

"""Notebook orchestration: parameters, destination probe, batch execution and export.

Part of the embedded runtime. Functions receive ``spark`` and widget values
explicitly, so the same code runs inside a Databricks notebook and in local
integration tests with a plain SparkSession.

The notebook only *reads* sources. It writes exclusively to one new directory
per run under the configured ``output_dir``; existing files are never
overwritten.
"""

import datetime
import os
import secrets
import time
from collections.abc import Callable, Mapping
from typing import Any



class DestinationError(RuntimeError):
    """Raised when the output directory cannot be used."""


def new_run_id(moment: datetime.datetime) -> str:
    """Return a unique run identifier such as ``20260921T183000Z-1a2b3c4d``."""
    utc = moment.astimezone(datetime.timezone.utc)
    return utc.strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)


def prepare_run(
    *,
    spark: Any,
    widget_values: Mapping[str, str],
    generated_config: Mapping[str, Any],
    schemas: Mapping[str, Mapping[str, Any]],
    generation: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate parameters and the destination before any table is read.

    Raises :class:`ConfigError` for invalid parameters and
    :class:`DestinationError` when the run directory cannot be created.
    """
    config, sources = resolve_parameters(generated_config, widget_values, schemas["config"])
    problems = execution_errors(config)
    if problems:
        raise ConfigError("the notebook cannot run yet", problems)
    started = utc_now()
    run_id = new_run_id(started)
    run_dir = os.path.join(config["output_dir"], run_id)
    try:
        os.makedirs(config["output_dir"], exist_ok=True)
        os.makedirs(run_dir, exist_ok=False)
        write_files(
            run_dir,
            {
                "manifest.json": pretty_json(
                    running_manifest(run_id, format_utc(started), generation.get("generation_id"))
                )
            },
        )
    except OSError as exc:
        raise DestinationError(
            f"cannot write to output_dir {config['output_dir']!r} ({type(exc).__name__}: "
            f"{exc.strerror or exc}). "
            "Use a path you can write to, for example a Unity Catalog volume "
            "/Volumes/<catalog>/<schema>/<volume>/tabledossier (requires READ VOLUME and WRITE "
            "VOLUME), "
            "or a workspace folder such as /Workspace/Users/<you>/tabledossier."
        ) from exc
    return {
        "run_id": run_id,
        "started": started,
        "started_at": format_utc(started),
        "reference_time": format_utc(started),
        "config": config,
        "parameter_sources": sources,
        "run_dir": run_dir,
        "environment": spark_environment(spark),
        "capabilities": detect_capabilities(spark),
        "generation": dict(generation),
    }


def describe_plan(ctx: Mapping[str, Any]) -> str:
    """Return a human-readable plan of the run (nothing has been read yet)."""
    config = ctx["config"]
    sampling = config["sampling"]
    limits = config["limits"]
    lines = [
        f"Run {ctx['run_id']} — analysis level: {config['analysis_level']}",
        f"Results directory: {ctx['run_dir']}",
        f"Tables ({len(config['tables'])}, processed sequentially):",
    ]
    lines += [f"  - {name}" for name in config["tables"]]
    lines.append("Per table, the engine will be asked for:")
    lines.append(
        "  - catalog metadata: DESCRIBE TABLE EXTENDED / DETAIL, key constraints (no row scan)"
    )
    if config["analysis_level"] in ("standard", "deep"):
        pinning = (
            "Delta tables are pinned to one version (VERSION AS OF)"
            if config["consistency"]["pin_delta_version"]
            else "snapshot pinning disabled"
        )
        lines.append(f"  - consistency: {pinning}")
        if sampling["method"] == "none":
            lines.append("  - sample: disabled (format and JSON inference not computed)")
        else:
            lines.append(
                f"  - one {sampling['method']} sample of up to {sampling['max_rows']} rows, "
                f"{sampling['max_bytes']} bytes retained, values cut at "
                f"{sampling['max_value_chars']} characters"
            )
        lines.append(
            f"  - up to {limits['max_aggregate_passes']} shared aggregation pass(es) of at most "
            f"{limits['max_expressions_per_pass']} expressions over at most "
            f"{limits['max_fields']} fields"
        )
        if config["analysis_level"] == "deep":
            deep = config["deep"]
            targets = (
                "every array, map and probable-JSON field within the budgets"
                if not isinstance(deep["targets"], list)
                else f"{len(deep['targets'])} explicitly listed field(s)"
            )
            lines += [
                f"  - deep level ({targets}):",
                "      element metrics of arrays and maps inside the shared passes (higher-order "
                "functions, no explode; lowest priority)",
                f"      at most {deep['max_extra_passes']} extra pass(es) per table: aggregation "
                "overflow and one element explode pass "
                + (
                    f"over a sample of up to {deep['max_explode_rows']} rows and "
                    f"{deep['max_elements']} elements"
                    if deep["element_distinct"] == "sample"
                    else (
                        f"over the full scope when it has at most {deep['max_elements']} elements"
                        if deep["element_distinct"] == "full_scope"
                        else "(disabled)"
                    )
                ),
                f"      JSON paths from the sample (up to {deep['max_json_paths']} paths, depth "
                f"{deep['max_json_depth']})"
                + (
                    ", validated over the full scope when the runtime supports it"
                    if deep["json_full_scope_validation"]
                    else ""
                ),
            ]
            uniqueness = deep["uniqueness"]
            sources = [
                name
                for name, enabled in (
                    (f"{len(uniqueness['keys'])} listed key(s)", bool(uniqueness["keys"])),
                    ("declared keys", uniqueness["declared_keys"]),
                    ("identifier candidates", uniqueness["identifier_candidates"]),
                )
                if enabled
            ]
            lines.append(
                f"      exact uniqueness ({', '.join(sources)}): up to {uniqueness['max_keys']} "
                f"key(s) per table in at most {uniqueness['max_passes']} grouped pass(es); counts "
                "only"
                if sources
                else "      exact uniqueness: no key requested (deep.uniqueness)"
            )
            referential = deep["referential"]
            origins = [
                name
                for name, enabled in (
                    ("configured", referential["configured"]),
                    ("declared", referential["declared"]),
                )
                if enabled
            ]
            lines.append(
                f"  - referential validation of {' and '.join(origins)} relationships between "
                f"tables of this run: up to {referential['max_relationships']} check(s) per run, "
                "one anti join each, "
                + (
                    "over the full source scope"
                    if referential["mode"] == "full_scope"
                    else f"over a sample of at most {referential['max_sample_rows']} source rows"
                )
                + " (both tables at their recorded versions; counts only)"
                if origins
                else "  - referential validation: not requested (deep.referential)"
            )
            hypotheses = deep["relationship_hypotheses"]
            lines.append(
                f"  - relationship hypotheses: up to {hypotheses['max_pairs']} pair(s) per run "
                "chosen by type and measured ranges (never names), one inclusion check each "
                + (
                    f"over a sample of at most {hypotheses['max_sample_rows']} source rows"
                    if hypotheses["inclusion_scope"] == "sample"
                    else "over the full source scope"
                )
                + f"; listed from {hypotheses['min_inclusion_ratio']:.0%} inclusion"
                if hypotheses["enabled"]
                else "  - relationship hypotheses: off (deep.relationship_hypotheses.enabled)"
            )
    else:
        lines.append("  - no table rows are read at the metadata level")
    capabilities = ctx["capabilities"]
    lines.append(
        "Detected capabilities: "
        + ", ".join(
            f"{name}={'yes' if item['available'] else 'no'}"
            for name, item in sorted(capabilities.items())
        )
    )
    return "\n".join(lines)


def execute_run(
    spark: Any, ctx: Mapping[str, Any], log: Callable[[str], None] = print
) -> dict[str, Any]:
    """Profile every table sequentially; one failing table never stops the others."""
    config = ctx["config"]
    tables = []
    for position, name in enumerate(config["tables"], start=1):
        log(f"[tabledossier] ({position}/{len(config['tables'])}) profiling {name}")
        try:
            table = profile_table(
                spark,
                name,
                config,
                capabilities=ctx["capabilities"],
                reference_time=ctx["reference_time"],
                now=lambda: format_utc(utc_now()),
                log=log,
            )
        except Exception as exc:  # noqa: BLE001 - isolate unexpected failures per table
            parts = parse_table_identifier(name)
            table = new_table(
                name, parts, table_key(parts), table_id(parts), quote_table_identifier(parts)
            )
            table["errors"].append(error_record(exc, "assemble"))
            finalize_table(table, config)
            log(f"[tabledossier] {name}: failed unexpectedly ({type(exc).__name__})")
        tables.append(table)
    relationships, referential, hypotheses = _db_integrity(spark, tables, config, log)
    finished = utc_now()
    return build_profile(
        run_id=ctx["run_id"],
        started_at=ctx["started_at"],
        finished_at=format_utc(finished),
        duration_ms=max(0, int((finished - ctx["started"]).total_seconds() * 1000)),
        reference_time=ctx["reference_time"],
        environment=ctx["environment"],
        config=config,
        parameter_sources=ctx["parameter_sources"],
        generation=ctx["generation"],
        capabilities=ctx["capabilities"],
        tables=tables,
        relationships=relationships,
        referential_validation=referential,
        relationship_hypotheses=hypotheses,
    )


def _db_integrity(
    spark: Any, tables: list[dict[str, Any]], config: Mapping[str, Any], log: Callable[[str], None]
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
    """Run referential validation and hypotheses; an unexpected failure never loses the run."""
    try:
        relationships, referential = validate_relationships(spark, tables, config, log=log)
    except Exception as exc:  # noqa: BLE001 - the profiles of every table are kept
        log(f"[tabledossier] referential validation failed unexpectedly ({type(exc).__name__})")
        relationships = known_relationships(tables, config)
        reason = f"referential validation failed unexpectedly ({type(exc).__name__})"
        for relationship in relationships:
            relationship["validation_detail"] = not_validated_detail(reason)
        referential = (
            referential_summary(relationships, config, planned=0, limited=[])
            if config["analysis_level"] == "deep"
            else None
        )
    try:
        hypotheses = evaluate_hypotheses(spark, tables, relationships, config, log=log)
    except Exception as exc:  # noqa: BLE001 - the profiles of every table are kept
        log(f"[tabledossier] relationship hypotheses failed unexpectedly ({type(exc).__name__})")
        hypotheses = hypotheses_record(
            config,
            None,
            [],
            evaluated=0,
            rejected={},
            reason=f"hypothesis evaluation failed unexpectedly ({type(exc).__name__})",
        )
    return relationships, referential, hypotheses


def summary_text(profile: Mapping[str, Any]) -> str:
    """Return a short plain-text summary of a profile."""
    run = profile["run"]
    summary = profile["summary"]
    lines = [
        f"Run {run['run_id']}: {run['status']} ({summary['tables_succeeded']} succeeded, "
        f"{summary['tables_partial']} partial, {summary['tables_failed']} failed)",
    ]
    for table in profile["tables"]:
        rows = next((m for m in table["table_metrics"] if m["name"] == "row_count"), None)
        rows_text = (
            f"{rows['value']:,} rows in scope"
            if rows and rows["status"] == "measured"
            else "rows not measured"
        )
        lines.append(
            f"  - {table['table_key']}: {table['status']}, {rows_text}, "
            f"{table['summary'].get('fields_profiled', 0)} field(s) profiled, "
            f"{len(table['findings'])} finding(s)"
        )
        for error in table["errors"]:
            lines.append(
                f"      error [{error['stage']}] {error['condition'] or error['error_class']}: "
                f"{error['message']}"
            )
    checks = summary["checks"]
    lines.append(
        f"Checks: {checks['pass']} pass, {checks['fail']} fail, {checks['not_evaluated']} not "
        "evaluated, "
        f"{checks['error']} error. Findings: {summary['findings']['warning']} warning, "
        f"{summary['findings']['info']} info."
    )
    return "\n".join(lines)


def export_run(
    ctx: Mapping[str, Any],
    profile: Mapping[str, Any],
    documents: Mapping[str, str],
    schemas: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate the profile and write the complete result package."""
    start = time.perf_counter()
    errors = validate_profile(profile, schemas["profile"])
    files = {"profile.json": pretty_json(profile), **documents}
    manifest = run_manifest(
        profile,
        files,
        status=profile["run"]["status"],
        validation_errors=errors,
        generation_id=ctx["generation"].get("generation_id"),
    )
    files["manifest.json"] = pretty_json(manifest)
    written = write_files(ctx["run_dir"], files, replaceable=("manifest.json",))
    return {
        "run_dir": ctx["run_dir"],
        "files": written,
        "validation_errors": errors,
        "duration_ms": max(0, int((time.perf_counter() - start) * 1000)),
    }


def transfer_instructions(run_dir: str, run_id: str) -> str:
    """Return concrete instructions to copy the results to a local machine."""
    lines = [f"Results written to: {run_dir}", "", "Copy them to your computer with one of:"]
    if run_dir.startswith("/Volumes/"):
        lines += [
            f"  databricks fs cp -r dbfs:{run_dir} ./downloaded/{run_id}",
            "  or Catalog Explorer > the volume > select the files > Download.",
        ]
    elif run_dir.startswith("/Workspace/"):
        lines += [
            f"  databricks workspace export-dir {run_dir} ./downloaded/{run_id}",
            "  or the workspace file browser > the folder > Download.",
        ]
    else:
        lines += ["  the file transfer mechanism approved for this location in your environment."]
    lines += [
        "",
        "Then, offline:",
        f"  tabledossier validate --profile downloaded/{run_id}/profile.json",
        f"  tabledossier render --input downloaded/{run_id}/profile.json --output "
        f"docs/generated/{run_id}",
    ]
    return "\n".join(lines)


def prepare_documents(profile: Mapping[str, Any]) -> dict[str, str]:
    """Render the documentation package for a profile (same code as the CLI)."""
    return build_documents(profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate parameters and environment
# MAGIC
# MAGIC Checks the widgets and the configuration, then creates the run directory and writes a `running` manifest so that permission problems appear before any table is read.

# COMMAND ----------

# DBTITLE 1,Validate
td_ctx = prepare_run(
    spark=spark,
    widget_values=read_widgets(dbutils),
    generated_config=TD_GENERATED_CONFIG,
    schemas=TD_SCHEMAS,
    generation=TD_GENERATION,
)
print("Parameters are valid. Results directory:", td_ctx["run_dir"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Analysis plan
# MAGIC
# MAGIC What will be asked of the engine for each table. Nothing has been read yet.

# COMMAND ----------

# DBTITLE 1,Plan
print(describe_plan(td_ctx))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execution
# MAGIC
# MAGIC Tables are profiled one after another. A failure in one table is recorded with a sanitized message and does not stop the others (the run status becomes `partial`).

# COMMAND ----------

# DBTITLE 1,Execute
td_profile = execute_run(spark, td_ctx)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary
# MAGIC
# MAGIC Status per table and an overview rendered from the profile.

# COMMAND ----------

# DBTITLE 1,Summary
td_documents = prepare_documents(td_profile)
print(summary_text(td_profile))
print()
print(td_documents["overview.md"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data dictionary
# MAGIC
# MAGIC Descriptions come only from source comments (and, locally, from your annotations). Unknown meanings stay unknown.

# COMMAND ----------

# DBTITLE 1,Data dictionary
print(td_documents["data_dictionary.md"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Quality and limitations
# MAGIC
# MAGIC Observed quality report: executed checks, completeness, heuristic alerts, proposals and limits.

# COMMAND ----------

# DBTITLE 1,Quality report
print(td_documents["quality_report.md"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Export
# MAGIC
# MAGIC Validates the profile and writes the result package. Existing files are never overwritten.

# COMMAND ----------

# DBTITLE 1,Export
td_export = export_run(td_ctx, td_profile, td_documents, TD_SCHEMAS)
if td_export["validation_errors"]:
    print("WARNING: the profile did not pass validation; see manifest.json:")
    for td_error in td_export["validation_errors"][:20]:
        print("  -", td_error)
print(transfer_instructions(td_export["run_dir"], td_ctx["run_id"]))
