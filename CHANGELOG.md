# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-09-22

"Deep I": elements of arrays and maps, JSON paths, and a Spark Connect test baseline.

### Added

- `deep` analysis level (widget, configuration, `init --level deep`) = `standard` plus opt-in operations with their
  own budgets in a new `deep` configuration section: explicit targets per table and column or
  `all_within_budget`, `max_extra_passes`, `max_explode_rows`, `max_elements`, `max_json_paths`, `max_json_depth`,
  `max_json_object_keys`, `element_distinct` (`sample`, `full_scope`, `off`) and `json_full_scope_validation`.
- Element fields of arrays and maps (`items[]`, `items[].sku`, `attrs{key}`, `attrs{value}`) are profiled: nulls
  (also while the parent struct is present), extremes, lengths and value counts computed per row with higher-order
  functions inside the shared aggregation passes (exact, no explode), with denominators in elements or entries.
  Deep expressions never displace standard metrics and are reported as `deep_budget` omissions when they do not fit.
- Distinct counts of element values in one `element_explode_pass` per table (bounded sample by default, or the full
  scope when its measured size fits `deep.max_elements`), labelled with their scope.
- JSON path catalogues of string fields from the transient sample (presence, types, heterogeneity, map-like and rare
  keys collapsed into `*`), and full-scope presence/type validation with variant functions or presence with
  `get_json_object`, detected at run time (`variant_functions`, `get_json_object` capabilities).
- Profile contract 1.1 (additive): `deep` level, `element_context` and `json_paths` on field profiles, per-table
  `deep` coverage record, operation kinds `deep_aggregate_pass` and `element_explode_pass`. The notebook writes 1.1;
  the CLI reads 1.0 (frozen schema, `tabledossier schema profile-1.0`) and 1.1.
- Data dictionary rows for element fields with their denominators and a JSON path table; a "Deep analysis: coverage
  and budget" section in the DQR.
- Spark Connect test mode (`TD_TEST_SPARK_MODE=connect`, local `SparkSession.builder.remote("local[2]")`) and CI
  jobs for Spark Connect 3.5 (+Delta) and 4.0; a synthetic deep profile in `examples/demo/output/deep`.
- Decision record 0004 (deep level) and a deep step in the Databricks smoke test.

### Fixed

- Error conditions are read from the `[CONDITION]` prefix of the message when the client does not expose them
  (PySpark 3.5 Spark Connect), so `TABLE_OR_VIEW_NOT_FOUND` and similar are recorded in Connect sessions.
- `DESCRIBE DETAIL` rejected for a non-Delta source is recorded as a `skipped` operation with its reason, not
  `failed`.
- Map sizes and entry counts are labelled `entries` instead of `elements`, as documented.

### Not yet validated

- Execution on Databricks workspaces, including shared access mode and serverless compute (Spark Connect was
  exercised with a local Connect server only) and variant functions on 15.4/16.4 LTS.

## [0.1.0] - 2026-09-21

### Added

- Offline CLI: `init`, `validate` (configuration, profile, annotations), `generate`, `render`, `schema`, with stable
  exit codes.
- Self-contained Databricks source notebook generated from the package's own modules, with widgets
  (`tables_json`, `analysis_level`, `output_dir`, `config_json`), destination probe, sequential batch with per-table
  failure isolation and result export.
- Spark runtime: catalog metadata, Unity Catalog key constraints, Delta CHECK constraints, Delta snapshot pinning,
  bounded projected sampling, shared aggregation passes with expression budgets, capability detection
  (`try_parse_json`).
- `metadata` and `standard` analysis levels; metrics for numeric, string, boolean, temporal, binary, struct, array
  and map fields with explicit status, scope, accuracy, source and denominators.
- Observed-format detectors, JSON shape analysis, Wilson intervals, candidate roles, heuristic findings, configured
  checks and proposed rules.
- Profile contract 1.0 (JSON Schema) with standard-library and formal validation; run and generation manifests.
- Documentation renderer: overview, data dictionary, data quality report, relationships, Mermaid ERD, suggested rules;
  human annotations.
- Synthetic demo data, demo outputs, English documentation and a Portuguese (pt-BR) overview and quickstart.

### Not yet validated

- Execution on Databricks workspaces (see `docs/compatibility.md`).

[0.2.0]: https://github.com/ruanpato/tabledossier/releases/tag/v0.2.0
[0.1.0]: https://github.com/ruanpato/tabledossier/releases/tag/v0.1.0
