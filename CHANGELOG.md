# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/).

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

[0.1.0]: https://github.com/ruanpato/tabledossier/releases/tag/v0.1.0
