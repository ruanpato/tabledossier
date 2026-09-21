# 0002 — Canonical profile contract and two-layer validation

Date: 2026-09-21 · Status: accepted

## Context

Profiles are produced inside Databricks (no third-party packages available) and consumed locally by the CLI, CI
and future connectors. Metric values must keep their types (integers, decimals, timestamps, non-finite floats) and
the difference between "zero" and "not measured".

## Decision

- The JSON Schema files in `src/tabledossier/schemas/` are the single definition of the contract (version 1.0).
- The notebook validates with `schemacheck`, a standard-library interpreter of the subset of Draft 2020-12
  keywords the schemas use, plus cross-field invariants in `contract`. Unknown keywords are forbidden in the
  schemas (tested), so nothing is silently ignored.
- The CLI and CI additionally validate with `jsonschema` (Draft 2020-12). Tests assert both validators agree on
  valid documents and on systematically mutated ones.
- Profiles are plain JSON built from dictionaries by builder functions; there are no parallel model classes that
  could drift from the schema.
- Metrics carry four independent axes (status, scope, accuracy, source). Decimals are strings; non-finite floats
  are explicit string markers; paths are typed segments.
- Readers accept only known versions and say which ones they support.

## Consequences

- The contract is inspectable (`tabledossier schema profile`) and documented in `docs/contract.md`.
- Adding a field requires a schema change; the strict schema catches typos and drift.
- Consumers can rely on a stable meaning for every field regardless of the engine that produced it.
