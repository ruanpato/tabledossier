# 0003 — Limits of inference

Date: 2026-09-21 · Status: accepted

## Decision

TableDossier reports observations, not conclusions:

- **Physical type, observed format and candidate role are separate.** Formats come from small, testable
  detectors with real parsing (JSON, ISO dates); results can be `unknown`, `mixed`, `ambiguous` or
  `insufficient_data`.
- **Samples stay samples.** Sample-based numbers have `scope: sample`; distinct counts from a sample are never
  extrapolated. Prefix samples are labelled potentially biased. Truncated values are excluded from inferences that
  need the whole value.
- **Confidence intervals are descriptive.** Wilson intervals describe the observed proportion under independent
  sampling; they do not remove bias or prove meaning.
- **Approximations are labelled.** HyperLogLog distinct counts do not prove uniqueness or keys; a hexadecimal
  format does not prove a hash algorithm.
- **Relationships are never inferred from names.** Only declared constraints and people provide relationships;
  cardinality is drawn only when a person provided it. Unity Catalog PK/FK are informational and reported as not
  enforced.
- **Business meaning is unknown unless stated** by a source comment or an annotation, and both are labelled with
  their origin.
- **Findings are heuristics** with configurable thresholds and explained severities; they never fail a run. Checks
  are only those configured by people. No global quality score is computed.
- **Temporal checks declare their reference.** Values after the reference instant are counted, not called errors.

## Consequences

Reports are sometimes less assertive than users expect, but every statement can be traced to evidence and to
its limitations. Stronger claims (exact uniqueness, referential validation, element-level profiling) belong to
the future `deep` level with explicit read budgets.
