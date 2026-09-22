# 0004 — Deep level: budgeted operations over the shared passes

Date: 2026-09-22 · Status: accepted

## Context

Release 0.2.0 profiles what the standard level only documented: elements of arrays and maps and the paths inside
JSON strings. Both can be expensive (explode multiplies rows; JSON parsing is CPU-heavy) and both can expose values
(array elements and JSON keys are often data). The rules of the project still hold: the planner describes and the
executor acts, adding metrics never adds one Spark action per column, samples stay samples and nothing is persisted
by default.

## Decision

- `deep` is `standard` plus **opt-in operations with their own budgets** (`deep` configuration section), each one
  declared in `operations.planned` and summarized in a per-table `deep` record that lists what the budgets limited.
- **Element metrics use higher-order functions per row, summed inside the shared passes.** They are exact over the
  analysed scope, need no explode, and always rank below every standard metric, so a deep run never loses a standard
  metric to deep work. Extra aggregation passes for deep expressions are bounded by `deep.max_extra_passes`.
- **Metrics that need one row per element** (distinct counts) run in **one** explode pass per table for all
  collections (typed slots per element field, concatenated and exploded once), over a bounded sample by default or the
  full scope only when its measured size fits the element budget. `scope`, `source` and `details` state which.
- **Denominators of element metrics are elements or entries**, never rows (checked by invariants).
- **JSON paths are catalogued from the existing sample** and labelled as sample; a catalogue is not a schema.
  Full-scope validation uses capabilities detected at run time (`variant` functions, else `get_json_object`) and
  documents each method's limits (`get_json_object` cannot tell a JSON null from an absent path).
- **Key names are the privacy boundary of JSON paths.** They follow `value_policy.json_key_names` and
  `redact_columns`; keys of map-like objects and rare keys are collapsed into `*`. String elements get lengths and
  counts, never values or extremes.
- **Contract 1.1 is additive.** The released 1.0 schema is kept verbatim for reading older profiles; the CLI
  validates each profile against the schema of its version.

## Consequences

- Deep runs cost at most `max_extra_passes` more Spark actions per table, whatever the number of columns.
- Element distinct counts are usually sample-based and say so; exact full-scope distinct counts, uniqueness and
  referential checks are left to the next part of the deep level.
- Some JSON paths are shown only as `*`; users who need names can use explicit targets with a more permissive value
  policy, knowingly.
