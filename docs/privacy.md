# Privacy and value exposure

## Defaults

- **No sampled values, raw records or frequent-value labels are persisted.** Sampled string values are inspected
  transiently on the Databricks driver for format and JSON inference and then discarded.
- Sample concentration is summarized without labels (distinct count, top-1 and top-5 shares).
- Top-level JSON key names are listed when they look like structural field names and there are at most 50 of
  them; map-like objects and unusual keys are not listed. Set `value_policy.json_key_names = "redact"` to never
  list keys.
- **Deep level.** Array elements and map entries are described by counts, lengths and (numeric/temporal)
  extremes; string elements never expose values or extremes. Element distinct counts return only numbers. JSON path
  catalogues list key names only (never values), under the same `json_key_names` and `redact_columns` rules; keys of
  map-like objects (more than `deep.max_json_object_keys` distinct keys, or keys that do not look like field names)
  and keys seen in fewer than two sampled documents or in less than 10% of the documents containing their object are
  collapsed into `*`, because such keys are often data (identifiers, e-mails, dates). Tests plant secret markers in
  elements, map keys and values, JSON keys and JSON values and check that none reaches the profile or the documents.
- **Deep level, part II.** Exact uniqueness, referential validation and relationship hypotheses return only counts
  to the driver: duplicated key values, orphan values and matching values are never collected, persisted or
  rendered (tests plant secret markers in duplicated keys and orphan values). Profiles name the key columns and the
  relationships that were checked.
- Engine error messages are reduced to their first line with quoted literals and URIs replaced by placeholders,
  both in profiles and in notebook logs. Identifiers you asked to profile (backtick-quoted) are kept.
- Storage locations, table owners and table properties are not recorded (only Delta `CHECK` constraint
  expressions are).
- Nothing is sent to external services. There is no telemetry.

## What a profile still reveals

Profiles are **not anonymized**. They contain schema, comments, declared constraints, aggregate statistics
(including numeric and temporal minimum, maximum, mean and quantiles by default), counts, observed formats and the
filter values used to define the scope. Store and share them under the same policy as the table metadata.

## Controls

| Need | Setting |
| --- | --- |
| No sample at all | `sampling.method = "none"` (format and JSON inference become `not_computed`) |
| No numeric/temporal extremes or distributions | `value_policy.aggregate_extremes = "redact"` |
| Redact specific columns (extremes, examples, JSON keys; listing a collection also covers its elements) | `value_policy.redact_columns` |
| No JSON key names at all (standard shapes and deep path catalogues) | `value_policy.json_key_names = "redact"` |
| No element or JSON path profiling | `analysis_level = "standard"`, or `deep.collections` / `deep.json_paths = false` |
| Persist a few example values for chosen columns | `value_policy.persist_examples = true` and `example_columns` (bounded by `max_examples_per_column` and `max_example_chars`) |

## Repository hygiene

Only synthetic examples are versioned. `.gitignore` excludes `results/`, `downloaded/`, `docs/generated/`, local
configuration (`profile.config.json`, `*.local.json`), environment files and keys. Choose an output directory
whose access matches your data policy.
