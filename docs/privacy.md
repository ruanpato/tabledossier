# Privacy and value exposure

## Defaults

- **No sampled values, raw records or frequent-value labels are persisted.** Sampled string values are inspected
  transiently on the Databricks driver for format and JSON inference and then discarded.
- Sample concentration is summarized without labels (distinct count, top-1 and top-5 shares).
- Top-level JSON key names are listed when they look like structural field names and there are at most 50 of
  them; map-like objects and unusual keys are not listed. Set `value_policy.json_key_names = "redact"` to never
  list keys.
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
| Redact specific columns (extremes, examples, JSON keys) | `value_policy.redact_columns` |
| Persist a few example values for chosen columns | `value_policy.persist_examples = true` and `example_columns` (bounded by `max_examples_per_column` and `max_example_chars`) |

## Repository hygiene

Only synthetic examples are versioned. `.gitignore` excludes `results/`, `downloaded/`, `docs/generated/`, local
configuration (`profile.config.json`, `*.local.json`), environment files and keys. Choose an output directory
whose access matches your data policy.
