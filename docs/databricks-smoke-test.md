# Databricks smoke test (manual, reproducible)

This procedure validates a release on a real workspace. It has **not** been executed for 0.1.0 or 0.2.0; record
your results in the table at the end (or in an issue) before claiming support for a runtime.

## Prerequisites

- A workspace with Unity Catalog, a catalog/schema where you may create tables (e.g. `demo.analytics`) and a
  volume you may write (e.g. `/Volumes/demo/analytics/results`).
- Compute on the runtime under test (16.4 LTS, 15.4 LTS or 17.3 LTS); repeat for single-user and shared access
  modes, and serverless if available.
- Locally: a checkout of this repository installed with `python -m pip install .`, and the Databricks CLI if you
  want to test downloads.

## Steps

1. **Generate** locally, offline:
   ```bash
   tabledossier generate --config examples/databricks/demo.config.json --output dist/smoke_databricks.py
   ```
2. **Create demo data**: in a SQL editor attached to the compute, run `USE CATALOG demo; USE SCHEMA analytics;`
   then [`examples/demo/create_demo_tables.sql`](../examples/demo/create_demo_tables.sql), then
   [`examples/demo/add_demo_constraints.sql`](../examples/demo/add_demo_constraints.sql).
   *Expected*: four tables; `customers` has column mapping enabled.
3. **Import** `dist/smoke_databricks.py` into the workspace. *Expected*: cells, Markdown sections 1–9 and titles
   render; no cell has output.
4. **Run the Parameters cell only.** *Expected*: four widgets appear with the generated defaults.
5. The widgets already contain the four demo tables and `/Volumes/demo/analytics/results/tabledossier`
   (adjust `output_dir` to your volume). Append `"demo.analytics.missing"` to `tables_json`, then *Run all*.
   *Expected*: the run finishes with status `partial` (only `missing` failed with `TABLE_OR_VIEW_NOT_FOUND`); the
   summary, dictionary and quality report print, with configured checks 3 pass, 1 fail (`orders_amount_range`),
   1 not evaluated (`returns_reason_nulls`); the export cell prints the results path and a `databricks fs cp`
   command.
6. **Re-run the Parameters cell.** *Expected*: widget values are unchanged.
7. **Download** with the printed command (or Catalog Explorer) and run locally:
   ```bash
   tabledossier validate --profile downloaded/<run_id>/profile.json      # exit code 5 (partial) expected
   tabledossier render --input downloaded/<run_id>/profile.json --output docs/generated/<run_id>
   ```
   *Expected*: validation passes; rendered files are identical to those written by the notebook.
8. **Inspect `profile.json`**:
   - `run.environment.execution_context` is `databricks` and `databricks_runtime_version` matches the compute;
   - `consistency.mode` is `pinned_delta_version` for the four demo tables;
   - `constraints` of `orders` include `orders_pk` and `orders_customer_fk` (`information_schema`,
     `not_enforced`), and `relationships` include the declared foreign key;
   - `run.capabilities.try_parse_json.available` and, for `order_events.payload`, `json_invalid_count` is
     `measured` (40) when available, otherwise `unsupported`;
   - no `SECRET`/sample values appear anywhere; errors are sanitized.
9. **Deep level** (0.2.0): set `analysis_level` to `deep` (keep `config_json` = `{}`), run again. *Expected*:
   - `run.analysis_level` is `deep`, `schema_version` is `1.1`, and `tabledossier validate` passes locally;
   - `run.environment.spark_connect` is `true` on shared access mode and serverless, `false` on single-user compute;
   - `orders` has profiled element fields (`items[]`, `items[].sku`, `items[].qty`, `items[].price`,
     `attributes{key}`, `attributes{value}`, `tags[]`) whose denominators are `elements` or `entries`; for
     example `items[].qty` has `null_count` 500 of `element_count` 3000;
   - `orders.deep.extra_passes.planned` ≤ 2 and `operations.planned` contains one `element_explode_pass`;
   - `order_events` field `payload` has `json_paths` with `$.status`, `$.amount`, `$.channel` and `$[*]`;
     `json_paths.full_scope.method` is `variant` when `run.capabilities.variant_functions.available` is true, else
     `get_json_object`;
   - the quality report has the section "Deep analysis: coverage and budget"; no sampled value appears anywhere.
10. **Metadata level**: set `analysis_level` to `metadata`, run again. *Expected*: no sample or aggregation
   operations; row counts `unavailable` unless statistics exist.
11. **Destination error**: set `output_dir` to a path without write permission. *Expected*: the validation cell
    fails with a message about the output directory before any table is read.

## Record

| Date | Runtime | Access mode | Steps passed | Notes / issues |
| --- | --- | --- | --- | --- |
| | | | | |
