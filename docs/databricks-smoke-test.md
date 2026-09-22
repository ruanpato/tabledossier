# Databricks smoke test (manual, reproducible)

This procedure validates a release on a real workspace. It has **not** been executed on a workspace for any release
yet: until results are recorded in the table at the end (or in an issue), every Databricks item of
[compatibility](compatibility.md) stays *pending*. Use only the synthetic demo tables, and record no workspace, user,
catalog or path names of a private environment.

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
   command; the last cell ("Job summary") shows "Notebook exited" with a one-line JSON summary whose `status` is
   `partial` and whose `tables` are 4 succeeded, 1 failed (0.4.0).
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
     `not_enforced`), and `relationships` include the declared foreign key; `orders.notes` has no note about an
     unresolved foreign key, and `operations.observed` shows `op_constraints` as `succeeded` (not `skipped`);
   - `run.capabilities.try_parse_json.available` and, for `order_events.payload`, `json_invalid_count` is
     `measured` (40) when available, otherwise `unsupported`;
   - no `SECRET`/sample values appear anywhere; errors are sanitized.
9. **Deep level** (0.2.0): set `analysis_level` to `deep` (keep `config_json` = `{}`), run again. *Expected*:
   - `run.analysis_level` is `deep`, `schema_version` is `1.2`, and `tabledossier validate` passes locally;
   - `run.environment.spark_connect` is `true` on shared access mode and serverless, `false` on single-user compute;
   - `orders` has profiled element fields (`items[]`, `items[].sku`, `items[].qty`, `items[].price`,
     `attributes{key}`, `attributes{value}`, `tags[]`) whose denominators are `elements` or `entries`; for
     example `items[].qty` has `null_count` 500 of `element_count` 3000;
   - `orders.deep.extra_passes.planned` ≤ 2 and `operations.planned` contains one `element_explode_pass`;
   - `order_events` field `payload` has `json_paths` with `$.status`, `$.amount`, `$.channel` and `$[*]`;
     `json_paths.full_scope.method` is `variant` when `run.capabilities.variant_functions.available` is true, else
     `get_json_object`;
   - the quality report has the section "Deep analysis: coverage and budget"; no sampled value appears anywhere;
   - record `timings_ms` of each table and the duration of the run: the cost of the deep level on real storage is not
     known yet.
10. **Deep level, part II** (0.3.0), in the profile of the run of step 9 (the generated configuration enables
    `deep.uniqueness`, `deep.referential` and `deep.relationship_hypotheses`). *Expected*:
    - `customers` key `customer_id` and `orders` key `order_id` are `measured` with outcome `unique`, and their
      origins include `configured` and `declared_primary_key` (the constraints of `add_demo_constraints.sql`);
    - `order_events` key `event_id` has outcome `duplicates` with `duplicate_key_groups` 3 and
      `rows_in_duplicate_groups` 6, and key `order_id, event_type` has 1,000 duplicate groups;
    - every table has at most one `uniqueness_pass` in `operations.planned`;
    - relationships `orders_customer` (configuration) and `orders_customer_fk` (declared) are `violated` with
      `orphan_rows` 5 (0.50%), `events_order` is `validated`; each `validation_detail.from.delta_version` and
      `to.delta_version` equals the `consistency.delta_version` of that table; `referential_validation.planned` is 3;
    - `relationship_hypotheses.hypotheses` lists `analytics.customers (referrer_id)` →
      `analytics.customers (customer_id)` with inclusion 100% (sample) and `cardinality` `null`;
    - the quality report has "7. Uniqueness (exact)" and "8. Referential integrity", `relationships.md` has
      "Referential validation" and "Hypotheses (data-driven, not relationships)", and `erd.mmd` draws only
      `orders_customer`;
    - the duplicated event id `evt-000000` appears nowhere in the result package.
11. **Metadata level**: set `analysis_level` to `metadata`, run again. *Expected*: no sample or aggregation
   operations; row counts `unavailable` unless statistics exist.
12. **Destination error**: set `output_dir` to a path without write permission. *Expected*: the validation cell
    fails with a message about the output directory before any table is read.
13. **Job** (0.4.0; [running as a Job](databricks-jobs.md)): create a Job with one notebook task on the imported
    notebook, with parameters `tables_json` = `["demo.analytics.orders", "demo.analytics.customers"]`,
    `analysis_level` = `standard`, `output_dir` = your volume path and `config_json` = `{}`; run it. *Expected*:
    - the run succeeds and writes a new `<run_id>` directory; the widget defaults of the notebook did not replace the
      parameters (`run.parameter_sources.widgets` and `run.effective_config.tables` show the two tables);
    - the task output (`databricks jobs get-run-output <task run id>`, field `notebook_output.result`, or the task
      page) is the job summary: valid against `tabledossier schema job_summary`, same `run_id` and `run_dir` as the
      result package, `status` `succeeded`, `tables.total` 2;
    - with `config_json` = `{"jobs": {"exit_summary": false}}`, the run succeeds without a notebook output.

## Only measurable on Databricks

These points cannot be reproduced with local Spark, even through a local Spark Connect server. Note what you observe
for each runtime and access mode in the record below.

| Point | Where to look | Local evidence so far |
| --- | --- | --- |
| Cost and memory of the deep level on large tables (higher-order functions over large arrays, the element explode pass, uniqueness and referential passes) | `timings_ms` per table, run duration, the compute's metrics; failures would show as `error` metrics or failed operations | Only small synthetic tables; action counts are bounded and tested, cost is not |
| Unity Catalog volumes from serverless compute (create the run directory, write, no overwrite) | Step 5 on serverless; `manifest.json` written; step 12 message | Local file system only |
| `spark.conf.get` restricted (serverless, shared access mode) | `run.environment.session_timezone` and `ansi_mode`: `null` means the configuration was hidden; the run must still succeed | Works locally, classic and Connect; a failure is caught and recorded as `null` |
| `spark.catalog.functionExists` restricted | `run.capabilities.*.detail`: `= unknown` means the check was refused; JSON validity then becomes `unsupported` and JSON path validation falls back | Works locally, classic and Connect; a failure is caught |
| `DataFrame.toLocalIterator` on serverless (the sample of the standard level) | `tables[].sample.rows` > 0 and no `sample` error; `operations.observed` of `op_sample` | Works locally, classic and Connect |
| Unity Catalog `information_schema` (declared keys and foreign keys, referenced constraints in other catalogs) | Steps 8 and 10; table notes about unresolved foreign keys | Simulated `information_schema` with the same column layout |
| Jobs parameters and `dbutils.notebook.exit` | Step 13 | Local harness: widgets and a recording `dbutils.notebook.exit` |

## Record

| Date | TableDossier | Runtime | Access mode | Steps passed | Notes / issues |
| --- | --- | --- | --- | --- | --- |
| — | — | — | — | — | Not executed yet (pending) |

For each run, record the date, the TableDossier version, the runtime version string
(`run.environment.databricks_runtime_version`), the access mode (single user, shared/standard, serverless), the steps
that passed and, for the others, the observed behaviour. Generic descriptions only: no workspace URLs, user names,
catalog names other than the demo ones, or values.
