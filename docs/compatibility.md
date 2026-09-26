# Compatibility

Last reviewed: 2026-09-26 (no new measurements since release 0.4.0, 2026-09-22; every row keeps its own date and run id).

## Targets

Chosen after consulting the official [Databricks Runtime release notes](https://docs.databricks.com/aws/en/release-notes/runtime/)
on 2026-09-21 (supported LTS releases: 18 LTS, 17.3 LTS, 16.4 LTS, 15.4 LTS, 14.3 LTS):

| Databricks Runtime | Spark | Python | Role |
| --- | --- | --- | --- |
| 16.4 LTS | 3.5.2 | 3.12.3 | Primary target |
| 15.4 LTS | 3.5.0 | 3.11.11 | Secondary target |
| 17.3 LTS | 4.0.0 | 3.12.3 | Secondary target |

The notebook uses only public PySpark APIs available since Spark 3.4 (parameterized `spark.sql`,
`functions.call_function` is used only when detected) and avoids RDD/`sparkContext` APIs so that it can run on
Spark Connect sessions (shared access mode, serverless). Every integration test also runs through a *local* Spark
Connect server for Spark 3.5 and 4.0 (below). **None of this has been verified on Databricks yet.**

## What was actually tested

| Area | Environment | Evidence |
| --- | --- | --- |
| CLI and all unit tests | Python 3.12.13 on macOS arm64; CI: Ubuntu with Python 3.10–3.14, macOS and Windows with Python 3.12, installed from the built wheel, without PySpark | 295 unit tests pass (including 1.0/1.1/1.2 contract reading, validator agreement on the 1.1 and 1.2 additions and their invariants, deep planning, JSON path catalogue, key selection and budgets, referential status rules, hypothesis planning without names, privacy, Unity Catalog constraint assembly from synthetic `information_schema` rows, the job summary and its schema, and the release script) |
| Notebook runtime, classic sessions | Python 3.12.13, PySpark 3.5.9 + delta-spark 3.3.3, OpenJDK 17.0.20, local mode | 78 integration tests pass: standard and deep levels against independent Spark SQL ground truth (explode, `from_json`, `GROUP BY ... HAVING` for exact uniqueness, `LEFT ANTI JOIN` for orphans, `LEFT SEMI JOIN` for hypothesis inclusion), Delta pinning (including the two tables of a relationship after later writes), budgets and action counts, privacy (secret markers in duplicated keys and orphan values), isolation of unexpected failures, declared constraints through a simulated `information_schema`, generated notebook end to end (including the job summary) |
| Notebook runtime, Spark Connect | Same, through a local Spark Connect server (`SparkSession.builder.remote("local[2]")`, spark-connect 3.5.9 JAR from Maven Central), with Delta | 78 integration tests pass; `run.environment.spark_connect` recorded `true`; `toLocalIterator`, `catalog.functionExists`, `spark.conf.get`, `spark.sql(..., args=...)` (on synthetic `information_schema`-shaped tables), `DESCRIBE` statements and `Row.asDict` exercised for real |
| Notebook runtime, classic and Spark Connect | Python 3.12.13, PySpark 4.0.4 (bundled Connect server; ANSI mode on by default), OpenJDK 17.0.20 | 74 integration tests pass and 4 Delta tests are skipped in each mode (delta-spark not installed there: Delta pinning of profiles and of relationships); JSON paths validated with variant functions (`try_parse_json`, `try_variant_get`, `is_variant_null`, `schema_of_variant`) |
| Deep level, part II | PySpark 3.5.9 (+ Delta) and 4.0.4, classic and Spark Connect | Exact uniqueness (keys packed in one explode + grouped pass), referential checks (grouped target, left join, cross-joined aggregates) and hypothesis inclusion run unchanged in the four modes; declared PRIMARY KEY/UNIQUE/FOREIGN KEY constraints tested end to end with a simulated `information_schema` (local tables with its column layout, read by the real parameterized queries) and their assembly with synthetic rows |
| Deep level, JSON path methods | PySpark 3.5.9: `get_json_object` (no variant functions); PySpark 4.0.4: variant functions | Presence (and type with variant) compared with `from_json` ground truth; `get_json_object` returns NULL for both JSON `null` and absent paths (observed) |
| Jobs integration | Local harness (`examples/demo/run_notebook_locally.py`): widgets passed in, `dbutils.notebook.exit` recorded; the four Spark modes above | The generated notebook returns one job summary equal to `job_summary(profile, ...)` and valid against its schema; nothing is returned with `jobs.exit_summary = false` or without `dbutils.notebook`; widgets passed in are never recreated. Never run as a Databricks Job |
| Release workflow | GitHub Actions dry run on pull requests ([run 35732144301](https://github.com/ruanpato/tabledossier/actions/runs/35732144301), PR #11) | Version check, build, 279 unit tests against the installed wheel, notes from the CHANGELOG, `SHA256SUMS` verified, artefacts uploaded; the publish job was skipped (no tag) |
| Mermaid output | Mermaid 11 (jsDelivr) in a browser, `securityLevel: strict` (0.1.0) | Demo ERD, auxiliary flowchart and hostile-name cases parse and render |
| Offline installation | Python 3.13 wheelhouse + `pip install --no-index` (0.1.0) | CLI installed and ran |
| Continuous integration | GitHub Actions [run 35737813091](https://github.com/ruanpato/tabledossier/actions/runs/35737813091) (commit `87fa704`, release 0.4.0): Ubuntu with Python 3.10–3.14; Windows and macOS with Python 3.12; Spark jobs on Ubuntu with Temurin 17 | All 12 jobs green: lint + mypy, unit tests on every OS/Python (295 each), PySpark 3.5 + Delta (78 passed), Spark Connect 3.5 + Delta (78 passed), PySpark 4.0 (74 passed, 4 skipped), Spark Connect 4.0 (74 passed, 4 skipped) |
| Release workflow on the release pull request | [Run 35737813284](https://github.com/ruanpato/tabledossier/actions/runs/35737813284) (dry run, commit `87fa704`) | Tag `v0.4.0` derived from `__version__`, dated CHANGELOG section and link accepted without warnings, wheel and sdist of 0.4.0 built, 295 unit tests against the installed wheel, notes extracted, `SHA256SUMS` verified; publish job skipped |
| Release workflow on a real tag | [Run 35740789902](https://github.com/ruanpato/tabledossier/actions/runs/35740789902) (tag `v0.4.0` on merge commit `76e2f57`) | Tag, version, CHANGELOG and branch checks passed; wheel and sdist built, unit tests against the wheel, notes and `SHA256SUMS`; the publish job created the **draft** pre-release "TableDossier 0.4.0 (alpha)" with the three files (checksums re-verified after download) |

Delta works with a local Spark Connect server (the server session is started with the Delta extensions), so the
Delta tests are not skipped in Connect mode. With PySpark 3.5 on Python 3.12, the Connect client needs `setuptools`
(it still imports `distutils`).

## Pending validation (needs a Databricks workspace)

Status: **pending** for every runtime and access mode — no smoke-test result has been recorded yet.

- Importing the `.py` source notebook and rendering of cells/widgets.
- Widget behaviour when re-running and when parameters are passed by Jobs; the job summary returned with
  `dbutils.notebook.exit` (Jobs output, `dbutils.notebook.run`).
- Writing to Unity Catalog volumes and workspace files (including from serverless compute); transfer with the Databricks CLI.
- Databricks Spark Connect sessions (shared access mode, serverless): the same APIs pass with a local Connect
  server, but Databricks may restrict some of them (`spark.conf.get`, `catalog.functionExists`, and
  `toLocalIterator` used by the sample). The runtime records a refused configuration read as `null` and a refused
  function check as `unknown`; the [smoke test](databricks-smoke-test.md#only-measurable-on-databricks) says where to
  look.
- Unity Catalog `information_schema` constraint queries (tested locally against a simulated `information_schema` with the same column layout, never against Unity Catalog).
- `try_parse_json` and the variant functions on 15.4/16.4 LTS (detected at run time; JSON validity falls back to
  `unsupported` and JSON path validation to `get_json_object`).
- Deep level cost and memory on large tables (higher-order functions over large arrays, the element explode pass).
- Delta features specific to Databricks (deletion vectors, liquid clustering columns in `DESCRIBE DETAIL`).

Follow [the smoke test](databricks-smoke-test.md) and record the results before claiming support.

## Python for the CLI

`requires-python >= 3.10`. Python 3.10 reaches end of life in October 2026; support may be dropped in a later
release.
