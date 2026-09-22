# Compatibility

Last reviewed: 2026-09-22 (release 0.2.0).

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
| CLI and all unit tests | Python 3.12.13 on macOS arm64; CI: Ubuntu with Python 3.10–3.14, macOS and Windows with Python 3.12, installed from the built wheel, without PySpark | 199 unit tests pass (including 1.0/1.1 contract reading, validator agreement on the 1.1 additions, deep planning, JSON path catalogue and privacy) |
| Notebook runtime, classic sessions | Python 3.12.13, PySpark 3.5.9 + delta-spark 3.3.3, OpenJDK 17.0.20, local mode | 49 integration tests pass: standard and deep levels against independent Spark SQL ground truth (explode, `from_json`), Delta pinning, budgets, privacy, generated notebook end to end |
| Notebook runtime, Spark Connect | Same, through a local Spark Connect server (`SparkSession.builder.remote("local[2]")`, spark-connect 3.5.9 JAR from Maven Central), with Delta | 49 integration tests pass; `run.environment.spark_connect` recorded `true`; `toLocalIterator`, `catalog.functionExists`, `spark.conf.get`, `spark.sql(..., args=...)` (on synthetic `information_schema`-shaped tables), `DESCRIBE` statements and `Row.asDict` exercised for real |
| Notebook runtime, classic and Spark Connect | Python 3.12.13, PySpark 4.0.4 (bundled Connect server; ANSI mode on by default), OpenJDK 17.0.20 | 46 integration tests pass and 3 Delta tests are skipped in each mode (delta-spark not installed there); JSON paths validated with variant functions (`try_parse_json`, `try_variant_get`, `is_variant_null`, `schema_of_variant`) |
| Deep level, JSON path methods | PySpark 3.5.9: `get_json_object` (no variant functions); PySpark 4.0.4: variant functions | Presence (and type with variant) compared with `from_json` ground truth; `get_json_object` returns NULL for both JSON `null` and absent paths (observed) |
| Mermaid output | Mermaid 11 (jsDelivr) in a browser, `securityLevel: strict` (0.1.0) | Demo ERD, auxiliary flowchart and hostile-name cases parse and render |
| Offline installation | Python 3.13 wheelhouse + `pip install --no-index` (0.1.0) | CLI installed and ran |
| Continuous integration | GitHub Actions [run 35686154929](https://github.com/ruanpato/tabledossier/actions/runs/35686154929) (commit `f7eb6ae`): Ubuntu with Python 3.10–3.14; Windows and macOS with Python 3.12; Spark jobs on Ubuntu with Temurin 17 | All 12 jobs green: lint + mypy, unit tests on every OS/Python (199 each), PySpark 3.5 + Delta (49 passed), Spark Connect 3.5 + Delta (49 passed), PySpark 4.0 (46 passed, 3 skipped), Spark Connect 4.0 (46 passed, 3 skipped) |

Delta works with a local Spark Connect server (the server session is started with the Delta extensions), so the
Delta tests are not skipped in Connect mode. With PySpark 3.5 on Python 3.12, the Connect client needs `setuptools`
(it still imports `distutils`).

## Pending validation (needs a Databricks workspace)

- Importing the `.py` source notebook and rendering of cells/widgets.
- Widget behaviour when re-running and when parameters are passed by Jobs.
- Writing to Unity Catalog volumes and workspace files; transfer with the Databricks CLI.
- Databricks Spark Connect sessions (shared access mode, serverless): the same APIs pass with a local Connect
  server, but Databricks may restrict some of them (e.g. configuration reads, catalog functions).
- Unity Catalog `information_schema` constraint queries (not testable locally).
- `try_parse_json` and the variant functions on 15.4/16.4 LTS (detected at run time; JSON validity falls back to
  `unsupported` and JSON path validation to `get_json_object`).
- Deep level cost and memory on large tables (higher-order functions over large arrays, the element explode pass).
- Delta features specific to Databricks (deletion vectors, liquid clustering columns in `DESCRIBE DETAIL`).

Follow [the smoke test](databricks-smoke-test.md) and record the results before claiming support.

## Python for the CLI

`requires-python >= 3.10`. Python 3.10 reaches end of life in October 2026; support may be dropped in a later
release.
