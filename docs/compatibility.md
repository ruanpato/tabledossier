# Compatibility

Last reviewed: 2026-09-21.

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
Spark Connect sessions (shared access mode, serverless). **None of this has been verified on Databricks yet.**

## What was actually tested

| Area | Environment | Evidence |
| --- | --- | --- |
| CLI (`init`, `validate`, `generate`, `render`, `schema`) and all unit tests | Python 3.10.19, 3.11.14, 3.12.13, 3.13.12, 3.14.6 on macOS arm64, installed from the built wheel, without PySpark | 159 unit tests pass on each; the generated notebook is byte-identical across versions |
| Notebook runtime, including the generated file executed end to end | Python 3.12.13, PySpark 3.5.9, delta-spark 3.3.3, OpenJDK 17.0.20, local mode | 25 integration tests pass (ground truth computed independently with Spark SQL; Delta pinning under a concurrent commit) |
| Notebook runtime | Python 3.12.13, PySpark 4.0.4 (ANSI mode on by default), OpenJDK 17.0.20, local mode | 22 integration tests pass, 3 Delta tests skipped (delta-spark not installed there); full-scope `try_parse_json` validation measured |
| Mermaid output | Mermaid 11 (jsDelivr) in a browser, `securityLevel: strict` | Demo ERD, auxiliary flowchart and hostile-name cases parse and render |
| Offline installation | Python 3.13 wheelhouse + `pip install --no-index` | CLI installed and ran |
| CI workflow | `.github/workflows/ci.yml` | Written, **not executed** (no remote repository yet) |

## Pending validation (needs a Databricks workspace)

- Importing the `.py` source notebook and rendering of cells/widgets.
- Widget behaviour when re-running and when parameters are passed by Jobs.
- Writing to Unity Catalog volumes and workspace files; transfer with the Databricks CLI.
- Spark Connect sessions (shared access mode, serverless): `toLocalIterator` fallback, higher-order functions,
  `catalog.functionExists`.
- Unity Catalog `information_schema` constraint queries (not testable locally).
- `try_parse_json` availability on 15.4/16.4 LTS (detected at run time; falls back to `unsupported`).
- Delta features specific to Databricks (deletion vectors, liquid clustering columns in `DESCRIBE DETAIL`).

Follow [the smoke test](databricks-smoke-test.md) and record the results before claiming support.

## Python for the CLI

`requires-python >= 3.10`. Python 3.10 reaches end of life in October 2026; support may be dropped in a later
release.
