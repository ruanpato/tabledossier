# Examples (synthetic only)

| Path | What it is |
| --- | --- |
| [`demo/create_demo_tables.sql`](demo/create_demo_tables.sql) | Creates four synthetic tables in the current schema: `customers` (simple types, special names, all-null and constant columns), `orders` (nested struct, arrays, map, decimals, an outlier, a 1,500-element array), `order_events` (JSON in a string column with invalid, `null`, array and oversized values; sparse columns; `TIMESTAMP_NTZ`) and `returns` (empty). Portable between Databricks (Delta) and local Spark. |
| [`demo/add_demo_constraints.sql`](demo/add_demo_constraints.sql) | Optional Unity Catalog PK/FK constraints for the demo tables. |
| [`demo/demo.config.json`](demo/demo.config.json) | Configuration used for the committed demo (local two-part names): filters, checks, a relationship with cardinality and one without, one allow-listed example column. |
| [`databricks/demo.config.json`](databricks/demo.config.json) | Same configuration for `demo.analytics.*` on Databricks (used by the [smoke test](../docs/databricks-smoke-test.md)). |
| [`demo/annotations.json`](demo/annotations.json) | Human annotations: descriptions, owner, tags. |
| [`demo/output/notebook/`](demo/output/notebook) | The notebook generated from `demo.config.json` and its generation manifest. |
| [`demo/output/run/`](demo/output/run) | The complete result package written by that notebook when executed on local Spark with the demo tables. |
| [`demo/output/annotated/`](demo/output/annotated) | The documents re-rendered offline from `run/profile.json` with `annotations.json`. |
| [`demo/output/deep/`](demo/output/deep) | The result package of the same notebook run at `analysis_level = deep` with the default deep budgets (contract 1.1): element fields of `orders`, the JSON path catalogue of `order_events.payload`, and the deep coverage section of the quality report. |
| [`demo/run_notebook_locally.py`](demo/run_notebook_locally.py) | Development harness: executes a generated notebook with a local SparkSession (only `dbutils.widgets` is simulated). Not a supported way to profile production data. |
| [`demo/build_demo_outputs.py`](demo/build_demo_outputs.py) | Rebuilds everything under `demo/output` (needs PySpark and Java; uses Delta when delta-spark is installed). |

The committed profiles record `execution_context: spark` because they were produced by local Spark, not Databricks.
