# Architecture

## Principles

1. **Generate offline, execute where the data lives, document offline.** The CLI never connects to a source.
2. **One contract.** Every engine adapter produces the same engine-neutral profile; every document is derived
   from it.
3. **One source of code.** The notebook runtime *is* the package's modules, embedded verbatim.
4. **Planner describes, executor acts.** Adding metrics adds expressions to bounded shared passes, never one
   Spark action per column.
5. **Say what is known.** Status, scope, accuracy and source accompany every number; absence is never zero.

## Modules

| Module | Role | Embedded in notebook | Imports |
| --- | --- | --- | --- |
| `widgets` | Create/read notebook widgets without resetting values | yes (Parameters section) | stdlib |
| `_version` | Version string | yes | — |
| `jsonutil` | Canonical JSON, fingerprints, value encoding, UTC formatting | yes | stdlib |
| `schemacheck` | Interpreter of the JSON Schema subset used by the schemas | yes | stdlib |
| `errors` | Sanitized error records | yes | stdlib |
| `paths` | Table identifiers, quoting, typed field paths | yes | stdlib |
| `config` | Defaults, merge, validation, widget precedence | yes | stdlib |
| `metrics` | Metric records (status/scope/accuracy/source) | yes | stdlib |
| `planning` | Schema tree, field selection, sample plan, aggregate specs and budgets | yes | stdlib |
| `semantic` | Format detectors, Wilson intervals, JSON shape, candidate roles | yes | stdlib |
| `deep` | Deep level: targets, element nodes and metric specs, explode-pass plan, JSON path catalogue and validation plan | yes | stdlib |
| `findings` | Heuristic findings | yes | stdlib |
| `quality` | Configured checks, proposed rules | yes | stdlib |
| `relationships` | Declared and provided relationships | yes | stdlib |
| `contract` | Version checks and profile invariants | yes | stdlib |
| `render` | Markdown and Mermaid documents | yes | stdlib |
| `package` | Result package, manifests, no-overwrite writing | yes | stdlib |
| `assemble` | Engine-neutral assembly of metrics, tables and the profile | yes | stdlib |
| `runtime.spark` | Spark adapter: metadata, snapshot, sample, aggregate passes, deep expressions and the element explode pass | yes | stdlib + PySpark |
| `runtime.databricks` | Run orchestration: parameters, destination probe, batch, export | yes | stdlib + PySpark (via `runtime.spark`) |
| `resources`, `validation`, `notebook`, `cli` | Packaged schemas, formal validation (`jsonschema`), notebook composition, CLI | no | stdlib + jsonschema |

Importing `tabledossier`, validating, generating and rendering never import PySpark (a test runs this in a
subprocess and inspects `sys.modules`). There is no plugin framework: a second engine (PostgreSQL) will be an
adapter next to `runtime/spark.py` that compiles the same planned specs.

## Notebook composition

`tabledossier.notebook` builds a Databricks *source* notebook:

```text
# Databricks notebook source
[md] presentation (no results yet, safety, how to use, license)
[md] 1. Parameters            [code] widgets module   [code] generated config literal + ensure_widgets(...)
[md] 2. Runtime definitions   [code] JSON Schemas (exact file text)   [code] one cell per runtime module …
[md] 3. Validate …            [code] prepare_run(...)        (validates widgets, creates the run dir, writes a "running" manifest)
[md] 4. Analysis plan         [code] describe_plan(...)
[md] 5. Execution             [code] execute_run(...)        (tables sequentially, failures isolated)
[md] 6. Summary  7. Data dictionary  8. Quality and limitations
[md] 9. Export                [code] export_run(...) + transfer instructions
```

Each runtime cell is the module source copied verbatim, headed by its source path, SHA-256 and license, with
only the `from tabledossier… import …` statements removed. The composer enforces, with the AST:

- only standard-library imports (plus `pyspark` in `runtime.*`); no relative or `__future__` imports;
- intra-package imports import names defined by earlier modules (dependency order);
- top-level names are unique across modules and import bindings are consistent, so concatenation cannot
  shadow anything;
- no line that Databricks would read as a cell separator or magic command;
- the finished notebook parses as a single Python module.

Configuration values enter only as Python literals produced by `pprint`/`repr` (strings are escaped), so a
hostile value cannot create cells or code (tested). Generation is deterministic: no timestamps, stable ordering,
identical bytes across Python 3.10–3.14 (verified).

Because the notebook is a valid Python module, integration tests execute the generated file itself with a local
SparkSession and a minimal `dbutils.widgets` stand-in (`examples/demo/run_notebook_locally.py`). Only widgets are
simulated; every Spark call is real. This is not a substitute for validation on Databricks.

## Execution per table (standard level)

1. `DESCRIBE TABLE EXTENDED` (resolution + catalog metadata), `DESCRIBE DETAIL` (Delta), Unity Catalog
   `information_schema` key constraints (parameterized SQL, quoted identifiers).
2. Delta: `DESCRIBE HISTORY … LIMIT 1`, then `SELECT * FROM <table> VERSION AS OF <n>` for every read.
3. Schema tree from the snapshot's `DataFrame.schema`; filters (DataFrame API) and column projection.
4. One projected sample: `substring(col, 1, max_value_chars)` + truncation flag per string field, `limit(max_rows)`,
   iterated with a retained-bytes budget.
5. Semantic inference on the sample (in the driver, transient), including real JSON parsing.
6. Aggregate plan → compiled Column expressions → up to `max_aggregate_passes` `agg` calls. Expressions rejected
   by the analyzer (no data read) are isolated; runtime failures fail only their pass.
7. Assembly into metrics, findings, checks and proposals; sanitized errors; timings.

No UDFs, no caching, no `toPandas()`, no `groupBy` per column, no writes to sources.

## Execution per table (deep level)

The deep level runs the standard steps above and adds, per table:

1. **Targets.** `deep.targets` (explicit fields, or every array/map and probable-JSON field) selects collections
   and JSON strings. Element nodes are the schema nodes inside one collection level; nested collections are omitted.
2. **Element metrics in the shared passes.** For each element node, the planner emits specs such as
   `el_count_null` or `el_max`; the adapter compiles them to per-row higher-order functions (`filter`, `transform`,
   `array_min`/`array_max`, `map_keys`/`map_values`) summed over rows. They are exact, need no explode and are the
   lowest-priority expressions of the plan: standard metrics are planned exactly as at the standard level, deep ones
   use the room left in the last standard pass plus at most `deep.max_extra_passes` extra passes.
3. **JSON paths.** Catalogues are built in the driver from the sample already collected (values are discarded
   right after). Listed paths become one or three expressions each (presence, JSON null, dominant type) with the
   detected method (`variant` functions, else `get_json_object`), also inside the aggregation passes.
4. **One element explode pass**, if an extra pass remains: every atomic element field of the table is wrapped in a
   struct with one typed slot per field, concatenated per row and exploded once (`posexplode`), over a bounded
   sample (rows, then elements) or the full scope when its measured size fits `deep.max_elements`. Only counts
   (`count`, `count(DISTINCT)`) return to the driver.
5. **Coverage.** `table.deep` records targets, extra passes used against the budget, omitted expressions and
   everything a budget limited.

The number of Spark actions per table is bounded by the configuration (one sample, `max_aggregate_passes` plus
`deep.max_extra_passes` aggregations and explodes); it does not grow with the number of columns or metrics (tested).

## Testing against Spark Connect

Databricks shared access mode and serverless compute are Spark Connect clients. `TD_TEST_SPARK_MODE=connect` runs
the whole integration suite, including the generated notebook, through a local Spark Connect server started in the
test process (`SparkSession.builder.remote("local[2]")`); CI runs it for Spark 3.5 (with Delta) and 4.0. The recorded
`run.environment.spark_connect` is observed from the session object and asserted in those runs.

## Decisions

- [0001 — Self-contained notebook generated from the package modules](decisions/0001-self-contained-notebook.md)
- [0002 — Canonical profile contract and two-layer validation](decisions/0002-canonical-contract.md)
- [0003 — Limits of inference](decisions/0003-inference-limits.md)
- [0004 — Deep level: budgeted operations over the shared passes](decisions/0004-deep-level.md)
