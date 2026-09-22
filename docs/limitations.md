# Limitations

Known limits of release 0.2.0, by design or not yet addressed.

## Execution

- **Not yet validated on Databricks.** See [compatibility](compatibility.md) and the [smoke test](databricks-smoke-test.md).
  Spark Connect is exercised only with a *local* Connect server (Spark 3.5 and 4.0), not with Databricks shared
  access mode or serverless compute.
- Tables run sequentially, up to `limits.max_tables` per run. Catalogs are never scanned automatically.
- Aggregation costs are not predicted. One `agg` call can still require several jobs and full scans; wide schemas,
  distinct-count sketches, quantiles, string/regex expressions, higher-order functions over large arrays and JSON
  parsing use CPU and memory. Physical scans, bytes read and cost are reported as `unknown`.
- `prefix` samples are cheap but biased (often one file or partition). `random` sampling may evaluate every row.
  The retained-bytes budget bounds kept values, not the transferred batch (rows × sampled columns ×
  `max_value_chars`) nor process memory.
- Only Delta tables are pinned to a snapshot. Views and other formats are read without a consistency guarantee
  between the sample, the aggregation passes and the element explode pass. Catalog metadata (size, comments,
  constraints) always describes the state at capture time.
- Filters are structured only; arbitrary SQL predicates are not supported.

## Metrics and inference

- Distinct counts of columns are approximate (HyperLogLog++); quantiles are approximate (`percentile_approx`).
  Uniqueness and referential integrity are not verified (planned for the second part of the deep level).
- Standard deviation of decimals is computed in double precision.
- Format detection covers a small set of formats (JSON object/array, UUID, numeric/boolean strings, ISO
  date/timestamp, URL, e-mail candidate). No PII detection is attempted.
- Full-scope JSON validity requires `try_parse_json`; otherwise it is sample-based and reported as `unsupported`.
- Temporal checks count values after the run's reference instant; they are descriptive, not errors.
  `timestamp_ntz` values have no unambiguous reference and are not compared.
- VARIANT, interval and other types receive null counts only.

## Deep level (part I)

- Only one level of collections is profiled: elements of collections nested inside collections (`matrix[][]`,
  `items[].tags[]`) are omitted (`nested_collection`); the nested collection itself gets null counts.
- Element metrics have no mean, standard deviation or quantiles, and string elements get lengths and counts only
  (no values or extremes).
- Element distinct counts come from one explode pass. With `element_distinct = sample` they describe a bounded,
  potentially biased sample (the element limit stops the pass, so collections late in a row may be examined less);
  with `full_scope` they are computed only when the measured element count fits `deep.max_elements`.
- The element explode pass runs only when an extra pass is left after the aggregation overflow
  (`deep.max_extra_passes`).
- JSON paths are catalogued from the standard sample (string fields only, within `sampling.max_columns`); JSON inside
  array elements is not catalogued. The catalogue is never a complete schema: paths absent from the sample may
  exist.
- Rare or map-like JSON keys are collapsed into `*` by design; legitimate optional keys seen in fewer than two
  sampled documents (or less than 10% of the documents containing their object) are not named.
- Full-scope JSON path validation covers key-only paths (no `[*]` or `*` wildcards, no keys with quotes, brackets or
  backslashes). With `get_json_object` a JSON `null` and an absent path are indistinguishable and types are not
  validated; the `variant` method needs `try_parse_json`, `try_variant_get`, `is_variant_null` and
  `schema_of_variant` (available in local Spark 4.0; not in Spark 3.5).

## Documentation

- Business meaning comes only from source comments and human annotations.
- ER edges are drawn only when a person provided cardinality; declared foreign keys appear in an auxiliary
  diagram. Mermaid entity names are sanitized; original names are listed in comments.
- Relationships are never validated against data in this release.
- No global quality score is computed.

## Packaging

- The package is not published; install from a checkout or a wheelhouse.
- Only the Databricks source format (`.py`) is generated; `.ipynb` is planned.
