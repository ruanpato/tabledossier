# Limitations

Known limits of release 0.1.0, by design or not yet addressed.

## Execution

- **Not yet validated on Databricks.** See [compatibility](compatibility.md) and the [smoke test](databricks-smoke-test.md).
- Tables run sequentially, up to `limits.max_tables` per run. Catalogs are never scanned automatically.
- Aggregation costs are not predicted. One `agg` call can still require several jobs and full scans; wide schemas,
  distinct-count sketches, quantiles and string/regex expressions use CPU and memory. Physical scans, bytes read and
  cost are reported as `unknown`.
- `prefix` samples are cheap but biased (often one file or partition). `random` sampling may evaluate every row.
  The retained-bytes budget bounds kept values, not the transferred batch (rows × sampled columns ×
  `max_value_chars`) nor process memory.
- Only Delta tables are pinned to a snapshot. Views and other formats are read without a consistency guarantee
  between the sample and the aggregation passes. Catalog metadata (size, comments, constraints) always describes
  the state at capture time.
- Filters are structured only; arbitrary SQL predicates are not supported.

## Metrics and inference

- Standard level only: elements of arrays, map entries and fields inside them are documented but not profiled;
  JSON paths are not extracted; uniqueness and referential integrity are not verified (`deep` is planned).
- Distinct counts are approximate (HyperLogLog++); quantiles are approximate (`percentile_approx`).
- Standard deviation of decimals is computed in double precision.
- Format detection covers a small set of formats (JSON object/array, UUID, numeric/boolean strings, ISO
  date/timestamp, URL, e-mail candidate). No PII detection is attempted.
- Full-scope JSON validity requires `try_parse_json`; otherwise it is sample-based and reported as `unsupported`.
- Temporal checks count values after the run's reference instant; they are descriptive, not errors.
  `timestamp_ntz` values have no unambiguous reference and are not compared.
- VARIANT, interval and other types receive null counts only.

## Documentation

- Business meaning comes only from source comments and human annotations.
- ER edges are drawn only when a person provided cardinality; declared foreign keys appear in an auxiliary
  diagram. Mermaid entity names are sanitized; original names are listed in comments.
- Relationships are never validated against data in this release.
- No global quality score is computed.

## Packaging

- The package is not published; install from a checkout or a wheelhouse.
- Only the Databricks source format (`.py`) is generated; `.ipynb` is planned.
