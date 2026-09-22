# Limitations

Known limits of release 0.3.0, by design or not yet addressed.

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
  Uniqueness and referential integrity are verified only at the deep level, for the keys and relationships requested
  (see [Deep level, part II](#deep-level-part-ii)).
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

## Deep level, part II

- Exact uniqueness covers the keys requested (explicit keys, declared keys, identifier candidates) within
  `deep.uniqueness.max_keys` per table. Key columns must be atomic and outside arrays and maps. Floating-point key
  columns follow Spark grouping (NaN equals NaN, -0.0 equals 0.0). Each pass explodes every row once per key it
  checks: many keys in one pass multiply the shuffled rows.
- Referential validation runs only between tables profiled in the same run (tables outside the run are never read)
  and within `deep.referential.max_relationships` per run. Key columns must have compatible types (same kind, or
  integer and decimal); no implicit conversion between strings and numbers is attempted. The target is read in full,
  without its filters; the source keeps its scope.
- A `sample` referential check can only find violations; it never validates a relationship. Prefix samples are
  potentially biased.
- Tables that are not Delta (views, other formats) are re-read in their current state by referential and hypothesis
  checks; the validation detail says so.
- Relationship hypotheses consider single-column targets measured exactly unique only, of integer, scale-0 decimal,
  string or date type; composite relationships are never hypothesized. Candidate pairs come from type compatibility
  and ranges measured at the standard level (values or lengths); pairs beyond `max_pairs` are not measured, so a
  missing hypothesis proves nothing. Inclusion can be coincidental (small integer ranges, codes): a hypothesis is a
  prompt for a person, not a relationship.
- Declared PRIMARY KEY, UNIQUE and FOREIGN KEY constraints come from Unity Catalog `information_schema` and have not
  been validated on a workspace yet. Locally, the same queries run against a simulated `information_schema` (tables
  with its column layout) and the assembly is tested with synthetic rows. A foreign key whose referenced constraint
  cannot be read or resolved is not documented as a relationship (a table note says why).

## Documentation

- Business meaning comes only from source comments and human annotations.
- ER edges are drawn only when a person provided cardinality; declared foreign keys appear in an auxiliary
  diagram. Mermaid entity names are sanitized; original names are listed in comments.
- Relationships are validated against data only at the deep level, when `deep.referential` requests it; a person
  still provides cardinality, and hypotheses are never drawn.
- No global quality score is computed.

## Packaging

- The package is not published; install from a checkout or a wheelhouse.
- Only the Databricks source format (`.py`) is generated; `.ipynb` is planned.
