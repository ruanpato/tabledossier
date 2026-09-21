# Profile contract (schema 1.0)

`profile.json` is the canonical, engine-independent result of one run. Every document
(`data_dictionary.md`, `quality_report.md`, `relationships.md`, `erd.mmd`, `suggested_rules.json`) is
derived from it, in the notebook and by `tabledossier render`. The formal definition is the JSON Schema
shipped in the package:

```bash
tabledossier schema profile        # also: config, annotations, suggested_rules, manifest
```

Validation happens in two layers. In the notebook, a standard-library interpreter of the same schema
file (`tabledossier.schemacheck`) plus cross-field invariants (`tabledossier.contract`) run before export.
In the CLI and CI, the formal Draft 2020-12 validator (`jsonschema`) runs as well. Tests prove that both
validators agree on valid and mutated documents and that the schemas only use keywords the embedded
validator enforces.

## Versioning

- `schema_version` is `"1.0"`. Readers accept only versions they know. `tabledossier validate` rejects
  other versions with a message naming the supported ones.
- Additive or breaking changes produce a new version; the notebook and the CLI of the same release always
  agree because the notebook embeds the schema of the release that generated it.

## Top level

| Field | Meaning |
| --- | --- |
| `kind` | Always `tabledossier.profile`. |
| `schema_version` | Contract version (`1.0`). |
| `tool` | `{name, version}` of the TableDossier release that produced the profile. |
| `run` | Run identity, timing, environment, effective configuration (see below). |
| `value_exposure` | What kinds of values the profile may contain under the configured policy. |
| `tables` | One record per requested table, in request order. |
| `relationships` | Declared (constraints) and configuration-provided relationships. |
| `summary` | Counts by table status, checks, findings and proposals. |

### `run`

| Field | Meaning |
| --- | --- |
| `run_id` | `YYYYMMDDTHHMMSSZ-<8 hex>`; also the name of the results directory. |
| `status` | `succeeded` (all tables), `partial` (mixed), `failed` (all tables failed). |
| `analysis_level` | `metadata` or `standard`. |
| `started_at`, `finished_at` | UTC timestamps `YYYY-MM-DDTHH:MM:SS.mmmZ`. Re-rendering never changes them. |
| `duration_ms` | Wall-clock duration measured by the notebook. |
| `reference_time` | Instant used by temporal metrics (`after_reference_count`); dates compare with its UTC date. |
| `environment` | `engine`, `execution_context` (`databricks` when `DATABRICKS_RUNTIME_VERSION` is set, else `spark`), Python, Spark and runtime versions, session time zone, ANSI mode, whether Spark Connect was used. No user or host names. |
| `effective_config` | Configuration after defaults, generated values and widgets. The schema forbids unknown keys, so it cannot carry credentials. Filter values are included because they define the population. |
| `config_fingerprint` | `sha256:` of the canonical JSON of `effective_config`. |
| `parameter_sources` | Precedence applied and which widgets differed from the generated defaults. |
| `generation` | `generator_version` and `generation_id` of the notebook that produced the run. |
| `purpose` | Optional run purpose from the configuration. |
| `capabilities` | Engine features detected at run time (e.g. `try_parse_json`), each with `available` and `detail`. |

## Tables

| Field | Meaning |
| --- | --- |
| `table_id` | Stable id (`t_` + hash of the case-folded name parts); used as Markdown anchor. |
| `table_key` | Canonical display name (`catalog.schema.table`, backticks when needed). |
| `identifier` | `input` as given, parsed `parts`, and the fully `quoted` form used in SQL. |
| `status` | `succeeded`, `partial` (some stage failed) or `failed` (not resolvable or invalid options). |
| `errors` | Sanitized errors: `stage`, `error_class`, engine `condition`, `message` (first line, quoted literals and URIs removed). |
| `purpose` | Per-table purpose from configuration. |
| `source` | Catalog metadata: type, provider, comment, timestamps, partitioning/clustering columns, `size_in_bytes` and `file_count` as metrics, `metadata_state` (`current_at_capture`). Storage locations and owners are never recorded. |
| `consistency` | `mode` (`pinned_delta_version`, `unpinned`, `metadata_only`), pinned `delta_version`, its commit timestamp and a human-readable `guarantee`. |
| `scope` | `population` (`full`/`filtered`), `scope_label`, the structured `filters`, `selected_columns` and the row semantics statement. |
| `sample` | The transient sample: method, bias, limits, rows collected, bytes retained, truncated values, why collection stopped. |
| `summary` | Counts: columns, schema nodes, fields profiled/omitted, findings, checks, proposals. |
| `table_metrics` | Table-level metrics (`row_count`). |
| `schema` | Schema tree (see below) with `captured_from` = `pinned_snapshot` or `current_table`. |
| `field_profiles` | One record per schema node, profiled or not (with `omission_reason`). |
| `constraints` | Declared constraints: primary/foreign/unique keys from Unity Catalog `information_schema` (`enforcement: not_enforced`), Delta CHECK constraints (`enforced`). |
| `findings`, `quality_checks`, `suggested_rules` | See below. |
| `omissions` | What was not documented or measured and why (`not_selected`, `inside_collection`, `max_depth`, `max_fields`, `expression_budget`). |
| `unsupported` | Capabilities the runtime lacked. |
| `operations` | `planned` operations (what was asked of the engine, with `reads_user_data`) and `observed` outcomes with measured durations. `physical_scans`, `bytes_read` and `monetary_cost` are `unknown`: they are never estimated from Python calls. |
| `timings_ms` | Measured durations per stage (`metadata`, `sample`, `aggregate`, `total`). |
| `notes` | Other statements about this table. |

## Field paths and the schema tree

A path is a list of typed segments:

```json
[{"kind": "field", "name": "shipping"}, {"kind": "field", "name": "address"}, {"kind": "field", "name": "city"}]
[{"kind": "field", "name": "a.b"}]
[{"kind": "field", "name": "items"}, {"kind": "array_element"}, {"kind": "field", "name": "sku"}]
[{"kind": "field", "name": "attributes"}, {"kind": "map_value"}]
```

The display forms are `shipping.address.city`, `` `a.b` ``, `items[].sku` and `attributes{value}`. Names that
are not simple identifiers are quoted with backticks, so a column literally named `a.b` never collides with
field `b` of struct `a`. Display paths are documentation keys (also used in annotations), not guaranteed SQL.
`field_id` is `f_` + a hash of the segments.

Schema nodes carry `type` (`kind` + engine `physical_type`, precision/scale for decimals), declared
`nullable`, source `comment`, `children` and `children_omitted` with its reason. The tree is built
breadth-first within `limits.max_depth` and `limits.max_fields`, so every top-level column is documented
before nested fields.

## Metrics

```json
{
  "name": "null_count", "status": "measured", "value": 12, "value_type": "integer", "unit": "rows",
  "scope": "filtered_snapshot", "accuracy": "exact", "source": "aggregate",
  "method": "rows where the field is null (includes rows whose parent struct is null)",
  "denominator": 1000, "denominator_unit": "rows"
}
```

| Axis | Values |
| --- | --- |
| `status` | `measured`; or, always with `value: null` and a `reason`: `not_computed`, `unsupported`, `insufficient_data`, `redacted`, `error`, `unavailable`. |
| `scope` | `full_snapshot` / `filtered_snapshot` (pinned Delta version), `full_table` / `filtered_table` (not pinned), `sample` (transient sample only), `table_metadata` (catalog metadata, never a filtered scope). |
| `accuracy` | `exact`; `approximate` (HyperLogLog++ distinct counts, `percentile_approx` quantiles); `as_recorded` (metadata or pre-existing statistics of unknown freshness); `unknown`. |
| `source` | `aggregate`, `derived` (computed from other metrics), `sample`, `catalog_metadata`, `table_statistics`, `table_history`. |

A value can be exact for the sample and still say nothing about the table; the four axes keep these
statements apart. An empty table has `row_count = 0` and ratios with status `insufficient_data` (never 100%
completeness from a division by zero).

### Value encoding

| `value_type` | JSON encoding |
| --- | --- |
| `integer` | JSON integer (parse with arbitrary precision if values may exceed 2^53). |
| `float` | JSON number; non-finite values are the strings `"NaN"`, `"Infinity"`, `"-Infinity"`. |
| `decimal` | JSON string with the exact decimal text (`"95000.00"`). |
| `date` | `"YYYY-MM-DD"`. |
| `timestamp` | ISO 8601 instant with offset (`...Z` in UTC sessions), formatted by the engine in the session time zone. |
| `timestamp_ntz` | ISO 8601 local timestamp without offset. |
| `boolean`, `string` | JSON boolean/string. |
| `ratio` | JSON number in [0, 1], with a positive `denominator`. |
| `quantiles` | List of `{"probability", "value"}`; `details.element_type` gives the element encoding. |

### Metric catalog (standard level)

| Kind | Metrics |
| --- | --- |
| Table | `row_count` |
| All profiled fields | `null_count`, `non_null_count`, `null_ratio` (denominator: rows in scope) |
| Nested struct fields | `null_count_parent_present`, `null_ratio_given_parent_present` (denominator: rows whose parent is not null) |
| Orderable atomic types | `approx_distinct_count` (with `relative_standard_deviation`), `all_values_equal` (min = max) |
| Integer, decimal, float | `min`, `max`, `mean`, `stddev` (sample), `quantiles`, `zero_count`, `negative_count`, `positive_count` |
| Float only | `nan_count`, `positive_infinity_count`, `negative_infinity_count`, `finite_count`; min/max/mean/stddev/quantiles and sign counts use finite values only (denominator: `finite_count`) |
| String | `empty_count`, `whitespace_only_count`, `min_length`, `max_length`, `mean_length`, `length_quantiles` (characters), `json_invalid_count` (full scope, only with `try_parse_json`) |
| Boolean | `true_count`, `false_count` |
| Date, timestamp | `min`, `max`, `after_reference_count`; for `timestamp_ntz` the last one is `not_computed` (no unambiguous reference instant) |
| Binary | `min_length`, `max_length` (bytes) |
| Array | `empty_count`, `min_size`, `max_size`, `mean_size`, `total_element_count`, `null_element_count` (denominator: elements) |
| Map | `empty_count`, sizes, `total_entry_count`, `null_value_count` (denominator: entries) |
| Struct, variant, interval, other | `null_count` family only |

Metadata-level `row_count` comes from catalog statistics when present (`as_recorded`, freshness unknown)
and is `unavailable` otherwise.

## Semantics (sample-based)

`field_profiles[].semantics.observed_format` (string fields in the sample) contains `status` (`detected`,
`mixed`, `ambiguous`, `unknown`, `insufficient_data`), the detected `format` (`json_object`, `json_array`,
`uuid`, `numeric_string`, `boolean_string`, `iso_date`, `iso_timestamp`, `url`, `email_candidate`),
`eligible_observations` (non-null, non-blank, non-truncated sampled values), excluded counts, and per-format
`candidates` with `matches`, `match_ratio` and a Wilson score interval. The interval describes the observed
proportion under independent sampling; it neither removes sampling bias nor establishes business meaning.

`candidate_roles` (`identifier_candidate`, `categorical_candidate`, `json_document_candidate`) list evidence
and limitations. Approximate distinct counts never prove uniqueness.

`json_profile` separates SQL NULL, the JSON literal `null`, invalid text, scalars, objects and arrays;
values truncated by `sampling.max_value_chars` are excluded, never counted as invalid. It reports
heterogeneity (kinds present, distinct key sets, keys with conflicting types) and, when allowed, top-level key
names with presence ratios.

`concentration` summarizes the sample (distinct count, top-1 and top-5 shares) without labels. `examples`
exists only for allow-listed columns.

## Findings, checks, proposals

- **Findings** are heuristic signals (`all_null`, `high_null_ratio`, `possible_constant`, `possible_categorical`,
  `identifier_candidate`, `probable_json`, `extreme_numeric_tail`, `large_arrays`) with severity `info` or
  `warning`, `threshold`, `evidence`, `severity_reason` and `limitations`. They never fail a run.
- **Quality checks** are configured by you and evaluated from measured metrics: `pass`, `fail`,
  `not_evaluated` (the needed metric was not measured) or `error` (the check does not fit the data).
- **Suggested rules** (`not_null`, `unique`, `accepted_values`, `valid_json`, `matches_format`) are always
  `status: proposed` and `requires_review: true`. `accepted_values` never lists the observed values.

## Relationships

Each relationship has `origin` (`declared_constraint`, `configuration`, `annotation`), participating columns
(composite keys preserved), `cardinality` (only when provided by a person), `enforcement`, `validation`
(`not_validated` in this release) and `scope`. Nothing is inferred from column names.

## Other documents

- **Run manifest** (`manifest.json`, kind `tabledossier.run_manifest`): status, SHA-256 and size of every
  file, and the profile validation result. A `running` manifest is written before any table is read.
- **Generation manifest** (`*.generation.json`, kind `tabledossier.generation_manifest`): notebook hash,
  embedded modules and schemas with their hashes, `contains_results: false`.
- **Suggested rules** (`suggested_rules.json`): neutral proposals collected from all tables.
- **Annotations** (`annotations.json`, kind `tabledossier.annotations`): descriptions, purpose, owner, tags per
  table and per column (keyed by display path), and relationships. They are only read by `render`.
