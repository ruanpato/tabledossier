# Profile contract (schema 1.1)

`profile.json` is the canonical, engine-independent result of one run. Every document
(`data_dictionary.md`, `quality_report.md`, `relationships.md`, `erd.mmd`, `suggested_rules.json`) is
derived from it, in the notebook and by `tabledossier render`. The formal definition is the JSON Schema
shipped in the package:

```bash
tabledossier schema profile        # contract 1.1; also: profile-1.0, config, annotations, suggested_rules, manifest
```

Validation happens in two layers. In the notebook, a standard-library interpreter of the same schema
file (`tabledossier.schemacheck`) plus cross-field invariants (`tabledossier.contract`) run before export.
In the CLI and CI, the formal Draft 2020-12 validator (`jsonschema`) runs as well. Tests prove that both
validators agree on valid and mutated documents and that the schemas only use keywords the embedded
validator enforces.

## Versioning

- The notebook of release 0.2 writes `schema_version` `"1.1"`. The CLI (`validate`, `render`) reads `"1.0"`
  and `"1.1"` and validates each profile against the schema of its own version: 1.0 profiles against the frozen
  schema of 0.1.x (`tabledossier schema profile-1.0`), 1.1 profiles against the current one. Other versions are
  rejected with a message naming the supported ones.
- **1.1 only adds to 1.0**: the `deep` analysis level, `element_context` and `json_paths` on field profiles, the
  per-table `deep` record and two operation kinds (`deep_aggregate_pass`, `element_explode_pass`). These
  properties are optional in the 1.1 schema (a 1.0 document relabelled `1.1` is valid); the 0.2 notebook always
  writes them, as `null` when not applicable. A 1.0 document cannot use them.
- Map sizes and entry counts are labelled `entries` (0.1 wrote `elements`); both are accepted.
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
| `analysis_level` | `metadata`, `standard` or `deep` (1.1). |
| `started_at`, `finished_at` | UTC timestamps `YYYY-MM-DDTHH:MM:SS.mmmZ`. Re-rendering never changes them. |
| `duration_ms` | Wall-clock duration measured by the notebook. |
| `reference_time` | Instant used by temporal metrics (`after_reference_count`); dates compare with its UTC date. |
| `environment` | `engine`, `execution_context` (`databricks` when `DATABRICKS_RUNTIME_VERSION` is set, else `spark`), Python, Spark and runtime versions, session time zone, ANSI mode, whether Spark Connect was used. No user or host names. |
| `effective_config` | Configuration after defaults, generated values and widgets. The schema forbids unknown keys, so it cannot carry credentials. Filter values are included because they define the population. |
| `config_fingerprint` | `sha256:` of the canonical JSON of `effective_config`. |
| `parameter_sources` | Precedence applied and which widgets differed from the generated defaults. |
| `generation` | `generator_version` and `generation_id` of the notebook that produced the run. |
| `purpose` | Optional run purpose from the configuration. |
| `capabilities` | Engine features detected at run time (`try_parse_json`, `parameterized_sql`, `higher_order_functions`, `variant_functions`, `get_json_object`), each with `available` and `detail`. |

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
| `omissions` | What was not documented or measured and why (`not_selected`, `inside_collection`, `max_depth`, `max_fields`, `expression_budget`; deep level: `deep_budget`, `deep_not_selected`, `nested_collection`). |
| `unsupported` | Capabilities the runtime lacked. |
| `operations` | `planned` operations (what was asked of the engine, with `reads_user_data`; kinds `catalog_metadata`, `table_detail`, `table_history`, `information_schema`, `sample_collect`, `aggregate_pass`, and at the deep level `deep_aggregate_pass`, `element_explode_pass`) and `observed` outcomes with measured durations (`succeeded`, `failed`, or `skipped` with a reason, e.g. `DESCRIBE DETAIL` on a non-Delta source). `physical_scans`, `bytes_read` and `monetary_cost` are `unknown`: they are never estimated from Python calls. |
| `timings_ms` | Measured durations per stage (`metadata`, `sample`, `aggregate`, `deep_elements`, `total`). |
| `notes` | Other statements about this table. |
| `deep` | 1.1, deep level only (else `null`): see [Deep level](#deep-level-11). |

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
| Map | `empty_count`, sizes, `total_entry_count`, `null_value_count` (unit and denominator: entries) |
| Struct, variant, interval, other | `null_count` family only |

Metadata-level `row_count` comes from catalog statistics when present (`as_recorded`, freshness unknown)
and is `unavailable` otherwise.

## Deep level (1.1)

### Element fields

At the deep level, the schema nodes inside one array or map (`items[]`, `items[].sku`, `attrs{key}`,
`attrs{value}`) are profiled. Their field profile has `profiled: true` and an `element_context`:

```json
{"collection_field_id": "f_…", "collection_display_path": "items", "collection_kind": "array",
 "segment": "array_element", "inner": ["sku"], "unit": "elements"}
```

Every count of an element field is a count of **elements** (arrays) or **entries** (maps) of the collection in
scope, never of rows; ratios use those denominators. Invariants reject element metrics whose unit or denominator is
`rows`.

| Kind | Metrics (per element or entry) |
| --- | --- |
| All element fields | `element_count` (derived from the collection's total), `null_count`, `non_null_count`, `null_ratio`; leaves inside struct elements also `null_count_parent_present`, `null_ratio_given_parent_present` |
| Integer, decimal, float | `min`, `max` (`array_min`/`array_max` per row, then over rows), `zero_count`, `negative_count`, `positive_count`; floats also `nan_count`, infinity counts, `finite_count` (extremes and sign counts use finite values) |
| String | `empty_count`, `min_length`, `max_length`, `whitespace_only_count` (no string values or extremes) |
| Boolean | `true_count`, `false_count` |
| Date, timestamp | `min`, `max`, `after_reference_count` (`not_computed` for `timestamp_ntz`) |
| Binary | `min_length`, `max_length` (bytes) |
| Atomic kinds | `distinct_count` from the element explode pass |

Those metrics are `accuracy: exact`, `source: aggregate` and have the table's scope: they are computed per row with
higher-order functions inside the aggregation passes, without explode. `distinct_count` comes from the single
`element_explode_pass`: with `deep.element_distinct = sample` it has `scope: sample`, `source: sample` and is exact
only for the elements examined (`details` gives `elements_examined`, `rows_with_elements`, the row and element
limits and whether the element limit stopped the pass); with `full_scope` it has the table's scope and
`source: aggregate`. Elements of collections nested inside collections are omitted (`nested_collection`).
Aggregate extremes of element fields follow `value_policy.aggregate_extremes` and `redact_columns` (listing a
collection redacts its elements).

### JSON paths

A string field targeted by the deep level has `json_paths`, a catalogue built from the transient standard sample:

| Field | Meaning |
| --- | --- |
| `scope`, `source` | Always `sample`. |
| `documents` | Sampled values whose root is a JSON object or array (the denominator of presence ratios); `counts` also gives scalars, JSON `null` literals and invalid values. |
| `paths` | Up to `deep.max_json_paths` paths (breadth-first, most present first), or `null` when the value policy hides key names (`paths_omitted_reason`). |
| `paths[].path` / `segments` | Display form (`$.customer.id`, `$["a.b"]`, `$.items[*].sku`, `$.attrs.*`) and typed segments (`key`, `items`, `any_key`). |
| `paths[].present_in`, `presence_ratio`, `occurrences` | Sampled documents containing the path, its share, and values seen (array items count once each). |
| `paths[].types`, `dominant_type`, `heterogeneous` | JSON types observed (`object`, `array`, `string`, `number`, `boolean`, `null`); heterogeneous when more than one non-null type occurs. |
| `paths[].map_like` | Keys below this path were collapsed into `*` (map-like object or rare keys). |
| `paths[].full_scope` | Full-scope validation of this path (`measured`, `incomplete` or `not_computed` with a reason) and its metrics. |
| `full_scope` | `method` (`variant`, `get_json_object` or `null`), `status`, the `documents` metric over the full scope, and the method's limitations. |
| `depth_truncated`, `tracking_truncated`, `paths_observed`, `paths_omitted`, `map_like_paths`, `heterogeneous_paths` | What the limits cut and summary counts. |

Full-scope metrics (`unit: documents`, `accuracy: exact`, denominator: JSON documents in scope):

| Method (detected) | Metrics | Limitation |
| --- | --- | --- |
| `variant` (`try_parse_json`, `try_variant_get`, `is_variant_null`, `schema_of_variant`) | `path_present_count` (including JSON `null`), `path_json_null_count`, `path_type_match_count` (documents where the path has the sample's dominant type) | Numbers are integer, decimal or double variants. |
| `get_json_object` | `path_non_null_count` | Returns NULL both for a JSON `null` and for an absent path; cannot tell types apart; more lenient parser. |

Wildcard paths (`[*]`, `*`) and keys with quotes, brackets or backslashes are not validated over the full scope.
The sample catalogue is never a complete or guaranteed schema: paths absent from the sample may exist.

### The `deep` record

| Field | Meaning |
| --- | --- |
| `targets`, `requested_targets`, `not_eligible` | Selection mode, explicit targets of this table and the ones that were not eligible (with reason). |
| `collections` | Targeted arrays and maps with the number of element fields profiled and omitted. |
| `json_fields` | String fields with a catalogue (`catalogued`) or not (`not_catalogued` with reason), paths listed and validated, full-scope method. |
| `budget` | The deep budgets in effect. |
| `extra_passes` | `budget`, `planned` = `aggregate` (overflow aggregation passes) + `element_explode` (0 or 1). |
| `expressions` | Deep expressions planned and omitted by the budget. |
| `element_distinct` | Mode, status, reason, rows with elements and elements examined, and why the pass stopped. |
| `limited` | Everything a budget limited: `deep_budget`, `deep_pass_budget`, `element_budget`, `json_path_budget`, `json_depth_budget`. |

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
