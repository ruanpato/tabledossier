# Profile contract (schema 1.2)

`profile.json` is the canonical, engine-independent result of one run. Every document
(`data_dictionary.md`, `quality_report.md`, `relationships.md`, `erd.mmd`, `suggested_rules.json`) is
derived from it, in the notebook and by `tabledossier render`. The formal definition is the JSON Schema
shipped in the package:

```bash
tabledossier schema profile        # contract 1.2; also: profile-1.0, profile-1.1, config, annotations, suggested_rules, manifest
```

Validation happens in two layers. In the notebook, a standard-library interpreter of the same schema
file (`tabledossier.schemacheck`) plus cross-field invariants (`tabledossier.contract`) run before export.
In the CLI and CI, the formal Draft 2020-12 validator (`jsonschema`) runs as well. Tests prove that both
validators agree on valid and mutated documents and that the schemas only use keywords the embedded
validator enforces.

## Versioning

- The notebook of release 0.3 writes `schema_version` `"1.2"`. The CLI (`validate`, `render`) reads `"1.0"`,
  `"1.1"` and `"1.2"` and validates each profile against the schema of its own version: 1.0 profiles against the
  frozen schema of 0.1.x (`tabledossier schema profile-1.0`), 1.1 profiles against the frozen schema of 0.2.x
  (`tabledossier schema profile-1.1`), 1.2 profiles against the current one. Other versions are rejected with a
  message naming the supported ones.
- **1.1 only adds to 1.0**: the `deep` analysis level, `element_context` and `json_paths` on field profiles, the
  per-table `deep` record and two operation kinds (`deep_aggregate_pass`, `element_explode_pass`). These
  properties are optional in the 1.1 schema (a 1.0 document relabelled `1.1` is valid); the notebook always
  writes them, as `null` when not applicable. A 1.0 document cannot use them.
- **1.2 only adds to 1.1**: the per-table `uniqueness` record, `validation_detail` on relationships, the top-level
  `referential_validation` and `relationship_hypotheses` records, three summary counters (`uniqueness`,
  `relationships`, `relationship_hypotheses`) and three operation kinds (`uniqueness_pass`, `referential_check`,
  `relationship_hypothesis_check`). They are optional in the 1.2 schema (a 1.1 document relabelled `1.2` is
  valid); the 0.3 notebook always writes them, as `null` when not applicable. A 1.1 document cannot use them.
- Map sizes and entry counts are labelled `entries` (0.1 wrote `elements`); both are accepted.
- Additive or breaking changes produce a new version; the notebook and the CLI of the same release always
  agree because the notebook embeds the schema of the release that generated it.

## Top level

| Field | Meaning |
| --- | --- |
| `kind` | Always `tabledossier.profile`. |
| `schema_version` | Contract version (`1.2`; readers also accept `1.0` and `1.1`). |
| `tool` | `{name, version}` of the TableDossier release that produced the profile. |
| `run` | Run identity, timing, environment, effective configuration (see below). |
| `value_exposure` | What kinds of values the profile may contain under the configured policy. |
| `tables` | One record per requested table, in request order. |
| `relationships` | Declared (constraints) and configuration-provided relationships, with their validation. |
| `summary` | Counts by table status, checks, findings and proposals; 1.2 adds measured keys by outcome, relationships by validation status and the number of hypotheses. |
| `referential_validation` | 1.2, deep level only (else `null`): what referential validation was requested, its budget and results. See [Referential validation](#referential-validation-12). |
| `relationship_hypotheses` | 1.2, deep level only (else `null`): data-driven hypotheses, kept apart from `relationships`. See [Relationship hypotheses](#relationship-hypotheses-12). |

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
| `operations` | `planned` operations (what was asked of the engine, with `reads_user_data`; kinds `catalog_metadata`, `table_detail`, `table_history`, `information_schema`, `sample_collect`, `aggregate_pass`, and at the deep level `deep_aggregate_pass`, `element_explode_pass`, `uniqueness_pass`, `referential_check`, `relationship_hypothesis_check`) and `observed` outcomes with measured durations (`succeeded`, `failed`, or `skipped` with a reason, e.g. `DESCRIBE DETAIL` on a non-Delta source). `physical_scans`, `bytes_read` and `monetary_cost` are `unknown`: they are never estimated from Python calls. |
| `timings_ms` | Measured durations per stage (`metadata`, `sample`, `aggregate`, `deep_elements`, `uniqueness`, `total`). |
| `notes` | Other statements about this table. |
| `deep` | 1.1, deep level only (else `null`): see [Deep level](#deep-level-11). |
| `uniqueness` | 1.2, deep level only (else `null`): exact uniqueness of keys. See [Uniqueness](#uniqueness-12). |

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

## Deep level, part II (1.2)

Uniqueness, referential validation and hypotheses record **counts only**: duplicated key values, orphan values and
matching values are never collected by the notebook nor written to the profile. Every check is one Spark action,
declared in `operations.planned` of the table it reads (the source table for relationships) and bounded by the
budgets of the `deep` configuration.

### Uniqueness (1.2)

`table.uniqueness` lists the keys requested for the table (explicit keys, declared PRIMARY KEY/UNIQUE constraints,
identifier candidates) and what was measured:

| Field | Meaning |
| --- | --- |
| `keys[].key_id` | `k_` + a hash of the key's field ids (stable across runs). |
| `keys[].origins`, `names` | `configured`, `declared_primary_key`, `declared_unique`, `identifier_candidate` (a key requested twice keeps both origins), and the configured id or constraint names. |
| `keys[].columns`, `field_ids` | Display paths and field ids of the key columns, in the requested order. |
| `keys[].status`, `reason` | `measured`, `not_computed` (budget), `not_eligible` (column missing, inside a collection, or of a type that cannot be compared exactly) or `error`. |
| `keys[].outcome` | `unique` (no NULL and no duplicate), `unique_non_null` (no duplicate among complete keys, some rows with NULL), `duplicates`, or `empty` (no complete key in scope); `null` unless measured. |
| `keys[].scope`, `operation_id`, `metrics` | The table's scope label, the `uniqueness_pass` that measured it, and the metrics below (`accuracy: exact`). |
| `sources`, `budget`, `passes` | Which sources were enabled, `max_keys`/`max_passes`, and the passes planned against the budget. |
| `limited`, `null_semantics`, `notes` | Keys beyond the budget, the NULL semantics stated below, other statements. |

| Metric | Unit | Meaning |
| --- | --- | --- |
| `rows_in_scope` | rows | Rows read by the uniqueness pass (the analysed scope). |
| `rows_with_null_key` | rows | Rows with NULL in at least one key column (denominator: rows in scope). |
| `rows_with_complete_key` | rows | Derived: rows in scope minus rows with a NULL key. |
| `distinct_keys` | keys | Exact number of distinct complete key values. |
| `duplicate_key_groups` | keys | Key values that occur in more than one row. |
| `rows_in_duplicate_groups` | rows | Rows whose key value occurs more than once (denominator: rows with a complete key). |
| `surplus_duplicate_rows` | rows | Derived: rows with a complete key minus distinct keys (rows beyond the first of each value). |
| `max_rows_per_key` | rows | Largest number of rows sharing one complete key value. |

NULL semantics: a row with NULL in any key column is excluded from distinct and duplicate counts (NULLs are not
equal, as in a SQL UNIQUE constraint) and counted in `rows_with_null_key`; a PRIMARY KEY also forbids NULLs, so it
holds only when the outcome is `unique`. Invariants reject counts that contradict each other or the outcome.

### Referential validation (1.2)

Every relationship has `validation` (`validated`, `violated`, `not_validated`) and `validation_detail`:

| Field | Meaning |
| --- | --- |
| `status`, `reason` | The same status as `validation`; `not_validated` always has a reason (not requested, not the deep level, budget, table not profiled, incompatible or ineligible columns, read failure, sample without orphans). |
| `mode` | `full_scope` or `sample` (source rows limited to `max_sample_rows`). |
| `from`, `to` | Table, scope label, consistency mode and the Delta version read. The source keeps its analysed scope (filters); the target is read in full at its recorded version. |
| `operation_id` | The `referential_check` operation of the source table. |
| `type_compatibility` | One record per column pair: physical types, `compatible` and the rule applied. |
| `target_key_unique` | Whether the target key had no duplicate value (evidence for cardinality; never a cardinality). |
| `metrics` | `source_rows`, `source_rows_with_null_key`, `source_rows_with_complete_key`, `orphan_rows`, `orphan_ratio` (denominator: source rows with a complete key), `target_rows`, `target_rows_with_null_key`, `target_distinct_keys`, `target_duplicate_key_groups`. |
| `limitations` | For example unpinned tables or sample semantics. |

`validated` requires a full-scope check with no orphan; `violated` requires at least one orphan (an orphan found in
a sample is an orphan of the table); a sample without orphans stays `not_validated`. Invariants enforce these rules.
`referential_validation` summarizes the run: requested sources, mode, budget, checks planned, counts by status and
what the budget limited.

### Relationship hypotheses (1.2)

`relationship_hypotheses` is separate from `relationships` and never feeds the ER diagram:

| Field | Meaning |
| --- | --- |
| `enabled`, `reason`, `budget` | Whether hypotheses were evaluated, why not, and `max_pairs`, `max_sample_rows`, `inclusion_scope`, `min_inclusion_ratio`. |
| `targets` | Single-column keys measured exactly unique in this run (the only possible targets). |
| `pairs_considered`, `pairs_evaluated`, `pairs_not_evaluated` | Candidate pairs after the type and range filters, pairs measured, pairs left out by `max_pairs`. |
| `pairs_known_excluded`, `pairs_disjoint_excluded`, `pairs_rejected` | Pairs skipped because they are known relationships or their ranges are disjoint, and evaluated pairs rejected by reason. |
| `hypotheses[]` | `hypothesis_id`, `status: hypothesis`, `from`, `to`, `cardinality: null`, `evidence` (inclusion scope, both sides with versions, `included_rows`, `inclusion_ratio` and the target counts, the target key id, `target_key_unique`, type compatibility, `range_relation` and `range_basis`), `operation_id`, `limitations`. |
| `method`, `limitations` | How candidates were chosen (types and measured ranges, never names) and what a hypothesis does not prove. |

Invariants reject a hypothesis that repeats a known relationship, lacks an unique target or falls below the
inclusion threshold.

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
(composite keys preserved), `cardinality` (only when provided by a person), `enforcement`, `validation` with its
`validation_detail` (1.2, see [Referential validation](#referential-validation-12)) and `scope`. Nothing is inferred
from column names: data-driven candidates are listed only as `relationship_hypotheses`.

## Other documents

- **Run manifest** (`manifest.json`, kind `tabledossier.run_manifest`): status, SHA-256 and size of every
  file, and the profile validation result. A `running` manifest is written before any table is read.
- **Generation manifest** (`*.generation.json`, kind `tabledossier.generation_manifest`): notebook hash,
  embedded modules and schemas with their hashes, `contains_results: false`.
- **Suggested rules** (`suggested_rules.json`): neutral proposals collected from all tables.
- **Job summary** (kind `tabledossier.job_summary`, `summary_version` 1.0, `tabledossier schema job_summary`): not a
  file of the package but the compact JSON the notebook returns with `dbutils.notebook.exit` (`jobs.exit_summary`):
  run id, status, analysis level, results directory, whether the profile passed validation, and counts of tables by
  status, configured checks, relationships by validation status and hypotheses. Counts only.
- **Annotations** (`annotations.json`, kind `tabledossier.annotations`): descriptions, purpose, owner, tags per
  table and per column (keyed by display path), and relationships. They are only read by `render`.
