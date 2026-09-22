# Configuration reference

A configuration is a JSON file (`kind: tabledossier.config`, `config_version: "1.0"`). Only those two keys
are required; everything else has a default. `tabledossier init` writes the complete default configuration
so every option is visible. JSON has no comments, so this page documents each option.

**Never put credentials or secrets in a configuration.** Unknown keys are rejected, and the effective
configuration is recorded in every profile.

Validate with `tabledossier validate --config <file>`. The formal schema is `tabledossier schema config`.

## Precedence at run time

```text
built-in defaults < generated configuration < config_json widget < tables_json / analysis_level / output_dir widgets
```

`config_json` may override any section except `tables`, `analysis_level`, `output_dir`, `kind` and
`config_version` (use the dedicated widgets). Objects are merged; lists and scalars are replaced;
`table_options` entries are replaced per table.

## Top-level options

| Key | Default | Meaning |
| --- | --- | --- |
| `tables` | `[]` | Default table identifiers. May be empty when generating; running requires at least one. |
| `analysis_level` | `"standard"` | `metadata` (no row reads), `standard`, or `deep` (standard + the budgeted operations of the [`deep`](#deep) section). |
| `output_dir` | `""` | Directory in the execution environment, e.g. `/Volumes/<catalog>/<schema>/<volume>/tabledossier`. Must be an absolute POSIX path (not a `dbfs:` URI). |
| `purpose` | `null` | Why the run exists; shown in the overview. |

## `limits`

| Key | Default | Meaning |
| --- | --- | --- |
| `max_tables` | 10 | Maximum tables per run (processed sequentially). Catalogs are never scanned automatically. |
| `max_fields` | 200 | Maximum schema nodes documented and profiled per table (breadth-first). |
| `max_depth` | 3 | Maximum nesting depth (top-level columns are depth 1). |
| `max_expressions_per_pass` | 800 | Aggregate expressions in one `agg` call. |
| `max_aggregate_passes` | 2 | Maximum `agg` calls per table. When the budget is exceeded, the most expensive metric tiers (quantiles, whitespace, then distinct counts…) are dropped first and recorded as `not_computed` with omission records. |

## `sampling`

| Key | Default | Meaning |
| --- | --- | --- |
| `method` | `"prefix"` | `prefix`: `LIMIT` after projection; cheap but **potentially biased**. `random`: Bernoulli sampling (`DataFrame.sample`) before the limit; may evaluate every row. `none`: no sample (format/JSON inference not computed). |
| `max_rows` | 2000 | Rows requested. |
| `max_bytes` | 16777216 | Budget for retained sampled values (UTF-8 bytes). Collection stops when the next row would exceed it. This is a payload budget, not a limit on process memory. |
| `max_value_chars` | 8192 | Values are cut server-side to this many characters; longer values are flagged as truncated and excluded from inference. |
| `max_columns` | 100 | String fields included in the sample. |
| `random_fraction` | `null` | Required for `random` (0 < f ≤ 1). The fraction does not reduce I/O proportionally. |
| `seed` | 42 | Seed for `random`. |

Only string fields are sampled. Numbers, dates and collections are measured by aggregations.

## `consistency`

| Key | Default | Meaning |
| --- | --- | --- |
| `pin_delta_version` | `true` | Resolve the latest Delta version (`DESCRIBE HISTORY … LIMIT 1`) and read every row through `VERSION AS OF` that version. |

## `metrics`

| Key | Default | Meaning |
| --- | --- | --- |
| `quantiles` | `[0.05, 0.25, 0.5, 0.75, 0.95]` | Probabilities for `percentile_approx` (empty list disables quantiles). |
| `quantile_accuracy` | 10000 | `percentile_approx` accuracy parameter. |
| `approx_distinct_rsd` | 0.02 | Relative standard deviation for `approx_count_distinct`. |
| `json_full_scope_validation` | `true` | For probable-JSON columns, count invalid JSON over the full scope when `try_parse_json` exists. |

## `semantic`

| Key | Default | Meaning |
| --- | --- | --- |
| `min_observations` | 30 | Below this many eligible sampled values the format is `insufficient_data`. |
| `detect_threshold` | 0.95 | Match ratio for `detected` (two formats above it → `ambiguous`). |
| `mixed_threshold` | 0.2 | Match ratio for `mixed`. |
| `confidence_level` | 0.95 | Level of the reported Wilson intervals. |

## `thresholds` (heuristic findings)

| Key | Default | Finding |
| --- | --- | --- |
| `high_null_ratio` | 0.5 | `high_null_ratio` (info). |
| `categorical_max_distinct`, `categorical_min_rows` | 20, 100 | `possible_categorical`. |
| `identifier_min_distinct_ratio`, `identifier_min_rows`, `identifier_max_null_ratio` | 0.95, 30, 0.01 | `identifier_candidate` (integers, strings, scale-0 decimals). |
| `json_min_ratio` | 0.8 | `probable_json` and full-scope JSON validation. |
| `constant_min_rows` | 2 | `possible_constant`. |
| `tail_iqr_multiplier` | 10 | `extreme_numeric_tail` (min/max beyond k × IQR). |
| `large_array_size` | 1000 | `large_arrays` (warning). |

## `value_policy`

| Key | Default | Meaning |
| --- | --- | --- |
| `persist_examples` | `false` | Allow persisting sampled example values… |
| `example_columns` | `[]` | …only for these `{"table", "column"}` entries (column = display path). |
| `max_examples_per_column`, `max_example_chars` | 5, 64 | Limits for persisted examples. |
| `aggregate_extremes` | `"include"` | `redact` omits min/max/mean/stddev/quantiles for every field (status `redacted`). |
| `json_key_names` | `"include"` | `redact` omits top-level JSON key names from JSON shape summaries. |
| `redact_columns` | `[]` | `{"table", "column"}` entries whose extremes, examples and JSON keys are always redacted. |

## `deep`

Used only when `analysis_level = "deep"`. The deep level is the standard level **plus** opt-in operations; each
one is bounded by the budgets below and declared in `operations.planned` of the profile. Whatever a budget
prevents is recorded as an omission and summarized in the table's `deep.limited` list (and in the DQR). Increasing
the number of metrics or fields never adds one Spark action per column.

| Key | Default | Meaning |
| --- | --- | --- |
| `targets` | `"all_within_budget"` | `"all_within_budget"`: every profiled array and map, and every string field the sample shows to be probable JSON (`thresholds.json_min_ratio`), within the budgets. Or an explicit list of `{"table", "column"}` entries (column = display path of an array, map or string field); tables without entries get no deep operation. Targets that are not eligible are listed with a reason. |
| `collections` | `true` | Profile elements of arrays (`items[]`, `items[].sku`) and entries of maps (`attrs{key}`, `attrs{value}`). |
| `json_paths` | `true` | Catalogue JSON paths of string fields from the transient sample. |
| `element_distinct` | `"sample"` | Distinct counts of element values need one row per element, so they run in **one** `element_explode_pass` per table for all collections. `sample`: explode a bounded sample (`sampling.method` prefix or random) of at most `max_explode_rows` rows, stopped at `max_elements` elements; metrics have `scope: sample`. `full_scope`: explode the whole scope, only when the measured element count of those collections is at most `max_elements` (otherwise `not_computed`). `off`: no explode pass. |
| `json_full_scope_validation` | `true` | Validate the presence (and, with variant functions, the type) of listed JSON paths over the full scope, when the runtime supports it. |
| `max_extra_passes` | 2 | Spark actions the deep level may add per table beyond the standard sample and passes: aggregation passes for deep expressions that do not fit the room left in the standard passes, then the element explode pass. `0` keeps deep expressions inside the standard passes only. |
| `max_explode_rows` | 1000 | Rows read by the element explode pass in `sample` mode. |
| `max_elements` | 100000 | Maximum exploded elements aggregated by the element explode pass (both modes); in `full_scope` mode, also the maximum measured element count in scope. |
| `max_json_paths` | 50 | JSON paths listed (and validated) per string field; further paths are counted, not listed. |
| `max_json_depth` | 3 | Maximum nesting depth of catalogued JSON paths (`$.a` is depth 1). |
| `max_json_object_keys` | 50 | Objects with more distinct keys than this in the sample are treated as maps: their keys are collapsed into `*` and never listed. |

How the budget is applied, in order:

1. Standard metrics are planned exactly as at the standard level (`limits`). Deep expressions (element metrics, JSON
   path validation) are always dropped before any standard metric.
2. Deep expressions first fill the room left in the last standard pass, then up to `max_extra_passes` extra
   aggregation passes (`deep_aggregate_pass`). What still does not fit is dropped from the lowest priority up —
   first whitespace counts, after-reference counts and JSON path validation, then extremes, lengths and value
   counts, and last element null counts — and reported as `deep_budget` omissions with `not_computed` metrics.
3. The element explode pass runs only if an extra pass is still available.

Element metrics are exact over the analysed scope: they are computed per row with higher-order functions
(`filter`, `transform`, `array_min`/`array_max`, `map_keys`/`map_values`) and summed inside the aggregation passes,
without explode. Their denominators are the elements or entries of the collection, never rows. Elements of
collections nested inside collections (`matrix[][]`) are not profiled in this release (`nested_collection`).

JSON path names are listed only when `value_policy.json_key_names = "include"` and the column is not in
`redact_columns`. Keys of map-like objects (too many distinct keys, or keys that do not look like field names) and
keys that occur in fewer than two sampled documents or in less than 10% of the documents containing their object
are collapsed into `*`. See [privacy](privacy.md).

```json
"analysis_level": "deep",
"deep": {
  "targets": [
    {"table": "demo.analytics.orders", "column": "items"},
    {"table": "demo.analytics.order_events", "column": "payload"}
  ],
  "max_extra_passes": 1,
  "element_distinct": "sample",
  "max_explode_rows": 500
}
```

## `table_options`

Keyed by table identifier (matched case-insensitively):

```json
"table_options": {
  "demo.analytics.orders": {
    "columns": ["order_id", "customer_id", "amount", "shipping"],
    "filters": [
      {"column": "order_ts", "operator": "ge", "value": "2025-01-01T00:00:00Z", "value_type": "timestamp"},
      {"column": ["shipping", "method"], "operator": "in", "value": ["express", "standard"]},
      {"column": "a.b", "operator": "is_not_null"}
    ],
    "purpose": "Orders since 2025",
    "checks": [
      {"id": "orders_min_rows", "type": "min_row_count", "min": 1},
      {"id": "orders_customer", "type": "max_null_ratio", "column": "customer_id", "max": 0.0},
      {"id": "orders_amount", "type": "value_range", "column": "amount", "min": 0, "max": 10000}
    ]
  }
}
```

- `columns`: top-level column names to profile (absent/`null` = all). Unselected columns stay documented in the
  schema tree with `omission_reason: not_selected`.
- **Column references**: a string is a *literal* top-level name (`"a.b"` is the column named `a.b`); a list
  navigates nested struct fields (`["shipping", "method"]`).
- **Filters** (combined with AND): `eq`, `ne`, `lt`, `le`, `gt`, `ge` (one value), `in`, `not_in` (non-empty list,
  up to 1000 values), `between` (`[low, high]`), `like` (string pattern), `is_null`, `is_not_null` (no value).
  Optional `value_type` casts literals: `string`, `integer` (bigint), `double`, `decimal` (use a string value to keep
  precision), `boolean`, `date`, `timestamp` (ISO 8601). SQL three-valued logic applies: comparisons exclude rows
  where the column is NULL. Filters are built with the DataFrame API and literals, never with SQL text.
- **Checks**: `min_row_count` (`min`), `max_row_count` (`max`), `max_null_ratio` / `max_null_count`
  (`column`, `max`), `max_empty_string_ratio` (`column`, `max`; empty strings / non-null values),
  `value_range` (`column`, `min` and/or `max`; finite values of numeric columns). Ids are unique per table.

Checks never fail the notebook. `tabledossier validate --profile … --fail-on-check-failures` exits with code 7
when any configured check failed.

## `relationships`

```json
"relationships": [
  {
    "id": "orders_customer",
    "from": {"table": "demo.analytics.orders", "columns": ["customer_id"]},
    "to": {"table": "demo.analytics.customers", "columns": ["customer_id"]},
    "cardinality": {"from": "zero_or_more", "to": "exactly_one"},
    "description": "Each order references one customer."
  }
]
```

`cardinality.from` describes how many `from` rows relate to one `to` row, and `cardinality.to` how many `to` rows
relate to one `from` row (`zero_or_one`, `exactly_one`, `zero_or_more`, `one_or_more`). Without cardinality the
relationship is documented but not drawn as an ER edge. Relationships are recorded as `not_validated`.
