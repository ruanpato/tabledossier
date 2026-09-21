"""Spark adapter: metadata capture, sampling and shared aggregations.

Part of the embedded runtime. It only needs the PySpark already present in the
execution environment (classic or Spark Connect sessions) and public APIs:

* catalog metadata through ``DESCRIBE TABLE EXTENDED``/``DESCRIBE DETAIL``/
  ``DESCRIBE HISTORY`` and Unity Catalog ``information_schema`` (never by
  reading ``_delta_log`` or data files directly);
* Delta time travel (``VERSION AS OF``) to pin every row read of a table to
  one snapshot when possible;
* one bounded, projected sample and a bounded number of ``agg`` passes per
  table. No Python UDFs, no automatic caching, no per-column jobs, no
  ``toPandas()`` on sources, and no writes to sources.
"""

import os
import platform
import re
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pyspark.sql import functions as F

from tabledossier.assemble import (
    field_metrics,
    field_profile,
    finalize_table,
    new_table,
    policy_field_ids,
)
from tabledossier.config import table_options_for
from tabledossier.errors import error_record, sanitize_message
from tabledossier.metrics import measured, not_measured
from tabledossier.paths import (
    column_reference_segments,
    field_id,
    parse_table_identifier,
    quote_name,
    quote_table_identifier,
    table_id,
    table_key,
    table_lookup_key,
)
from tabledossier.planning import (
    build_schema_tree,
    iter_nodes,
    operation,
    plan_aggregates,
    plan_sample,
    scope_label,
    select_profile_fields,
)
from tabledossier.semantic import (
    infer_format,
    is_json_like,
    json_shape,
    value_concentration,
    value_examples,
)

TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
TIMESTAMP_NTZ_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
_SP_STATS = re.compile(r"(\d+)\s+bytes(?:,\s*(\d+)\s+rows)?")
_SP_UNITY_EXCLUDED = ("spark_catalog", "hive_metastore")
_SP_CAST_TYPES = {
    "string": "string",
    "integer": "bigint",
    "double": "double",
    "decimal": "decimal(38,18)",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


# --------------------------------------------------------------------------- timing


def _sp_ms(start: float) -> int:
    return max(0, int((time.perf_counter() - start) * 1000))


def _sp_observed(
    op_id: str, status: str, start: float | None, rows: int | None = None, detail: str | None = None
) -> dict[str, Any]:
    return {
        "operation_id": op_id,
        "status": status,
        "duration_ms": _sp_ms(start) if start is not None else None,
        "rows_returned": rows,
        "detail": detail,
    }


# --------------------------------------------------------------------------- environment


def _sp_conf(spark: Any, key: str) -> str | None:
    try:
        value = spark.conf.get(key)
    except Exception:  # noqa: BLE001 - configuration may be hidden (e.g. serverless)
        return None
    return None if value is None else str(value)


def spark_environment(spark: Any) -> dict[str, Any]:
    """Describe the actual execution environment (no user or host names)."""
    runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    ansi = _sp_conf(spark, "spark.sql.ansi.enabled")
    return {
        "engine": "spark",
        "execution_context": "databricks" if runtime else "spark",
        "python_version": platform.python_version(),
        "spark_version": str(spark.version),
        "databricks_runtime_version": runtime,
        "session_timezone": _sp_conf(spark, "spark.sql.session.timeZone"),
        "ansi_mode": None if ansi is None else ansi.lower() == "true",
        "spark_connect": type(spark).__module__.startswith("pyspark.sql.connect"),
    }


def _sp_version_tuple(version: str) -> tuple[int, int]:
    match = re.match(r"(\d+)\.(\d+)", version)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)


def detect_capabilities(spark: Any) -> dict[str, dict[str, Any]]:
    """Detect optional engine features instead of assuming them."""
    capabilities: dict[str, dict[str, Any]] = {}
    has_call = hasattr(F, "call_function")
    exists = None
    try:
        exists = bool(spark.catalog.functionExists("try_parse_json"))
    except Exception:  # noqa: BLE001
        exists = None
    capabilities["try_parse_json"] = {
        "available": bool(exists) and has_call,
        "detail": (
            "catalog.functionExists('try_parse_json') = "
            + ("unknown" if exists is None else str(exists).lower())
            + f"; pyspark.sql.functions.call_function available = {str(has_call).lower()}"
        ),
    }
    version = _sp_version_tuple(str(spark.version))
    capabilities["parameterized_sql"] = {
        "available": version >= (3, 4),
        "detail": f"spark.sql(query, args=...) requires Spark 3.4+ (running {spark.version})",
    }
    capabilities["higher_order_functions"] = {
        "available": hasattr(F, "filter"),
        "detail": "pyspark.sql.functions.filter used for null elements inside arrays and maps",
    }
    return capabilities


# --------------------------------------------------------------------------- types


def neutral_type(data_type: Any) -> dict[str, Any]:
    """Convert a Spark ``DataType`` into the engine-neutral type descriptor."""
    name = type(data_type).__name__
    simple = data_type.simpleString()
    if name in ("ByteType", "ShortType", "IntegerType", "LongType"):
        return {"kind": "integer", "physical_type": simple}
    if name in ("FloatType", "DoubleType"):
        return {"kind": "float", "physical_type": simple}
    if name == "DecimalType":
        return {
            "kind": "decimal",
            "physical_type": simple,
            "precision": data_type.precision,
            "scale": data_type.scale,
        }
    if name in ("StringType", "VarcharType", "CharType"):
        return {"kind": "string", "physical_type": simple}
    if name == "BooleanType":
        return {"kind": "boolean", "physical_type": simple}
    if name == "DateType":
        return {"kind": "date", "physical_type": simple}
    if name == "TimestampType":
        return {"kind": "timestamp", "physical_type": simple}
    if name == "TimestampNTZType":
        return {"kind": "timestamp_ntz", "physical_type": simple}
    if name == "BinaryType":
        return {"kind": "binary", "physical_type": simple}
    if name == "StructType":
        return {"kind": "struct", "physical_type": "struct", "fields": neutral_fields(data_type)}
    if name == "ArrayType":
        element = neutral_type(data_type.elementType)
        return {
            "kind": "array",
            "physical_type": f"array<{_sp_short(data_type.elementType)}>",
            "element_type": element,
            "contains_null": bool(data_type.containsNull),
        }
    if name == "MapType":
        return {
            "kind": "map",
            "physical_type": (
                f"map<{_sp_short(data_type.keyType)},{_sp_short(data_type.valueType)}>"
            ),
            "key_type": neutral_type(data_type.keyType),
            "value_type": neutral_type(data_type.valueType),
            "value_contains_null": bool(data_type.valueContainsNull),
        }
    if name == "VariantType":
        return {"kind": "variant", "physical_type": simple}
    if name in ("DayTimeIntervalType", "YearMonthIntervalType", "CalendarIntervalType"):
        return {"kind": "interval", "physical_type": simple}
    if name == "NullType":
        return {"kind": "null", "physical_type": simple}
    return {"kind": "other", "physical_type": simple}


def _sp_short(data_type: Any) -> str:
    name = type(data_type).__name__
    if name == "StructType":
        return "struct"
    if name in ("ArrayType", "MapType"):
        return name[:-4].lower()
    text = str(data_type.simpleString())
    return text if len(text) <= 60 else text[:57] + "..."


def neutral_fields(struct_type: Any) -> list[dict[str, Any]]:
    """Convert a Spark ``StructType`` into neutral top-level field descriptors."""
    fields = []
    for field in struct_type.fields:
        ntype = neutral_type(field.dataType)
        metadata = dict(field.metadata or {})
        declared = metadata.get("__CHAR_VARCHAR_TYPE_STRING")
        if declared and ntype["kind"] == "string":
            ntype["physical_type"] = str(declared)
        comment = metadata.get("comment")
        fields.append(
            {
                "name": field.name,
                "type": ntype,
                "nullable": bool(field.nullable),
                "comment": str(comment) if comment else None,
            }
        )
    return fields


# --------------------------------------------------------------------------- expressions


def column_for(path: Sequence[Mapping[str, Any]]) -> Any:
    """Return a Column for a path of struct field segments (names never parsed)."""
    if not path or any(segment["kind"] != "field" for segment in path):
        raise ValueError("only struct field paths can be resolved to columns")
    column = F.col(quote_name(path[0]["name"]))
    for segment in path[1:]:
        column = column.getField(segment["name"])
    return column


def _sp_literal(value: Any, value_type: str | None) -> Any:
    literal = F.lit(value)
    return literal.cast(_SP_CAST_TYPES[value_type]) if value_type else literal


def filter_condition(filters: list[Mapping[str, Any]]) -> Any:
    """Build a filter Column from structured filters (values are literals, never SQL)."""
    condition = None
    for item in filters:
        column = column_for(column_reference_segments(item["column"]))
        operator = item["operator"]
        value_type = item.get("value_type")
        value: Any = item.get("value")
        if operator == "is_null":
            expr = column.isNull()
        elif operator == "is_not_null":
            expr = column.isNotNull()
        elif operator in ("in", "not_in"):
            expr = column.isin(*[_sp_literal(v, value_type) for v in value])
            if operator == "not_in":
                expr = ~expr
        elif operator == "between":
            expr = column.between(
                _sp_literal(value[0], value_type), _sp_literal(value[1], value_type)
            )
        elif operator == "like":
            expr = column.like(str(value))
        else:
            literal = _sp_literal(value, value_type)
            expr = {
                "eq": column == literal,
                "ne": column != literal,
                "lt": column < literal,
                "le": column <= literal,
                "gt": column > literal,
                "ge": column >= literal,
            }[operator]
        condition = expr if condition is None else condition & expr
    return condition


def _sp_finite(column: Any) -> Any:
    return (
        column.isNotNull()
        & ~F.isnan(column)
        & (column != F.lit(float("inf")))
        & (column != F.lit(float("-inf")))
    )


def compile_spec(
    spec: Mapping[str, Any],
    node: Mapping[str, Any] | None,
    parent: Mapping[str, Any] | None,
    ctx: Mapping[str, Any],
) -> Any:
    """Compile a planned metric spec into an aggregate Column expression."""
    op = spec["op"]
    if op == "count_all":
        return F.count(F.lit(1))
    assert node is not None
    column = column_for(node["path"])
    kind = node["type"]["kind"]
    present = _sp_finite(column) if kind == "float" else column.isNotNull()
    values = F.when(present, column) if kind == "float" else column
    params = spec["params"]
    if op == "count_null":
        return F.count(F.when(column.isNull(), 1))
    if op == "count_null_parent_present":
        assert parent is not None
        return F.count(F.when(column_for(parent["path"]).isNotNull() & column.isNull(), 1))
    if op == "approx_distinct":
        return F.approx_count_distinct(column, params["rsd"])
    if op == "all_values_equal":
        return F.min(column) == F.max(column)
    if op == "count_nan":
        return F.count(F.when(F.isnan(column), 1))
    if op == "count_pos_inf":
        return F.count(F.when(column == F.lit(float("inf")), 1))
    if op == "count_neg_inf":
        return F.count(F.when(column == F.lit(float("-inf")), 1))
    if op == "count_finite":
        return F.count(F.when(present, 1))
    if op in ("min_value", "max_value"):
        agg = F.min(values) if op == "min_value" else F.max(values)
        if kind == "timestamp":
            return F.date_format(agg, TIMESTAMP_PATTERN)
        if kind == "timestamp_ntz":
            return F.date_format(agg, TIMESTAMP_NTZ_PATTERN)
        return agg
    if op == "mean_value":
        return F.avg(values)
    if op == "stddev_value":
        return F.stddev_samp(values)
    if op == "count_zero":
        return F.count(F.when(present & (column == F.lit(0)), 1))
    if op == "count_negative":
        return F.count(F.when(present & (column < F.lit(0)), 1))
    if op == "count_positive":
        return F.count(F.when(present & (column > F.lit(0)), 1))
    if op == "quantiles":
        return F.percentile_approx(values, list(params["probabilities"]), params["accuracy"])
    if op == "count_empty_string":
        return F.count(F.when(column == F.lit(""), 1))
    if op == "count_whitespace_only":
        return F.count(F.when((column != F.lit("")) & column.rlike("^\\s+$"), 1))
    if op == "min_length":
        return F.min(F.length(column))
    if op == "max_length":
        return F.max(F.length(column))
    if op == "mean_length":
        return F.avg(F.length(column))
    if op == "length_quantiles":
        return F.percentile_approx(
            F.length(column), list(params["probabilities"]), params["accuracy"]
        )
    if op == "count_true":
        return F.count(F.when(column == F.lit(True), 1))
    if op == "count_false":
        return F.count(F.when(column == F.lit(False), 1))
    if op == "count_after_reference":
        reference = ctx["reference_date"] if kind == "date" else ctx["reference_timestamp"]
        return F.count(F.when(column > reference, 1))
    if op == "count_empty_collection":
        return F.count(F.when(column.isNotNull() & (F.size(column) == F.lit(0)), 1))
    if op == "min_size":
        return F.min(F.when(column.isNotNull(), F.size(column)))
    if op == "max_size":
        return F.max(F.when(column.isNotNull(), F.size(column)))
    if op == "mean_size":
        return F.avg(F.when(column.isNotNull(), F.size(column)))
    if op == "sum_size":
        return F.sum(F.when(column.isNotNull(), F.size(column)))
    if op == "count_null_elements":
        return F.sum(
            F.when(column.isNotNull(), F.size(F.filter(column, lambda item: item.isNull())))
        )
    if op == "count_null_map_values":
        return F.sum(
            F.when(
                column.isNotNull(),
                F.size(F.filter(F.map_values(column), lambda item: item.isNull())),
            )
        )
    if op == "count_json_invalid":
        parsed = F.call_function("try_parse_json", column)
        return F.count(F.when(column.isNotNull() & parsed.isNull(), 1))
    raise ValueError(f"unknown aggregate op: {op}")


# --------------------------------------------------------------------------- metadata


def _sp_formatted_rows(frame: Any) -> list[dict[str, Any]]:
    """Collect a small metadata result, formatting timestamps server-side as ISO 8601."""
    columns = []
    for field in frame.schema.fields:
        type_name = type(field.dataType).__name__
        if type_name == "TimestampType":
            columns.append(
                F.date_format(F.col(quote_name(field.name)), TIMESTAMP_PATTERN).alias(field.name)
            )
        else:
            columns.append(F.col(quote_name(field.name)))
    return [row.asDict(recursive=True) for row in frame.select(*columns).collect()]


def read_describe_extended(spark: Any, quoted: str) -> dict[str, str]:
    """Return the ``# Detailed Table Information`` section of DESCRIBE TABLE EXTENDED."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {quoted}").collect()
    info: dict[str, str] = {}
    in_detail = False
    for row in rows:
        name = (row[0] or "").strip()
        value = "" if row[1] is None else str(row[1])
        if name.startswith("# Detailed Table Information"):
            in_detail = True
            continue
        if in_detail and name and not name.startswith("#"):
            info[name] = value
    return info


def read_describe_detail(spark: Any, quoted: str) -> dict[str, Any]:
    """Return DESCRIBE DETAIL (Delta) without location, owner or identifiers."""
    rows = _sp_formatted_rows(spark.sql(f"DESCRIBE DETAIL {quoted}"))
    if not rows:
        return {}
    row = rows[0]
    keep = (
        "format",
        "sizeInBytes",
        "numFiles",
        "partitionColumns",
        "clusteringColumns",
        "createdAt",
        "lastModified",
    )
    detail = {key: row.get(key) for key in keep if key in row}
    properties = row.get("properties") or {}
    detail["check_constraints"] = {
        key[len("delta.constraints.") :]: str(value)
        for key, value in sorted(properties.items())
        if key.startswith("delta.constraints.")
    }
    return detail


def read_latest_version(spark: Any, quoted: str) -> tuple[int, str | None]:
    """Return the latest Delta version and its commit timestamp (DESCRIBE HISTORY LIMIT 1)."""
    rows = _sp_formatted_rows(spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT 1"))
    if not rows:
        raise ValueError("DESCRIBE HISTORY returned no rows")
    return int(rows[0]["version"]), rows[0].get("timestamp")


def _sp_full_name(spark: Any, parts: list[str]) -> list[str] | None:
    if len(parts) == 3:
        return list(parts)
    try:
        catalog = spark.catalog.currentCatalog()
        if len(parts) == 2:
            return [catalog, *parts]
        return [catalog, spark.catalog.currentDatabase(), parts[0]]
    except Exception:  # noqa: BLE001
        return None


def read_unity_constraints(spark: Any, parts: list[str]) -> tuple[list[dict[str, Any]], str | None]:
    """Read PRIMARY/FOREIGN KEY/UNIQUE constraints from Unity Catalog information_schema.

    Returns ``(constraints, note)``; ``note`` explains when nothing could be read.
    """
    full = _sp_full_name(spark, parts)
    if full is None or full[0].casefold() in _SP_UNITY_EXCLUDED:
        return [], "declared key constraints are read from Unity Catalog information_schema only"
    catalog, schema, table = full
    info = quote_name(catalog) + ".information_schema"
    query = (
        "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position, "
        "kcu.position_in_unique_constraint, rc.unique_constraint_catalog, "
        "rc.unique_constraint_schema, "
        "rc.unique_constraint_name "
        f"FROM {info}.table_constraints tc "
        f"JOIN {info}.key_column_usage kcu ON tc.constraint_catalog = kcu.constraint_catalog "
        "AND tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = "
        "kcu.constraint_name "
        f"LEFT JOIN {info}.referential_constraints rc ON tc.constraint_catalog = "
        "rc.constraint_catalog "
        "AND tc.constraint_schema = rc.constraint_schema AND tc.constraint_name = "
        "rc.constraint_name "
        "WHERE lower(tc.table_schema) = lower(:schema_name) AND lower(tc.table_name) = "
        "lower(:table_name) "
        "ORDER BY tc.constraint_name, kcu.ordinal_position"
    )
    rows = [
        row.asDict()
        for row in spark.sql(query, args={"schema_name": schema, "table_name": table}).collect()
    ]
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = grouped.setdefault(
            row["constraint_name"],
            {
                "type": str(row["constraint_type"]).upper(),
                "columns": [],
                "positions": [],
                "ref": None,
            },
        )
        entry["columns"].append(row["column_name"])
        entry["positions"].append(row.get("position_in_unique_constraint"))
        if row.get("unique_constraint_name"):
            entry["ref"] = (
                row["unique_constraint_catalog"],
                row["unique_constraint_schema"],
                row["unique_constraint_name"],
            )
    constraints = []
    kinds = {"PRIMARY KEY": "primary_key", "FOREIGN KEY": "foreign_key", "UNIQUE": "unique"}
    for name, entry in grouped.items():
        kind = kinds.get(entry["type"])
        if kind is None:
            continue
        referenced = None
        if kind == "foreign_key" and entry["ref"]:
            ref_catalog, ref_schema, ref_name = entry["ref"]
            ref_rows = spark.sql(
                "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position "
                f"FROM {quote_name(ref_catalog)}.information_schema.key_column_usage "
                "WHERE lower(constraint_schema) = lower(:schema_name) AND constraint_name = "
                ":constraint_name "
                "ORDER BY ordinal_position",
                args={"schema_name": ref_schema, "constraint_name": ref_name},
            ).collect()
            if ref_rows:
                by_position = {int(r["ordinal_position"]): r["column_name"] for r in ref_rows}
                columns = [
                    by_position.get(int(position) if position is not None else index + 1, "?")
                    for index, position in enumerate(entry["positions"])
                ]
                first = ref_rows[0]
                referenced = {
                    "table": table_key(
                        [first["table_catalog"], first["table_schema"], first["table_name"]]
                    ),
                    "columns": columns,
                }
        constraints.append(
            {
                "name": name,
                "constraint_type": kind,
                "columns": list(entry["columns"]),
                "expression": None,
                "referenced": referenced,
                "enforcement": "not_enforced",
                "source": "information_schema",
            }
        )
    return constraints, None


# --------------------------------------------------------------------------- sample


def collect_sample(
    frame: Any, nodes: Sequence[Mapping[str, Any]], plan: Mapping[str, Any]
) -> dict[str, Any]:
    """Collect a projected, bounded sample of string fields.

    Values are cut server-side to ``max_value_chars`` (truncated values are
    flagged and excluded from inference). Rows are retained until the payload
    budget ``max_bytes`` would be exceeded; iteration then stops. The payload
    budget accounts for retained values, not total process memory.
    """
    max_chars = plan["max_value_chars"]
    expressions = []
    for index, node in enumerate(nodes):
        column = column_for(node["path"])
        expressions.append(F.substring(column, 1, max_chars).alias(f"s{index}"))
        expressions.append((F.length(column) > F.lit(max_chars)).alias(f"t{index}"))
    source = frame
    if plan["method"] == "random":
        source = source.sample(
            withReplacement=False, fraction=float(plan["random_fraction"]), seed=plan["seed"]
        )
    limited = source.select(*expressions).limit(plan["max_rows"])
    try:
        rows_iter: Any = limited.toLocalIterator()
    except (AttributeError, NotImplementedError):
        rows_iter = iter(limited.collect())
    fields: dict[str, dict[str, Any]] = {
        node["field_id"]: {"values": [], "nulls": 0, "truncated": 0} for node in nodes
    }
    rows = 0
    used = 0
    stopped = "exhausted"
    for row in rows_iter:
        pending = []
        row_bytes = 0
        for index, node in enumerate(nodes):
            value = row[f"s{index}"]
            if value is None:
                pending.append((node["field_id"], "null", None))
            elif row[f"t{index}"]:
                pending.append((node["field_id"], "truncated", None))
            else:
                row_bytes += len(value.encode("utf-8"))
                pending.append((node["field_id"], "value", value))
        if used + row_bytes > plan["max_bytes"]:
            stopped = "byte_budget"
            break
        used += row_bytes
        rows += 1
        for fid, status, value in pending:
            if status == "null":
                fields[fid]["nulls"] += 1
            elif status == "truncated":
                fields[fid]["truncated"] += 1
            else:
                fields[fid]["values"].append(value)
    if stopped == "exhausted" and rows >= plan["max_rows"]:
        stopped = "row_limit"
    return {"rows": rows, "bytes": used, "stopped_reason": stopped, "fields": fields}


# --------------------------------------------------------------------------- aggregates


def run_aggregates(
    frame: Any,
    plan: Mapping[str, Any],
    nodes_by_id: Mapping[str, Mapping[str, Any]],
    ctx: Mapping[str, Any],
    observed: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]:
    """Execute the planned aggregation passes.

    Returns ``(results by alias, failed aliases with reasons, errors)``. An
    expression rejected at analysis time (no data read) is isolated so the
    rest of its pass still runs; a failure during execution fails the pass.
    """
    compiled: dict[str, Any] = {}
    failed: dict[str, str] = {}
    errors: list[dict[str, Any]] = []
    for spec in plan["specs"]:
        node = nodes_by_id.get(spec["field_id"]) if spec["field_id"] else None
        parent = nodes_by_id.get(spec["params"].get("parent_field_id", ""))
        try:
            compiled[spec["alias"]] = compile_spec(spec, node, parent, ctx).alias(spec["alias"])
        except Exception as exc:  # noqa: BLE001
            failed[spec["alias"]] = "expression could not be built: " + sanitize_message(
                str(exc), 200
            )
    results: dict[str, Any] = {}
    for index, aliases in enumerate(plan["passes"]):
        op_id = f"op_aggregate_{index + 1}"
        active = [alias for alias in aliases if alias in compiled]
        if not active:
            observed.append(_sp_observed(op_id, "skipped", None, detail="no valid expressions"))
            continue
        start = time.perf_counter()
        try:
            try:
                aggregated = frame.agg(*[compiled[alias] for alias in active])
                _ = aggregated.schema
            except Exception:  # noqa: BLE001 - isolate expressions rejected by the analyzer
                valid = []
                for alias in active:
                    try:
                        _ = frame.agg(compiled[alias]).schema
                        valid.append(alias)
                    except Exception as exc:  # noqa: BLE001
                        record = error_record(exc, "aggregate")
                        failed[alias] = "expression rejected by the engine: " + (
                            record["condition"] or record["error_class"]
                        )
                active = valid
                if not active:
                    observed.append(
                        _sp_observed(op_id, "skipped", start, detail="all expressions rejected")
                    )
                    continue
                aggregated = frame.agg(*[compiled[alias] for alias in active])
            row = aggregated.collect()[0]
            results.update(row.asDict())
            observed.append(_sp_observed(op_id, "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "aggregate")
            errors.append(record)
            for alias in active:
                failed.setdefault(
                    alias,
                    "aggregation pass failed: " + (record["condition"] or record["error_class"]),
                )
            observed.append(
                _sp_observed(
                    op_id, "failed", start, detail=record["condition"] or record["error_class"]
                )
            )
    return results, failed, errors


# --------------------------------------------------------------------------- table profile


def _sp_source(
    extended: Mapping[str, str], detail: Mapping[str, Any] | None, captured_at: str
) -> dict[str, Any]:
    table_type = extended.get("Type") or None
    source_type = "unknown"
    if table_type:
        source_type = "view" if "VIEW" in table_type.upper() else "table"
    provider = extended.get("Provider") or None
    notes = [
        "Catalog metadata reflects the table state when it was captured, not necessarily the "
        "pinned snapshot."
    ]
    size_metric: dict[str, Any]
    files_metric: dict[str, Any]
    if detail and detail.get("sizeInBytes") is not None:
        size_metric = measured(
            "size_in_bytes",
            int(detail["sizeInBytes"]),
            unit="bytes",
            scope="table_metadata",
            accuracy="as_recorded",
            source="catalog_metadata",
            method="DESCRIBE DETAIL sizeInBytes (latest table state)",
        )
    else:
        size_metric = not_measured(
            "size_in_bytes",
            "unavailable",
            "not provided by catalog metadata for this source",
            scope="table_metadata",
            source="catalog_metadata",
        )
    if detail and detail.get("numFiles") is not None:
        files_metric = measured(
            "file_count",
            int(detail["numFiles"]),
            unit="files",
            scope="table_metadata",
            accuracy="as_recorded",
            source="catalog_metadata",
            method="DESCRIBE DETAIL numFiles (latest table state)",
        )
    else:
        files_metric = not_measured(
            "file_count",
            "unavailable",
            "not provided by catalog metadata for this source",
            scope="table_metadata",
            source="catalog_metadata",
        )
    return {
        "source_type": source_type,
        "table_type": table_type,
        "provider": provider,
        "comment": extended.get("Comment") or None,
        "created_at": (detail or {}).get("createdAt") or extended.get("Created Time") or None,
        "last_modified": (detail or {}).get("lastModified"),
        "partition_columns": [str(c) for c in (detail or {}).get("partitionColumns") or []],
        "clustering_columns": [str(c) for c in (detail or {}).get("clusteringColumns") or []],
        "size_in_bytes": size_metric,
        "file_count": files_metric,
        "metadata_state": "current_at_capture",
        "captured_at": captured_at,
        "notes": notes,
    }


def _sp_statistics_row_count(extended: Mapping[str, str]) -> dict[str, Any]:
    match = _SP_STATS.search(extended.get("Statistics", ""))
    if match and match.group(2) is not None:
        return measured(
            "row_count",
            int(match.group(2)),
            unit="rows",
            scope="table_metadata",
            accuracy="as_recorded",
            source="table_statistics",
            method="row count recorded in catalog statistics (e.g. by an earlier ANALYZE TABLE)",
            details={
                "freshness": "unknown",
                "limitations": "Pre-existing statistics may be stale and describe the whole table, "
                "never a filtered scope.",
            },
        )
    return not_measured(
        "row_count",
        "unavailable",
        "no row count recorded in catalog statistics; the metadata level does not scan rows "
        "and never runs ANALYZE TABLE",
        scope="table_metadata",
        source="table_statistics",
    )


def _sp_omission_reason(
    fid: str,
    top_name: str,
    reasons: Mapping[str, str],
    parents: Mapping[str, Mapping[str, Any]],
    selected: set[str] | None,
) -> str:
    current: str | None = fid
    while current is not None:
        if current in reasons:
            return reasons[current]
        parent = parents.get(current)
        current = parent["field_id"] if parent else None
    if selected is not None and top_name not in selected:
        return "not_selected"
    return "inside_collection"


def profile_table(
    spark: Any,
    input_text: str,
    config: Mapping[str, Any],
    *,
    capabilities: Mapping[str, Mapping[str, Any]],
    reference_time: str,
    now: Callable[[], str],
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Profile one table and return its contract record (never raises for data errors)."""
    total_start = time.perf_counter()
    parts = parse_table_identifier(input_text)
    key = table_key(parts)
    quoted = quote_table_identifier(parts)
    table = new_table(input_text, parts, key, table_id(parts), quoted)
    options = table_options_for(config, table_lookup_key(parts))
    level = config["analysis_level"]
    filters = list(options.get("filters", []))
    selected = options.get("columns")
    table["purpose"] = options.get("purpose")
    table["scope"]["filters"] = filters
    table["scope"]["selected_columns"] = list(selected) if selected is not None else None
    table["scope"]["population"] = "filtered" if filters else "full"
    planned = table["operations"]["planned"]
    observed = table["operations"]["observed"]
    timings = table["timings_ms"]

    # 1. catalog metadata -------------------------------------------------------
    metadata_start = time.perf_counter()
    planned.append(
        operation(
            "op_describe",
            "catalog_metadata",
            "DESCRIBE TABLE EXTENDED (catalog metadata)",
            reads_user_data=False,
        )
    )
    try:
        extended = read_describe_extended(spark, quoted)
        observed.append(_sp_observed("op_describe", "succeeded", metadata_start))
    except Exception as exc:  # noqa: BLE001
        observed.append(
            _sp_observed(
                "op_describe", "failed", metadata_start, detail="table could not be resolved"
            )
        )
        table["errors"].append(error_record(exc, "resolve"))
        log(
            f"[tabledossier] {key}: could not be resolved "
            f"({table['errors'][-1]['condition'] or table['errors'][-1]['error_class']})"
        )
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    is_view = "VIEW" in (extended.get("Type") or "").upper()
    detail = None
    if not is_view:
        planned.append(
            operation(
                "op_detail",
                "table_detail",
                "DESCRIBE DETAIL (Delta format, size and files)",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            detail = read_describe_detail(spark, quoted)
            observed.append(_sp_observed("op_detail", "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "metadata")
            observed.append(
                _sp_observed(
                    "op_detail",
                    "failed",
                    start,
                    detail=record["condition"] or record["error_class"],
                )
            )
            table["notes"].append(
                "DESCRIBE DETAIL is not available for this source (not a Delta table or not "
                "permitted)."
            )
    table["source"] = _sp_source(extended, detail, now())
    is_delta = (detail or {}).get("format") == "delta" or (
        extended.get("Provider") or ""
    ).lower() == "delta"

    version = None
    version_timestamp = None
    if level == "standard" and is_delta and config["consistency"]["pin_delta_version"]:
        planned.append(
            operation(
                "op_history",
                "table_history",
                "DESCRIBE HISTORY LIMIT 1 (Delta version to pin)",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            version, version_timestamp = read_latest_version(spark, quoted)
            observed.append(_sp_observed("op_history", "succeeded", start, rows=1))
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "snapshot")
            observed.append(
                _sp_observed(
                    "op_history",
                    "failed",
                    start,
                    detail=record["condition"] or record["error_class"],
                )
            )
            table["notes"].append(
                "The Delta version could not be resolved; reads are not pinned to one snapshot."
            )

    constraints: list[dict[str, Any]] = []
    for name, expression in sorted(((detail or {}).get("check_constraints") or {}).items()):
        constraints.append(
            {
                "name": name,
                "constraint_type": "check",
                "columns": [],
                "expression": expression,
                "referenced": None,
                "enforcement": "enforced",
                "source": "delta_table_property",
            }
        )
    if capabilities.get("parameterized_sql", {}).get("available"):
        planned.append(
            operation(
                "op_constraints",
                "information_schema",
                "Unity Catalog information_schema key constraints",
                reads_user_data=False,
            )
        )
        start = time.perf_counter()
        try:
            declared, note = read_unity_constraints(spark, parts)
            constraints.extend(declared)
            observed.append(
                _sp_observed(
                    "op_constraints", "skipped" if note else "succeeded", start, detail=note
                )
            )
            if note:
                table["notes"].append(note[0].upper() + note[1:] + ".")
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "metadata")
            observed.append(
                _sp_observed(
                    "op_constraints",
                    "failed",
                    start,
                    detail=record["condition"] or record["error_class"],
                )
            )
            table["notes"].append(
                "Declared key constraints could not be read from information_schema."
            )
    table["constraints"] = constraints

    try:
        if version is not None:
            base = spark.sql(f"SELECT * FROM {quoted} VERSION AS OF {int(version)}")
        else:
            base = spark.table(quoted)
        schema = base.schema
    except Exception as exc:  # noqa: BLE001
        table["errors"].append(error_record(exc, "resolve"))
        timings["metadata"] = _sp_ms(metadata_start)
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    limits = config["limits"]
    tree = build_schema_tree(
        neutral_fields(schema), max_depth=limits["max_depth"], max_fields=limits["max_fields"]
    )
    tree["captured_from"] = "pinned_snapshot" if version is not None else "current_table"
    table["schema"] = tree
    timings["metadata"] = _sp_ms(metadata_start)

    pinned = version is not None
    if level == "metadata":
        table["consistency"] = {
            "mode": "metadata_only",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                "Metadata only: no table rows were read. The schema reflects the current "
                "table definition and catalog metadata reflects the state at capture time."
            ),
            "notes": [],
        }
        table["scope"]["scope_label"] = "table_metadata"
    elif pinned:
        table["consistency"] = {
            "mode": "pinned_delta_version",
            "delta_version": int(version) if version is not None else None,
            "version_timestamp": version_timestamp,
            "guarantee": f"All row reads (sample and aggregations) used Delta version {version}, "
            "resolved at the start of this table's profile.",
            "notes": [
                "Catalog metadata (size, files, comments) describes the latest state at capture "
                "time."
            ],
        }
    else:
        unpinned_reason = (
            "the source is a view"
            if is_view
            else "the source is not a Delta table"
            if not is_delta
            else "pinning is disabled or the version could not be resolved"
        )
        table["consistency"] = {
            "mode": "unpinned",
            "delta_version": None,
            "version_timestamp": None,
            "guarantee": (
                f"No snapshot was pinned ({unpinned_reason}). The sample and each aggregation pass "
                "read the source independently and may observe different data if it "
                "changes during the run."
            ),
            "notes": [],
        }

    top_names = [field.name for field in schema.fields]
    if selected is not None:
        unknown = [name for name in selected if name not in top_names]
        if unknown:
            table["errors"].append(
                {
                    "stage": "resolve",
                    "error_class": "ConfigurationError",
                    "condition": "SELECTED_COLUMN_NOT_FOUND",
                    "message": f"{len(unknown)} selected column(s) not found in the table schema",
                }
            )
    schema_ids = {node["field_id"] for node in iter_nodes(tree["fields"])}
    for item in filters:
        if field_id(column_reference_segments(item["column"])) not in schema_ids:
            table["errors"].append(
                {
                    "stage": "resolve",
                    "error_class": "ConfigurationError",
                    "condition": "FILTER_COLUMN_NOT_FOUND",
                    "message": "a filter references a column that is not in the table schema",
                }
            )

    profiled, omissions = select_profile_fields(tree, selected)
    table["omissions"].extend(omissions)
    profiled_ids = {node["field_id"] for node in profiled}
    reasons = {item["field_id"]: item["reason"] for item in omissions if item["field_id"]}
    parents: dict[str, Any] = {}
    for node in iter_nodes(tree["fields"]):
        for child in node["children"]:
            parents[child["field_id"]] = node
    nodes_by_id = {node["field_id"]: node for node in iter_nodes(tree["fields"])}
    wanted = set(selected) if selected is not None else None
    for node in iter_nodes(tree["fields"]):
        reason: str | None = None
        if node["field_id"] not in profiled_ids:
            reason = _sp_omission_reason(
                node["field_id"], node["path"][0]["name"], reasons, parents, wanted
            )
        table["field_profiles"].append(
            field_profile(node, profiled=node["field_id"] in profiled_ids, omission_reason=reason)
        )
    if level == "metadata":
        for field in table["field_profiles"]:
            field["profiled"] = False
            field["omission_reason"] = "metadata_level"
        table["table_metrics"].append(_sp_statistics_row_count(extended))
        timings["total"] = _sp_ms(total_start)
        finalize_table(table, config)
        return table
    if any(error["stage"] == "resolve" for error in table["errors"]):
        timings["total"] = _sp_ms(total_start)
        table["schema"] = tree
        finalize_table(table, config)
        return table

    scope = scope_label(pinned, bool(filters))
    table["scope"]["scope_label"] = scope
    frame = base
    if filters:
        frame = frame.filter(filter_condition(filters))
    if selected is not None:
        frame = frame.select(*[F.col(quote_name(name)) for name in selected])

    # 2. transient sample ---------------------------------------------------------
    sample_plan = plan_sample(profiled, config)
    sample_nodes = [nodes_by_id[fid] for fid in sample_plan["field_ids"]]
    sample_record: dict[str, Any] = {
        "enabled": sample_plan["enabled"],
        "method": sample_plan["method"]
        if sample_plan["enabled"]
        else ("none" if config["sampling"]["method"] == "none" else config["sampling"]["method"]),
        "biased": True
        if sample_plan["method"] == "prefix"
        else (False if sample_plan["method"] == "random" else None),
        "reads_full_source": True if sample_plan["method"] == "random" else None,
        "max_rows": sample_plan["max_rows"],
        "max_bytes": sample_plan["max_bytes"],
        "max_value_chars": sample_plan["max_value_chars"],
        "random_fraction": sample_plan["random_fraction"],
        "seed": sample_plan["seed"],
        "rows_collected": None,
        "bytes_retained": None,
        "values_truncated": None,
        "stopped_reason": "disabled"
        if config["sampling"]["method"] == "none"
        else "not_applicable",
        "fields_sampled": len(sample_nodes),
        "fields_omitted": len(sample_plan["omitted_field_ids"]),
        "notes": [sample_plan["reason"]] if sample_plan["reason"] else [],
    }
    table["sample"] = sample_record
    json_ids: set[str] = set()
    semantic_by_id: dict[str, dict[str, Any]] = {}
    sample_start = time.perf_counter()
    if sample_plan["enabled"]:
        sample_record["notes"] = [
            "Values are inspected transiently for format and JSON inference; they are not "
            "persisted.",
            "prefix: the first rows produced by the engine; potentially biased, not random."
            if sample_plan["method"] == "prefix"
            else (
                "random: Bernoulli sampling may evaluate every row in scope; the fraction "
                "does not reduce I/O proportionally."
            ),
        ]
        planned.append(
            operation(
                "op_sample",
                "sample_collect",
                f"projected {sample_plan['method']} sample of string fields, values cut to "
                f"{sample_plan['max_value_chars']} characters",
                reads_user_data=True,
                max_rows=sample_plan["max_rows"],
                max_bytes=sample_plan["max_bytes"],
                columns=len(sample_nodes),
            )
        )
        try:
            sample = collect_sample(frame, sample_nodes, sample_plan)
            observed.append(
                _sp_observed("op_sample", "succeeded", sample_start, rows=sample["rows"])
            )
            sample_record.update(
                {
                    "rows_collected": sample["rows"],
                    "bytes_retained": sample["bytes"],
                    "values_truncated": sum(
                        item["truncated"] for item in sample["fields"].values()
                    ),
                    "stopped_reason": sample["stopped_reason"],
                }
            )
            example_ids = (
                policy_field_ids(config, table_lookup_key(parts), "example_columns")
                if config["value_policy"]["persist_examples"]
                else set()
            )
            json_keys = config["value_policy"]["json_key_names"] == "include"
            redacted_ids = policy_field_ids(config, table_lookup_key(parts), "redact_columns")
            for node in sample_nodes:
                data = sample["fields"][node["field_id"]]
                fmt = infer_format(
                    data["values"],
                    sql_nulls=data["nulls"],
                    truncated=data["truncated"],
                    sample_method=sample_plan["method"],
                    semantic_config=config["semantic"],
                )
                entry: dict[str, Any] = {
                    "observed_format": fmt,
                    "json_profile": None,
                    "concentration": None,
                    "examples": None,
                }
                if is_json_like(data["values"]) or fmt.get("format") in (
                    "json_object",
                    "json_array",
                ):
                    shape = json_shape(
                        data["values"],
                        sql_nulls=data["nulls"],
                        truncated=data["truncated"],
                        include_key_names=json_keys and node["field_id"] not in redacted_ids,
                        sample_method=sample_plan["method"],
                    )
                    entry["json_profile"] = shape
                    eligible = shape["eligible_observations"]
                    structured = shape["counts"]["object"] + shape["counts"]["array"]
                    if eligible and structured / eligible >= config["thresholds"]["json_min_ratio"]:
                        json_ids.add(node["field_id"])
                entry["concentration"] = value_concentration(data["values"])
                if node["field_id"] in example_ids and node["field_id"] not in redacted_ids:
                    entry["examples"] = value_examples(
                        data["values"],
                        max_examples=config["value_policy"]["max_examples_per_column"],
                        max_chars=config["value_policy"]["max_example_chars"],
                    )
                semantic_by_id[node["field_id"]] = entry
            del sample
        except Exception as exc:  # noqa: BLE001
            record = error_record(exc, "sample")
            table["errors"].append(record)
            sample_record["stopped_reason"] = "error"
            observed.append(
                _sp_observed(
                    "op_sample",
                    "failed",
                    sample_start,
                    detail=record["condition"] or record["error_class"],
                )
            )
    timings["sample"] = _sp_ms(sample_start)

    # 3. shared aggregation passes -----------------------------------------------
    redacted = policy_field_ids(config, table_lookup_key(parts), "redact_columns")
    if config["value_policy"]["aggregate_extremes"] == "redact":
        redacted = set(profiled_ids)
    caps = {name: bool(item.get("available")) for name, item in capabilities.items()}
    agg_plan = plan_aggregates(
        profiled,
        config=config,
        capabilities=caps,
        scope=scope,
        json_field_ids=json_ids,
        redacted_field_ids=redacted,
    )
    for omission in agg_plan["omissions"]:
        table["omissions"].append(omission)
    for index, aliases in enumerate(agg_plan["passes"]):
        specs = [spec for spec in agg_plan["specs"] if spec["alias"] in set(aliases)]
        planned.append(
            operation(
                f"op_aggregate_{index + 1}",
                "aggregate_pass",
                "one shared aggregation over the analysed scope (single agg call; not a guarantee "
                "of a single physical scan)",
                reads_user_data=True,
                expressions=sum(spec["cost"] for spec in specs),
                fields=len({spec["field_id"] for spec in specs if spec["field_id"]}),
            )
        )
    ctx = {
        "reference_timestamp": F.lit(reference_time).cast("timestamp"),
        "reference_date": F.lit(reference_time[:10]).cast("date"),
    }
    aggregate_start = time.perf_counter()
    results, failed, agg_errors = run_aggregates(frame, agg_plan, nodes_by_id, ctx, observed)
    table["errors"].extend(agg_errors)
    timings["aggregate"] = _sp_ms(aggregate_start)

    specs_by_field: dict[str | None, list[dict[str, Any]]] = {}
    for spec in agg_plan["specs"]:
        specs_by_field.setdefault(spec["field_id"], []).append(spec)
    static_by_field: dict[str, list[dict[str, Any]]] = {}
    for item in agg_plan["static_metrics"]:
        static_by_field.setdefault(item["field_id"], []).append(item)
    row_spec = specs_by_field[None][0]
    if row_spec["alias"] in failed or results.get(row_spec["alias"]) is None:
        row_count = None
        table["table_metrics"].append(
            not_measured(
                "row_count",
                "error",
                failed.get(row_spec["alias"], "row count not returned"),
                scope=scope,
                source="aggregate",
            )
        )
    else:
        row_count = int(results[row_spec["alias"]])
        table["table_metrics"].append(
            measured(
                "row_count",
                row_count,
                unit="rows",
                scope=scope,
                accuracy="exact",
                source="aggregate",
                method="count(*) over the analysed scope",
            )
        )
    null_counts: dict[str, int | None] = {}
    for spec in agg_plan["specs"]:
        if spec["metric"] == "null_count" and spec["alias"] not in failed:
            value = results.get(spec["alias"])
            null_counts[spec["field_id"]] = int(value) if value is not None else None
    for field in table["field_profiles"]:
        if not field["profiled"]:
            continue
        node = nodes_by_id[field["field_id"]]
        parent_id = node.get("parent_field_id")
        field["metrics"] = field_metrics(
            node,
            specs_by_field.get(field["field_id"], []),
            results,
            failed,
            scope=scope,
            row_count=row_count,
            parent_null_count=null_counts.get(parent_id) if parent_id else None,
            static=static_by_field.get(field["field_id"], []),
        )
        inferred = semantic_by_id.get(field["field_id"])
        if inferred is not None:
            field["semantics"] = {
                "observed_format": inferred["observed_format"],
                "candidate_roles": [],
            }
            field["json_profile"] = inferred["json_profile"]
            field["concentration"] = inferred["concentration"]
            field["examples"] = inferred["examples"]
    if not capabilities.get("try_parse_json", {}).get("available") and json_ids:
        table["unsupported"].append(
            {
                "capability": "try_parse_json",
                "detail": (
                    "full-scope JSON validation is unavailable in this runtime; "
                    "JSON validity is sample-based"
                ),
            }
        )
    timings["total"] = _sp_ms(total_start)
    finalize_table(table, config)
    log(
        f"[tabledossier] {key}: {table['status']} — {len(profiled)} field(s) profiled in "
        f"{len(agg_plan['passes'])} aggregation pass(es)"
    )
    return table
