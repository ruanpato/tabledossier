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
    element_field_metrics,
    field_metrics,
    field_profile,
    finalize_table,
    known_relationships,
    new_table,
    policy_field_ids,
)
from tabledossier.config import ROW_READING_LEVELS, table_options_for
from tabledossier.deep import (
    JSON_TYPE_PATTERNS,
    attach_json_validation,
    json_path_catalog,
    plan_deep_elements,
    plan_element_distinct,
    plan_element_specs,
    plan_json_validation,
)
from tabledossier.errors import error_record, sanitize_message
from tabledossier.integrity import (
    end_segments,
    not_validated_detail,
    referential_requested,
    referential_summary,
    type_compatibility,
    validation_detail,
)
from tabledossier.keys import (
    key_columns_problem,
    plan_uniqueness,
    requested_keys,
    uniqueness_record,
)
from tabledossier.metrics import measured, not_measured
from tabledossier.paths import (
    IdentifierError,
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
    candidate_roles,
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
_SP_VARIANT_FUNCTIONS = (
    "try_parse_json",
    "try_variant_get",
    "is_variant_null",
    "schema_of_variant",
)
_SP_INF = float("inf")
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
    found: dict[str, bool | None] = {}
    for name in (*_SP_VARIANT_FUNCTIONS, "get_json_object"):
        try:
            found[name] = bool(spark.catalog.functionExists(name))
        except Exception:  # noqa: BLE001
            found[name] = None
    capabilities["variant_functions"] = {
        "available": has_call and all(found[name] for name in _SP_VARIANT_FUNCTIONS),
        "detail": "; ".join(
            f"functionExists('{name}') = {_sp_flag(found[name])}" for name in _SP_VARIANT_FUNCTIONS
        )
        + "; used by the deep level for full-scope JSON path presence and type checks",
    }
    capabilities["get_json_object"] = {
        "available": bool(found["get_json_object"]) and hasattr(F, "get_json_object"),
        "detail": f"functionExists('get_json_object') = {_sp_flag(found['get_json_object'])}; "
        "deep-level fallback for full-scope JSON path presence (cannot tell a JSON null from an "
        "absent path, nor types)",
    }
    return capabilities


def _sp_flag(value: bool | None) -> str:
    return "unknown" if value is None else str(value).lower()


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
    if op.startswith("el_"):
        return compile_element_spec(spec, node, ctx)
    if op.startswith("json_"):
        return compile_json_spec(spec, node)
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


# --------------------------------------------------------------------------- deep expressions


def _sp_inner(value: Any, names: Sequence[str]) -> Any:
    for name in names:
        value = value.getField(name)
    return value


def _sp_elements(params: Mapping[str, Any], nodes_by_id: Mapping[str, Any]) -> tuple[Any, Any]:
    """Return ``(collection column, per-row array of its elements, keys or values)``."""
    collection = column_for(nodes_by_id[params["collection_field_id"]]["path"])
    if params["segment"] == "map_key":
        return collection, F.map_keys(collection)
    if params["segment"] == "map_value":
        return collection, F.map_values(collection)
    return collection, collection


def _sp_leaf_values(elements: Any, inner: Sequence[str]) -> Any:
    names = list(inner)
    return F.transform(elements, lambda item: _sp_inner(item, names)) if names else elements


def _sp_finite_value(value: Any) -> Any:
    return (
        value.isNotNull() & ~F.isnan(value) & (value != F.lit(_SP_INF)) & (value != F.lit(-_SP_INF))
    )


def compile_element_spec(
    spec: Mapping[str, Any], node: Mapping[str, Any], ctx: Mapping[str, Any]
) -> Any:
    """Compile an element metric: per-row higher-order functions aggregated over rows.

    No ``explode``: every expression reads each row's collection once, inside
    the shared aggregation pass, and is exact over the analysed scope.
    """
    op = spec["op"]
    params = spec["params"]
    kind = node["type"]["kind"]
    collection, elements = _sp_elements(params, ctx["nodes_by_id"])
    inner = list(params["inner"])
    values = _sp_leaf_values(elements, inner)

    def present(value: Any) -> Any:
        return _sp_finite_value(value) if kind == "float" else value.isNotNull()

    def count(predicate: Callable[[Any], Any]) -> Any:
        return F.sum(F.when(collection.isNotNull(), F.size(F.filter(values, predicate))))

    if op == "el_count_null":
        return count(lambda value: value.isNull())
    if op == "el_count_null_parent_present":
        parent = list(params["parent_inner"])
        return F.sum(
            F.when(
                collection.isNotNull(),
                F.size(
                    F.filter(
                        elements,
                        lambda item: (
                            _sp_inner(item, parent).isNotNull() & _sp_inner(item, inner).isNull()
                        ),
                    )
                ),
            )
        )
    if op in ("el_min", "el_max"):
        kept = F.filter(values, present) if kind == "float" else values
        agg = F.min(F.array_min(kept)) if op == "el_min" else F.max(F.array_max(kept))
        if kind == "timestamp":
            return F.date_format(agg, TIMESTAMP_PATTERN)
        if kind == "timestamp_ntz":
            return F.date_format(agg, TIMESTAMP_NTZ_PATTERN)
        return agg
    if op == "el_count_zero":
        return count(lambda value: present(value) & (value == F.lit(0)))
    if op == "el_count_negative":
        return count(lambda value: present(value) & (value < F.lit(0)))
    if op == "el_count_positive":
        return count(lambda value: present(value) & (value > F.lit(0)))
    if op == "el_count_nan":
        return count(lambda value: F.isnan(value))
    if op == "el_count_pos_inf":
        return count(lambda value: value == F.lit(_SP_INF))
    if op == "el_count_neg_inf":
        return count(lambda value: value == F.lit(-_SP_INF))
    if op == "el_count_finite":
        return count(_sp_finite_value)
    if op == "el_count_empty":
        return count(lambda value: value == F.lit(""))
    if op == "el_count_whitespace_only":
        return count(lambda value: (value != F.lit("")) & value.rlike("^\\s+$"))
    if op in ("el_min_length", "el_max_length"):
        lengths = F.transform(values, lambda value: F.length(value))
        if op == "el_min_length":
            return F.min(F.array_min(lengths))
        return F.max(F.array_max(lengths))
    if op == "el_count_true":
        return count(lambda value: value == F.lit(True))
    if op == "el_count_false":
        return count(lambda value: value == F.lit(False))
    if op == "el_count_after_reference":
        reference = ctx["reference_date"] if kind == "date" else ctx["reference_timestamp"]
        return count(lambda value: value > reference)
    raise ValueError(f"unknown element op: {op}")


def compile_json_spec(spec: Mapping[str, Any], node: Mapping[str, Any]) -> Any:
    """Compile a full-scope JSON path check (the path is a literal argument, never SQL)."""
    op = spec["op"]
    params = spec["params"]
    column = column_for(node["path"])
    if params["method"] == "variant":
        parsed = F.call_function("try_parse_json", column)
        if op == "json_count_documents":
            root = F.call_function("schema_of_variant", parsed)
            return F.count(F.when(root.rlike("^(OBJECT|ARRAY)"), 1))
        value = F.call_function("try_variant_get", parsed, F.lit(params["path"]), F.lit("variant"))
        if op == "json_path_present":
            return F.count(F.when(value.isNotNull(), 1))
        if op == "json_path_null":
            return F.count(F.when(F.call_function("is_variant_null", value), 1))
        if op == "json_path_type_match":
            pattern = JSON_TYPE_PATTERNS[params["expected_type"]]
            return F.count(F.when(F.call_function("schema_of_variant", value).rlike(pattern), 1))
    elif params["method"] == "get_json_object":
        if op == "json_count_documents":
            root = F.ltrim(F.get_json_object(column, "$"))
            return F.count(F.when(F.substring(root, 1, 1).isin("{", "["), 1))
        if op == "json_path_non_null":
            return F.count(F.when(F.get_json_object(column, params["path"]).isNotNull(), 1))
    raise ValueError(f"unknown JSON op or method: {op} ({params.get('method')})")


def _sp_data_type(schema: Any, path: Sequence[Mapping[str, Any]]) -> Any:
    """Return the Spark DataType at a typed path of a DataFrame schema."""
    data_type = schema
    for segment in path:
        kind = segment["kind"]
        if kind == "field":
            data_type = data_type[segment["name"]].dataType
        elif kind == "array_element":
            data_type = data_type.elementType
        elif kind == "map_key":
            data_type = data_type.keyType
        else:
            data_type = data_type.valueType
    return data_type


def _sp_slot_builder(index: int, types: Sequence[Any]) -> Callable[[Any], Any]:
    """Wrap one element value in a struct with one typed slot per leaf (only its own is set)."""

    def build(value: Any) -> Any:
        slots = [
            (value if slot == index else F.lit(None).cast(types[slot])).alias(f"s{slot}")
            for slot in range(len(types))
        ]
        return F.struct(F.lit(index).alias("l"), *slots)

    return build


def run_element_distinct(
    frame: Any, plan: Mapping[str, Any], nodes_by_id: Mapping[str, Any]
) -> dict[str, Any]:
    """Run the single explode pass of the deep level and return raw counts.

    All element fields are exploded together in one action: each row's
    elements become structs tagged with their leaf, concatenated and exploded
    once. In ``sample`` mode only a bounded prefix (or random) sample of rows is
    read; in every mode at most ``max_elements`` exploded elements are
    aggregated. Only counts come back to the driver, never values.
    """
    leaves = plan["leaves"]
    schema = frame.schema
    types = [_sp_data_type(schema, nodes_by_id[leaf["field_id"]]["path"]) for leaf in leaves]
    arrays = []
    for index, leaf in enumerate(leaves):
        _, elements = _sp_elements(leaf, nodes_by_id)
        values = _sp_leaf_values(elements, leaf["inner"])
        empty = F.array_repeat(F.lit(None).cast(types[index]), 0)
        arrays.append(F.transform(F.coalesce(values, empty), _sp_slot_builder(index, types)))
    combined = arrays[0] if len(arrays) == 1 else F.concat(*arrays)
    source = frame
    if plan["mode"] == "sample":
        if plan["sampling_method"] == "random":
            source = source.sample(
                withReplacement=False, fraction=float(plan["random_fraction"]), seed=plan["seed"]
            )
        source = source.limit(plan["max_rows"])
    exploded = source.select(F.posexplode(combined).alias("pos", "e")).limit(plan["max_elements"])
    element = F.col("e")
    aggregates = [
        F.count(F.lit(1)).alias("n"),
        F.count(F.when(F.col("pos") == F.lit(0), 1)).alias("rows"),
    ]
    for index in range(len(leaves)):
        slot = element.getField(f"s{index}")
        aggregates += [
            F.count(F.when(element.getField("l") == F.lit(index), 1)).alias(f"n{index}"),
            F.count(slot).alias(f"nn{index}"),
            F.countDistinct(slot).alias(f"d{index}"),
        ]
    row = exploded.agg(*aggregates).collect()[0]
    return {key: (0 if value is None else int(value)) for key, value in row.asDict().items()}


def _sp_any_null(columns: Sequence[Any]) -> Any:
    condition = columns[0].isNull()
    for column in columns[1:]:
        condition = condition | column.isNull()
    return condition


def run_uniqueness_pass(
    frame: Any, keys: Sequence[Mapping[str, Any]], nodes_by_id: Mapping[str, Any]
) -> dict[int, dict[str, Any]]:
    """Check several keys exactly in one Spark action and return counts per key position.

    Each row becomes one entry per key: a struct with the key position, a flag
    for NULL in any key column and one typed slot per key column of every key
    (only the entry's own slots are set). The entries are exploded once,
    grouped by position, flag and slots, and the group sizes are aggregated per
    key. Only counts come back to the driver, never key values.
    """
    schema = frame.schema
    slots: list[tuple[int, Any, Any]] = []
    for position, key in enumerate(keys):
        for fid in key["field_ids"]:
            path = nodes_by_id[fid]["path"]
            slots.append((position, column_for(path), _sp_data_type(schema, path)))
    names = [f"s{index}" for index in range(len(slots))]

    def entry(position: int) -> Any:
        own = [column for owner, column, _ in slots if owner == position]
        values = [
            (column if owner == position else F.lit(None).cast(data_type)).alias(names[index])
            for index, (owner, column, data_type) in enumerate(slots)
        ]
        return F.struct(F.lit(position).alias("l"), _sp_any_null(own).alias("z"), *values)

    if len(keys) == 1:
        exploded = frame.select(entry(0).alias("e"))
    else:
        entries = F.array(*[entry(position) for position in range(len(keys))])
        exploded = frame.select(F.explode(entries).alias("e"))
    element = F.col("e")
    flat = exploded.select(*[element.getField(name).alias(name) for name in ("l", "z", *names)])
    groups = flat.groupBy("l", "z", *names).agg(F.count(F.lit(1)).alias("n"))
    complete = ~F.col("z")
    repeated = complete & (F.col("n") > F.lit(1))
    stats = groups.groupBy("l").agg(
        F.sum("n").alias("rows"),
        F.sum(F.when(F.col("z"), F.col("n"))).alias("null_rows"),
        F.count(F.when(complete, 1)).alias("distinct"),
        F.count(F.when(repeated, 1)).alias("dup_groups"),
        F.sum(F.when(repeated, F.col("n"))).alias("dup_rows"),
        F.max(F.when(complete, F.col("n"))).alias("max_n"),
    )
    out: dict[int, dict[str, Any]] = {}
    for row in stats.collect():
        values = row.asDict()
        out[int(values.pop("l"))] = {
            key: (None if value is None else int(value)) for key, value in values.items()
        }
    return out


def run_inclusion_check(
    source: Any,
    source_columns: Sequence[Any],
    target: Any,
    target_columns: Sequence[Any],
    *,
    sample: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Count source rows whose complete key is absent from the target, in one Spark action.

    The target is grouped by its key (distinct values and their multiplicity),
    the source is left-joined to it and both sides are aggregated; the two
    single-row aggregates are cross-joined and collected once. With ``sample``
    the source is limited to a bounded prefix (or random) sample first. Only
    counts come back to the driver, never key values.
    """
    keys = [column.alias(f"k{index}") for index, column in enumerate(source_columns)]
    rows = source.select(*keys, _sp_any_null(list(source_columns)).alias("z"))
    if sample is not None:
        if sample["method"] == "random":
            rows = rows.sample(
                withReplacement=False, fraction=float(sample["fraction"]), seed=sample["seed"]
            )
        rows = rows.limit(sample["max_rows"])
    names = [f"t{index}" for index in range(len(target_columns))]
    aliased = [column.alias(name) for column, name in zip(target_columns, names, strict=True)]
    groups = (
        target.select(*aliased, _sp_any_null(list(target_columns)).alias("tz"))
        .groupBy(*names, "tz")
        .agg(F.count(F.lit(1)).alias("tn"))
    )
    distinct = groups.where(~F.col("tz")).select(*names, "tn")
    condition = None
    for index, name in enumerate(names):
        equal = F.col(f"k{index}") == F.col(name)
        condition = equal if condition is None else condition & equal
    joined = rows.join(distinct, on=condition, how="left")
    source_stats = joined.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("z"), 1)).alias("null_rows"),
        F.count(F.when(~F.col("z") & F.col("tn").isNull(), 1)).alias("orphans"),
    )
    target_stats = groups.agg(
        F.sum("tn").alias("t_rows"),
        F.sum(F.when(F.col("tz"), F.col("tn"))).alias("t_null_rows"),
        F.count(F.when(~F.col("tz"), 1)).alias("t_distinct"),
        F.count(F.when(~F.col("tz") & (F.col("tn") > F.lit(1)), 1)).alias("t_dup_groups"),
    )
    row = source_stats.crossJoin(target_stats).collect()[0]
    return {key: (0 if value is None else int(value)) for key, value in row.asDict().items()}


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
    return (
        query_key_constraints(
            spark, catalog, schema, table, lambda name: quote_name(name) + ".information_schema"
        ),
        None,
    )


def query_key_constraints(
    spark: Any,
    catalog: str,
    schema: str,
    table: str,
    information_schema: Callable[[str], str],
) -> list[dict[str, Any]]:
    """Query key constraints of one table from ``information_schema``-shaped views.

    ``information_schema`` maps a catalog name to the quoted prefix of its
    ``information_schema`` (Unity Catalog: ``<catalog>.information_schema``).
    Table and schema names are bound parameters, never interpolated.
    """
    info = information_schema(catalog)
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
                f"FROM {information_schema(ref_catalog)}.key_column_usage "
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
    return constraints


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
            cause = record["condition"] or record["error_class"]
            provider = (extended.get("Provider") or "").strip().lower()
            if provider and provider != "delta":
                # Expected: engines without Delta (or Delta itself) may reject DESCRIBE DETAIL
                # for other formats. Size and file count are then simply unavailable.
                observed.append(
                    _sp_observed(
                        "op_detail",
                        "skipped",
                        start,
                        detail=f"not available for this non-Delta source (provider {provider}; "
                        f"{cause})",
                    )
                )
                table["notes"].append(
                    f"DESCRIBE DETAIL is not available for this non-Delta source (provider "
                    f"{provider}); size and file count are unavailable."
                )
            else:
                observed.append(_sp_observed("op_detail", "failed", start, detail=cause))
                table["notes"].append(
                    "DESCRIBE DETAIL failed for this source (not permitted or not supported)."
                )
    table["source"] = _sp_source(extended, detail, now())
    is_delta = (detail or {}).get("format") == "delta" or (
        extended.get("Provider") or ""
    ).lower() == "delta"

    version = None
    version_timestamp = None
    if level in ROW_READING_LEVELS and is_delta and config["consistency"]["pin_delta_version"]:
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
    deep_plan = None
    if level == "deep":
        deep_plan = plan_deep_elements(tree, profiled, config, table_lookup_key(parts))
        replaced = set(deep_plan["contexts"]) | {o["field_id"] for o in deep_plan["omissions"]}
        omissions = [
            item
            for item in omissions
            if not (item["reason"] == "inside_collection" and item["field_id"] in replaced)
        ]
        omissions.extend(deep_plan["omissions"])
    element_contexts: dict[str, dict[str, Any]] = deep_plan["contexts"] if deep_plan else {}
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
        fid = node["field_id"]
        reason: str | None = None
        is_profiled = fid in profiled_ids or fid in element_contexts
        if not is_profiled:
            reason = _sp_omission_reason(fid, node["path"][0]["name"], reasons, parents, wanted)
        record = field_profile(node, profiled=is_profiled, omission_reason=reason)
        if fid in element_contexts:
            record["element_context"] = dict(element_contexts[fid])
        table["field_profiles"].append(record)
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
    scoped = base.filter(filter_condition(filters)) if filters else base
    frame = scoped
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
    json_catalogs: dict[str, dict[str, Any]] = {}
    redacted_ids = policy_field_ids(config, table_lookup_key(parts), "redact_columns")
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
            if deep_plan is not None:
                json_catalogs = _sp_json_catalogs(
                    deep_plan, sample, json_ids, config, redacted_ids, sample_plan["method"]
                )
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

    # 3. shared aggregation passes (+ deep expressions) --------------------------
    redacted = set(redacted_ids)
    if config["value_policy"]["aggregate_extremes"] == "redact":
        redacted = set(profiled_ids) | set(element_contexts)
    caps = {name: bool(item.get("available")) for name, item in capabilities.items()}
    deep_candidates: list[dict[str, Any]] = []
    deep_static: list[dict[str, Any]] = []
    json_method, json_unsupported = _sp_json_method(config, caps) if deep_plan else (None, None)
    if deep_plan is not None:
        deep_candidates, deep_static = plan_element_specs(
            deep_plan["element_nodes"], element_contexts, scope=scope, redacted_field_ids=redacted
        )
        deep_candidates.extend(plan_json_validation(json_catalogs, json_method))
    agg_plan = plan_aggregates(
        profiled,
        config=config,
        capabilities=caps,
        scope=scope,
        json_field_ids=json_ids,
        redacted_field_ids=redacted,
        deep_candidates=deep_candidates,
        max_extra_passes=config["deep"]["max_extra_passes"] if deep_plan else 0,
    )
    for omission in agg_plan["omissions"]:
        table["omissions"].append(omission)
    for index, aliases in enumerate(agg_plan["passes"]):
        specs = [spec for spec in agg_plan["specs"] if spec["alias"] in set(aliases)]
        extra_pass = index >= agg_plan["standard_passes"]
        details: dict[str, Any] = {
            "expressions": sum(spec["cost"] for spec in specs),
            "fields": len({spec["field_id"] for spec in specs if spec["field_id"]}),
        }
        if deep_plan is not None:
            details["deep_expressions"] = sum(spec["cost"] for spec in specs if spec.get("group"))
        planned.append(
            operation(
                f"op_aggregate_{index + 1}",
                "deep_aggregate_pass" if extra_pass else "aggregate_pass",
                (
                    "extra aggregation for deep expressions that did not fit the standard passes "
                    "(counts against deep.max_extra_passes; single agg call)"
                    if extra_pass
                    else "one shared aggregation over the analysed scope (single agg call; not a "
                    "guarantee of a single physical scan)"
                ),
                reads_user_data=True,
                **details,
            )
        )
    distinct_plan = None
    if deep_plan is not None:
        distinct_plan = plan_element_distinct(deep_plan["element_nodes"], element_contexts, config)
        left = config["deep"]["max_extra_passes"] - agg_plan["extra_passes"]
        if distinct_plan["enabled"] and left < 1:
            distinct_plan["enabled"] = False
            distinct_plan["reason"] = (
                "no extra pass left: deep.max_extra_passes = "
                f"{config['deep']['max_extra_passes']} used by {agg_plan['extra_passes']} "
                "aggregation overflow pass(es)"
            )
            distinct_plan["budget_limited"] = True
        if distinct_plan["enabled"]:
            planned.append(
                operation(
                    "op_deep_elements",
                    "element_explode_pass",
                    (
                        f"one explode of the element fields over a {config['sampling']['method']} "
                        f"sample of at most {distinct_plan['max_rows']} rows, stopped at "
                        f"{distinct_plan['max_elements']} elements (distinct counts only)"
                        if distinct_plan["mode"] == "sample"
                        else "one explode of the element fields over the full scope when its "
                        f"measured size is at most {distinct_plan['max_elements']} elements "
                        "(distinct counts only)"
                    ),
                    reads_user_data=True,
                    mode=distinct_plan["mode"],
                    max_rows=distinct_plan["max_rows"]
                    if distinct_plan["mode"] == "sample"
                    else None,
                    max_elements=distinct_plan["max_elements"],
                    fields=len(distinct_plan["leaves"]),
                )
            )
    ctx = {
        "reference_timestamp": F.lit(reference_time).cast("timestamp"),
        "reference_date": F.lit(reference_time[:10]).cast("date"),
        "nodes_by_id": nodes_by_id,
    }
    aggregate_start = time.perf_counter()
    results, failed, agg_errors = run_aggregates(frame, agg_plan, nodes_by_id, ctx, observed)
    table["errors"].extend(agg_errors)
    timings["aggregate"] = _sp_ms(aggregate_start)

    specs_by_field: dict[str | None, list[dict[str, Any]]] = {}
    for spec in agg_plan["specs"]:
        if spec.get("group") != "json_path":
            specs_by_field.setdefault(spec["field_id"], []).append(spec)
    static_by_field: dict[str, list[dict[str, Any]]] = {}
    for item in [*agg_plan["static_metrics"], *deep_static]:
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
    distinct_metrics: dict[str, list[dict[str, Any]]] = {}
    if distinct_plan is not None:
        distinct_metrics = _sp_run_element_distinct(
            frame,
            distinct_plan,
            nodes_by_id,
            _sp_collection_totals(element_contexts, specs_by_field, results, failed),
            scope=scope,
            table=table,
            timings=timings,
        )
    for field in table["field_profiles"]:
        if not field["profiled"]:
            continue
        node = nodes_by_id[field["field_id"]]
        parent_id = node.get("parent_field_id")
        context = element_contexts.get(field["field_id"])
        if context is not None:
            totals = _sp_collection_totals(
                {field["field_id"]: context}, specs_by_field, results, failed
            )
            field["metrics"] = element_field_metrics(
                node,
                context,
                specs_by_field.get(field["field_id"], []),
                results,
                failed,
                scope=scope,
                element_total=totals.get(context["collection_field_id"]),
                parent_null_count=null_counts.get(parent_id)
                if parent_id and parent_id in element_contexts
                else None,
                static=static_by_field.get(field["field_id"], []),
                extra=distinct_metrics.get(field["field_id"], []),
            )
            continue
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
    if deep_plan is not None:
        _sp_finish_deep(
            table,
            deep_plan,
            json_catalogs,
            agg_plan,
            distinct_plan,
            results,
            failed,
            config=config,
            json_method=json_method,
            json_unsupported=json_unsupported,
            scope=scope,
        )
        _sp_uniqueness(table, scoped, tree, nodes_by_id, config, scope=scope, log=log)
    timings["total"] = _sp_ms(total_start)
    finalize_table(table, config)
    extra = (
        f" (+{(table['deep'] or {}).get('extra_passes', {}).get('planned', 0)} deep extra pass(es))"
        if deep_plan is not None
        else ""
    )
    log(
        f"[tabledossier] {key}: {table['status']} — {len(profiled) + len(element_contexts)} "
        f"field(s) profiled in {len(agg_plan['passes'])} aggregation pass(es){extra}"
    )
    return table


# --------------------------------------------------------------------------- deep helpers


def _sp_identifier_candidates(
    table: Mapping[str, Any], config: Mapping[str, Any]
) -> list[list[dict[str, Any]]]:
    """Paths of the fields the standard metrics mark as identifier candidates (schema order)."""
    out = []
    for field in table["field_profiles"]:
        if not field["profiled"] or field.get("element_context") or not field["metrics"]:
            continue
        roles = candidate_roles(
            field["type_kind"],
            field["metrics"],
            (field.get("semantics") or {}).get("observed_format"),
            config["thresholds"],
            field["physical_type"],
        )
        if any(role["role"] == "identifier_candidate" for role in roles):
            out.append([dict(segment) for segment in field["path"]])
    return out


def _sp_uniqueness(
    table: dict[str, Any],
    frame: Any,
    tree: Mapping[str, Any],
    nodes_by_id: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    scope: str,
    log: Callable[[str], None],
) -> None:
    """Plan, run and record the exact uniqueness checks of one table (deep level)."""
    lookup = table_lookup_key(table["identifier"]["parts"])
    requested = requested_keys(
        config, lookup, table["constraints"], _sp_identifier_candidates(table, config)
    )
    plan = plan_uniqueness(tree, requested, config)
    planned = table["operations"]["planned"]
    observed = table["operations"]["observed"]
    for number, members in enumerate(plan["passes"], start=1):
        planned.append(
            operation(
                f"op_uniqueness_{number}",
                "uniqueness_pass",
                "exact uniqueness of "
                f"{len(members)} key(s) in one grouped aggregation over the analysed scope (each "
                "row is exploded once per key; only counts are collected)",
                reads_user_data=True,
                keys=len(members),
                columns=sum(len(plan["keys"][m]["field_ids"]) for m in members),
            )
        )
    results: dict[int, dict[str, Any]] = {}
    errors: dict[int, str] = {}
    start = time.perf_counter()
    for number, members in enumerate(plan["passes"], start=1):
        op_id = f"op_uniqueness_{number}"
        pass_start = time.perf_counter()
        try:
            raw = run_uniqueness_pass(frame, [plan["keys"][m] for m in members], nodes_by_id)
        except Exception as exc:  # noqa: BLE001 - a failed pass leaves its keys unmeasured
            record = error_record(exc, "aggregate")
            table["errors"].append(record)
            cause = record["condition"] or record["error_class"]
            for member in members:
                errors[member] = "uniqueness pass failed: " + cause
            observed.append(_sp_observed(op_id, "failed", pass_start, detail=cause))
            continue
        for index, member in enumerate(members):
            results[member] = raw.get(index, {})
        observed.append(_sp_observed(op_id, "succeeded", pass_start, rows=len(raw)))
    if plan["passes"]:
        table["timings_ms"]["uniqueness"] = _sp_ms(start)
        log(
            f"[tabledossier] {table['table_key']}: {sum(len(m) for m in plan['passes'])} key(s) "
            f"checked for exact uniqueness in {len(plan['passes'])} pass(es)"
        )
    table["uniqueness"] = uniqueness_record(
        plan, results, errors, config=config, scope=scope, requested_any=bool(requested)
    )


def _sp_json_method(
    config: Mapping[str, Any], caps: Mapping[str, bool]
) -> tuple[str | None, str | None]:
    """Choose the full-scope JSON path method: ``(method, unsupported reason)``."""
    if not config["deep"]["json_full_scope_validation"]:
        return None, None
    if caps.get("variant_functions"):
        return "variant", None
    if caps.get("get_json_object"):
        return "get_json_object", None
    return None, "neither variant functions nor get_json_object are available in this runtime"


def _sp_json_catalogs(
    deep_plan: Mapping[str, Any],
    sample: Mapping[str, Any],
    json_ids: set[str],
    config: Mapping[str, Any],
    redacted_ids: set[str],
    sample_method: str,
) -> dict[str, dict[str, Any]]:
    """Build JSON path catalogues from the transient sample (values are not kept)."""
    deep = config["deep"]
    if deep_plan["json_nodes"] is None:
        targets = [fid for fid in sample["fields"] if fid in json_ids] if deep["json_paths"] else []
    else:
        targets = [node["field_id"] for node in deep_plan["json_nodes"]]
    catalogs = {}
    policy_names = config["value_policy"]["json_key_names"] == "include"
    for fid in targets:
        data = sample["fields"].get(fid)
        if data is None:
            continue
        include = policy_names and fid not in redacted_ids
        catalogs[fid] = json_path_catalog(
            data["values"],
            sql_nulls=data["nulls"],
            truncated=data["truncated"],
            include_names=include,
            names_omitted_reason=None
            if include
            else (
                "value policy (json_key_names = redact)"
                if not policy_names
                else "value policy (redact_columns)"
            ),
            max_depth=deep["max_json_depth"],
            max_paths=deep["max_json_paths"],
            max_object_keys=deep["max_json_object_keys"],
            sample_method=sample_method,
        )
    return catalogs


def _sp_collection_totals(
    contexts: Mapping[str, Mapping[str, Any]],
    specs_by_field: Mapping[str | None, Sequence[Mapping[str, Any]]],
    results: Mapping[str, Any],
    failed: Mapping[str, str],
) -> dict[str, int | None]:
    """Measured element/entry totals of the collections behind element fields."""
    totals: dict[str, int | None] = {}
    for context in contexts.values():
        fid = context["collection_field_id"]
        if fid in totals:
            continue
        totals[fid] = None
        for spec in specs_by_field.get(fid, []):
            if (
                spec["metric"] in ("total_element_count", "total_entry_count")
                and spec["alias"] not in failed
            ):
                value = results.get(spec["alias"])
                totals[fid] = 0 if value is None else int(value)
    return totals


def _sp_run_element_distinct(
    frame: Any,
    plan: dict[str, Any],
    nodes_by_id: Mapping[str, Any],
    totals: Mapping[str, int | None],
    *,
    scope: str,
    table: dict[str, Any],
    timings: dict[str, int],
) -> dict[str, list[dict[str, Any]]]:
    """Run (or explain) the element explode pass and return ``distinct_count`` metrics."""
    observed = table["operations"]["observed"]
    sample_mode = plan["mode"] == "sample"
    metric_scope = "sample" if sample_mode else scope
    source = "sample" if sample_mode else "aggregate"
    out: dict[str, list[dict[str, Any]]] = {}
    plan["result"] = None

    def unmeasured(status: str, reason: str) -> None:
        for leaf in plan["leaves"]:
            out[leaf["field_id"]] = [
                {
                    "field_id": leaf["field_id"],
                    "metric": not_measured(
                        "distinct_count", status, reason, scope=metric_scope, source=source
                    ),
                }
            ]

    if not plan["enabled"]:
        if plan["mode"] != "off" and plan["leaves"]:
            unmeasured("not_computed", plan["reason"] or "not planned")
        return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    start = time.perf_counter()
    if not sample_mode:
        wanted = {leaf["collection_field_id"] for leaf in plan["leaves"]}
        sizes = [totals.get(fid) for fid in wanted]
        if any(size is None for size in sizes) or sum(s or 0 for s in sizes) > plan["max_elements"]:
            reason = (
                "the measured elements in scope exceed deep.max_elements = "
                f"{plan['max_elements']} (or were not measured)"
            )
            plan["skip_reason"] = reason
            plan["budget_limited"] = True
            observed.append(_sp_observed("op_deep_elements", "skipped", start, detail=reason))
            unmeasured("not_computed", reason)
            return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    try:
        raw = run_element_distinct(frame, plan, nodes_by_id)
    except Exception as exc:  # noqa: BLE001
        record = error_record(exc, "aggregate")
        table["errors"].append(record)
        cause = record["condition"] or record["error_class"]
        plan["error"] = cause
        observed.append(_sp_observed("op_deep_elements", "failed", start, detail=cause))
        unmeasured("error", "element distinct pass failed: " + cause)
        timings["deep_elements"] = _sp_ms(start)
        return {fid: [item["metric"] for item in items] for fid, items in out.items()}
    observed.append(_sp_observed("op_deep_elements", "succeeded", start, rows=1))
    timings["deep_elements"] = _sp_ms(start)
    plan["result"] = raw
    stopped = raw["n"] >= plan["max_elements"]
    method = (
        "count(DISTINCT) over the elements of one explode of a bounded "
        f"{plan['sampling_method']} sample (at most {plan['max_rows']} rows, stopped at "
        f"{plan['max_elements']} elements); exact for the elements examined only"
        if sample_mode
        else "count(DISTINCT) over every element in scope (one explode; measured size within "
        "deep.max_elements)"
    )
    for index, leaf in enumerate(plan["leaves"]):
        examined, non_null, distinct = raw[f"n{index}"], raw[f"nn{index}"], raw[f"d{index}"]
        details = {
            "elements_examined": examined,
            "rows_with_elements": raw["rows"],
            "max_rows": plan["max_rows"] if sample_mode else None,
            "max_elements": plan["max_elements"],
            "stopped_by_element_limit": stopped,
        }
        if non_null == 0:
            metric = not_measured(
                "distinct_count",
                "insufficient_data",
                f"no non-null {leaf['unit']} among the elements examined",
                scope=metric_scope,
                source=source,
            )
        else:
            metric = measured(
                "distinct_count",
                distinct,
                unit="values",
                scope=metric_scope,
                accuracy="exact",
                source=source,
                method=method,
                denominator=non_null,
                denominator_unit=leaf["unit"],
                details=details,
            )
        out[leaf["field_id"]] = [{"field_id": leaf["field_id"], "metric": metric}]
    return {fid: [item["metric"] for item in items] for fid, items in out.items()}


def _sp_finish_deep(
    table: dict[str, Any],
    deep_plan: Mapping[str, Any],
    catalogs: Mapping[str, dict[str, Any]],
    agg_plan: Mapping[str, Any],
    distinct_plan: Mapping[str, Any] | None,
    results: Mapping[str, Any],
    failed: Mapping[str, str],
    *,
    config: Mapping[str, Any],
    json_method: str | None,
    json_unsupported: str | None,
    scope: str,
) -> None:
    """Attach JSON path catalogues and the deep coverage record to a table."""
    deep = config["deep"]
    fields = {field["field_id"]: field for field in table["field_profiles"]}
    json_specs = [spec for spec in agg_plan["specs"] if spec.get("group") == "json_path"]
    dropped = list(agg_plan["deep_dropped"])
    limited: list[dict[str, str]] = []
    json_fields = []
    for fid, catalog in catalogs.items():
        attach_json_validation(
            catalog,
            [spec for spec in json_specs if spec["field_id"] == fid],
            results,
            failed,
            [spec for spec in dropped if spec["field_id"] == fid],
            method=json_method,
            unsupported_reason=json_unsupported,
            scope=scope,
        )
        fields[fid]["json_paths"] = catalog
        validated = sum(
            1
            for item in catalog.get("paths") or []
            if (item.get("full_scope") or {}).get("status") == "measured"
        )
        json_fields.append(
            {
                "field_id": fid,
                "display_path": fields[fid]["display_path"],
                "status": "catalogued",
                "reason": catalog["paths_omitted_reason"],
                "paths_listed": catalog["paths_listed"],
                "paths_validated": validated,
                "full_scope_method": json_method if catalog.get("paths") else None,
            }
        )
        if catalog["paths_observed"] > catalog["paths_listed"] and catalog.get("paths") is not None:
            limited.append(
                {
                    "item": fields[fid]["display_path"],
                    "reason": "json_path_budget",
                    "detail": f"{catalog['paths_observed'] - catalog['paths_listed']} JSON path(s) "
                    f"beyond deep.max_json_paths = {deep['max_json_paths']}",
                }
            )
        if catalog["depth_truncated"]:
            limited.append(
                {
                    "item": fields[fid]["display_path"],
                    "reason": "json_depth_budget",
                    "detail": f"nesting beyond deep.max_json_depth = {deep['max_json_depth']}",
                }
            )
    if deep_plan["json_nodes"] is not None:
        for node in deep_plan["json_nodes"]:
            if node["field_id"] not in catalogs:
                json_fields.append(
                    {
                        "field_id": node["field_id"],
                        "display_path": node["display_path"],
                        "status": "not_catalogued",
                        "reason": "not in the transient sample (sampling disabled or failed, or "
                        "beyond sampling.max_columns)",
                        "paths_listed": 0,
                        "paths_validated": 0,
                        "full_scope_method": None,
                    }
                )
    if json_unsupported and any(catalog.get("paths") for catalog in catalogs.values()):
        table["unsupported"].append(
            {"capability": "json_path_functions", "detail": json_unsupported}
        )
    for omission in agg_plan["omissions"]:
        if omission["reason"] == "deep_budget":
            limited.append(
                {
                    "item": omission["display_path"] or "table",
                    "reason": "deep_budget",
                    "detail": f"{', '.join(omission['metrics'])}: {omission['detail']}",
                }
            )
    element_distinct = None
    explode = 0
    if distinct_plan is not None and (distinct_plan["leaves"] or distinct_plan["mode"] == "off"):
        raw = distinct_plan.get("result")
        explode = 1 if distinct_plan["enabled"] else 0
        status = "measured" if raw else ("error" if distinct_plan.get("error") else "not_computed")
        stopped = "not_applicable"
        if raw:
            stopped = "element_limit" if raw["n"] >= distinct_plan["max_elements"] else "exhausted"
        elif distinct_plan.get("error"):
            stopped = "error"
        element_distinct = {
            "mode": distinct_plan["mode"],
            "status": status,
            "reason": distinct_plan.get("skip_reason")
            or distinct_plan.get("reason")
            or (f"failed: {distinct_plan['error']}" if distinct_plan.get("error") else None),
            "fields": len(distinct_plan["leaves"]),
            "method": distinct_plan["sampling_method"]
            if distinct_plan["mode"] == "sample"
            else "none",
            "max_rows": distinct_plan["max_rows"] if distinct_plan["mode"] == "sample" else None,
            "max_elements": distinct_plan["max_elements"],
            "rows_with_elements": raw["rows"] if raw else None,
            "elements_examined": raw["n"] if raw else None,
            "stopped_reason": stopped,
        }
        if distinct_plan.get("budget_limited"):
            limited.append(
                {
                    "item": "element distinct counts",
                    "reason": "deep_pass_budget"
                    if distinct_plan["mode"] == "sample"
                    else "element_budget",
                    "detail": element_distinct["reason"] or "",
                }
            )
        elif stopped == "element_limit":
            limited.append(
                {
                    "item": "element distinct counts",
                    "reason": "element_budget",
                    "detail": f"stopped at deep.max_elements = {distinct_plan['max_elements']} "
                    "elements; later elements were not examined",
                }
            )
    collections = []
    for node in deep_plan["collections"]:
        fid = node["field_id"]
        profiled = sum(1 for c in deep_plan["contexts"].values() if c["collection_field_id"] == fid)
        omitted = sum(
            1
            for item in deep_plan["omissions"]
            if item["reason"] == "nested_collection"
            and (item["display_path"] or "").startswith(node["display_path"])
        )
        collections.append(
            {
                "field_id": fid,
                "display_path": node["display_path"],
                "kind": node["type"]["kind"],
                "element_fields_profiled": profiled,
                "element_fields_omitted": omitted,
            }
        )
    notes = [
        "Element metrics are exact over the analysed scope and computed per row with "
        "higher-order functions inside the shared aggregation passes (no explode); their "
        "denominators are elements or entries, never rows.",
        "JSON path catalogues come from the transient sample; they are not a complete schema.",
    ]
    if deep_plan["mode"] == "explicit" and not deep_plan["requested"]:
        notes.append("deep.targets lists no field of this table; nothing was profiled in depth.")
    extra_aggregate = agg_plan["extra_passes"]
    table["deep"] = {
        "targets": deep_plan["mode"],
        "requested_targets": deep_plan["requested"],
        "not_eligible": list(deep_plan["not_eligible"]),
        "collections": collections,
        "json_fields": json_fields,
        "budget": {
            key: deep[key]
            for key in (
                "max_extra_passes",
                "max_explode_rows",
                "max_elements",
                "max_json_paths",
                "max_json_depth",
                "max_json_object_keys",
            )
        },
        "extra_passes": {
            "budget": deep["max_extra_passes"],
            "planned": extra_aggregate + explode,
            "aggregate": extra_aggregate,
            "element_explode": explode,
        },
        "expressions": {
            "planned": agg_plan["deep_expressions"],
            "omitted": sum(spec["cost"] for spec in dropped),
        },
        "element_distinct": element_distinct,
        "limited": limited,
        "notes": notes,
    }


# --------------------------------------------------------------------------- relationships


def _sp_table_frame(spark: Any, table: Mapping[str, Any], *, filtered: bool) -> Any:
    """Re-read a profiled table at the Delta version recorded in its profile."""
    quoted = table["identifier"]["quoted"]
    consistency = table.get("consistency") or {}
    version = consistency.get("delta_version")
    if consistency.get("mode") == "pinned_delta_version" and version is not None:
        frame = spark.sql(f"SELECT * FROM {quoted} VERSION AS OF {int(version)}")
    else:
        frame = spark.table(quoted)
    filters = table["scope"]["filters"]
    return frame.filter(filter_condition(filters)) if filtered and filters else frame


def _sp_side(table: Mapping[str, Any], scope: str) -> dict[str, Any]:
    consistency = table.get("consistency") or {}
    return {
        "table": table["table_key"],
        "scope": scope,
        "consistency_mode": consistency.get("mode", "unpinned"),
        "delta_version": consistency.get("delta_version"),
    }


def _sp_full_scope(table: Mapping[str, Any]) -> str:
    consistency = table.get("consistency") or {}
    return "full_snapshot" if consistency.get("mode") == "pinned_delta_version" else "full_table"


def _sp_run_tables(spark: Any, tables: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Index profiled tables by their name as given and, when known, by their full name."""
    index: dict[str, Any] = {}
    for table in tables:
        parts = table["identifier"]["parts"]
        index.setdefault(table_lookup_key(parts), table)
        full = _sp_full_name(spark, list(parts))
        if full is not None:
            index.setdefault(table_lookup_key(full), table)
    return index


def _sp_find_table(index: Mapping[str, Any], text: str) -> Any:
    try:
        return index.get(table_lookup_key(parse_table_identifier(text)))
    except IdentifierError:
        return None


def _sp_end_nodes(
    relationship: Mapping[str, Any], end: str, table: Mapping[str, Any]
) -> tuple[list[Any], str]:
    by_id = {
        node["field_id"]: node for node in iter_nodes((table["schema"] or {}).get("fields", []))
    }
    try:
        paths = end_segments(relationship, end)
    except IdentifierError as exc:
        return [], f"{end} columns: {exc}"
    nodes = [by_id.get(field_id(path)) for path in paths]
    problem = key_columns_problem(nodes, [str(c) for c in relationship[end]["columns"]])
    return nodes, (f"{end} key: {problem}" if problem else "")


def _sp_referential_problem(
    relationship: Mapping[str, Any], index: Mapping[str, Any], config: Mapping[str, Any]
) -> tuple[str, Any, Any, list[Any], list[Any], list[dict[str, Any]]]:
    """Return ``(reason, source, target, source nodes, target nodes, compatibility)``."""
    source = _sp_find_table(index, relationship["from"]["table"])
    target = _sp_find_table(index, relationship["to"]["table"])
    empty: list[Any] = []
    if source is None:
        return "the source table was not profiled in this run", None, None, empty, empty, []
    if target is None:
        return (
            "the target table was not profiled in this run (tables outside the run are never read)",
            source,
            None,
            empty,
            empty,
            [],
        )
    for side, table in (("source", source), ("target", target)):
        if table["status"] == "failed" or table["schema"] is None:
            return f"the {side} table could not be profiled", source, target, empty, empty, []
    from_nodes, problem = _sp_end_nodes(relationship, "from", source)
    to_nodes, other = _sp_end_nodes(relationship, "to", target)
    if problem or other:
        return problem or other, source, target, empty, empty, []
    compatibility = type_compatibility(from_nodes, to_nodes)
    incompatible = [item for item in compatibility if not item["compatible"]]
    if incompatible:
        pairs = "; ".join(
            f"{item['from_column']} ({item['from_type']}) vs {item['to_column']} "
            f"({item['to_type']})"
            for item in incompatible
        )
        return f"incompatible column types: {pairs}", source, target, [], [], compatibility
    settings = config["deep"]["referential"]
    if settings["mode"] == "sample" and config["sampling"]["method"] == "none":
        return (
            "sample mode needs a sampling method (sampling.method = none)",
            source,
            target,
            [],
            [],
            compatibility,
        )
    return "", source, target, from_nodes, to_nodes, compatibility


def validate_relationships(
    spark: Any,
    tables: Sequence[dict[str, Any]],
    config: Mapping[str, Any],
    *,
    log: Callable[[str], None] = print,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Validate the known relationships requested by ``deep.referential``.

    Returns ``(relationships, referential_validation)``. Each check is one
    Spark action, recorded as a ``referential_check`` operation of the source
    table and bounded by ``max_relationships`` per run. Both tables are read at
    the Delta versions recorded when they were profiled; the source keeps its
    analysed scope, the target is read in full.
    """
    relationships = known_relationships(tables, config)
    if config["analysis_level"] != "deep":
        return relationships, None
    settings = config["deep"]["referential"]
    sampling = config["sampling"]
    index = _sp_run_tables(spark, tables)
    planned = 0
    limited: list[dict[str, str]] = []
    for relationship in relationships:
        reason = referential_requested(relationship, config)
        if reason:
            relationship["validation_detail"] = not_validated_detail(reason)
            continue
        reason, source, target, from_nodes, to_nodes, compatibility = _sp_referential_problem(
            relationship, index, config
        )
        if not reason and planned >= settings["max_relationships"]:
            reason = f"beyond deep.referential.max_relationships = {settings['max_relationships']}"
            limited.append(
                {"item": relationship["name"], "reason": "referential_budget", "detail": reason}
            )
        if reason:
            relationship["validation_detail"] = not_validated_detail(
                reason, type_compatibility=compatibility
            )
            continue
        planned += 1
        op_id = f"op_referential_{planned}"
        sample = (
            {
                "method": sampling["method"],
                "max_rows": settings["max_sample_rows"],
                "fraction": sampling.get("random_fraction"),
                "seed": sampling.get("seed"),
            }
            if settings["mode"] == "sample"
            else None
        )
        source["operations"]["planned"].append(
            operation(
                op_id,
                "referential_check",
                f"orphan count of {relationship['name']} against {target['table_key']} (one "
                "anti join; the target is read in full; only counts are collected)",
                reads_user_data=True,
                relationship_id=relationship["relationship_id"],
                target_table=target["table_key"],
                mode=settings["mode"],
                max_sample_rows=settings["max_sample_rows"] if sample else None,
            )
        )
        start = time.perf_counter()
        from_info = _sp_side(source, "sample" if sample else source["scope"]["scope_label"])
        to_info = _sp_side(target, _sp_full_scope(target))
        try:
            raw = run_inclusion_check(
                _sp_table_frame(spark, source, filtered=True),
                [column_for(node["path"]) for node in from_nodes],
                _sp_table_frame(spark, target, filtered=False),
                [column_for(node["path"]) for node in to_nodes],
                sample=sample,
            )
        except Exception as exc:  # noqa: BLE001 - one failed check never stops the run
            record = error_record(exc, "aggregate")
            cause = record["condition"] or record["error_class"]
            source["operations"]["observed"].append(
                _sp_observed(op_id, "failed", start, detail=cause)
            )
            relationship["validation_detail"] = not_validated_detail(
                f"the check could not read the data ({cause})",
                mode=settings["mode"],
                **{"from": from_info, "to": to_info},
                type_compatibility=compatibility,
            )
            continue
        source["operations"]["observed"].append(_sp_observed(op_id, "succeeded", start, rows=1))
        detail = validation_detail(
            raw,
            mode=settings["mode"],
            sample_rows=settings["max_sample_rows"] if sample else None,
            from_info=from_info,
            to_info=to_info,
            compatibility=compatibility,
            operation_id=op_id,
        )
        relationship["validation"] = detail["status"]
        relationship["validation_detail"] = detail
        log(f"[tabledossier] relationship {relationship['name']}: {detail['status']}")
    return relationships, referential_summary(
        relationships, config, planned=planned, limited=limited
    )
