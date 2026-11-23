"""PostgreSQL connector: catalog mapping and SQL compilation (pure; never imports a driver).

Everything here turns catalog rows and planned specs into :class:`~tabledossier.connectors.
sqlcompose.Sql` values and back: neutral field types, filters, one aggregate expression per
planned metric, the sample and catalog queries, and the decoding of the few results that the
driver cannot hand over as-is. Names only enter SQL as identifiers and values only as bound
parameters. Function and type names are written schema-qualified (``pg_catalog``) and every
transaction also empties ``search_path``, so objects created in other schemas cannot replace
the built-ins.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Any

from tabledossier.connectors.sqlcompose import Sql, ident, join, param, sql

ENGINE = "postgresql"
# Types written in pg_catalog that the neutral kinds understand (anything else is "other").
_PG_KINDS = {
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "float4": "float",
    "float8": "float",
    "numeric": "decimal",
    "text": "string",
    "varchar": "string",
    "bpchar": "string",
    "name": "string",
    "uuid": "string",
    "bool": "boolean",
    "date": "date",
    "timestamptz": "timestamp",
    "timestamp": "timestamp_ntz",
    "interval": "interval",
    "bytea": "binary",
    "json": "variant",
    "jsonb": "variant",
}
# String types compared as themselves; other string kinds (uuid, enums) are compared as text.
NATIVE_TEXT_TYPES = frozenset({"text", "varchar", "bpchar", "name"})
# One SELECT target list holds at most 1664 entries in PostgreSQL.
MAX_EXPRESSIONS_PER_STATEMENT = 1600
WHITESPACE_PATTERN = r"^[ \t\n\v\f\r]+$"
_SNAPSHOT_ID = re.compile(r"^[0-9A-F]+-[0-9A-F]+-[0-9]+$")
_PG_TIMESTAMP = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d{1,6}))?"
    r"(?P<offset>[+-]\d{2}(?::?\d{2})?)?$"
)
_FILTER_CASTS = {
    "string": "pg_catalog.text",
    "integer": "pg_catalog.int8",
    "double": "pg_catalog.float8",
    "decimal": "pg_catalog.numeric",
    "boolean": "pg_catalog.bool",
    "date": "pg_catalog.date",
    "timestamp": "pg_catalog.timestamptz",
}
_KIND_CASTS = {
    "integer": "pg_catalog.int8",
    "float": "pg_catalog.float8",
    "decimal": "pg_catalog.numeric",
    "string": "pg_catalog.text",
    "boolean": "pg_catalog.bool",
    "date": "pg_catalog.date",
    "timestamp": "pg_catalog.timestamptz",
    "timestamp_ntz": "pg_catalog.timestamp",
}
_COMPARISONS = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_C = ' COLLATE pg_catalog."C"'


class UnsupportedOperation(ValueError):
    """A planned operation that the PostgreSQL connector does not compile."""


class FilterValueError(ValueError):
    """A filter value that cannot be used with its type (reported as a configuration error)."""


# --------------------------------------------------------------------------- types


@dataclass(frozen=True)
class ColumnInfo:
    """How the compiler addresses one top-level column."""

    name: str
    kind: str
    physical_type: str
    native_text: bool = True
    composite: bool = False
    scale: int | None = None
    readable: bool = True


def _numeric_typmod(typmod: int | None) -> tuple[int | None, int | None]:
    """Return ``(precision, scale)`` of a numeric type modifier (scales may be < 0 on 15+)."""
    if typmod is None or typmod < 4:
        return None, None
    value = typmod - 4
    precision = (value >> 16) & 0xFFFF
    scale = ((value & 0x7FF) ^ 1024) - 1024
    return precision, scale


def resolve_type(
    oid: int, typmod: int | None, types: Mapping[int, Mapping[str, Any]]
) -> dict[str, Any]:
    """Follow domains to their base type; return its name, category and modifier.

    ``types`` maps ``pg_type`` oids to rows with ``typname``, ``typtype``, ``typcategory``,
    ``typbasetype``, ``typtypmod``, ``typelem`` and ``builtin`` (namespace is ``pg_catalog``).
    """
    seen: set[int] = set()
    current = types.get(oid)
    effective = typmod
    while current is not None and current.get("typtype") == "d" and oid not in seen:
        seen.add(oid)
        if current.get("typtypmod") is not None and int(current["typtypmod"]) >= 0:
            effective = int(current["typtypmod"])
        oid = int(current["typbasetype"])
        current = types.get(oid)
    if current is None:
        return {"oid": oid, "typname": None, "typtype": None, "category": None, "typmod": effective}
    return {
        "oid": oid,
        "typname": current.get("typname"),
        "typtype": current.get("typtype"),
        "category": current.get("typcategory"),
        "builtin": bool(current.get("builtin")),
        "elem": int(current.get("typelem") or 0),
        "typmod": effective,
    }


def _scalar_kind(base: Mapping[str, Any]) -> str:
    if base.get("typtype") == "e":
        return "string"
    if base.get("builtin") and base.get("typtype") == "b":
        return _PG_KINDS.get(str(base.get("typname")), "other")
    return "other"


def map_column(
    row: Mapping[str, Any], types: Mapping[int, Mapping[str, Any]]
) -> tuple[dict[str, Any], ColumnInfo]:
    """Map one ``pg_attribute`` row to a neutral field descriptor and its compiler info.

    ``row`` has ``name``, ``not_null``, ``comment``, ``type_oid``, ``typmod``,
    ``physical_type`` (``format_type``), ``element_physical_type`` and ``readable``.
    """
    base = resolve_type(int(row["type_oid"]), row.get("typmod"), types)
    physical = str(row["physical_type"])
    composite = base.get("typtype") == "c"
    is_array = bool(base.get("category") == "A" and base.get("elem") and base.get("typtype") == "b")
    ntype: dict[str, Any]
    if is_array:
        element = resolve_type(int(base["elem"]), None, types)
        element_kind = _scalar_kind(element)
        if element.get("typtype") == "c" or element.get("category") == "A":
            ntype = {"kind": "other", "physical_type": physical}
        else:
            ntype = {
                "kind": "array",
                "physical_type": physical,
                "element_type": {
                    "kind": element_kind,
                    "physical_type": str(row.get("element_physical_type") or element["typname"]),
                },
                "contains_null": True,
            }
    else:
        kind = _scalar_kind(base)
        ntype = {"kind": kind, "physical_type": physical}
        if kind == "decimal":
            precision, scale = _numeric_typmod(base.get("typmod"))
            if precision is not None and scale is not None:
                ntype["precision"] = precision
                ntype["scale"] = scale
    field = {
        "name": str(row["name"]),
        "type": ntype,
        "nullable": not bool(row.get("not_null")),
        "comment": row.get("comment"),
    }
    info = ColumnInfo(
        name=str(row["name"]),
        kind=ntype["kind"],
        physical_type=physical,
        native_text=ntype["kind"] != "string"
        or (base.get("typname") in NATIVE_TEXT_TYPES and bool(base.get("builtin"))),
        composite=composite,
        scale=ntype.get("scale"),
        readable=bool(row.get("readable", True)),
    )
    return field, info


# --------------------------------------------------------------------------- expressions


def column(info: ColumnInfo) -> Sql:
    """Return the column itself."""
    return ident(info.name)


def text_value(info: ColumnInfo) -> Sql:
    """Return the column as text (string kinds that are not text types are cast)."""
    if info.native_text and info.kind == "string":
        return column(info)
    return sql("CAST(", column(info), " AS pg_catalog.text)")


def collated_text(info: ColumnInfo) -> Sql:
    """Return the column as text compared byte by byte, as Spark compares strings."""
    return sql(text_value(info), _C)


def null_test(info: ColumnInfo) -> Sql:
    """Test for SQL NULL (a composite of NULL fields is not NULL)."""
    if info.composite:
        return sql("pg_catalog.num_nulls(", column(info), ") = 1")
    return sql(column(info), " IS NULL")


def finite_test(info: ColumnInfo) -> Sql:
    """Test for non-null values that are neither NaN nor infinite (floating point)."""
    return sql(
        "(",
        column(info),
        " IS NOT NULL AND ",
        column(info),
        " NOT IN ('NaN'::pg_catalog.float8, 'Infinity'::pg_catalog.float8, "
        "'-Infinity'::pg_catalog.float8))",
    )


def present_test(info: ColumnInfo) -> Sql:
    """Test for the values numeric metrics use: finite for floats, non-null otherwise."""
    if info.kind == "float":
        return finite_test(info)
    return sql(column(info), " IS NOT NULL")


def _sign_test(info: ColumnInfo) -> Sql:
    if info.kind == "float":
        return finite_test(info)
    if info.kind == "decimal":
        return sql(
            "(", column(info), " IS NOT NULL AND ", column(info), " <> 'NaN'::pg_catalog.numeric)"
        )
    return sql(column(info), " IS NOT NULL")


def count_where(condition: Sql) -> Sql:
    """``count(*)`` of the rows where ``condition`` holds (0, never NULL, on empty input)."""
    return sql("pg_catalog.count(*) FILTER (WHERE ", condition, ")")


def _aggregate(function: str, value: Sql, condition: Sql | None = None) -> Sql:
    out = sql(f"pg_catalog.{function}(", value, ")")
    return sql(out, " FILTER (WHERE ", condition, ")") if condition is not None else out


def _cast(value: Sql, target: str) -> Sql:
    return sql("CAST(", value, f" AS {target})")


def _length(info: ColumnInfo) -> Sql:
    if info.kind == "binary":
        return sql("pg_catalog.octet_length(", column(info), ")")
    return sql("pg_catalog.char_length(", text_value(info), ")")


def _percentiles(value: Sql, probabilities: Sequence[float], condition: Sql | None) -> Sql:
    out = sql(
        "pg_catalog.percentile_disc(CAST(",
        param([float(p) for p in probabilities]),
        " AS pg_catalog.float8[])) WITHIN GROUP (ORDER BY ",
        value,
        ")",
    )
    return sql(out, " FILTER (WHERE ", condition, ")") if condition is not None else out


def _distinct_value(info: ColumnInfo) -> Sql:
    return collated_text(info) if info.kind == "string" else column(info)


def _equal_extremes(info: ColumnInfo) -> Sql:
    if info.kind == "boolean":
        value = column(info)
        return sql("pg_catalog.bool_and(", value, ") = pg_catalog.bool_or(", value, ")")
    if info.kind == "string":
        value = collated_text(info)
    elif info.kind == "binary":
        value = sql("pg_catalog.encode(", column(info), ", 'hex')")
    else:
        value = column(info)
    return sql("pg_catalog.min(", value, ") = pg_catalog.max(", value, ")")


def _extreme(function: str, info: ColumnInfo) -> Sql:
    if info.kind in ("date", "timestamp", "timestamp_ntz"):
        # Text output, decoded per alias: infinite values are excluded and dates outside
        # Python's range fail one alias instead of the whole pass.
        finite = sql("pg_catalog.isfinite(", column(info), ")")
        return _cast(_aggregate(function, column(info), finite), "pg_catalog.text")
    if info.kind == "float":
        return _aggregate(function, column(info), finite_test(info))
    return _aggregate(function, column(info))


def _mean(info: ColumnInfo) -> Sql:
    if info.kind == "decimal" and info.scale is not None:
        # Exact sum and count; the mean is computed in Python and rounded HALF_UP to scale + 4,
        # as Spark types the average of decimal(p, s).
        return sql(
            "ARRAY[pg_catalog.sum(",
            column(info),
            "), CAST(pg_catalog.count(",
            column(info),
            ") AS pg_catalog.numeric)]",
        )
    if info.kind == "decimal":
        return _aggregate("avg", column(info))
    condition = finite_test(info) if info.kind == "float" else None
    return _cast(_aggregate("avg", column(info), condition), "pg_catalog.float8")


def compile_metric(spec: Mapping[str, Any], info: ColumnInfo | None, reference_time: str) -> Sql:
    """Return the aggregate expression of one planned spec (one scalar per spec)."""
    op = spec["op"]
    if op == "count_all":
        return sql("pg_catalog.count(*)")
    if info is None:
        raise UnsupportedOperation(f"{op} needs a column")
    value = column(info)
    if op == "count_null":
        return count_where(null_test(info))
    if op == "approx_distinct":
        return sql("pg_catalog.count(DISTINCT ", _distinct_value(info), ")")
    if op == "all_values_equal":
        return _equal_extremes(info)
    if op in ("count_nan", "count_pos_inf", "count_neg_inf"):
        literal = {"count_nan": "NaN", "count_pos_inf": "Infinity", "count_neg_inf": "-Infinity"}
        return count_where(sql(value, f" = '{literal[op]}'::pg_catalog.float8"))
    if op == "count_finite":
        return count_where(finite_test(info))
    if op in ("min_value", "max_value"):
        return _extreme("min" if op == "min_value" else "max", info)
    if op == "mean_value":
        return _mean(info)
    if op == "stddev_value":
        condition = finite_test(info) if info.kind == "float" else None
        return _cast(_aggregate("stddev_samp", value, condition), "pg_catalog.float8")
    if op in ("count_zero", "count_negative", "count_positive"):
        operator = {"count_zero": " = 0", "count_negative": " < 0", "count_positive": " > 0"}
        return count_where(sql(_sign_test(info), " AND ", value, operator[op]))
    if op == "quantiles":
        # Ordered by the canonical type: percentile_disc returns an array of the ordering type,
        # and arrays of domains would come back as unparsed text.
        condition = finite_test(info) if info.kind == "float" else None
        ordered = _cast(value, _KIND_CASTS.get(info.kind, "pg_catalog.numeric"))
        return _percentiles(ordered, spec["params"]["probabilities"], condition)
    if op == "count_empty_string":
        return count_where(sql(text_value(info), " = ''"))
    if op == "count_whitespace_only":
        text = text_value(info)
        return count_where(sql(text, " <> '' AND ", text, " ~ ", param(WHITESPACE_PATTERN)))
    if op in ("min_length", "max_length"):
        return _aggregate("min" if op == "min_length" else "max", _length(info))
    if op == "mean_length":
        return _cast(_aggregate("avg", _length(info)), "pg_catalog.float8")
    if op == "length_quantiles":
        return _percentiles(_length(info), spec["params"]["probabilities"], None)
    if op == "count_true":
        return count_where(sql(value, " IS TRUE"))
    if op == "count_false":
        return count_where(sql(value, " IS FALSE"))
    if op == "count_after_reference":
        if info.kind == "date":
            return count_where(
                sql(value, " > CAST(", param(reference_time[:10]), " AS pg_catalog.date)")
            )
        return count_where(
            sql(value, " > CAST(", param(reference_time), " AS pg_catalog.timestamptz)")
        )
    if info.kind == "array":
        size = sql("pg_catalog.cardinality(", value, ")")
        if op == "count_empty_collection":
            return count_where(sql(value, " IS NOT NULL AND ", size, " = 0"))
        if op in ("min_size", "max_size"):
            return _aggregate("min" if op == "min_size" else "max", size)
        if op == "mean_size":
            return _cast(_aggregate("avg", size), "pg_catalog.float8")
        if op == "sum_size":
            return _cast(_aggregate("sum", size), "pg_catalog.int8")
        if op == "count_null_elements":
            nulls = sql(
                "(SELECT pg_catalog.count(*) FROM pg_catalog.unnest(",
                value,
                ") AS td_e(v) WHERE td_e.v IS NULL)",
            )
            return _cast(_aggregate("sum", nulls), "pg_catalog.int8")
    if op == "count_json_invalid":
        return count_where(
            sql(
                value,
                " IS NOT NULL AND NOT pg_catalog.pg_input_is_valid(",
                text_value(info),
                ", 'json')",
            )
        )
    raise UnsupportedOperation(f"{op} is not supported by the PostgreSQL connector")


def decode_kind(spec: Mapping[str, Any], info: ColumnInfo | None) -> str | None:
    """Return how the raw value of ``spec`` must be decoded (None: use it as returned)."""
    if info is None:
        return None
    if spec["op"] in ("min_value", "max_value") and info.kind in (
        "date",
        "timestamp",
        "timestamp_ntz",
    ):
        return info.kind
    if spec["op"] == "mean_value" and info.kind == "decimal" and info.scale is not None:
        return f"decimal_mean:{info.scale}"
    return None


def decode_value(how: str | None, raw: Any) -> Any:
    """Decode one raw result; raises ``ValueError`` for values outside the supported range."""
    if raw is None or how is None:
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = bytes(raw).decode("utf-8", errors="replace")
    if how == "date":
        return date.fromisoformat(str(raw))
    if how in ("timestamp", "timestamp_ntz"):
        return format_timestamp(str(raw), with_offset=how == "timestamp")
    if how.startswith("decimal_mean:"):
        total, count = raw
        if not count:
            return None
        return decimal_mean(Decimal(total), int(count), int(how.split(":", 1)[1]))
    raise ValueError(f"unknown decoder {how}")


def format_timestamp(text: str, *, with_offset: bool) -> str:
    """Turn PostgreSQL ISO output into Spark's ``yyyy-MM-dd'T'HH:mm:ss.SSSSSS[XXX]`` text.

    The session time zone is UTC, so offsets are ``+00`` and become ``Z``.
    """
    match = _PG_TIMESTAMP.match(text.strip())
    if match is None:
        raise ValueError("timestamp outside the representable range")
    fraction = (match.group("frac") or "").ljust(6, "0")
    out = f"{match.group('date')}T{match.group('time')}.{fraction}"
    if not with_offset:
        return out
    offset = match.group("offset") or "+00"
    if offset.replace(":", "") in ("+00", "+0000", "-00", "-0000"):
        return out + "Z"
    sign, rest = offset[0], offset[1:].replace(":", "")
    return f"{out}{sign}{rest[:2]}:{rest[2:4] or '00'}"


def decimal_mean(total: Decimal, count: int, scale: int) -> Decimal:
    """Return ``total / count`` rounded HALF_UP to ``scale + 4`` digits (Spark's average)."""
    if not total.is_finite():
        return total
    with localcontext() as context:
        context.prec = max(60, len(total.as_tuple().digits) + scale + 10)
        mean = total / Decimal(count)
        try:
            return mean.quantize(Decimal(1).scaleb(-(scale + 4)), rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return mean


def labels(spec: Mapping[str, Any], info: ColumnInfo | None) -> dict[str, Any] | None:
    """Return how PostgreSQL computes ``spec`` when METRIC_INFO's Spark wording would be wrong."""
    op = spec["op"]
    if op == "approx_distinct":
        return {
            "accuracy": "exact",
            "method": "count(DISTINCT field) over non-null values (exact)",
            "details": {"relative_standard_deviation": None},
        }
    if op == "quantiles":
        return {
            "accuracy": "exact",
            "method": "percentile_disc over non-null (finite) values (exact; returns values from "
            "the data)",
            "details": {"accuracy_parameter": None},
        }
    if op == "length_quantiles":
        return {
            "accuracy": "exact",
            "method": "percentile_disc of length over non-null values (exact)",
            "details": {"accuracy_parameter": None},
        }
    if op == "count_whitespace_only":
        return {
            "method": "non-empty strings made only of ASCII whitespace "
            "(regex ^[ \\t\\n\\v\\f\\r]+$)"
        }
    if op == "count_json_invalid":
        return {"method": "non-null strings that pg_input_is_valid(value, 'json') rejects"}
    if op == "stddev_value":
        return {"method": "sample standard deviation (stddev_samp), returned as double precision"}
    if (
        info is not None
        and op in ("min_value", "max_value")
        and info.kind
        in (
            "date",
            "timestamp",
            "timestamp_ntz",
        )
    ):
        word = "minimum" if op == "min_value" else "maximum"
        return {"method": f"{word} over finite non-null values (±infinity excluded)"}
    if info is not None and op == "mean_value" and info.kind == "decimal":
        if info.scale is not None:
            return {
                "method": "average over non-null values: exact sum / count rounded half up to "
                "the column scale + 4"
            }
        return {"method": "average over non-null values (PostgreSQL numeric avg)"}
    signs = {"count_zero": "equal to", "count_negative": "below", "count_positive": "above"}
    if info is not None and info.kind == "decimal" and op in signs:
        return {"method": f"values {signs[op]} zero (numeric NaN excluded)"}
    return None


# --------------------------------------------------------------------------- filters


def _filter_text(value: Any, target: str) -> str:
    """Normalise one filter value to the text PostgreSQL casts to ``target``."""
    if isinstance(value, bool):
        if target not in ("pg_catalog.bool", "pg_catalog.text"):
            raise FilterValueError("a boolean value cannot filter this column")
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if target == "pg_catalog.int8":
            if not value.is_integer():
                raise FilterValueError("a non-integral number cannot filter an integer value")
            return str(int(value))
        return repr(value)
    return str(value)


def filter_operands(item: Mapping[str, Any], info: ColumnInfo) -> tuple[str, Sql]:
    """Return ``(cast target of the values, compared column expression)`` for one filter.

    Strings (and kinds without a cast, compared as text) use byte order, as Spark does; a
    ``value_type`` that does not fit the column is left to the server, which rejects it
    while the filters are planned at resolve time.
    """
    value_type = item.get("value_type")
    as_text = (info.kind == "string" and value_type in (None, "string")) or (
        value_type is None and info.kind not in _KIND_CASTS
    )
    if as_text:
        return "pg_catalog.text", collated_text(info)
    target = _FILTER_CASTS[value_type] if value_type else _KIND_CASTS[info.kind]
    return target, column(info)


def compile_filter(item: Mapping[str, Any], info: ColumnInfo) -> Sql:
    """Compile one structured filter; values are bound as text and cast server-side."""
    operator = item["operator"]
    if operator == "is_null":
        return null_test(info)
    if operator == "is_not_null":
        return sql("NOT (", null_test(info), ")")
    if operator == "like":
        return sql(collated_text(info), " LIKE ", param(str(item["value"])))
    target, left = filter_operands(item, info)
    if operator in _COMPARISONS:
        right = sql("CAST(", param(_filter_text(item["value"], target)), f" AS {target})")
        return sql(left, f" {_COMPARISONS[operator]} ", right)
    if operator == "between":
        low, high = item["value"]
        return sql(
            left,
            " BETWEEN CAST(",
            param(_filter_text(low, target)),
            f" AS {target}) AND CAST(",
            param(_filter_text(high, target)),
            f" AS {target})",
        )
    if operator in ("in", "not_in"):
        values = [_filter_text(value, target) for value in item["value"]]
        members = sql("CAST(CAST(", param(values), f" AS pg_catalog.text[]) AS {target}[])")
        test = sql(left, " = ANY(", members, ")")
        return sql("NOT (", test, ")") if operator == "not_in" else test
    raise FilterValueError(f"unknown filter operator {operator!r}")


def where_clause(filters: Sequence[Mapping[str, Any]], columns: Mapping[str, ColumnInfo]) -> Sql:
    """Return `` WHERE ...`` (AND of the filters), or an empty fragment."""
    if not filters:
        return sql()
    parts = []
    for item in filters:
        name = item["column"] if isinstance(item["column"], str) else None
        if name is None or name not in columns:
            raise FilterValueError("a filter references a column that is not addressable")
        parts.append(sql("(", compile_filter(item, columns[name]), ")"))
    return sql(" WHERE ", join(" AND ", parts))


# --------------------------------------------------------------------------- statements


def relation(schema: str, name: str) -> Sql:
    """Return the schema-qualified relation."""
    return ident(schema, name)


def aggregate_statement(rel: Sql, expressions: Sequence[tuple[str, Sql]], where: Sql) -> Sql:
    """One shared aggregation: ``SELECT <expr> AS <alias>, ... FROM rel [WHERE ...]``."""
    items = [sql(expression, " AS ", ident(alias)) for alias, expression in expressions]
    return sql("SELECT ", join(", ", items), " FROM ", rel, where)


def probe_statement(rel: Sql, expression: Sql, where: Sql) -> Sql:
    """Plan one expression without reading data."""
    return sql("EXPLAIN SELECT ", expression, " FROM ", rel, where)


def filter_probe_statement(rel: Sql, where: Sql) -> Sql:
    """Plan the filters alone (resolve-time check, reads no data)."""
    return sql("EXPLAIN SELECT 1 FROM ", rel, where)


def sample_statement(
    rel: Sql,
    infos: Sequence[ColumnInfo],
    where: Sql,
    *,
    max_value_chars: int,
    max_rows: int,
    method: str,
    tablesample: bool,
    random_fraction: float | None,
    seed: int | None,
) -> Sql:
    """Projected, bounded sample of string fields; values cut server-side."""
    items: list[Sql] = []
    for index, info in enumerate(infos):
        text = text_value(info)
        items.append(
            sql(
                "pg_catalog.substr(",
                text,
                ", 1, ",
                param(max_value_chars),
                ") AS ",
                ident(f"s{index}"),
            )
        )
        items.append(
            sql(
                "pg_catalog.char_length(",
                text,
                ") > ",
                param(max_value_chars),
                " AS ",
                ident(f"t{index}"),
            )
        )
    source = sql(rel)
    condition = where
    if method == "random" and random_fraction:
        if tablesample:
            source = sql(
                rel,
                " TABLESAMPLE BERNOULLI (",
                param(float(random_fraction) * 100.0),
                ") REPEATABLE (",
                param(float(seed if seed is not None else 0)),
                ")",
            )
        else:
            test = sql("pg_catalog.random() < ", param(float(random_fraction)))
            condition = sql(where, " AND ", test) if where.parts else sql(" WHERE ", test)
    return sql(
        "SELECT ", join(", ", items), " FROM ", source, condition, " LIMIT ", param(max_rows)
    )


def snapshot_literal(snapshot_id: str) -> str:
    """Validate a snapshot id produced by ``pg_export_snapshot()`` (used as a literal)."""
    if not _SNAPSHOT_ID.match(snapshot_id):
        raise ValueError("unexpected snapshot identifier")
    return f"'{snapshot_id}'"


# --------------------------------------------------------------------------- catalog


def holder_statement() -> Sql:
    """First statement of the snapshot holder: export the snapshot and read the server facts.

    The holder must stay open for the whole run, so it disables its own idle-in-transaction
    timeout. ``current_database()`` is read only to check 3-part names; it is never recorded.
    """
    return sql(
        "SELECT pg_catalog.pg_export_snapshot() AS snapshot_id, "
        "pg_catalog.set_config('idle_in_transaction_session_timeout', '0', true) AS idle, "
        "pg_catalog.current_setting('server_version_num') AS server_version_num, "
        "pg_catalog.current_setting('server_encoding') AS server_encoding, "
        "pg_catalog.current_schemas(false) AS search_schemas, "
        "pg_catalog.current_database() AS database_name, "
        "pg_catalog.transaction_timestamp() AS started_at"
    )


def holder_timeout_statement() -> Sql:
    """PostgreSQL 17+: the holder also disables its own transaction_timeout."""
    return sql("SELECT pg_catalog.set_config('transaction_timeout', '0', true)")


def worker_settings_statement(*, statement_timeout: str, lock_timeout: str, work_mem: str) -> Sql:
    """Transaction-local settings of every worker transaction (after SET TRANSACTION SNAPSHOT).

    An empty ``search_path`` leaves only ``pg_catalog`` for functions, operators, types and
    collations, so objects created in other schemas cannot shadow the built-ins.
    """
    return sql(
        "SELECT pg_catalog.set_config('search_path', '', true), "
        "pg_catalog.set_config('statement_timeout', ",
        param(statement_timeout),
        ", true), pg_catalog.set_config('lock_timeout', ",
        param(lock_timeout),
        ", true), pg_catalog.set_config('work_mem', ",
        param(work_mem),
        ", true), pg_catalog.set_config('TimeZone', 'UTC', true), "
        "pg_catalog.set_config('DateStyle', 'ISO, YMD', true), "
        "pg_catalog.set_config('synchronize_seqscans', 'off', true)",
    )


def relation_lookup_statement(table: str, schemas: Sequence[str]) -> Sql:
    """Candidate relations named ``table`` (case-insensitively) in one of ``schemas``."""
    return sql(
        "SELECT c.oid AS oid, n.nspname AS schema_name, c.relname AS table_name, "
        "c.relkind AS relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
        "ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') "
        "AND pg_catalog.lower(c.relname) = pg_catalog.lower(",
        param(table),
        ") AND pg_catalog.lower(n.nspname) = ANY(CAST(",
        param([schema.lower() for schema in schemas]),
        " AS pg_catalog.text[]))",
    )


def relation_detail_statement(oid: int) -> Sql:
    """Comment, statistics, size, privileges and row-level security of one relation."""
    return sql(
        "SELECT c.relkind AS relkind, c.reltuples AS reltuples, "
        "pg_catalog.obj_description(c.oid, 'pg_class') AS comment, "
        "pg_catalog.has_any_column_privilege(c.oid, 'SELECT') AS can_select, "
        "c.relrowsecurity AS rls_enabled, c.relforcerowsecurity AS rls_forced, "
        "pg_catalog.row_security_active(c.oid) AS rls_active, "
        "CASE WHEN c.relkind IN ('r', 'm') THEN pg_catalog.pg_total_relation_size(c.oid) "
        "WHEN c.relkind = 'p' THEN (SELECT CAST(pg_catalog.sum("
        "pg_catalog.pg_total_relation_size(t.relid)) AS pg_catalog.int8) "
        "FROM pg_catalog.pg_partition_tree(c.oid) t) END AS size_bytes "
        "FROM pg_catalog.pg_class c WHERE c.oid = CAST(",
        param(oid),
        " AS pg_catalog.oid)",
    )


def columns_statement(oid: int) -> Sql:
    """Columns of one relation in attribute order."""
    return sql(
        "SELECT a.attnum AS attnum, a.attname AS name, a.attnotnull AS not_null, "
        "a.atttypid AS type_oid, a.atttypmod AS typmod, "
        "pg_catalog.format_type(a.atttypid, a.atttypmod) AS physical_type, "
        "pg_catalog.col_description(a.attrelid, a.attnum) AS comment, "
        "pg_catalog.has_column_privilege(a.attrelid, a.attnum, 'SELECT') AS readable, "
        "CASE WHEN t.typcategory = 'A' AND t.typelem <> 0 "
        "THEN pg_catalog.format_type(t.typelem, NULL) END AS element_physical_type "
        "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
        "WHERE a.attrelid = CAST(",
        param(oid),
        " AS pg_catalog.oid) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
    )


def types_statement(oids: Sequence[int]) -> Sql:
    """``pg_type`` rows needed to resolve domains and array elements."""
    return sql(
        "SELECT t.oid AS oid, t.typname AS typname, t.typtype AS typtype, "
        "t.typcategory AS typcategory, t.typbasetype AS typbasetype, t.typtypmod AS typtypmod, "
        "t.typelem AS typelem, t.typnamespace = CAST('pg_catalog' AS pg_catalog.regnamespace) "
        "AS builtin FROM pg_catalog.pg_type t WHERE t.oid = ANY(CAST(",
        param(sorted({int(oid) for oid in oids})),
        " AS pg_catalog.oid[]))",
    )


def partition_key_statement(oid: int) -> Sql:
    """Partition key columns of a partitioned table (attnum 0 marks an expression)."""
    return sql(
        "SELECT k.ord AS ord, k.attnum AS attnum, a.attname AS name "
        "FROM pg_catalog.pg_partitioned_table pt, "
        "pg_catalog.unnest(CAST(pt.partattrs AS pg_catalog.int2[])) "
        "WITH ORDINALITY AS k(attnum, ord) "
        "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = pt.partrelid AND a.attnum = k.attnum "
        "WHERE pt.partrelid = CAST(",
        param(oid),
        " AS pg_catalog.oid) ORDER BY k.ord",
    )


def constraints_statement(oid: int, *, server_version_num: int) -> Sql:
    """PRIMARY KEY, UNIQUE, FOREIGN KEY and CHECK constraints of one relation, one row per column.

    Read from ``pg_constraint`` (visible to every role; ``information_schema`` hides the
    constraints of tables the role may only read). Internal copies of a foreign key that
    PostgreSQL adds for each partition of a referenced partitioned table are left out.
    """
    enforced = "con.conenforced" if server_version_num >= 180000 else "true"
    return sql(
        "SELECT con.oid AS constraint_oid, con.conname AS name, con.contype AS contype, "
        f"con.convalidated AS validated, {enforced} AS enforced, "
        "k.ord AS ord, a.attname AS column_name, rn.nspname AS ref_schema, "
        "rc.relname AS ref_table, ra.attname AS ref_column, "
        "pg_catalog.pg_get_constraintdef(con.oid) AS definition "
        "FROM pg_catalog.pg_constraint con "
        "LEFT JOIN LATERAL pg_catalog.unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true "
        "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum "
        "LEFT JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid "
        "LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace "
        "LEFT JOIN pg_catalog.pg_attribute ra ON ra.attrelid = con.confrelid "
        "AND ra.attnum = con.confkey[k.ord] "
        "WHERE con.conrelid = CAST(",
        param(oid),
        " AS pg_catalog.oid) AND con.contype IN ('p', 'u', 'f', 'c') AND NOT EXISTS ("
        "SELECT 1 FROM pg_catalog.pg_constraint parent WHERE parent.oid = con.conparentid "
        "AND parent.conrelid = con.conrelid) ORDER BY con.conname, k.ord",
    )
