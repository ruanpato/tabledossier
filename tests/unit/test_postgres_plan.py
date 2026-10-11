"""PostgreSQL compiler: composition, type mapping, filters, metrics, decoders (no driver, no server)."""

import re
from datetime import date
from decimal import Decimal

import pytest

from tabledossier.config import default_config
from tabledossier.connectors import postgres_plan as pp
from tabledossier.connectors.sqlcompose import (
    Ident,
    Sql,
    debug_render,
    ident,
    identifiers,
    join,
    param,
    parameters,
    sql,
    text_parts,
)
from tabledossier.planning import build_schema_tree, plan_aggregates, select_profile_fields

HOSTILE = [
    'x"; DROP TABLE t; --',
    "a'b",
    "100%",
    "%s",
    "%(name)s",
    "%%",
    "naïve ✓",
    "line\nbreak",
]
FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|ANALYZE|"
    r"LOCK|CALL|DO|REFRESH|CLUSTER|REINDEX|NOTIFY|LISTEN|PREPARE|EXECUTE|DISCARD|RESET)\b",
    re.IGNORECASE,
)


def info(name="c", kind="string", physical="text", **extra):
    return pp.ColumnInfo(name=name, kind=kind, physical_type=physical, **extra)


# --------------------------------------------------------------------------- composition


def test_composition_keeps_text_identifiers_and_parameters_apart():
    query = sql("SELECT ", ident("s", 'we"ird'), " FROM t WHERE x = ", param("v'1"), " AND 1 = 1")
    text, values = debug_render(query)
    assert text == 'SELECT "s"."we""ird" FROM t WHERE x = %s AND 1 = 1'
    assert values == ["v'1"]
    assert identifiers(query) == [("s", 'we"ird')]
    assert parameters(query) == ["v'1"]
    assert join(", ", [ident("a"), ident("b")]) == Sql((Ident(("a",)), ", ", Ident(("b",))))


@pytest.mark.parametrize("names", [(), ("",), ("a\x00b",)])
def test_identifiers_reject_empty_names_and_nul(names):
    with pytest.raises(ValueError):
        Ident(names)


def test_composition_rejects_foreign_objects():
    with pytest.raises(TypeError):
        sql("SELECT ", 1)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- types


def _types():
    def row(oid, name, typtype="b", category="N", base=0, typmod=-1, elem=0, builtin=True):
        return {
            "oid": oid,
            "typname": name,
            "typtype": typtype,
            "typcategory": category,
            "typbasetype": base,
            "typtypmod": typmod,
            "typelem": elem,
            "builtin": builtin,
        }

    return {
        23: row(23, "int4"),
        701: row(701, "float8"),
        1700: row(1700, "numeric"),
        25: row(25, "text", category="S"),
        1043: row(1043, "varchar", category="S"),
        2950: row(2950, "uuid", category="U"),
        1184: row(1184, "timestamptz", category="D"),
        1114: row(1114, "timestamp", category="D"),
        3802: row(3802, "jsonb", category="U"),
        17: row(17, "bytea", category="U"),
        1007: row(1007, "_int4", category="A", elem=23),
        90001: row(90001, "mood", typtype="e", category="E", builtin=False),
        90002: row(
            90002, "money_d", typtype="d", category="N", base=1700, typmod=655366, builtin=False
        ),
        90003: row(90003, "pair", typtype="c", category="C", builtin=False),
        90004: row(90004, "_pair", category="A", elem=90003, builtin=False),
        90005: row(90005, "int4", builtin=False),  # a user type that shadows a built-in name
    }


@pytest.mark.parametrize(
    ("oid", "typmod", "kind", "native", "composite"),
    [
        (23, -1, "integer", True, False),
        (701, -1, "float", True, False),
        (1700, -1, "decimal", True, False),
        (25, -1, "string", True, False),
        (1043, 14, "string", True, False),
        (2950, -1, "string", False, False),
        (1184, -1, "timestamp", True, False),
        (1114, -1, "timestamp_ntz", True, False),
        (3802, -1, "variant", True, False),
        (17, -1, "binary", True, False),
        (1007, -1, "array", True, False),
        (90001, -1, "string", False, False),
        (90002, -1, "decimal", True, False),
        (90003, -1, "other", True, True),
        (90004, -1, "other", True, False),
        (90005, -1, "other", True, False),
    ],
)
def test_type_mapping(oid, typmod, kind, native, composite):
    row = {
        "name": "c",
        "not_null": True,
        "comment": "a comment",
        "type_oid": oid,
        "typmod": typmod,
        "physical_type": "physical",
        "element_physical_type": "integer",
        "readable": True,
    }
    field, column = pp.map_column(row, _types())
    assert field["type"]["kind"] == kind
    assert field["nullable"] is False and field["comment"] == "a comment"
    assert column.native_text is native and column.composite is composite
    if kind == "array":
        assert field["type"]["element_type"] == {"kind": "integer", "physical_type": "integer"}
        assert field["type"]["contains_null"] is True


def test_numeric_precision_and_scale_follow_domains_and_negative_scales():
    row = {"name": "m", "type_oid": 90002, "typmod": -1, "physical_type": "s.money_d"}
    field, column = pp.map_column(row, _types())
    assert (field["type"]["precision"], field["type"]["scale"]) == (10, 2)
    assert column.scale == 2
    assert pp._numeric_typmod(-1) == (None, None)
    assert pp._numeric_typmod(((5 << 16) | (-2 & 0x7FF)) + 4) == (5, -2)


# --------------------------------------------------------------------------- filters


def test_filters_bind_values_as_text_and_cast_server_side():
    integer = info("i", "integer", "integer")
    text, values = debug_render(
        pp.compile_filter({"column": "i", "operator": "between", "value": [0, 2.0]}, integer)
    )
    assert text == '"i" BETWEEN CAST(%s AS pg_catalog.int8) AND CAST(%s AS pg_catalog.int8)'
    assert values == ["0", "2"]
    with pytest.raises(pp.FilterValueError):
        pp.compile_filter({"column": "i", "operator": "eq", "value": 1.5}, integer)
    floats = info("f", "float", "double precision")
    text, values = debug_render(
        pp.compile_filter({"column": "f", "operator": "in", "value": [1, 2.5]}, floats)
    )
    assert "CAST(CAST(%s AS pg_catalog.text[]) AS pg_catalog.float8[])" in text
    assert values == [["1", "2.5"]]


def test_string_filters_compare_bytes_and_non_text_strings_as_text():
    uuid = info("u", "string", "uuid", native_text=False)
    text, values = debug_render(
        pp.compile_filter({"column": "u", "operator": "not_in", "value": ["a", "b"]}, uuid)
    )
    assert text == (
        'NOT (CAST("u" AS pg_catalog.text) COLLATE pg_catalog."C" = '
        "ANY(CAST(CAST(%s AS pg_catalog.text[]) AS pg_catalog.text[])))"
    )
    assert values == [["a", "b"]]
    text, _ = debug_render(
        pp.compile_filter({"column": "c", "operator": "lt", "value": "B"}, info())
    )
    assert text == '"c" COLLATE pg_catalog."C" < CAST(%s AS pg_catalog.text)'


def test_timestamp_filters_and_null_tests():
    ts = info("ts", "timestamp", "timestamp with time zone")
    text, values = debug_render(
        pp.compile_filter({"column": "ts", "operator": "ge", "value": "2025-01-01T00:00:00Z"}, ts)
    )
    assert text == '"ts" >= CAST(%s AS pg_catalog.timestamptz)'
    assert values == ["2025-01-01T00:00:00Z"]
    composite = info("co", "other", "s.pair", composite=True)
    assert (
        debug_render(pp.compile_filter({"column": "co", "operator": "is_null"}, composite))[0]
        == 'pg_catalog.num_nulls("co") = 1'
    )
    other = info("ip", "other", "inet")
    assert debug_render(
        pp.compile_filter({"column": "ip", "operator": "like", "value": "10.%"}, other)
    ) == ('CAST("ip" AS pg_catalog.text) COLLATE pg_catalog."C" LIKE %s', ["10.%"])


def test_where_clause_rejects_unknown_columns():
    assert pp.where_clause([], {}) == Sql(())
    with pytest.raises(pp.FilterValueError):
        pp.where_clause([{"column": ["a", "b"], "operator": "is_null"}], {"a": info("a")})


# --------------------------------------------------------------------------- metrics


KINDS = {
    "integer": info("i", "integer", "integer"),
    "float": info("f", "float", "double precision"),
    "decimal": info("n", "decimal", "numeric(12,2)", scale=2),
    "string": info("s", "string", "text"),
    "boolean": info("b", "boolean", "boolean"),
    "date": info("d", "date", "date"),
    "timestamp": info("ts", "timestamp", "timestamp with time zone"),
    "timestamp_ntz": info("tn", "timestamp_ntz", "timestamp without time zone"),
    "binary": info("by", "binary", "bytea"),
    "array": info("ar", "array", "integer[]"),
    "variant": info("j", "variant", "jsonb"),
    "interval": info("iv", "interval", "interval"),
    "other": info("o", "other", "inet"),
}


def _plan_for(kind):
    column = KINDS[kind]
    ntype = {"kind": kind, "physical_type": column.physical_type}
    if kind == "array":
        ntype.update(
            element_type={"kind": "integer", "physical_type": "integer"}, contains_null=True
        )
    fields = [{"name": column.name, "type": ntype, "nullable": True, "comment": None}]
    tree = build_schema_tree(fields, max_depth=3, max_fields=200)
    profiled, _ = select_profile_fields(tree, None)
    plan = plan_aggregates(
        profiled,
        config=default_config(),
        capabilities={"try_parse_json": True},
        scope="full_snapshot",
        json_field_ids={profiled[0]["field_id"]} if kind == "string" else frozenset(),
    )
    return plan, column


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_every_planned_metric_compiles_to_one_safe_expression(kind):
    plan, column = _plan_for(kind)
    for spec in plan["specs"]:
        expression = pp.compile_metric(
            spec, column if spec["field_id"] else None, "2026-01-01T00:00:00.000Z"
        )
        text = "".join(text_parts(expression))
        assert not FORBIDDEN.search(text), (spec["op"], text)
        assert text.count("pg_catalog.") >= 1


def test_metric_semantics_shapes():
    def render(op, column, **params):
        return debug_render(
            pp.compile_metric({"op": op, "params": params}, column, "2026-01-01T00:00:00.000Z")
        )

    assert render("approx_distinct", KINDS["string"])[0] == (
        'pg_catalog.count(DISTINCT "s" COLLATE pg_catalog."C")'
    )
    assert render("all_values_equal", KINDS["boolean"])[0] == (
        'pg_catalog.bool_and("b") = pg_catalog.bool_or("b")'
    )
    assert "pg_catalog.encode(\"by\", 'hex')" in render("all_values_equal", KINDS["binary"])[0]
    text, values = render("quantiles", KINDS["integer"], probabilities=[0.25, 0.75], accuracy=1)
    assert text.startswith("pg_catalog.percentile_disc(CAST(%s AS pg_catalog.float8[]))")
    assert 'ORDER BY CAST("i" AS pg_catalog.int8)' in text and values == [[0.25, 0.75]]
    assert "'NaN'::pg_catalog.float8" in render("min_value", KINDS["float"])[0]
    assert render("min_value", KINDS["date"])[0] == (
        'CAST(pg_catalog.min("d") FILTER (WHERE pg_catalog.isfinite("d")) AS pg_catalog.text)'
    )
    assert render("mean_value", KINDS["decimal"])[0].startswith("ARRAY[pg_catalog.sum(")
    assert render("count_whitespace_only", KINDS["string"])[1] == [pp.WHITESPACE_PATTERN]
    assert render("count_after_reference", KINDS["date"])[1] == ["2026-01-01"]
    assert render("count_after_reference", KINDS["timestamp"])[1] == ["2026-01-01T00:00:00.000Z"]
    assert "NOT pg_catalog.pg_input_is_valid" in render("count_json_invalid", KINDS["string"])[0]
    assert "pg_catalog.unnest" in render("count_null_elements", KINDS["array"])[0]
    assert "'NaN'::pg_catalog.numeric" in render("count_positive", KINDS["decimal"])[0]


@pytest.mark.parametrize("op", ["count_null_parent_present", "count_null_map_values", "count_elem"])
def test_operations_without_postgresql_semantics_are_rejected(op):
    with pytest.raises(pp.UnsupportedOperation):
        pp.compile_metric({"op": op, "params": {}}, KINDS["integer"], "")


def test_hostile_names_and_values_never_become_sql_text():
    for hostile in HOSTILE:
        column = info(hostile, "string", "text")
        queries = [
            pp.compile_filter({"column": hostile, "operator": "eq", "value": hostile}, column),
            pp.compile_filter({"column": hostile, "operator": "in", "value": [hostile]}, column),
            pp.compile_filter({"column": hostile, "operator": "like", "value": hostile}, column),
            pp.compile_metric({"op": "count_empty_string", "params": {}}, column, ""),
            pp.aggregate_statement(pp.relation(hostile, hostile), [("a0000", sql("1"))], sql()),
            pp.sample_statement(
                pp.relation(hostile, hostile),
                [column],
                sql(),
                max_value_chars=5,
                max_rows=10,
                method="prefix",
                tablesample=True,
                random_fraction=None,
                seed=None,
            ),
            pp.relation_lookup_statement(hostile, [hostile]),
        ]
        for query in queries:
            assert all(hostile not in part for part in text_parts(query))


# Unqualified words allowed before "(": SQL syntax and table aliases, never a function.
SQL_WORDS = {
    "CAST",
    "ANY",
    "ARRAY",
    "EXISTS",
    "IN",
    "FILTER",
    "GROUP",
    "AND",
    "OR",
    "NOT",
    "THEN",
    "WHEN",
    "ELSE",
    "k",
    "td_e",
}


def test_catalog_statements_are_read_only_and_qualified():
    statements = [
        pp.holder_statement(),
        pp.holder_timeout_statement(),
        pp.worker_settings_statement(statement_timeout="300s", lock_timeout="10s", work_mem="16MB"),
        pp.relation_lookup_statement("t", ["public"]),
        pp.relation_detail_statement(1),
        pp.columns_statement(1),
        pp.types_statement([1, 2]),
        pp.partition_key_statement(1),
        pp.constraints_statement(1, server_version_num=170000),
        pp.constraints_statement(1, server_version_num=180000),
        pp.filter_probe_statement(pp.relation("s", "t"), sql()),
    ]
    for statement in statements:
        text = "".join(text_parts(statement))
        assert not FORBIDDEN.search(text.replace("EXPLAIN SELECT", "SELECT")), text
        for function in re.findall(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)\s*\(", text):
            assert function in SQL_WORDS, (function, text)
    assert (
        "con.conenforced" in debug_render(pp.constraints_statement(1, server_version_num=180000))[0]
    )
    assert (
        "con.conenforced"
        not in debug_render(pp.constraints_statement(1, server_version_num=170011))[0]
    )
    assert "set_config('search_path', '', true)" in debug_render(statements[2])[0]


# --------------------------------------------------------------------------- decoding and labels


@pytest.mark.parametrize(
    ("text", "with_offset", "expected"),
    [
        ("2025-01-01 10:00:00+00", True, "2025-01-01T10:00:00.000000Z"),
        ("2025-06-01 00:00:00.123+00", True, "2025-06-01T00:00:00.123000Z"),
        ("2025-06-01 00:00:00.123456", False, "2025-06-01T00:00:00.123456"),
        ("2025-06-01 00:00:00+05:30", True, "2025-06-01T00:00:00.000000+05:30"),
    ],
)
def test_timestamps_are_formatted_like_spark(text, with_offset, expected):
    assert pp.format_timestamp(text, with_offset=with_offset) == expected


@pytest.mark.parametrize("text", ["0044-03-15 00:00:00 BC", "10000-01-01 00:00:00", "infinity"])
def test_timestamps_outside_the_range_fail_one_alias(text):
    with pytest.raises(ValueError):
        pp.format_timestamp(text, with_offset=False)


def test_decoders():
    assert pp.decode_value("date", "2025-01-01") == date(2025, 1, 1)
    with pytest.raises(ValueError):
        pp.decode_value("date", "0044-03-15 BC")
    assert pp.decode_value(
        "decimal_mean:2", [Decimal("12345678901234567890.3704"), Decimal(3)]
    ) == (Decimal("4115226300411522630.123467"))
    assert pp.decode_value("decimal_mean:2", [None, Decimal(0)]) is None
    assert pp.decode_value("decimal_mean:2", [Decimal("NaN"), Decimal(2)]).is_nan()
    assert pp.decimal_mean(Decimal("0.125"), 2, -2) == Decimal("0.06")
    assert pp.decimal_mean(Decimal("1"), 8, 0) == Decimal("0.1250")
    assert pp.decode_value(None, 5) == 5
    assert pp.decode_value("timestamp", b"2025-01-01 10:00:00+00") == "2025-01-01T10:00:00.000000Z"


def test_labels_describe_how_postgresql_computes_metrics():
    distinct = pp.labels({"op": "approx_distinct", "params": {"rsd": 0.02}}, KINDS["string"])
    assert distinct["accuracy"] == "exact"
    assert distinct["details"] == {"relative_standard_deviation": None}
    quantiles = pp.labels({"op": "quantiles", "params": {}}, KINDS["integer"])
    assert quantiles["accuracy"] == "exact" and "percentile_disc" in quantiles["method"]
    assert "±infinity" in pp.labels({"op": "max_value", "params": {}}, KINDS["date"])["method"]
    assert pp.labels({"op": "count_null", "params": {}}, KINDS["string"]) is None


def test_snapshot_ids_are_validated_before_use():
    assert pp.snapshot_literal("00000003-0000001B-1") == "'00000003-0000001B-1'"
    for bad in ["x'; DROP", "", "00000003-0000001B-1'"]:
        with pytest.raises(ValueError):
            pp.snapshot_literal(bad)
