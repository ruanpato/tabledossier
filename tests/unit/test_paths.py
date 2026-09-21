import pytest

from tabledossier.paths import (
    IdentifierError,
    column_reference_segments,
    display_path,
    field_id,
    parse_display_path,
    parse_table_identifier,
    quote_table_identifier,
    table_id,
    table_key,
    table_lookup_key,
)


@pytest.mark.parametrize(
    ("text", "parts"),
    [
        ("demo.analytics.orders", ["demo", "analytics", "orders"]),
        ("orders", ["orders"]),
        ("analytics.orders", ["analytics", "orders"]),
        ("demo.analytics.`order events`", ["demo", "analytics", "order events"]),
        ("demo.`an.alytics`.orders", ["demo", "an.alytics", "orders"]),
        ("`we``ird`", ["we`ird"]),
        ("  demo.analytics.orders  ", ["demo", "analytics", "orders"]),
    ],
)
def test_parse_table_identifier(text, parts):
    assert parse_table_identifier(text) == parts


@pytest.mark.parametrize(
    "text",
    [
        "",
        "a..b",
        "a.b.",
        "a.b.c.d",
        "demo.analytics.order events",
        "`unterminated",
        "`a`b",
        "demo.analytics.orders;DROP TABLE x",
        "a.\nb",
    ],
)
def test_parse_table_identifier_rejects(text):
    with pytest.raises(IdentifierError):
        parse_table_identifier(text)


def test_quoting_escapes_backticks():
    parts = ["demo", "an`alytics", "order events"]
    assert quote_table_identifier(parts) == "`demo`.`an``alytics`.`order events`"
    assert parse_table_identifier(table_key(parts)) == parts


def test_table_keys_are_case_insensitive_for_lookup_only():
    upper = parse_table_identifier("Demo.Analytics.Orders")
    lower = parse_table_identifier("demo.analytics.orders")
    assert table_key(upper) == "Demo.Analytics.Orders"
    assert table_lookup_key(upper) == table_lookup_key(lower)
    assert table_id(upper) == table_id(lower)


def test_literal_dotted_name_differs_from_nested_field():
    literal = [{"kind": "field", "name": "a.b"}]
    nested = [{"kind": "field", "name": "a"}, {"kind": "field", "name": "b"}]
    assert display_path(literal) == "`a.b`"
    assert display_path(nested) == "a.b"
    assert field_id(literal) != field_id(nested)
    assert parse_display_path("`a.b`") == literal
    assert parse_display_path("a.b") == nested


@pytest.mark.parametrize(
    "segments",
    [
        [
            {"kind": "field", "name": "items"},
            {"kind": "array_element"},
            {"kind": "field", "name": "sku"},
        ],
        [{"kind": "field", "name": "attrs"}, {"kind": "map_key"}],
        [
            {"kind": "field", "name": "attrs"},
            {"kind": "map_value"},
            {"kind": "field", "name": "x y"},
        ],
        [{"kind": "field", "name": "items[]"}],
        [{"kind": "field", "name": "display name"}],
        [{"kind": "field", "name": "tick`name"}],
    ],
)
def test_display_path_round_trip(segments):
    assert parse_display_path(display_path(segments)) == segments


def test_column_reference_strings_are_literal():
    assert column_reference_segments("a.b") == [{"kind": "field", "name": "a.b"}]
    assert column_reference_segments(["a", "b"]) == [
        {"kind": "field", "name": "a"},
        {"kind": "field", "name": "b"},
    ]
    with pytest.raises(IdentifierError):
        column_reference_segments([])
    with pytest.raises(IdentifierError):
        column_reference_segments(3)
