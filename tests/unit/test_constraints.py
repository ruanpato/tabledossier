"""Declared key constraints assembled from synthetic information_schema rows (no Spark).

Unity Catalog is not available locally, so these rows imitate what the queries of
``tabledossier.runtime.spark.query_key_constraints`` return: one row per key column,
with the referenced constraint of foreign keys, and the key columns of the referenced
constraints read from the information_schema of their own catalog.
"""

import pytest

from tabledossier.config import deep_merge, default_config
from tabledossier.integrity import end_segments
from tabledossier.keys import declared_column_segments, plan_uniqueness, requested_keys
from tabledossier.relationships import (
    declared_relationships,
    key_constraints_from_rows,
    referenced_constraint_keys,
)


def _row(name, kind, column, ordinal, *, position=None, ref=None, catalog="demo", schema="sales"):
    ref_catalog, ref_schema, ref_name = ref or (None, None, None)
    return {
        "constraint_catalog": catalog,
        "constraint_schema": schema,
        "constraint_name": name,
        "constraint_type": kind,
        "column_name": column,
        "ordinal_position": ordinal,
        "position_in_unique_constraint": position,
        "unique_constraint_catalog": ref_catalog,
        "unique_constraint_schema": ref_schema,
        "unique_constraint_name": ref_name,
    }


def _target(name, table, column, ordinal, *, catalog="demo", schema="sales"):
    return {
        "constraint_catalog": catalog,
        "constraint_schema": schema,
        "constraint_name": name,
        "table_catalog": catalog,
        "table_schema": schema,
        "table_name": table,
        "column_name": column,
        "ordinal_position": ordinal,
    }


def _by_name(constraints):
    return {item["name"]: item for item in constraints}


def test_composite_primary_key_and_unique_on_the_same_table():
    rows = [
        # Rows arrive in any order: columns follow ordinal_position, constraints their name.
        _row("lines_pk", "PRIMARY KEY", "line_no", 2),
        _row("lines_uk", "UNIQUE", "sku", 1),
        _row("lines_pk", "PRIMARY KEY", "order_id", 1),
        _row("lines_ck", "CHECK", "qty", 1),
    ]
    constraints, notes = key_constraints_from_rows(rows, [])
    assert [item["name"] for item in constraints] == ["lines_pk", "lines_uk"], "CHECK is ignored"
    assert constraints[0] == {
        "name": "lines_pk",
        "constraint_type": "primary_key",
        "columns": ["order_id", "line_no"],
        "expression": None,
        "referenced": None,
        "enforcement": "not_enforced",
        "source": "information_schema",
    }
    assert constraints[1]["constraint_type"] == "unique"
    assert constraints[1]["columns"] == ["sku"]
    assert notes == []
    assert referenced_constraint_keys(rows) == []


def test_composite_foreign_key_follows_position_in_unique_constraint():
    # The foreign key lists (region, customer) while the referenced key is (customer, region).
    rows = [
        _row(
            "orders_fk", "FOREIGN KEY", "cust_region", 1, position=2, ref=("demo", "sales", "c_pk")
        ),
        _row("orders_fk", "FOREIGN KEY", "cust_no", 2, position=1, ref=("demo", "sales", "c_pk")),
    ]
    targets = [
        _target("c_pk", "customers", "customer_no", 1),
        _target("c_pk", "customers", "region", 2),
    ]
    assert referenced_constraint_keys(rows) == [("demo", "sales", "c_pk")]
    constraints, notes = key_constraints_from_rows(rows, targets)
    fk = _by_name(constraints)["orders_fk"]
    assert fk["columns"] == ["cust_region", "cust_no"]
    assert fk["referenced"] == {
        "table": "demo.sales.customers",
        "columns": ["region", "customer_no"],
    }
    assert notes == []


def test_positions_default_to_the_column_order_when_absent():
    rows = [
        _row("fk", "FOREIGN KEY", "a", 1, ref=("demo", "sales", "pk")),
        _row("fk", "FOREIGN KEY", "b", 2, ref=("demo", "sales", "pk")),
    ]
    targets = [_target("pk", "t", "y", 2), _target("pk", "t", "x", 1)]
    constraints, _ = key_constraints_from_rows(rows, targets)
    assert constraints[0]["referenced"]["columns"] == ["x", "y"]


def test_reference_to_another_catalog_and_schema():
    ref = ("finance", "ledger", "accounts_pk")
    rows = [_row("payments_account_fk", "FOREIGN KEY", "account_id", 1, position=1, ref=ref)]
    targets = [_target("accounts_pk", "accounts", "id", 1, catalog="finance", schema="ledger")]
    assert referenced_constraint_keys(rows) == [ref]
    constraints, notes = key_constraints_from_rows(rows, targets)
    assert constraints[0]["referenced"] == {"table": "finance.ledger.accounts", "columns": ["id"]}
    assert notes == []


def test_missing_referenced_constraint_is_explained_and_not_documented():
    rows = [
        _row("orders_gone_fk", "FOREIGN KEY", "x", 1, position=1, ref=("demo", "other", "gone_pk")),
        _row("orders_bare_fk", "FOREIGN KEY", "y", 1),  # no referential_constraints row
    ]
    constraints, notes = key_constraints_from_rows(rows, [])
    assert all(item["referenced"] is None for item in constraints)
    assert len(notes) == 2
    assert any("demo.other.gone_pk" in note and "not visible" in note for note in notes)
    assert any("orders_bare_fk" in note and "referential_constraints" in note for note in notes)
    table = {"table_key": "demo.sales.orders", "constraints": constraints}
    assert declared_relationships([table]) == [], "an unresolved reference is never an edge"


def test_referenced_columns_that_do_not_match_are_not_guessed():
    rows = [
        _row("fk", "FOREIGN KEY", "a", 1, position=1, ref=("demo", "sales", "pk")),
        _row("fk", "FOREIGN KEY", "b", 2, position=3, ref=("demo", "sales", "pk")),
    ]
    targets = [_target("pk", "t", "x", 1), _target("pk", "t", "y", 2)]
    constraints, notes = key_constraints_from_rows(rows, targets)
    assert constraints[0]["referenced"] is None
    assert "do not match" in notes[0]


def test_names_and_types_are_compared_case_insensitively():
    rows = [
        _row("Orders_PK", "primary key", "Order_ID", 1, catalog="DEMO", schema="Sales"),
        _row("orders_pk", "PRIMARY  KEY", "Line", 2, catalog="demo", schema="sales"),
        _row("orders_fk", "Foreign Key", "cust", 1, position=1, ref=("Demo", "SALES", "CUST_PK")),
    ]
    targets = [_target("cust_pk", "Customers", "Cust", 1, catalog="demo", schema="sales")]
    constraints, notes = key_constraints_from_rows(rows, targets)
    by_name = _by_name(constraints)
    assert set(by_name) == {"Orders_PK", "orders_fk"}, "one constraint, however it is spelled"
    assert by_name["Orders_PK"]["columns"] == ["Order_ID", "Line"]
    assert by_name["orders_fk"]["referenced"] == {
        "table": "demo.sales.Customers",
        "columns": ["Cust"],
    }
    assert notes == []
    assert referenced_constraint_keys(rows + rows) == [("Demo", "SALES", "CUST_PK")]


def test_assembly_does_not_depend_on_row_order():
    rows = [
        _row("b_uk", "UNIQUE", "y", 1),
        _row("a_pk", "PRIMARY KEY", "k2", 2),
        _row("a_pk", "PRIMARY KEY", "k1", 1),
    ]
    forward, _ = key_constraints_from_rows(rows, [])
    backward, _ = key_constraints_from_rows(list(reversed(rows)), [])
    assert forward == backward
    assert [item["name"] for item in forward] == ["a_pk", "b_uk"]


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        (["order_id"], "order_id"),
        (["ORDER_ID"], "order_id"),  # the catalog spelling differs from the schema's
        (["missing"], "missing"),  # no match: kept, and reported as not in the schema
    ],
)
def test_declared_columns_match_the_schema_case_insensitively(columns, expected):
    fields = [{"name": "order_id"}, {"name": "Status"}, {"name": "status"}]
    assert declared_column_segments(columns, fields) == [[{"kind": "field", "name": expected}]]
    # Two columns that differ only by case are ambiguous: the literal name is kept.
    assert declared_column_segments(["STATUS"], fields) == [[{"kind": "field", "name": "STATUS"}]]


def test_declared_constraints_feed_uniqueness_and_relationships(deep_profile):
    """information_schema rows -> constraints -> declared keys and declared foreign keys."""
    orders = next(t for t in deep_profile["tables"] if t["table_key"] == "analytics.orders")
    customers = next(t for t in deep_profile["tables"] if t["table_key"] == "analytics.customers")
    rows = [
        _row("orders_pk", "PRIMARY KEY", "ORDER_ID", 1, schema="analytics"),
        _row("orders_uk", "UNIQUE", "order_id", 1, schema="analytics"),
        _row(
            "orders_customer_fk",
            "FOREIGN KEY",
            "Customer_Id",
            1,
            position=1,
            ref=("demo", "analytics", "customers_pk"),
            schema="analytics",
        ),
    ]
    targets = [_target("customers_pk", "customers", "CUSTOMER_ID", 1, schema="analytics")]
    constraints, notes = key_constraints_from_rows(rows, targets)
    assert notes == []

    config = deep_merge(
        default_config(),
        {"analysis_level": "deep", "deep": {"uniqueness": {"declared_keys": True}}},
    )
    requested = requested_keys(config, "analytics.orders", constraints, [])
    plan = plan_uniqueness(orders["schema"], requested, config)
    [key] = plan["keys"]
    assert key["status"] == "planned", key.get("reason")
    assert key["columns"] == ["order_id"], "the schema's spelling is used"
    assert key["origins"] == ["declared_primary_key", "declared_unique"]
    assert key["names"] == ["orders_pk", "orders_uk"]

    table = {"table_key": "analytics.orders", "constraints": constraints}
    [relationship] = declared_relationships([table])
    assert relationship["origin"] == "declared_constraint"
    assert relationship["to"] == {"table": "demo.analytics.customers", "columns": ["CUSTOMER_ID"]}
    assert end_segments(relationship, "from", orders["schema"]["fields"]) == [
        [{"kind": "field", "name": "customer_id"}]
    ]
    assert end_segments(relationship, "to", customers["schema"]["fields"]) == [
        [{"kind": "field", "name": "customer_id"}]
    ]
