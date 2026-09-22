"""Known relationships: declared constraints and human-provided links.

Part of the embedded runtime (standard library only).

Relationships are never inferred from column names (a column ending in
``_id`` proves nothing). Each record keeps its origin, participating columns
(composite keys included), enforcement, validation state and scope.
Cardinality is recorded only when a person provided it.

Declared PRIMARY KEY, UNIQUE and FOREIGN KEY constraints are assembled here
from ``information_schema`` rows (:func:`key_constraints_from_rows`); the
Spark adapter only runs the parameterized queries that return those rows.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from tabledossier.paths import (
    column_reference_segments,
    display_path,
    parse_table_identifier,
    table_key,
)

DECLARED_NOTE = (
    "Declared as a FOREIGN KEY constraint. Unity Catalog primary and foreign keys are "
    "informational (not enforced): the declaration alone does not prove the data."
)
PROVIDED_NOTE = "Provided by a person; the statement alone does not prove the data."
INFORMATION_SCHEMA_KINDS = {
    "PRIMARY KEY": "primary_key",
    "FOREIGN KEY": "foreign_key",
    "UNIQUE": "unique",
}


def _rel_text(value: Any) -> str:
    return "" if value is None else str(value)


def _rel_fold(*parts: Any) -> tuple[str, ...]:
    return tuple(_rel_text(part).casefold() for part in parts)


def _rel_kind(value: Any) -> str | None:
    return INFORMATION_SCHEMA_KINDS.get(" ".join(_rel_text(value).upper().split()))


def _rel_position(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _rel_reference(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    if not row.get("unique_constraint_name"):
        return None
    return (
        _rel_text(row.get("unique_constraint_catalog")),
        _rel_text(row.get("unique_constraint_schema")),
        _rel_text(row.get("unique_constraint_name")),
    )


def referenced_constraint_keys(rows: Iterable[Mapping[str, Any]]) -> list[tuple[str, str, str]]:
    """Return ``(catalog, schema, name)`` of the constraints referenced by foreign keys.

    ``rows`` are the ``information_schema`` rows of one table (see
    :func:`key_constraints_from_rows`). Each referenced constraint is listed
    once, compared case-insensitively, in the order of first appearance.
    """
    seen: set[tuple[str, ...]] = set()
    out = []
    for row in rows:
        reference = _rel_reference(row)
        if _rel_kind(row.get("constraint_type")) != "foreign_key" or reference is None:
            continue
        if _rel_fold(*reference) not in seen:
            seen.add(_rel_fold(*reference))
            out.append(reference)
    return out


def _rel_referenced(
    name: str,
    reference: tuple[str, str, str] | None,
    positions: list[int | None],
    targets: Mapping[tuple[str, ...], list[Mapping[str, Any]]],
) -> tuple[dict[str, Any] | None, str | None]:
    """Resolve the referenced table and columns of one foreign key, or say why not."""
    if reference is None:
        return None, (
            f"Foreign key {name}: information_schema.referential_constraints lists no referenced "
            "constraint, so the relationship is not documented."
        )
    target_rows = targets.get(_rel_fold(*reference), [])
    label = ".".join(reference)
    if not target_rows:
        return None, (
            f"Foreign key {name} references constraint {label}, whose columns are not visible in "
            "information_schema (missing, or not readable with these permissions), so the "
            "relationship is not documented."
        )
    tables = {
        _rel_fold(row.get("table_catalog"), row.get("table_schema"), row.get("table_name"))
        for row in target_rows
    }
    by_position: dict[int, str] = {}
    for row in target_rows:
        position = _rel_position(row.get("ordinal_position"))
        if position is not None:
            by_position.setdefault(position, _rel_text(row.get("column_name")))
    columns = [
        by_position.get(position if position is not None else index + 1)
        for index, position in enumerate(positions)
    ]
    if len(tables) != 1 or None in columns or len(by_position) != len(columns):
        return None, (
            f"Foreign key {name}: its columns do not match the columns of the referenced "
            f"constraint {label}, so the relationship is not documented."
        )
    first = target_rows[0]
    return {
        "table": table_key(
            [
                _rel_text(first.get("table_catalog")),
                _rel_text(first.get("table_schema")),
                _rel_text(first.get("table_name")),
            ]
        ),
        "columns": [str(column) for column in columns],
    }, None


def key_constraints_from_rows(
    rows: Iterable[Mapping[str, Any]], referenced_rows: Iterable[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Assemble PRIMARY KEY, UNIQUE and FOREIGN KEY records from ``information_schema`` rows.

    ``rows`` join ``table_constraints``, ``key_column_usage`` and
    ``referential_constraints`` for one table (``constraint_catalog``,
    ``constraint_schema``, ``constraint_name``, ``constraint_type``,
    ``column_name``, ``ordinal_position``, ``position_in_unique_constraint``,
    ``unique_constraint_catalog``, ``unique_constraint_schema``,
    ``unique_constraint_name``). ``referenced_rows`` are ``key_column_usage``
    rows of the constraints named by :func:`referenced_constraint_keys`
    (``constraint_catalog``, ``constraint_schema``, ``constraint_name``,
    ``table_catalog``, ``table_schema``, ``table_name``, ``column_name``,
    ``ordinal_position``).

    Catalog, schema and constraint names are compared case-insensitively, as
    Unity Catalog does, and row order does not matter: constraints are sorted
    by name and columns by ``ordinal_position``. Other constraint types (CHECK)
    are ignored. Returns ``(constraints, notes)``; a foreign key whose
    referenced columns cannot be resolved keeps ``referenced: None`` and a note
    says why (it is then not documented as a relationship).
    """
    grouped: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        kind = _rel_kind(row.get("constraint_type"))
        if kind is None:
            continue
        identity = _rel_fold(
            row.get("constraint_catalog"), row.get("constraint_schema"), row.get("constraint_name")
        )
        entry = grouped.setdefault(
            identity,
            {
                "name": _rel_text(row.get("constraint_name")),
                "kind": kind,
                "columns": {},
                "reference": None,
            },
        )
        position = _rel_position(row.get("ordinal_position"))
        order = position if position is not None else len(entry["columns"]) + 1
        entry["columns"].setdefault(
            order,
            (
                _rel_text(row.get("column_name")),
                _rel_position(row.get("position_in_unique_constraint")),
            ),
        )
        entry["reference"] = entry["reference"] or _rel_reference(row)
    targets: dict[tuple[str, ...], list[Mapping[str, Any]]] = {}
    for row in referenced_rows:
        identity = _rel_fold(
            row.get("constraint_catalog"), row.get("constraint_schema"), row.get("constraint_name")
        )
        targets.setdefault(identity, []).append(row)
    constraints = []
    notes = []
    for entry in sorted(grouped.values(), key=lambda item: (item["name"].casefold(), item["name"])):
        ordered = [entry["columns"][order] for order in sorted(entry["columns"])]
        referenced = None
        if entry["kind"] == "foreign_key":
            positions = [position for _, position in ordered]
            referenced, note = _rel_referenced(
                entry["name"], entry["reference"], positions, targets
            )
            if note:
                notes.append(note)
        constraints.append(
            {
                "name": entry["name"],
                "constraint_type": entry["kind"],
                "columns": [column for column, _ in ordered],
                "expression": None,
                "referenced": referenced,
                "enforcement": "not_enforced",
                "source": "information_schema",
            }
        )
    return constraints, notes


def _rel_table(text: str) -> str:
    return table_key(parse_table_identifier(text))


def _rel_columns(columns: Iterable[Any]) -> list[str]:
    return [display_path(column_reference_segments(column)) for column in columns]


def declared_relationships(tables: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build relationship records from declared FOREIGN KEY constraints."""
    out = []
    for table in tables:
        for constraint in table.get("constraints", []):
            if constraint.get("constraint_type") != "foreign_key" or not constraint.get(
                "referenced"
            ):
                continue
            referenced = constraint["referenced"]
            out.append(
                {
                    "relationship_id": f"declared:{table['table_key']}:{constraint['name']}",
                    "origin": "declared_constraint",
                    "name": constraint["name"],
                    "from": {"table": table["table_key"], "columns": list(constraint["columns"])},
                    "to": {"table": referenced["table"], "columns": list(referenced["columns"])},
                    "cardinality": None,
                    "cardinality_source": None,
                    "enforcement": constraint.get("enforcement", "unknown"),
                    "validation": "not_validated",
                    "scope": "declared_metadata",
                    "description": None,
                    "notes": [DECLARED_NOTE],
                }
            )
    return out


def provided_relationships(items: Iterable[Mapping[str, Any]], origin: str) -> list[dict[str, Any]]:
    """Build relationship records from configuration or annotation entries."""
    out = []
    for item in items:
        cardinality = item.get("cardinality")
        out.append(
            {
                "relationship_id": f"{origin}:{item['id']}",
                "origin": origin,
                "name": item["id"],
                "from": {
                    "table": _rel_table(item["from"]["table"]),
                    "columns": _rel_columns(item["from"]["columns"]),
                },
                "to": {
                    "table": _rel_table(item["to"]["table"]),
                    "columns": _rel_columns(item["to"]["columns"]),
                },
                "cardinality": dict(cardinality) if cardinality else None,
                "cardinality_source": "provided" if cardinality else None,
                "enforcement": "unknown",
                "validation": "not_validated",
                "scope": "human_provided",
                "description": item.get("description"),
                "notes": [PROVIDED_NOTE],
            }
        )
    return out


def merge_relationships(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Concatenate relationship groups, dropping exact duplicate identifiers."""
    seen: set[str] = set()
    out = []
    for group in groups:
        for item in group:
            if item["relationship_id"] in seen:
                continue
            seen.add(item["relationship_id"])
            out.append(dict(item))
    return out
