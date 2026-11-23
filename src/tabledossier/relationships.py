"""Known relationships: declared constraints and human-provided links.

Part of the embedded runtime (standard library only).

Relationships are never inferred from column names (a column ending in
``_id`` proves nothing). Each record keeps its origin, participating columns
(composite keys included), enforcement, validation state and scope.
Cardinality is recorded only when a person provided it.

Declared PRIMARY KEY, UNIQUE and FOREIGN KEY constraints are assembled here
from ``information_schema``-shaped rows (:func:`key_constraints_from_rows`);
the engine adapter only runs the queries that return those rows. Notes follow
the ``source`` and ``enforcement`` labels of each constraint.
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
ENFORCED_DECLARED_NOTE = (
    "Declared as a FOREIGN KEY constraint that the database enforces for writes; the declaration "
    "alone still does not prove every stored row (it can be bypassed, e.g. with "
    "session_replication_role = replica, or added NOT VALID)."
)
NOT_ENFORCED_DECLARED_NOTE = (
    "Declared as a FOREIGN KEY constraint that the database does not enforce: the declaration "
    "alone does not prove the data."
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


def _rel_identity(case_sensitive: bool, *parts: Any) -> tuple[str, ...]:
    return tuple(_rel_text(part) for part in parts) if case_sensitive else _rel_fold(*parts)


def _rel_catalog(source: str) -> str:
    return "information_schema" if source == "information_schema" else "the catalog"


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


def referenced_constraint_keys(
    rows: Iterable[Mapping[str, Any]], *, case_sensitive: bool = False
) -> list[tuple[str, str, str]]:
    """Return ``(catalog, schema, name)`` of the constraints referenced by foreign keys.

    ``rows`` are the ``information_schema`` rows of one table (see
    :func:`key_constraints_from_rows`). Each referenced constraint is listed
    once, compared case-insensitively unless ``case_sensitive``, in the order
    of first appearance.
    """
    seen: set[tuple[str, ...]] = set()
    out = []
    for row in rows:
        reference = _rel_reference(row)
        if _rel_kind(row.get("constraint_type")) != "foreign_key" or reference is None:
            continue
        identity = _rel_identity(case_sensitive, *reference)
        if identity not in seen:
            seen.add(identity)
            out.append(reference)
    return out


def _rel_referenced(
    name: str,
    reference: tuple[str, str, str] | None,
    positions: list[int | None],
    targets: Mapping[tuple[str, ...], list[Mapping[str, Any]]],
    *,
    source: str,
    case_sensitive: bool,
) -> tuple[dict[str, Any] | None, str | None]:
    """Resolve the referenced table and columns of one foreign key, or say why not."""
    if reference is None:
        listing = (
            "information_schema.referential_constraints"
            if source == "information_schema"
            else "the catalog"
        )
        return None, (
            f"Foreign key {name}: {listing} lists no referenced "
            "constraint, so the relationship is not documented."
        )
    target_rows = targets.get(_rel_identity(case_sensitive, *reference), [])
    label = ".".join(reference)
    if not target_rows:
        return None, (
            f"Foreign key {name} references constraint {label}, whose columns are not visible in "
            f"{_rel_catalog(source)} (missing, or not readable with these permissions), so the "
            "relationship is not documented."
        )
    tables = {
        _rel_identity(
            case_sensitive, row.get("table_catalog"), row.get("table_schema"), row.get("table_name")
        )
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
    rows: Iterable[Mapping[str, Any]],
    referenced_rows: Iterable[Mapping[str, Any]],
    *,
    source: str = "information_schema",
    case_sensitive: bool = False,
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
    Unity Catalog does, unless ``case_sensitive`` (PostgreSQL names that differ
    only by case are distinct), and row order does not matter: constraints are
    sorted by name and columns by ``ordinal_position``. Other constraint types
    (CHECK) are ignored. A row may also carry ``enforcement`` (default
    ``not_enforced``, as in Unity Catalog) and ``notes`` (appended to the
    returned notes once each). ``source`` labels every constraint.

    Returns ``(constraints, notes)``; a foreign key whose referenced columns
    cannot be resolved keeps ``referenced: None`` and a note says why (it is
    then not documented as a relationship).
    """
    grouped: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        kind = _rel_kind(row.get("constraint_type"))
        if kind is None:
            continue
        identity = _rel_identity(
            case_sensitive,
            row.get("constraint_catalog"),
            row.get("constraint_schema"),
            row.get("constraint_name"),
        )
        entry = grouped.setdefault(
            identity,
            {
                "name": _rel_text(row.get("constraint_name")),
                "kind": kind,
                "columns": {},
                "reference": None,
                "enforcement": row.get("enforcement") or "not_enforced",
                "notes": [],
            },
        )
        for note in row.get("notes") or []:
            if note not in entry["notes"]:
                entry["notes"].append(str(note))
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
        identity = _rel_identity(
            case_sensitive,
            row.get("constraint_catalog"),
            row.get("constraint_schema"),
            row.get("constraint_name"),
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
                entry["name"],
                entry["reference"],
                positions,
                targets,
                source=source,
                case_sensitive=case_sensitive,
            )
            if note:
                notes.append(note)
        for note in entry["notes"]:
            if note not in notes:
                notes.append(note)
        constraints.append(
            {
                "name": entry["name"],
                "constraint_type": entry["kind"],
                "columns": [column for column, _ in ordered],
                "expression": None,
                "referenced": referenced,
                "enforcement": entry["enforcement"],
                "source": source,
            }
        )
    return constraints, notes


def _rel_table(text: str) -> str:
    return table_key(parse_table_identifier(text))


def _rel_columns(columns: Iterable[Any]) -> list[str]:
    return [display_path(column_reference_segments(column)) for column in columns]


def _rel_declared_note(constraint: Mapping[str, Any]) -> str:
    """Return the note of a declared relationship, from the constraint's labels."""
    if constraint.get("enforcement") == "enforced":
        return ENFORCED_DECLARED_NOTE
    if constraint.get("source", "information_schema") == "information_schema":
        return DECLARED_NOTE
    return NOT_ENFORCED_DECLARED_NOTE


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
                    "notes": [_rel_declared_note(constraint)],
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
