"""Known relationships: declared constraints and human-provided links.

Part of the embedded runtime (standard library only).

Relationships are never inferred from column names (a column ending in
``_id`` proves nothing). Each record keeps its origin, participating columns
(composite keys included), enforcement, validation state and scope.
Cardinality is recorded only when a person provided it.
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
    "informational (not enforced); TableDossier did not verify the data against it."
)
PROVIDED_NOTE = "Provided by a person; not validated against the data by TableDossier."


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
