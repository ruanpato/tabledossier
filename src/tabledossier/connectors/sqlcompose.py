"""Composable SQL: trusted text, quoted identifiers and bound parameters.

A query is a flat tuple of parts. Text parts are constants written in this package; names that
come from a configuration or a catalog only enter as :class:`Ident`, and values only as
:class:`Param`, so neither can ever become SQL text. The driver layer turns a query into the
driver's own composition (``psycopg.sql.Identifier`` and placeholders); :func:`debug_render`
gives a readable form for tests and for nothing else.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Ident:
    """A quoted identifier (a name, or a qualified name ``schema.table``)."""

    names: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.names:
            raise ValueError("an identifier needs at least one name")
        for name in self.names:
            if not isinstance(name, str) or not name or "\x00" in name:
                raise ValueError("identifier names must be non-empty strings without NUL")


@dataclass(frozen=True)
class Param:
    """A value sent separately from the SQL text (a bound parameter)."""

    value: object


Part = str | Ident | Param


@dataclass(frozen=True)
class Sql:
    """A composed query or fragment."""

    parts: tuple[Part, ...]

    def __add__(self, other: Sql | Part) -> Sql:
        return sql(self, other)

    def __radd__(self, other: Part) -> Sql:
        return sql(other, self)


def sql(*items: Sql | Part) -> Sql:
    """Concatenate fragments, text constants, identifiers and parameters."""
    parts: list[Part] = []
    for item in items:
        if isinstance(item, Sql):
            parts.extend(item.parts)
        elif isinstance(item, (str, Ident, Param)):
            parts.append(item)
        else:
            raise TypeError(f"cannot compose {type(item).__name__} into SQL")
    merged: list[Part] = []
    for part in parts:
        if isinstance(part, str) and merged and isinstance(merged[-1], str):
            merged[-1] = merged[-1] + part
        elif not (isinstance(part, str) and part == ""):
            merged.append(part)
    return Sql(tuple(merged))


def ident(*names: str) -> Sql:
    """Return a fragment holding one quoted (possibly qualified) identifier."""
    return Sql((Ident(tuple(names)),))


def param(value: object) -> Sql:
    """Return a fragment holding one bound parameter."""
    return Sql((Param(value),))


def join(separator: str, items: Iterable[Sql | Part]) -> Sql:
    """Join fragments with a text separator."""
    out: list[Sql | Part] = []
    for index, item in enumerate(items):
        if index:
            out.append(separator)
        out.append(item)
    return sql(*out)


def quote_ident(name: str) -> str:
    """Quote one name as PostgreSQL does (double quotes, inner quotes doubled)."""
    return '"' + name.replace('"', '""') + '"'


def debug_render(query: Sql) -> tuple[str, list[object]]:
    """Return ``(text with %s placeholders, parameters)``; for tests and logs of shapes only."""
    text: list[str] = []
    values: list[object] = []
    for part in query.parts:
        if isinstance(part, str):
            text.append(part)
        elif isinstance(part, Ident):
            text.append(".".join(quote_ident(name) for name in part.names))
        else:
            text.append("%s")
            values.append(part.value)
    return "".join(text), values


def text_parts(query: Sql) -> list[str]:
    """Return the trusted text constants of ``query`` (what the package wrote itself)."""
    return [part for part in query.parts if isinstance(part, str)]


def identifiers(query: Sql) -> list[tuple[str, ...]]:
    """Return the identifiers of ``query``."""
    return [part.names for part in query.parts if isinstance(part, Ident)]


def parameters(query: Sql) -> list[object]:
    """Return the bound values of ``query`` in order."""
    return [part.value for part in query.parts if isinstance(part, Param)]
