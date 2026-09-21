"""Table identifiers and typed field paths.

Part of the embedded runtime (standard library only).

Table identifiers
    ``catalog.schema.table`` with optional backtick quoting for parts that
    contain other characters, e.g. ``demo.analytics.`order events```. A doubled
    backtick inside a quoted part is a literal backtick.

Field paths
    A field path is a list of typed segments, so that a column literally
    named ``a.b`` is distinct from field ``b`` nested inside struct ``a``::

        [{"kind": "field", "name": "a.b"}]                       -> `a.b`
        [{"kind": "field", "name": "a"}, {"kind": "field", "name": "b"}] -> a.b
        [{"kind": "field", "name": "items"}, {"kind": "array_element"}] -> items[]
        [{"kind": "field", "name": "attrs"}, {"kind": "map_value"}]     -> attrs{value}

    The display form is for documentation and annotation keys. It is not
    guaranteed to be valid SQL.
"""

import re
from typing import Any

from tabledossier.jsonutil import short_hash

SEGMENT_KINDS = ("field", "array_element", "map_key", "map_value")
_PT_BARE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PT_UNQUOTED_PART = re.compile(r"^[A-Za-z0-9_]+$")
_PT_CONTROL = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]")
MAX_IDENTIFIER_PART_LENGTH = 255


class IdentifierError(ValueError):
    """Raised when a table identifier or field path cannot be parsed."""


def _pt_check_part(part: str, text: str) -> None:
    if part == "":
        raise IdentifierError(f"empty name part in {text!r}")
    if _PT_CONTROL.search(part):
        raise IdentifierError(f"control characters are not allowed in {text!r}")
    if len(part) > MAX_IDENTIFIER_PART_LENGTH:
        raise IdentifierError(f"name part longer than {MAX_IDENTIFIER_PART_LENGTH} characters")


def _pt_read_quoted(text: str, start: int) -> tuple[str, int]:
    """Read a backtick-quoted name starting at ``text[start] == '`'``."""
    chars: list[str] = []
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "`":
            if index + 1 < len(text) and text[index + 1] == "`":
                chars.append("`")
                index += 2
                continue
            return "".join(chars), index + 1
        chars.append(char)
        index += 1
    raise IdentifierError(f"unterminated backtick quote in {text!r}")


def parse_table_identifier(text: str) -> list[str]:
    """Parse ``catalog.schema.table`` (1 to 3 parts) into its name parts."""
    if not isinstance(text, str):
        raise IdentifierError("table identifier must be a string")
    source = text.strip()
    if not source:
        raise IdentifierError("table identifier is empty")
    parts: list[str] = []
    index = 0
    while True:
        if index < len(source) and source[index] == "`":
            part, index = _pt_read_quoted(source, index)
        else:
            end = source.find(".", index)
            end = len(source) if end == -1 else end
            part = source[index:end]
            if part and not _PT_UNQUOTED_PART.match(part):
                raise IdentifierError(
                    f"invalid unquoted name {part!r} in {text!r}; "
                    "quote names with other characters using backticks"
                )
            index = end
        _pt_check_part(part, text)
        parts.append(part)
        if index == len(source):
            break
        if source[index] != ".":
            raise IdentifierError(f"expected '.' after a quoted name in {text!r}")
        index += 1
        if index == len(source):
            raise IdentifierError(f"identifier ends with '.': {text!r}")
    if len(parts) > 3:
        raise IdentifierError(f"expected at most 3 name parts (catalog.schema.table): {text!r}")
    return parts


def quote_name(name: str) -> str:
    """Quote one SQL identifier part with backticks (inner backticks doubled)."""
    return "`" + name.replace("`", "``") + "`"


def quote_table_identifier(parts: list[str]) -> str:
    """Return a fully quoted multi-part identifier safe to embed in Spark SQL."""
    return ".".join(quote_name(part) for part in parts)


def _pt_display_name(name: str) -> str:
    return name if _PT_BARE_NAME.match(name) else quote_name(name)


def table_key(parts: list[str]) -> str:
    """Return the canonical display form of a table identifier."""
    return ".".join(_pt_display_name(part) for part in parts)


def table_lookup_key(parts: list[str]) -> str:
    """Case-insensitive key used to match the same table spelled differently."""
    return table_key([part.casefold() for part in parts])


def table_id(parts: list[str]) -> str:
    """Return a stable identifier for a table (used for anchors and references)."""
    return "t_" + short_hash([part.casefold() for part in parts])


def field_segment(name: str) -> dict[str, Any]:
    """Return a struct/column field segment."""
    return {"kind": "field", "name": name}


def display_path(segments: list[dict[str, Any]]) -> str:
    """Return the human-readable form of a typed field path."""
    out = ""
    for segment in segments:
        kind = segment["kind"]
        if kind == "field":
            name = _pt_display_name(segment["name"])
            out = name if out == "" else f"{out}.{name}"
        elif kind == "array_element":
            out += "[]"
        elif kind == "map_key":
            out += "{key}"
        elif kind == "map_value":
            out += "{value}"
        else:
            raise IdentifierError(f"unknown path segment kind: {kind!r}")
    return out


def parse_display_path(text: str) -> list[dict[str, Any]]:
    """Parse a display path (as produced by :func:`display_path`) into segments."""
    if not isinstance(text, str) or not text:
        raise IdentifierError("field path must be a non-empty string")
    segments: list[dict[str, Any]] = []
    index = 0
    expect_name = True
    while index < len(text):
        if expect_name:
            if text[index] == "`":
                name, index = _pt_read_quoted(text, index)
            else:
                match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[index:])
                if not match:
                    raise IdentifierError(f"invalid field name at position {index} in {text!r}")
                name = match.group(0)
                index += len(name)
            _pt_check_part(name, text)
            segments.append(field_segment(name))
            expect_name = False
            continue
        if text.startswith("[]", index):
            segments.append({"kind": "array_element"})
            index += 2
        elif text.startswith("{key}", index):
            segments.append({"kind": "map_key"})
            index += 5
        elif text.startswith("{value}", index):
            segments.append({"kind": "map_value"})
            index += 7
        elif text[index] == ".":
            index += 1
            expect_name = True
            if index == len(text):
                raise IdentifierError(f"field path ends with '.': {text!r}")
        else:
            raise IdentifierError(f"unexpected character at position {index} in {text!r}")
    return segments


def column_reference_segments(reference: Any) -> list[dict[str, Any]]:
    """Convert a configuration column reference into typed segments.

    A string is a *literal* top-level column name (dots are not split); a list
    of strings navigates nested struct fields.
    """
    if isinstance(reference, str):
        _pt_check_part(reference, reference)
        return [field_segment(reference)]
    if isinstance(reference, list) and reference and all(isinstance(p, str) for p in reference):
        for part in reference:
            _pt_check_part(part, ".".join(reference))
        return [field_segment(part) for part in reference]
    raise IdentifierError(
        "a column reference must be a column name or a non-empty list of nested field names"
    )


def field_id(segments: list[dict[str, Any]]) -> str:
    """Return a stable identifier for a field path within a table."""
    return "f_" + short_hash(segments)
