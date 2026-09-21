"""Standard-library validator for the JSON Schema subset used by TableDossier.

Generated notebooks cannot install ``jsonschema``, so they validate the
configuration and the exported profile with this small interpreter of the
*same* schema files shipped in ``tabledossier/schemas``. The CLI and CI also
run the full ``jsonschema`` implementation; tests check that both agree.

Only the keywords listed in :data:`SUPPORTED_KEYWORDS` may appear in
TableDossier schemas. :func:`unsupported_keywords` lets tests prove that no
schema relies on a keyword this module would silently ignore.
"""

import re
from collections.abc import Iterator, Mapping
from typing import Any

SUPPORTED_KEYWORDS = frozenset(
    {
        "$ref",
        "$defs",
        "type",
        "enum",
        "const",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minProperties",
        "oneOf",
        "anyOf",
        "allOf",
        "not",
        "if",
        "then",
        "else",
    }
)
ANNOTATION_KEYWORDS = frozenset(
    {"$schema", "$id", "$comment", "title", "description", "default", "examples", "format"}
)

_SC_MAX_REPR = 80


def _sc_repr(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _SC_MAX_REPR else text[: _SC_MAX_REPR - 3] + "..."


def _sc_is_type(value: Any, type_name: str) -> bool:
    if type_name == "null":
        return value is None
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        if isinstance(value, bool):
            return False
        return isinstance(value, int) or (isinstance(value, float) and value.is_integer())
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "object":
        return isinstance(value, dict)
    raise ValueError(f"unknown JSON Schema type: {type_name}")


def _sc_equal(left: Any, right: Any) -> bool:
    """JSON equality: booleans never equal numbers; 1 equals 1.0."""
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _sc_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_sc_equal(left[k], right[k]) for k in left)
    return type(left) is type(right) and bool(left == right)


def _sc_resolve(ref: str, root: Mapping[str, Any]) -> Mapping[str, Any]:
    if not ref.startswith("#/"):
        raise ValueError(f"only local references are supported: {ref}")
    node: Any = root
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        node = node[token]
    if not isinstance(node, Mapping):
        raise ValueError(f"reference does not point to a schema: {ref}")
    return node


def _sc_validate(
    value: Any,
    schema: Mapping[str, Any] | bool,
    root: Mapping[str, Any],
    where: str,
    errors: list[str],
) -> None:
    if schema is True:
        return
    if schema is False:
        errors.append(f"{where}: no value is allowed here")
        return
    if "$ref" in schema:
        _sc_validate(value, _sc_resolve(schema["$ref"], root), root, where, errors)

    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_sc_is_type(value, t) for t in types):
            errors.append(f"{where}: expected {' or '.join(types)}, got {_sc_repr(value)}")
            return
    if "const" in schema and not _sc_equal(value, schema["const"]):
        errors.append(f"{where}: expected {_sc_repr(schema['const'])}, got {_sc_repr(value)}")
    if "enum" in schema and not any(_sc_equal(value, option) for option in schema["enum"]):
        errors.append(f"{where}: {_sc_repr(value)} is not one of {_sc_repr(schema['enum'])}")

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(f"{where}: shorter than {schema['minLength']} characters")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{where}: longer than {schema['maxLength']} characters")
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            errors.append(f"{where}: {_sc_repr(value)} does not match {schema['pattern']!r}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{where}: {value} is below the minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{where}: {value} is above the maximum {schema['maximum']}")
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            errors.append(f"{where}: {value} must be greater than {schema['exclusiveMinimum']}")
        if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
            errors.append(f"{where}: {value} must be less than {schema['exclusiveMaximum']}")

    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{where}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{where}: more than {schema['maxItems']} items")
        if schema.get("uniqueItems"):
            for index, item in enumerate(value):
                if any(_sc_equal(item, other) for other in value[:index]):
                    errors.append(f"{where}[{index}]: duplicate item")
        if "items" in schema:
            for index, item in enumerate(value):
                _sc_validate(item, schema["items"], root, f"{where}[{index}]", errors)

    if isinstance(value, dict):
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            errors.append(f"{where}: fewer than {schema['minProperties']} properties")
        for name in schema.get("required", ()):
            if name not in value:
                errors.append(f"{where}: missing required property {name!r}")
        properties = schema.get("properties", {})
        for name, item in value.items():
            if name in properties:
                _sc_validate(item, properties[name], root, f"{where}.{name}", errors)
            elif "additionalProperties" in schema:
                extra = schema["additionalProperties"]
                if extra is False:
                    errors.append(f"{where}: unexpected property {name!r}")
                else:
                    _sc_validate(item, extra, root, f"{where}.{name}", errors)

    for sub in schema.get("allOf", ()):
        _sc_validate(value, sub, root, where, errors)
    if "anyOf" in schema and not any(
        not _sc_collect(value, sub, root, where) for sub in schema["anyOf"]
    ):
        errors.append(f"{where}: does not match any allowed alternative")
    if "oneOf" in schema:
        matches = sum(1 for sub in schema["oneOf"] if not _sc_collect(value, sub, root, where))
        if matches != 1:
            errors.append(f"{where}: must match exactly one alternative (matched {matches})")
    if "not" in schema and not _sc_collect(value, schema["not"], root, where):
        errors.append(f"{where}: matches a forbidden schema")
    if "if" in schema:
        if not _sc_collect(value, schema["if"], root, where):
            if "then" in schema:
                _sc_validate(value, schema["then"], root, where, errors)
        elif "else" in schema:
            _sc_validate(value, schema["else"], root, where, errors)


def _sc_collect(
    value: Any, schema: Mapping[str, Any] | bool, root: Mapping[str, Any], where: str
) -> list[str]:
    errors: list[str] = []
    _sc_validate(value, schema, root, where, errors)
    return errors


def schema_errors(instance: Any, schema: Mapping[str, Any], max_errors: int = 50) -> list[str]:
    """Validate ``instance`` against ``schema`` and return readable error strings.

    An empty list means the instance is valid. At most ``max_errors`` messages
    are returned.
    """
    errors = _sc_collect(instance, schema, schema, "$")
    return errors[:max_errors]


def _sc_walk(schema: Any, where: str) -> Iterator[tuple[str, str]]:
    if isinstance(schema, bool):
        return
    if not isinstance(schema, Mapping):
        raise ValueError(f"{where}: schema must be an object or boolean")
    for key, sub in schema.items():
        if key not in SUPPORTED_KEYWORDS and key not in ANNOTATION_KEYWORDS:
            yield where, key
        if key in ("properties", "$defs"):
            for name, child in sub.items():
                yield from _sc_walk(child, f"{where}/{key}/{name}")
        elif key in ("items", "additionalProperties", "not", "if", "then", "else"):
            yield from _sc_walk(sub, f"{where}/{key}")
        elif key in ("oneOf", "anyOf", "allOf"):
            for index, child in enumerate(sub):
                yield from _sc_walk(child, f"{where}/{key}/{index}")


def unsupported_keywords(schema: Mapping[str, Any]) -> list[str]:
    """Return ``location:keyword`` entries this validator would not enforce."""
    return [f"{where}:{key}" for where, key in _sc_walk(schema, "#")]
