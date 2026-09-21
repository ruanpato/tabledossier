"""Formal JSON Schema validation for the CLI and CI (uses ``jsonschema``).

CLI-only module. Documents are validated twice: formally with the
``jsonschema`` Draft 2020-12 implementation and with the standard-library
checks shared with the notebook (version compatibility, schema subset and
cross-field invariants). A document is valid only if both agree.
"""

from typing import Any

from jsonschema import Draft202012Validator

from tabledossier.config import config_errors, normalize_config
from tabledossier.contract import (
    PROFILE_KIND,
    SUPPORTED_PROFILE_VERSIONS,
    validate_annotations,
    validate_profile,
    version_error,
)
from tabledossier.resources import load_schema
from tabledossier.schemacheck import schema_errors


def formal_errors(document: Any, schema_name: str) -> list[str]:
    """Return errors reported by the formal Draft 2020-12 validator."""
    validator = Draft202012Validator(load_schema(schema_name))
    errors = []
    for error in sorted(validator.iter_errors(document), key=lambda e: list(e.absolute_path)):
        location = "$" + "".join(
            f"[{p}]" if isinstance(p, int) else f".{p}" for p in error.absolute_path
        )
        errors.append(f"{location}: {error.message}")
    return errors


def check_config(document: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate a configuration file; return ``(normalized, errors)``."""
    errors = formal_errors(document, "config")
    if errors:
        return None, errors
    schema = load_schema("config")
    errors = schema_errors(document, schema)
    if errors:
        return None, errors
    normalized = normalize_config(document)
    errors = formal_errors(normalized, "config") or config_errors(normalized, schema)
    return (None, errors) if errors else (normalized, [])


def check_profile(document: Any) -> list[str]:
    """Validate a profile: version, formal schema, stdlib schema subset and invariants."""
    problem = version_error(document, PROFILE_KIND, "schema_version", SUPPORTED_PROFILE_VERSIONS)
    if problem:
        return [problem]
    errors = formal_errors(document, "profile")
    if errors:
        return errors
    return validate_profile(document, load_schema("profile"))


def check_annotations(document: Any) -> list[str]:
    """Validate an annotations file (formal schema plus key syntax)."""
    errors = formal_errors(document, "annotations")
    if errors:
        return errors
    return validate_annotations(document, load_schema("annotations"))


def check_document(document: Any, schema_name: str) -> list[str]:
    """Validate any other packaged document type formally."""
    return formal_errors(document, schema_name)
