"""JSON serialization conventions shared by the CLI and the notebook runtime.

This module is part of the embedded runtime: it is copied verbatim (minus
intra-package imports) into generated notebooks, so it must only use the
Python standard library.

Conventions (see ``docs/contract.md``):

* integers are JSON integers (consumers should parse them with arbitrary
  precision when values may exceed 2**53);
* decimals are JSON strings holding the canonical ``str(Decimal)`` form, so
  precision and scale are never lost;
* finite floats are JSON numbers; ``NaN``, ``Infinity`` and ``-Infinity`` are
  the JSON strings ``"NaN"``, ``"Infinity"`` and ``"-Infinity"``;
* dates are ``YYYY-MM-DD`` strings; timestamps are ISO 8601 strings, with an
  offset when the value is an instant and without one for local
  (timezone-less) timestamps.
"""

import datetime
import hashlib
import json
import math
from decimal import Decimal
from typing import Any

FLOAT_SPECIALS = ("NaN", "Infinity", "-Infinity")


def canonical_json(obj: Any) -> str:
    """Return a deterministic, compact JSON encoding used for fingerprints."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def pretty_json(obj: Any) -> str:
    """Return human-readable JSON (insertion order kept) ending with a newline."""
    return json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False) + "\n"


def fingerprint(obj: Any) -> str:
    """Return ``sha256:<hex>`` of the canonical JSON encoding of ``obj``."""
    return "sha256:" + hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def text_fingerprint(text: str) -> str:
    """Return ``sha256:<hex>`` of UTF-8 ``text``."""
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def short_hash(obj: Any, length: int = 12) -> str:
    """Return a short, stable hexadecimal digest of ``obj`` (for identifiers)."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()[:length]


def format_utc(moment: datetime.datetime) -> str:
    """Format an aware datetime as ``YYYY-MM-DDTHH:MM:SS.mmmZ`` in UTC."""
    if moment.tzinfo is None:
        raise ValueError("format_utc requires a timezone-aware datetime")
    utc = moment.astimezone(datetime.timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def utc_now() -> datetime.datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


def encode_float(value: float) -> float | str:
    """Encode a float, mapping non-finite values to their string markers."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def encode_scalar(value: Any) -> tuple[Any, str]:
    """Encode a Python scalar into ``(json_value, value_type)``.

    ``value_type`` is one of ``boolean``, ``integer``, ``float``, ``decimal``,
    ``string``, ``date``, ``timestamp`` or ``timestamp_ntz``.
    """
    if isinstance(value, bool):
        return value, "boolean"
    if isinstance(value, int):
        return value, "integer"
    if isinstance(value, float):
        return encode_float(value), "float"
    if isinstance(value, Decimal):
        if value.is_nan():
            return "NaN", "decimal"
        return str(value), "decimal"
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.isoformat(), "timestamp_ntz"
        return value.isoformat().replace("+00:00", "Z"), "timestamp"
    if isinstance(value, datetime.date):
        return value.isoformat(), "date"
    if isinstance(value, str):
        return value, "string"
    raise TypeError(f"unsupported scalar type for encoding: {type(value).__name__}")
