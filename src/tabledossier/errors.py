"""Sanitized error records.

Part of the embedded runtime (standard library only). Engine messages can
echo data values (for example cast errors quote the offending value), so only
the first line is kept, quoted literals and URIs are replaced, and the length
is bounded. Backtick-quoted identifiers are kept because they name objects the
user asked to profile.
"""

import re
from typing import Any

_ER_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_ER_URI = re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://\S+")


def sanitize_message(text: str, limit: int = 500) -> str:
    """Return the first line of ``text`` with quoted literals and URIs removed."""
    stripped = (text or "").strip()
    first = stripped.splitlines()[0] if stripped else ""
    first = _ER_URI.sub("<uri>", first)
    first = _ER_QUOTED.sub("<redacted>", first)
    return first[:limit]


def error_record(exc: BaseException, stage: str) -> dict[str, Any]:
    """Return a sanitized error record (stage, class, engine condition, message)."""
    condition = None
    for attribute in ("getCondition", "getErrorClass"):
        getter = getattr(exc, attribute, None)
        if callable(getter):
            try:
                condition = getter()
            except Exception:  # noqa: BLE001 - optional metadata only
                condition = None
            if condition:
                break
    message = sanitize_message(str(exc)) or type(exc).__name__
    return {
        "stage": stage,
        "error_class": type(exc).__name__,
        "condition": condition,
        "message": message,
    }
