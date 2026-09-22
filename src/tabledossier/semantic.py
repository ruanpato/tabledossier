"""Observed formats, JSON shape and candidate roles.

Part of the embedded runtime (standard library only).

Three concepts are kept apart:

* the *physical type* declared by the source (for example ``string``);
* the *observed format* of sampled values (UUID, JSON object, ISO date...);
* a *candidate role* (identifier, categorical) suggested by measurements.

None of them establishes business meaning, which requires source comments or
human annotations. Detectors use real parsing where it matters (JSON, dates)
and report ``unknown``, ``mixed``, ``ambiguous`` or ``insufficient_data``
instead of guessing. Sampled values are transient: they are inspected inside
the execution environment and never persisted unless the value policy
allow-lists a column.
"""

import datetime
import json
import math
import re
import statistics
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit

from tabledossier.metrics import metric_value

FORMAT_DETECTORS = (
    "json_object",
    "json_array",
    "uuid",
    "numeric_string",
    "boolean_string",
    "iso_date",
    "iso_timestamp",
    "url",
    "email_candidate",
)
_SEM_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SEM_NUMERIC = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")
_SEM_BOOLEAN = frozenset({"true", "false", "t", "f", "yes", "no", "y", "n", "0", "1"})
_SEM_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SEM_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)(?:[.,](\d{1,9}))?"
    r"(Z|[+-]\d{2}(?::?\d{2})?)?$"
)
_SEM_URL = re.compile(r"^(?:https?|ftp)://[^\s/?#]+[^\s]*$", re.IGNORECASE)
_SEM_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SEM_KEY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-. ]{0,63}$")
MAX_LISTED_JSON_KEYS = 50


def _sem_reject_constant(token: str) -> Any:
    raise ValueError(f"non-standard JSON constant {token}")


def parse_json_text(text: str) -> tuple[bool, Any]:
    """Parse strict JSON (``NaN``/``Infinity`` rejected); return ``(ok, value)``."""
    try:
        return True, json.loads(text, parse_constant=_sem_reject_constant)
    except (ValueError, RecursionError):
        return False, None


def _sem_is_timestamp(value: str) -> tuple[bool, bool]:
    """Return ``(is_iso_timestamp, has_offset)``."""
    match = _SEM_TIMESTAMP.match(value)
    if not match:
        return False, False
    day, clock, fraction, offset = match.groups()
    if len(clock) == 5:
        clock += ":00"
    text = f"{day}T{clock}"
    if fraction:
        text += "." + fraction[:6].ljust(6, "0")
    if offset:
        if offset == "Z":
            text += "+00:00"
        elif len(offset) == 3:
            text += offset + ":00"
        elif ":" not in offset:
            text += offset[:3] + ":" + offset[3:]
        else:
            text += offset
    try:
        datetime.datetime.fromisoformat(text)
    except ValueError:
        return False, False
    return True, bool(offset)


def detect_formats(value: str) -> set[str]:
    """Return the set of detector names matching ``value`` (exact, unstripped)."""
    found: set[str] = set()
    stripped = value.strip()
    if stripped[:1] in ("{", "["):
        ok, parsed = parse_json_text(stripped)
        if ok and isinstance(parsed, dict):
            found.add("json_object")
        elif ok and isinstance(parsed, list):
            found.add("json_array")
    if _SEM_UUID.match(value):
        found.add("uuid")
    if _SEM_NUMERIC.match(value):
        found.add("numeric_string")
    if value.casefold() in _SEM_BOOLEAN:
        found.add("boolean_string")
    if _SEM_DATE.match(value):
        try:
            datetime.date.fromisoformat(value)
            found.add("iso_date")
        except ValueError:
            pass
    elif _sem_is_timestamp(value)[0]:
        found.add("iso_timestamp")
    if _SEM_URL.match(value) and urlsplit(value).netloc:
        found.add("url")
    if len(value) <= 254 and _SEM_EMAIL.match(value):
        found.add("email_candidate")
    return found


def wilson_interval(successes: int, trials: int, level: float = 0.95) -> dict[str, Any] | None:
    """Return the Wilson score interval for an observed proportion.

    The interval describes uncertainty of the *observed* proportion under an
    independent-sampling assumption. It does not correct sampling bias (a
    prefix sample is not random) and says nothing about business meaning.
    """
    if trials <= 0:
        return None
    z = statistics.NormalDist().inv_cdf(0.5 + level / 2)
    p = successes / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denominator
    return {
        "method": "wilson_score",
        "level": level,
        "low": max(0.0, center - half),
        "high": min(1.0, center + half),
    }


def sample_limitations(sample_method: str, eligible: int) -> list[str]:
    """Return the standard limitation statements for sample-based inference."""
    notes = [
        f"Inferred from {eligible} transient sampled value(s); not guaranteed for the whole "
        "population.",
        "Confidence intervals assume independent sampling; they do not remove sampling bias "
        "and do not establish business meaning.",
    ]
    if sample_method == "prefix":
        notes.append(
            "The prefix sample returns the first rows produced by the engine and may be biased."
        )
    return notes


def infer_format(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    sample_method: str,
    semantic_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Infer the observed format of sampled string values.

    ``values`` are complete (non-truncated, non-null) sampled values. Empty and
    whitespace-only values are excluded from the eligible denominator.
    """
    blank = sum(1 for value in values if value.strip() == "")
    eligible_values = [value for value in values if value.strip() != ""]
    eligible = len(eligible_values)
    min_obs = semantic_config["min_observations"]
    detect = semantic_config["detect_threshold"]
    mixed = semantic_config["mixed_threshold"]
    level = semantic_config["confidence_level"]
    counts: Counter[str] = Counter()
    with_offset = 0
    for value in eligible_values:
        formats = detect_formats(value)
        counts.update(formats)
        if "iso_timestamp" in formats and _sem_is_timestamp(value)[1]:
            with_offset += 1
    candidates = []
    for name in FORMAT_DETECTORS:
        matches = counts.get(name, 0)
        if matches == 0 or eligible == 0:
            continue
        candidate: dict[str, Any] = {
            "format": name,
            "matches": matches,
            "match_ratio": matches / eligible,
            "interval": wilson_interval(matches, eligible, level),
        }
        if name == "iso_timestamp":
            candidate["with_offset"] = with_offset
        candidates.append(candidate)
    candidates.sort(key=lambda item: (-item["matches"], FORMAT_DETECTORS.index(item["format"])))
    result: dict[str, Any] = {
        "status": "unknown",
        "format": None,
        "scope": "sample",
        "eligible_observations": eligible,
        "excluded": {"sql_null": sql_nulls, "truncated": truncated, "blank": blank},
        "min_observations": min_obs,
        "thresholds": {"detect": detect, "mixed": mixed},
        "candidates": candidates,
        "method": "rule-based detectors with strict parsing (JSON, ISO dates) on sampled values",
        "limitations": sample_limitations(sample_method, eligible),
    }
    if eligible < min_obs:
        result["status"] = "insufficient_data"
        result["reason"] = f"{eligible} eligible observation(s); at least {min_obs} required"
        return result
    strong = [item for item in candidates if item["match_ratio"] >= detect]
    if len(strong) > 1:
        result["status"] = "ambiguous"
        result["reason"] = "several formats match: " + ", ".join(item["format"] for item in strong)
    elif strong:
        result["status"] = "detected"
        result["format"] = strong[0]["format"]
    elif candidates and candidates[0]["match_ratio"] >= mixed:
        result["status"] = "mixed"
        result["reason"] = "values match formats only partially"
    return result


def _sem_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def is_json_like(values: list[str], threshold: float = 0.5) -> bool:
    """Return True when at least ``threshold`` of non-blank values start like JSON."""
    eligible = [value.lstrip() for value in values if value.strip()]
    if not eligible:
        return False
    starts = sum(1 for value in eligible if value[:1] in ("{", "["))
    return starts / len(eligible) >= threshold


def json_shape(
    values: list[str],
    *,
    sql_nulls: int,
    truncated: int,
    include_key_names: bool,
    sample_method: str,
) -> dict[str, Any]:
    """Classify sampled values by real JSON parsing and summarize their shape.

    SQL NULLs and values truncated by the sampling policy are excluded (they
    are reported separately and are never counted as invalid JSON).
    """
    counts = {"object": 0, "array": 0, "scalar": 0, "json_null_literal": 0, "invalid": 0}
    key_presence: Counter[str] = Counter()
    key_types: dict[str, Counter[str]] = {}
    key_sets: Counter[tuple[str, ...]] = Counter()
    for value in values:
        ok, parsed = parse_json_text(value.strip()) if value.strip() else (False, None)
        if not ok:
            counts["invalid"] += 1
        elif parsed is None:
            counts["json_null_literal"] += 1
        elif isinstance(parsed, dict):
            counts["object"] += 1
            key_sets[tuple(sorted(parsed))] += 1
            for key, item in parsed.items():
                key_presence[key] += 1
                key_types.setdefault(key, Counter())[_sem_json_type(item)] += 1
        elif isinstance(parsed, list):
            counts["array"] += 1
        else:
            counts["scalar"] += 1
    eligible = len(values)
    kinds_present = sum(
        1 for name in ("object", "array", "scalar", "json_null_literal") if counts[name]
    )
    conflicting = sum(
        1 for types in key_types.values() if len([t for t in types if t != "null"]) > 1
    )
    shape: dict[str, Any] = {
        "scope": "sample",
        "eligible_observations": eligible,
        "excluded": {"sql_null": sql_nulls, "truncated": truncated},
        "counts": counts,
        "valid_ratio": (eligible - counts["invalid"]) / eligible if eligible else None,
        "heterogeneity": {
            "top_level_kinds_present": kinds_present,
            "distinct_object_key_sets": len(key_sets),
            "keys_with_conflicting_types": conflicting,
        },
        "keys": None,
        "keys_omitted_reason": None,
        "limitations": [
            *sample_limitations(sample_method, eligible),
            "Shape inferred from the sample is not a complete or guaranteed schema.",
            "Values truncated by sampling.max_value_chars are excluded, never counted as invalid.",
        ],
    }
    if not key_presence:
        shape["keys_omitted_reason"] = "no JSON objects observed"
    elif not include_key_names:
        shape["keys_omitted_reason"] = "value policy (json_key_names = redact)"
    elif len(key_presence) > MAX_LISTED_JSON_KEYS:
        shape["keys_omitted_reason"] = (
            f"{len(key_presence)} distinct keys (more than {MAX_LISTED_JSON_KEYS}); "
            "objects look map-like, so key names are not listed"
        )
    elif not all(_SEM_KEY_NAME.match(key) for key in key_presence):
        shape["keys_omitted_reason"] = "some keys do not look like structural field names"
    else:
        objects = counts["object"]
        shape["keys"] = [
            {
                "key": key,
                "present_in": present,
                "presence_ratio": present / objects,
                "types": dict(sorted(key_types[key].items())),
            }
            for key, present in sorted(key_presence.items(), key=lambda item: (-item[1], item[0]))
        ]
    return shape


def value_concentration(values: list[str]) -> dict[str, Any]:
    """Summarize how concentrated sampled values are, without exposing labels."""
    counter = Counter(values)
    total = len(values)
    top = [count for _, count in counter.most_common(5)]
    return {
        "scope": "sample",
        "accuracy": "exact_for_sample",
        "observations": total,
        "sample_distinct_count": len(counter),
        "top_1_share": top[0] / total if total else None,
        "top_5_share": sum(top) / total if total else None,
        "note": "Exact for the sample only; not extrapolated to the population.",
    }


def value_examples(values: list[str], *, max_examples: int, max_chars: int) -> dict[str, Any]:
    """Return the most frequent sampled values (only for allow-listed columns)."""
    counter = Counter(values)
    examples = []
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[
        :max_examples
    ]:
        examples.append(
            {"value": value[:max_chars], "sample_count": count, "truncated": len(value) > max_chars}
        )
    return {
        "scope": "sample",
        "policy": "allow-listed by value_policy.example_columns",
        "values": examples,
    }


def candidate_roles(
    type_kind: str,
    metrics: Iterable[Mapping[str, Any]],
    observed_format: Mapping[str, Any] | None,
    thresholds: Mapping[str, Any],
    physical_type: str = "",
) -> list[dict[str, Any]]:
    """Suggest candidate roles from measured metrics and observed format.

    Identifier candidates are limited to integers, strings and decimals with
    scale 0, with few nulls and an approximate distinct count close to the
    number of non-null values.
    """
    metric_list = list(metrics)
    roles: list[dict[str, Any]] = []
    non_null = metric_value(metric_list, "non_null_count")
    distinct = metric_value(metric_list, "approx_distinct_count")
    null_ratio = metric_value(metric_list, "null_ratio")
    fmt = (
        (observed_format or {}).get("format")
        if (observed_format or {}).get("status") == "detected"
        else None
    )
    scale = re.search(r"decimal\(\s*\d+\s*,\s*(\d+)\s*\)", physical_type)
    key_like = type_kind in ("integer", "string") or (
        type_kind == "decimal" and scale is not None and scale.group(1) == "0"
    )
    if isinstance(non_null, int) and isinstance(distinct, int) and non_null > 0:
        ratio = distinct / non_null
        if (
            key_like
            and non_null >= thresholds["identifier_min_rows"]
            and ratio >= thresholds["identifier_min_distinct_ratio"]
            and isinstance(null_ratio, (int, float))
            and null_ratio <= thresholds["identifier_max_null_ratio"]
        ):
            roles.append(
                {
                    "role": "identifier_candidate",
                    "evidence": [
                        {"metric": "approx_distinct_count", "value": distinct},
                        {"metric": "non_null_count", "value": non_null},
                        {"metric": "null_ratio", "value": null_ratio},
                        *([{"observed_format": fmt}] if fmt else []),
                    ],
                    "limitations": (
                        "Approximate distinct counts do not prove uniqueness or a primary key; "
                        "exact validation requires an additional read (deep.uniqueness at the "
                        "deep level)."
                    ),
                }
            )
        if (
            type_kind in ("string", "integer")
            and non_null >= thresholds["categorical_min_rows"]
            and 2 <= distinct <= thresholds["categorical_max_distinct"]
        ):
            roles.append(
                {
                    "role": "categorical_candidate",
                    "evidence": [
                        {"metric": "approx_distinct_count", "value": distinct},
                        {"metric": "non_null_count", "value": non_null},
                    ],
                    "limitations": (
                        "Few distinct values in the analysed scope; the set of allowed values "
                        "must be defined by a data owner."
                    ),
                }
            )
    if fmt in ("json_object", "json_array"):
        roles.append(
            {
                "role": "json_document_candidate",
                "evidence": [{"observed_format": fmt}],
                "limitations": "Based on a sample; see json_profile for validity and shape.",
            }
        )
    return roles
