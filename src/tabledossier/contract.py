"""Profile and annotation validation: version compatibility, schema and invariants.

Part of the embedded runtime (standard library only). The schema itself is
passed in by the caller; this module adds the cross-field invariants that JSON
Schema cannot express (status consistency, denominators, references).
"""

from collections.abc import Iterator, Mapping
from typing import Any

from tabledossier.paths import IdentifierError, parse_display_path, parse_table_identifier
from tabledossier.schemacheck import schema_errors

PROFILE_KIND = "tabledossier.profile"
# The notebook writes the latest version; readers (CLI, renderer) accept every listed one.
# 1.1 only adds to 1.0 (deep level, element fields, JSON paths, deep coverage).
PROFILE_SCHEMA_VERSION = "1.1"
SUPPORTED_PROFILE_VERSIONS = ("1.0", "1.1")
ANNOTATIONS_KIND = "tabledossier.annotations"
SUPPORTED_ANNOTATION_VERSIONS = ("1.0",)
COUNT_METRICS_WITH_ROW_DENOMINATOR = ("null_count", "non_null_count")


def version_error(
    document: Any, kind: str, version_key: str, supported: tuple[str, ...]
) -> str | None:
    """Return a clear message when ``document`` is not a supported version of ``kind``."""
    if not isinstance(document, Mapping):
        return f"expected a JSON object for {kind}"
    if document.get("kind") != kind:
        return f"not a {kind} document (kind = {document.get('kind')!r})"
    version = document.get(version_key)
    if version not in supported:
        return (
            f"{kind} {version_key} {version!r} is not supported by this TableDossier "
            f"version (supported: {', '.join(supported)}); use a matching tabledossier release"
        )
    return None


def _ct_nodes(nodes: list[Mapping[str, Any]]) -> Iterator[Mapping[str, Any]]:
    for node in nodes:
        yield node
        yield from _ct_nodes(node.get("children", []))


def _ct_metric_errors(metric: Mapping[str, Any], where: str, row_count: int | None) -> list[str]:
    errors: list[str] = []
    if metric.get("status") != "measured":
        return errors
    value = metric.get("value")
    if metric.get("value_type") == "ratio":
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 1:
            errors.append(f"{where}: ratio value must be a number between 0 and 1")
        if not metric.get("denominator"):
            errors.append(f"{where}: a measured ratio requires a positive denominator")
    is_count = metric.get("value_type") == "integer" and metric.get("unit") in (
        "rows",
        "values",
        "elements",
        "entries",
        "documents",
    )
    if is_count and isinstance(value, int) and value < 0:
        errors.append(f"{where}: counts cannot be negative")
    if (
        metric.get("name") in COUNT_METRICS_WITH_ROW_DENOMINATOR
        and metric.get("unit") == "rows"
        and metric.get("source") == "aggregate"
        and isinstance(row_count, int)
        and isinstance(value, int)
        and value > row_count
    ):
        errors.append(f"{where}: {metric['name']} exceeds the row count in scope")
    return errors


def profile_invariant_errors(profile: Mapping[str, Any]) -> list[str]:
    """Return violations of cross-field invariants of a schema-valid profile."""
    errors: list[str] = []
    run = profile["run"]
    if run["finished_at"] < run["started_at"]:
        errors.append("$.run: finished_at is earlier than started_at")
    statuses = [table["status"] for table in profile["tables"]]
    if statuses:
        if all(status == "succeeded" for status in statuses):
            expected = "succeeded"
        elif all(status == "failed" for status in statuses):
            expected = "failed"
        else:
            expected = "partial"
        if run["status"] != expected:
            errors.append(
                f"$.run.status: {run['status']!r} is inconsistent with table statuses (expected "
                f"{expected!r})"
            )
    summary = profile["summary"]
    counts = {
        "tables_total": len(statuses),
        "tables_succeeded": statuses.count("succeeded"),
        "tables_partial": statuses.count("partial"),
        "tables_failed": statuses.count("failed"),
    }
    for key, value in counts.items():
        if summary[key] != value:
            errors.append(f"$.summary.{key}: {summary[key]} does not match the tables ({value})")

    table_ids: set[str] = set()
    for index, table in enumerate(profile["tables"]):
        where = f"$.tables[{index}]"
        if table["table_id"] in table_ids:
            errors.append(f"{where}: duplicate table_id {table['table_id']}")
        table_ids.add(table["table_id"])
        if table["status"] == "failed" and not table["errors"]:
            errors.append(f"{where}: a failed table must report at least one error")
        schema_ids: set[str] = set()
        for node in _ct_nodes((table.get("schema") or {}).get("fields", [])):
            if node["field_id"] in schema_ids:
                errors.append(f"{where}.schema: duplicate field_id {node['field_id']}")
            schema_ids.add(node["field_id"])
        row_count = None
        for metric in table["table_metrics"]:
            if (
                metric["name"] == "row_count"
                and metric["status"] == "measured"
                and metric["source"] == "aggregate"
            ):
                row_count = metric["value"]
        for m_index, metric in enumerate(table["table_metrics"]):
            errors.extend(_ct_metric_errors(metric, f"{where}.table_metrics[{m_index}]", None))
        profile_ids: set[str] = set()
        for f_index, field in enumerate(table["field_profiles"]):
            f_where = f"{where}.field_profiles[{f_index}]"
            if field["field_id"] not in schema_ids:
                errors.append(f"{f_where}: field_id {field['field_id']} is not in the schema tree")
            if field["field_id"] in profile_ids:
                errors.append(f"{f_where}: duplicate field profile")
            profile_ids.add(field["field_id"])
            names = [metric["name"] for metric in field["metrics"]]
            if len(names) != len(set(names)):
                errors.append(f"{f_where}: duplicate metric names")
            for m_index, metric in enumerate(field["metrics"]):
                errors.extend(_ct_metric_errors(metric, f"{f_where}.metrics[{m_index}]", row_count))
            if field.get("element_context"):
                errors.extend(_ct_element_errors(field, f_where))
            if field.get("json_paths"):
                errors.extend(_ct_json_path_errors(field["json_paths"], f"{f_where}.json_paths"))
        for d_index, finding in enumerate(table["findings"]):
            if finding["field_id"] not in profile_ids:
                errors.append(
                    f"{where}.findings[{d_index}]: unknown field_id {finding['field_id']}"
                )
    return errors


def _ct_element_errors(field: Mapping[str, Any], where: str) -> list[str]:
    """Element fields count elements or entries of a collection, never rows."""
    errors: list[str] = []
    context = field["element_context"]
    kinds = [segment["kind"] for segment in field["path"]]
    if kinds.count("field") == len(kinds):
        errors.append(f"{where}: element_context on a field outside any collection")
    total = None
    for metric in field["metrics"]:
        if metric["name"] == "element_count" and metric["status"] == "measured":
            total = metric["value"]
    for m_index, metric in enumerate(field["metrics"]):
        if "rows" in (metric.get("unit"), metric.get("denominator_unit")):
            errors.append(f"{where}.metrics[{m_index}]: element metrics never count rows")
        if metric.get("denominator_unit") in ("elements", "entries") and (
            metric["denominator_unit"] != context["unit"]
        ):
            errors.append(f"{where}.metrics[{m_index}]: denominator unit differs from the element")
        if (
            metric["name"] in ("null_count", "non_null_count")
            and metric["status"] == "measured"
            and isinstance(total, int)
            and isinstance(metric["value"], int)
            and metric["value"] > total
        ):
            errors.append(f"{where}.metrics[{m_index}]: exceeds element_count")
    return errors


def _ct_json_path_errors(catalog: Mapping[str, Any], where: str) -> list[str]:
    errors: list[str] = []
    documents = catalog["documents"]
    paths = catalog.get("paths")
    if paths is not None and len(paths) != catalog["paths_listed"]:
        errors.append(f"{where}: paths_listed does not match the listed paths")
    for index, item in enumerate(paths or []):
        if item["present_in"] > documents:
            errors.append(f"{where}.paths[{index}]: present in more documents than sampled")
        ratio = item.get("presence_ratio")
        if ratio is not None and not 0 <= ratio <= 1:
            errors.append(f"{where}.paths[{index}]: presence_ratio must be between 0 and 1")
    return errors


def validate_profile(profile: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return every problem found in ``profile`` (empty list when valid)."""
    problem = version_error(profile, PROFILE_KIND, "schema_version", SUPPORTED_PROFILE_VERSIONS)
    if problem:
        return [problem]
    errors = schema_errors(profile, schema)
    if errors:
        return errors
    return profile_invariant_errors(profile)


def validate_annotations(document: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return every problem found in an annotations document."""
    problem = version_error(
        document, ANNOTATIONS_KIND, "annotations_version", SUPPORTED_ANNOTATION_VERSIONS
    )
    if problem:
        return [problem]
    errors = schema_errors(document, schema)
    if errors:
        return errors
    for key, table in document.get("tables", {}).items():
        try:
            parse_table_identifier(key)
        except IdentifierError as exc:
            errors.append(f"$.tables[{key!r}]: {exc}")
        for column in table.get("columns", {}):
            try:
                parse_display_path(column)
            except IdentifierError as exc:
                errors.append(f"$.tables[{key!r}].columns[{column!r}]: {exc}")
    for index, item in enumerate(document.get("relationships", [])):
        for end in ("from", "to"):
            try:
                parse_table_identifier(item[end]["table"])
            except IdentifierError as exc:
                errors.append(f"$.relationships[{index}].{end}.table: {exc}")
        if len(item["from"]["columns"]) != len(item["to"]["columns"]):
            errors.append(
                f"$.relationships[{index}]: 'from' and 'to' must list the same number of columns"
            )
    return errors
