"""Markdown and Mermaid documentation derived from a validated profile.

Part of the embedded runtime (standard library only): the notebook renders the
same documents that ``tabledossier render`` regenerates locally. Rendering
never recomputes metrics and never reads the source. Output is deterministic
(no rendering timestamp); documents state when the profile was *measured*.

All text coming from sources, configuration or annotations is treated as
content: it is escaped for Markdown/Mermaid and never interpreted.
"""

import re
from collections.abc import Iterable, Mapping
from typing import Any

from tabledossier.metrics import find_metric
from tabledossier.paths import parse_table_identifier, table_lookup_key
from tabledossier.planning import iter_nodes
from tabledossier.relationships import merge_relationships, provided_relationships

SCOPE_LABELS = {
    "full_snapshot": "all rows at a pinned snapshot",
    "filtered_snapshot": "filtered rows at a pinned snapshot",
    "full_table": "all rows (snapshot not pinned)",
    "filtered_table": "filtered rows (snapshot not pinned)",
    "sample": "transient sample",
    "table_metadata": "table metadata",
}
UNKNOWN_DESCRIPTION = "Unknown — no description in the source or in annotations."
_R_MD_SPECIAL = re.compile(r"([\\`*_\[\]#|~!])")
_R_CONTROL = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]+")
_R_MERMAID_UNSAFE = re.compile(r"[\"`<>{}\[\]|;#%\\]")
MAX_ERD_ATTRIBUTES = 60
ERD_LEFT = {"zero_or_one": "|o", "exactly_one": "||", "zero_or_more": "}o", "one_or_more": "}|"}
ERD_RIGHT = {"zero_or_one": "o|", "exactly_one": "||", "zero_or_more": "o{", "one_or_more": "|{"}


# --------------------------------------------------------------------------- escaping


def md_text(value: Any) -> str:
    """Escape arbitrary text for inline Markdown (safe inside table cells)."""
    text = _R_CONTROL.sub(" ", "" if value is None else str(value))
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _R_MD_SPECIAL.sub(r"\\\1", text).strip()


def md_code(value: Any) -> str:
    """Render text as an inline code span that is safe inside table cells."""
    text = _R_CONTROL.sub(" ", str(value)).replace("|", "\\|")
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * (longest + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def mermaid_text(value: Any) -> str:
    """Return single-line text with characters unsafe for Mermaid labels replaced."""
    text = _R_CONTROL.sub(" ", str(value))
    return _R_MERMAID_UNSAFE.sub("'", text).strip()


def mermaid_name(value: str, used: dict[str, str]) -> str:
    """Return a stable Mermaid-safe identifier, unique within ``used``."""
    base = re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_") or "x"
    if not base[0].isalpha():
        base = "n_" + base
    name = base
    counter = 2
    while name in used and used[name] != value:
        name = f"{base}_{counter}"
        counter += 1
    used[name] = value
    return name


def _r_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(" --- " for _ in headers) + "|"]
    lines += ["| " + " | ".join(cell if cell else " " for cell in row) + " |" for row in rows]
    return "\n".join(lines)


# --------------------------------------------------------------------------- formatting


def format_count(value: Any) -> str:
    """Format an integer count with thousands separators."""
    return f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else str(value)


def format_ratio(value: Any) -> str:
    """Format a ratio in [0, 1] as a percentage."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return str(value)
    if 0 < value < 0.0001:
        return "<0.01%"
    if 0.9999 < value < 1:
        return ">99.99%"
    return f"{value:.2%}"


def format_value(metric: Mapping[str, Any]) -> str:
    """Format a metric value for humans, or explain why there is none."""
    if metric.get("status") != "measured":
        reason = metric.get("reason") or ""
        return f"_{metric.get('status', 'unknown').replace('_', ' ')}_" + (
            f": {md_text(reason)}" if reason else ""
        )
    value = metric.get("value")
    value_type = metric.get("value_type")
    if value_type == "ratio":
        return format_ratio(value)
    if value_type == "quantiles" and isinstance(value, list):
        parts = [
            f"p{round(item['probability'] * 100):g}={_r_scalar(item['value'])}" for item in value
        ]
        return md_text(", ".join(parts))
    if value_type == "integer":
        return format_count(value)
    return md_text(_r_scalar(value))


def _r_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".6g")
    return str(value)


def _r_observed(observed: Mapping[str, Any] | None) -> str:
    if not observed:
        return "—"
    value = observed.get("value")
    name = str(observed.get("metric", ""))
    if isinstance(value, list):
        return md_text(
            ", ".join(
                f"{label} {item}"
                for label, item in zip(("min", "max"), value, strict=False)
                if item is not None
            )
        )
    if name.endswith("ratio") and isinstance(value, (int, float)) and not isinstance(value, bool):
        return format_ratio(value)
    return format_count(value) if isinstance(value, int) else md_text(value)


def _r_accuracy(metric: Mapping[str, Any]) -> str:
    if metric.get("status") != "measured":
        return ""
    return f"{metric.get('accuracy')}, {metric.get('source')}"


# --------------------------------------------------------------------------- annotations


def table_annotation(annotations: Mapping[str, Any] | None, key: str) -> dict[str, Any]:
    """Return the annotation entry for a table (matched case-insensitively)."""
    if not annotations:
        return {}
    wanted = table_lookup_key(parse_table_identifier(key))
    for name, entry in annotations.get("tables", {}).items():
        if table_lookup_key(parse_table_identifier(name)) == wanted:
            return dict(entry)
    return {}


def column_annotation(table_note: Mapping[str, Any], path: str) -> dict[str, Any]:
    """Return the annotation entry for a column display path."""
    columns = table_note.get("columns", {})
    if path in columns:
        return dict(columns[path])
    folded = path.casefold()
    for name, entry in columns.items():
        if name.casefold() == folded:
            return dict(entry)
    return {}


def all_relationships(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    """Return profile relationships plus relationships provided in annotations."""
    extra = provided_relationships((annotations or {}).get("relationships", []), "annotation")
    return merge_relationships(profile.get("relationships", []), extra)


# --------------------------------------------------------------------------- shared pieces


def provenance_header(profile: Mapping[str, Any], title: str) -> str:
    """Return the title and provenance block used by every Markdown document."""
    run = profile["run"]
    return "\n".join(
        [
            f"# {title}",
            "",
            f"> Generated by TableDossier {md_text(profile['tool']['version'])} from profile run "
            f"{md_code(run['run_id'])}.  ",
            f"> **Measured at:** {md_text(run['started_at'])} (UTC) · **Analysis level:** "
            f"`{run['analysis_level']}` · **Run status:** `{run['status']}`.  ",
            "> Re-rendering does not refresh measurements: every value below comes from that "
            "profile.",
            "",
        ]
    )


def _r_row_count(table: Mapping[str, Any]) -> str:
    metric = find_metric(table.get("table_metrics", []), "row_count")
    if metric is None:
        return "_not measured_"
    text = format_value(metric)
    if metric.get("status") == "measured" and metric.get("source") != "aggregate":
        text += f" ({md_text(metric.get('source'))}, {md_text(metric.get('accuracy'))})"
    return text


def _r_scope_text(table: Mapping[str, Any]) -> str:
    scope = table["scope"]
    consistency = table.get("consistency") or {}
    parts = [SCOPE_LABELS.get(scope["scope_label"], scope["scope_label"])]
    if consistency.get("mode") == "pinned_delta_version":
        parts.append(f"Delta version {consistency.get('delta_version')}")
    if scope.get("filters"):
        parts.append(f"{len(scope['filters'])} filter(s)")
    if scope.get("selected_columns") is not None:
        parts.append(f"{len(scope['selected_columns'])} selected column(s)")
    return md_text("; ".join(str(p) for p in parts))


def _r_filters(filters: Iterable[Mapping[str, Any]]) -> str:
    items = []
    for item in filters:
        column = item["column"] if isinstance(item["column"], str) else ".".join(item["column"])
        text = f"{column} {item['operator']}"
        if "value" in item:
            text += f" {item['value']!r}"
        items.append(md_code(text))
    return ", ".join(items) if items else "none"


def _r_anchor(table: Mapping[str, Any]) -> str:
    return f'<a id="{table["table_id"]}"></a>'


# --------------------------------------------------------------------------- overview


def render_overview(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``overview.md``: run, environment, tables, sampling and errors."""
    run = profile["run"]
    env = run["environment"]
    out = [provenance_header(profile, "Profile overview")]
    runtime = ", ".join(
        md_text(part)
        for part in (
            env.get("execution_context"),
            f"Spark {env['spark_version']}" if env.get("spark_version") else None,
            f"Databricks Runtime {env['databricks_runtime_version']}"
            if env.get("databricks_runtime_version")
            else None,
            f"Python {env.get('python_version')}",
        )
        if part
    )
    rows = [
        ["Run ID", md_code(run["run_id"])],
        ["Status", f"`{run['status']}`"],
        ["Analysis level", f"`{run['analysis_level']}`"],
        [
            "Started / finished (UTC)",
            f"{md_text(run['started_at'])} / {md_text(run['finished_at'])}",
        ],
        ["Duration", f"{run['duration_ms'] / 1000:.1f} s"],
        ["Execution environment", runtime],
        ["Session time zone", md_text(env.get("session_timezone") or "unknown")],
        ["Temporal reference instant", md_text(run["reference_time"])],
        ["Configuration fingerprint", md_code(run["config_fingerprint"])],
        [
            "Generated notebook",
            md_code(run["generation"]["generation_id"])
            if run["generation"].get("generation_id")
            else "not recorded",
        ],
        [
            "Purpose",
            md_text(run["purpose"]) + " _(configuration)_"
            if run.get("purpose")
            else "_not provided_",
        ],
    ]
    out += ["## Run", "", _r_table(["Item", "Value"], rows), ""]

    out += ["## Tables", ""]
    table_rows = []
    for table in profile["tables"]:
        summary = table.get("summary", {})
        checks = table.get("quality_checks", [])
        counts = {
            status: sum(1 for c in checks if c["status"] == status)
            for status in ("pass", "fail", "not_evaluated", "error")
        }
        consistency = (table.get("consistency") or {}).get("mode", "n/a")
        table_rows.append(
            [
                f"[{md_text(table['table_key'])}](data_dictionary.md#{table['table_id']})",
                f"`{table['status']}`",
                _r_row_count(table),
                _r_scope_text(table),
                md_text(consistency),
                format_count(summary.get("fields_profiled", 0)),
                format_count(summary.get("fields_omitted", 0)),
                format_count(len(table.get("findings", []))),
                "/".join(str(counts[s]) for s in ("pass", "fail", "not_evaluated", "error")),
            ]
        )
    if table_rows:
        out.append(
            _r_table(
                [
                    "Table",
                    "Status",
                    "Rows in scope",
                    "Scope",
                    "Consistency",
                    "Fields profiled",
                    "Fields omitted",
                    "Findings",
                    "Checks pass/fail/not evaluated/error",
                ],
                table_rows,
            )
        )
    else:
        out.append("No tables were part of this run.")
    out += [
        "",
        "Row counts are rows in the analysed scope (after filters). They are not counts of "
        "business entities: no deduplication is applied, including for historized tables.",
        "",
    ]

    exposure = profile["value_exposure"]
    out += [
        "## Sampling and value exposure",
        "",
        f"- Raw values persisted: **{'yes' if exposure['raw_values_persisted'] else 'no'}**.",
        f"- Aggregate extremes (min/max/mean/quantiles): `{exposure['aggregate_extremes']}`.",
        f"- JSON key names from samples: `{exposure['json_key_names']}`.",
    ]
    out += [f"- {md_text(note)}" for note in exposure.get("notes", [])]
    for table in profile["tables"]:
        sample = table.get("sample")
        if not sample:
            continue
        if sample["enabled"]:
            desc = (
                f"method `{sample['method']}`, {format_count(sample['rows_collected'])} row(s) "
                "collected "
                f"(limit {format_count(sample['max_rows'])}), stopped by "
                f"`{sample['stopped_reason']}`, "
                f"{format_count(sample['values_truncated'])} value(s) truncated"
            )
        else:
            desc = "not used — " + "; ".join(md_text(n) for n in sample.get("notes", [])[:1])
        out.append(f"- {md_text(table['table_key'])}: {desc}.")
    out.append("")

    capabilities = run.get("capabilities", {})
    if capabilities:
        out += ["## Engine capabilities detected", ""]
        out.append(
            _r_table(
                ["Capability", "Available", "Detail"],
                [
                    [md_code(name), "yes" if item["available"] else "no", md_text(item["detail"])]
                    for name, item in sorted(capabilities.items())
                ],
            )
        )
        out.append("")

    errors = [(table, error) for table in profile["tables"] for error in table.get("errors", [])]
    out += ["## Errors", ""]
    if errors:
        out.append(
            _r_table(
                ["Table", "Stage", "Error", "Message (sanitized)"],
                [
                    [
                        md_text(table["table_key"]),
                        md_text(error["stage"]),
                        md_code(error["condition"] or error["error_class"]),
                        md_text(error["message"]),
                    ]
                    for table, error in errors
                ],
            )
        )
    else:
        out.append("No errors were recorded.")
    out += [
        "",
        "## Files",
        "",
        "- `profile.json` — canonical profile (source of every document).",
        "- `data_dictionary.md` — tables, fields, types, descriptions and measurements.",
        "- `quality_report.md` — observed quality (DQR): checks, completeness, alerts, limits.",
        "- `relationships.md` and `erd.mmd` — known relationships and entity diagram.",
        "- `suggested_rules.json` — rule proposals (not applied).",
        "",
    ]
    return "\n".join(out)


# --------------------------------------------------------------------------- dictionary


def _r_description(node: Mapping[str, Any], note: Mapping[str, Any]) -> str:
    parts = []
    if note.get("description"):
        parts.append(f"{md_text(note['description'])} _(annotation)_")
    if node.get("comment"):
        parts.append(f"{md_text(node['comment'])} _(source comment)_")
    return "<br>".join(parts) if parts else f"_{UNKNOWN_DESCRIPTION}_"


def _r_nulls(profile_field: Mapping[str, Any] | None) -> str:
    if not profile_field or not profile_field.get("profiled"):
        return "—"
    metrics = profile_field["metrics"]
    count = find_metric(metrics, "null_count")
    ratio_metric = find_metric(metrics, "null_ratio")
    if count is None:
        return "—"
    if count.get("status") != "measured":
        return format_value(count)
    text = format_count(count["value"])
    if ratio_metric is not None and ratio_metric.get("status") == "measured":
        text += f" ({format_ratio(ratio_metric['value'])})"
    return text


def _r_distinct(profile_field: Mapping[str, Any] | None) -> str:
    if not profile_field or not profile_field.get("profiled"):
        return "—"
    metric = find_metric(profile_field["metrics"], "approx_distinct_count")
    if metric is None:
        return "—"
    if metric.get("status") != "measured":
        return format_value(metric)
    return "≈ " + format_count(metric["value"])


def _r_format(profile_field: Mapping[str, Any] | None) -> str:
    semantics = (profile_field or {}).get("semantics") or {}
    fmt = semantics.get("observed_format")
    if not fmt:
        return "—"
    if fmt["status"] == "detected":
        top = fmt["candidates"][0] if fmt["candidates"] else {}
        ratio = (
            f", {format_ratio(top['match_ratio'])} of {fmt['eligible_observations']}" if top else ""
        )
        return f"{md_code(fmt['format'])} (sample{ratio})"
    return f"_{fmt['status'].replace('_', ' ')}_"


def _r_roles(profile_field: Mapping[str, Any] | None) -> str:
    semantics = (profile_field or {}).get("semantics") or {}
    roles = [role["role"].replace("_", " ") for role in semantics.get("candidate_roles", [])]
    return md_text(", ".join(roles)) if roles else "—"


def _r_node_notes(
    node: Mapping[str, Any], field: Mapping[str, Any] | None, findings: list[str]
) -> str:
    notes = []
    if field is not None and not field.get("profiled") and field.get("omission_reason"):
        notes.append(f"not profiled ({field['omission_reason']})")
    if node.get("children_omitted"):
        notes.append(
            f"{node['children_omitted']} child field(s) omitted ({node['children_omitted_reason']})"
        )
    notes.extend(findings)
    return md_text("; ".join(notes)) if notes else ""


def render_data_dictionary(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``data_dictionary.md`` for every table in the profile."""
    out = [provenance_header(profile, "Data dictionary")]
    out += [
        "Descriptions come from human annotations or source comments, as labelled. When neither "
        "exists the description is unknown: TableDossier does not invent business definitions. "
        "*Declared nullable* is the source schema declaration; *Nulls* are observed in the "
        "analysed scope.",
        "",
    ]
    for table in profile["tables"]:
        note = table_annotation(annotations, table["table_key"])
        source = table.get("source") or {}
        out += [f"## {md_text(table['table_key'])}", "", _r_anchor(table), ""]
        description = []
        if note.get("description"):
            description.append(f"{md_text(note['description'])} _(annotation)_")
        if source.get("comment"):
            description.append(f"{md_text(source['comment'])} _(source comment)_")
        purpose = []
        if table.get("purpose"):
            purpose.append(f"{md_text(table['purpose'])} _(configuration)_")
        if note.get("purpose"):
            purpose.append(f"{md_text(note['purpose'])} _(annotation)_")
        consistency = table.get("consistency") or {}
        rows = [
            ["Description", "<br>".join(description) or f"_{UNKNOWN_DESCRIPTION}_"],
            ["Purpose", "<br>".join(purpose) or "_not provided_"],
            [
                "Owner",
                f"{md_text(note['owner'])} _(annotation)_"
                if note.get("owner")
                else "_not provided_",
            ],
            ["Tags", md_text(", ".join(note.get("tags", []))) or "—"],
            [
                "Source",
                md_text(
                    ", ".join(
                        str(v)
                        for v in (
                            source.get("source_type"),
                            source.get("table_type"),
                            source.get("provider"),
                        )
                        if v
                    )
                )
                or "—",
            ],
            ["Status", f"`{table['status']}`"],
            ["Rows in scope", _r_row_count(table)],
            ["Scope", _r_scope_text(table)],
            ["Filters", _r_filters(table["scope"].get("filters", []))],
            ["Consistency", md_text(consistency.get("guarantee", "not determined"))],
        ]
        out += [_r_table(["Property", "Value"], rows), ""]
        if table.get("errors"):
            out.append(
                "**Errors:** "
                + "; ".join(md_text(f"{e['stage']}: {e['message']}") for e in table["errors"])
            )
            out.append("")
        schema = table.get("schema")
        if not schema:
            out += ["_Schema not available for this table._", ""]
            continue
        fields = {item["field_id"]: item for item in table.get("field_profiles", [])}
        finding_codes: dict[str, list[str]] = {}
        for finding in table.get("findings", []):
            finding_codes.setdefault(finding["field_id"], []).append(
                finding["code"].replace("_", " ")
            )
        out += ["### Fields", ""]
        rows = []
        for node in iter_nodes(schema["fields"]):
            field = fields.get(node["field_id"])
            column_note = column_annotation(note, node["display_path"])
            nullable = {True: "yes", False: "no", None: "—"}[node.get("nullable")]
            rows.append(
                [
                    md_code(node["display_path"]),
                    md_code(node["type"]["physical_type"]),
                    nullable,
                    _r_description(node, column_note),
                    _r_nulls(field),
                    _r_distinct(field),
                    _r_format(field),
                    _r_roles(field),
                    _r_node_notes(node, field, finding_codes.get(node["field_id"], [])),
                ]
            )
        out.append(
            _r_table(
                [
                    "Field",
                    "Type",
                    "Declared nullable",
                    "Description",
                    "Nulls",
                    "Distinct",
                    "Observed format",
                    "Candidate roles",
                    "Notes",
                ],
                rows,
            )
        )
        out.append("")
        if schema.get("top_level_omitted"):
            out += [
                f"_{schema['top_level_omitted']} top-level column(s) beyond the field limit are "
                "not listed._",
                "",
            ]
        detailed = [
            field
            for field in table.get("field_profiles", [])
            if field.get("profiled") and field["metrics"]
        ]
        if detailed:
            out += ["### Field measurements", ""]
            for field in detailed:
                out += [f"#### {md_code(field['display_path'])}", ""]
                out.append(
                    _r_table(
                        ["Metric", "Value", "Accuracy, source", "Scope", "Denominator"],
                        [
                            [
                                md_code(metric["name"]),
                                format_value(metric),
                                md_text(_r_accuracy(metric)),
                                md_text(SCOPE_LABELS.get(metric["scope"], metric["scope"])),
                                (
                                    f"{format_count(metric['denominator'])} "
                                    f"{md_text(metric.get('denominator_unit') or '')}"
                                    if "denominator" in metric
                                    else ""
                                ),
                            ]
                            for metric in field["metrics"]
                        ],
                    )
                )
                json_profile = field.get("json_profile")
                if json_profile:
                    counts = json_profile["counts"]
                    out.append("")
                    out.append(
                        f"JSON shape (sample of {json_profile['eligible_observations']} value(s); "
                        f"{json_profile['excluded']['truncated']} truncated excluded): "
                        + ", ".join(f"{md_text(k)} {v}" for k, v in counts.items())
                        + "."
                    )
                    if json_profile.get("keys"):
                        keys = ", ".join(
                            f"{md_code(item['key'])} ({format_ratio(item['presence_ratio'])})"
                            for item in json_profile["keys"][:20]
                        )
                        out.append(f"Top-level keys: {keys}.")
                    elif json_profile.get("keys_omitted_reason"):
                        out.append(
                            f"Key names not listed: {md_text(json_profile['keys_omitted_reason'])}."
                        )
                concentration = field.get("concentration")
                if concentration and concentration.get("observations"):
                    out.append("")
                    out.append(
                        "Sample concentration (labels not shown): "
                        f"{concentration['sample_distinct_count']} "
                        f"distinct in {concentration['observations']} sampled value(s); most "
                        "frequent value "
                        f"share {format_ratio(concentration['top_1_share'])}."
                    )
                examples = field.get("examples")
                if examples and examples.get("values"):
                    out.append("")
                    out.append(
                        "Allow-listed sample values: "
                        + ", ".join(
                            f"{md_code(item['value'])} ×{item['sample_count']}"
                            for item in examples["values"]
                        )
                    )
                out.append("")
    return "\n".join(out)


# --------------------------------------------------------------------------- quality report


def render_quality_report(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``quality_report.md`` (observed Data Quality Report, not a certification)."""
    out = [provenance_header(profile, "Data Quality Report (observed)")]
    out += [
        "This report describes quality **observed** in the analysed scope. It is not a "
        "certification "
        "and computes no global quality score. It keeps four things apart: configured checks that "
        "were "
        "executed, descriptive measurements, heuristic alerts and proposed rules.",
        "",
        "## Summary",
        "",
    ]
    rows = []
    for table in profile["tables"]:
        checks = table.get("quality_checks", [])
        findings = table.get("findings", [])
        rows.append(
            [
                md_text(table["table_key"]),
                f"`{table['status']}`",
                _r_row_count(table),
                *[
                    str(sum(1 for c in checks if c["status"] == s))
                    for s in ("pass", "fail", "not_evaluated", "error")
                ],
                str(sum(1 for f in findings if f["severity"] == "warning")),
                str(sum(1 for f in findings if f["severity"] == "info")),
            ]
        )
    out += [
        _r_table(
            [
                "Table",
                "Status",
                "Rows in scope",
                "Checks pass",
                "fail",
                "not evaluated",
                "error",
                "Warnings",
                "Info",
            ],
            rows,
        )
        if rows
        else "No tables.",
        "",
    ]

    out += ["## 1. Configured checks (executed)", ""]
    check_rows = []
    for table in profile["tables"]:
        for check in table.get("quality_checks", []):
            observed = check.get("observed") or {}
            check_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(check["check_id"]),
                    md_code(check["target"]["column"]) if check["target"]["column"] else "table",
                    md_code(check["type"]),
                    md_text(", ".join(f"{k}={v}" for k, v in check["parameters"].items())),
                    _r_observed(observed),
                    f"**{check['status']}**",
                    md_text(check["message"]),
                ]
            )
    if check_rows:
        out.append(
            _r_table(
                ["Table", "Check", "Target", "Type", "Parameters", "Observed", "Status", "Message"],
                check_rows,
            )
        )
    else:
        out.append(
            "No checks were configured. Add `checks` under `table_options` to evaluate explicit "
            "rules."
        )
    out += [
        "",
        "`not_evaluated` means the metric needed by the check was not measured (for example an "
        "empty "
        "scope or the metadata level); it is neither a pass nor a failure.",
        "",
        "## 2. Completeness (descriptive)",
        "",
    ]
    completeness = []
    for table in profile["tables"]:
        for field in table.get("field_profiles", []):
            if not field.get("profiled"):
                continue
            nulls = find_metric(field["metrics"], "null_count")
            ratio_metric = find_metric(field["metrics"], "null_ratio")
            parent = find_metric(field["metrics"], "null_count_parent_present")
            if nulls is None:
                continue
            completeness.append(
                [
                    md_text(table["table_key"]),
                    md_code(field["display_path"]),
                    format_value(nulls),
                    format_value(ratio_metric) if ratio_metric else "—",
                    format_value(parent) if parent else "—",
                    md_text(SCOPE_LABELS.get(nulls["scope"], nulls["scope"])),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Field", "Null count", "Null ratio", "Nulls while parent present", "Scope"],
            completeness,
        )
        if completeness
        else "No completeness measurements (metadata level or no profiled fields)."
    )
    out += [
        "",
        "For nested fields, *Null count* includes rows where a parent struct is null; *Nulls while "
        "parent present* counts only rows whose parent exists.",
        "",
        "## 3. Heuristic alerts (not failures)",
        "",
    ]
    alert_rows = []
    for table in profile["tables"]:
        for finding in table.get("findings", []):
            alert_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(finding["display_path"]),
                    f"`{finding['severity']}`",
                    md_text(finding["title"]),
                    md_text(finding["message"]),
                    md_text(finding["severity_reason"]),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Field", "Severity", "Alert", "Observation", "Why this severity"], alert_rows
        )
        if alert_rows
        else "No heuristic alerts."
    )
    out += [
        "",
        "Alerts are exploratory signals with configurable thresholds. High nullity, constancy or "
        "heavy "
        "tails can be legitimate; alerts never fail a pipeline.",
        "",
        "## 4. Proposed rules (not applied)",
        "",
    ]
    rule_rows = []
    for table in profile["tables"]:
        for rule in table.get("suggested_rules", []):
            rule_rows.append(
                [
                    md_text(rule["table"]),
                    md_code(rule["column"]) if rule["column"] else "table",
                    md_code(rule["rule_type"]),
                    md_text(rule["rationale"]),
                ]
            )
    out.append(
        _r_table(["Table", "Field", "Rule", "Rationale"], rule_rows)
        if rule_rows
        else "No rules proposed."
    )
    out += [
        "",
        "Proposals require review by a data owner; see `suggested_rules.json`.",
        "",
        "## 5. Dimensions this profile does not establish",
        "",
        "- **Validity** is only evaluated through configured checks. Formats observed on samples "
        "are hypotheses.",
        "- **Uniqueness** is only approximated (HyperLogLog-based distinct counts); no exact "
        "uniqueness check ran.",
        "- **Referential integrity** is not verified: declared or provided relationships were not "
        "validated.",
        "- **Timeliness** needs a time column and an agreed SLA; `after_reference_count` is "
        "descriptive only.",
        "- **Business accuracy** cannot be inferred from distributions.",
        "",
        "## 6. Limitations",
        "",
    ]
    for table in profile["tables"]:
        lines = []
        consistency = table.get("consistency") or {}
        if consistency:
            lines.append(f"Consistency: {consistency.get('guarantee')}")
        sample = table.get("sample")
        if sample and sample.get("enabled"):
            lines.append(
                f"Sample: {sample['method']} "
                f"({'potentially biased' if sample.get('biased') else 'random'}), "
                f"{sample['rows_collected']} row(s), {sample['values_truncated']} truncated "
                "value(s)"
            )
        omissions = table.get("omissions", [])
        if omissions:
            lines.append(
                f"{len(omissions)} omission record(s): "
                + ", ".join(sorted({o["reason"] for o in omissions}))
            )
        for item in table.get("unsupported", []):
            lines.append(f"Unsupported: {item['capability']} — {item['detail']}")
        for error in table.get("errors", []):
            lines.append(f"Error during {error['stage']}: {error['message']}")
        lines.extend(table.get("notes", []))
        out.append(f"**{md_text(table['table_key'])}**")
        out.append("")
        out += [f"- {md_text(line)}" for line in lines] or ["- none recorded"]
        out.append("")
    return "\n".join(out)


# --------------------------------------------------------------------------- relationships / ERD


def _r_cardinality(rel: Mapping[str, Any]) -> str:
    card = rel.get("cardinality")
    if not card:
        return "_not asserted_"
    return md_text(f"{card['from']} → {card['to']} (provided)")


def render_relationships(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> str:
    """Render ``relationships.md``: known relationships, declared keys and hypotheses."""
    relationships = all_relationships(profile, annotations)
    out = [provenance_header(profile, "Relationships")]
    out += [
        "Relationships come only from declared constraints or from people (configuration or "
        "annotations). TableDossier never infers a relationship from column names, and it did not "
        "validate any relationship against the data.",
        "",
        "## Known relationships",
        "",
    ]
    if relationships:
        out.append(
            _r_table(
                [
                    "Relationship",
                    "Origin",
                    "From",
                    "To",
                    "Cardinality",
                    "Enforcement",
                    "Validation",
                    "Scope",
                ],
                [
                    [
                        md_code(rel["name"]),
                        md_text(rel["origin"]),
                        f"{md_text(rel['from']['table'])} "
                        f"({md_text(', '.join(rel['from']['columns']))})",
                        f"{md_text(rel['to']['table'])} "
                        f"({md_text(', '.join(rel['to']['columns']))})",
                        _r_cardinality(rel),
                        md_text(rel["enforcement"]),
                        md_text(rel["validation"]),
                        md_text(rel["scope"]),
                    ]
                    for rel in relationships
                ],
            )
        )
        uncharted = [rel for rel in relationships if not rel.get("cardinality")]
        if uncharted:
            used: dict[str, str] = {}
            out += [
                "",
                "### References without asserted cardinality",
                "",
                "These references are not drawn in `erd.mmd` because an ER edge requires a "
                "cardinality "
                "that the evidence does not determine. This auxiliary diagram only shows "
                "direction.",
                "",
                "```mermaid",
                "flowchart LR",
            ]
            for rel in uncharted:
                left = mermaid_name(rel["from"]["table"], used)
                right = mermaid_name(rel["to"]["table"], used)
                label = mermaid_text(
                    f"{rel['origin'].replace('_', ' ')}: {', '.join(rel['from']['columns'])} to "
                    f"{', '.join(rel['to']['columns'])}"
                )
                out.append(
                    f'    {left}["{mermaid_text(rel["from"]["table"])}"] -. "{label}" .-> '
                    f'{right}["{mermaid_text(rel["to"]["table"])}"]'
                )
            out.append("```")
    else:
        out.append(
            "No relationships are known for these tables: no FOREIGN KEY constraints were visible "
            "to "
            "the run and none were provided in configuration or annotations."
        )
    out += ["", "## Declared keys and constraints", ""]
    constraint_rows = []
    for table in profile["tables"]:
        for constraint in table.get("constraints", []):
            constraint_rows.append(
                [
                    md_text(table["table_key"]),
                    md_code(constraint["name"]),
                    md_text(constraint["constraint_type"]),
                    md_text(", ".join(constraint["columns"])) or "—",
                    md_code(constraint["expression"]) if constraint.get("expression") else "—",
                    md_text(constraint["enforcement"]),
                    md_text(constraint["source"]),
                ]
            )
    out.append(
        _r_table(
            ["Table", "Constraint", "Type", "Columns", "Expression", "Enforcement", "Source"],
            constraint_rows,
        )
        if constraint_rows
        else "No declared constraints were visible."
    )
    out += [
        "",
        "Declared PRIMARY KEY/FOREIGN KEY constraints in Unity Catalog are informational: they "
        "document "
        "intent and are not enforced, so they do not prove integrity of the data.",
        "",
        "## Hypotheses",
        "",
        "No data-driven relationship inference was performed. Candidate identifiers in the data "
        "dictionary are not keys; exact uniqueness and referential validation are planned for the "
        "`deep` level.",
        "",
    ]
    return "\n".join(out)


def _r_erd_type(node: Mapping[str, Any]) -> str:
    kind = node["type"]["kind"]
    physical = node["type"]["physical_type"]
    head = (physical.split("(")[0].split("<")[0].split() or [kind])[0]
    simple = re.sub(r"[^A-Za-z0-9_]", "_", head) or kind
    return simple if simple[0].isalpha() else kind


def render_erd(profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None) -> str:
    """Render ``erd.mmd`` (Mermaid erDiagram).

    Entities are the profiled tables with their top-level columns. Edges are
    drawn only when a person provided the cardinality; other known references
    are listed as comments and in ``relationships.md``.
    """
    run = profile["run"]
    relationships = all_relationships(profile, annotations)
    used: dict[str, str] = {}
    lines = [
        f"%% Generated by TableDossier {mermaid_text(profile['tool']['version'])} from profile run "
        f"{mermaid_text(run['run_id'])}, measured at {mermaid_text(run['started_at'])} UTC.",
        "%% Edges appear only for relationships whose cardinality was provided by a person.",
        "%% Line style is dashed (non-identifying); TableDossier does not assert identifying "
        "relationships.",
        "%% Entity names are sanitized; the original table names are:",
    ]
    names: dict[str, str] = {}
    for table in profile["tables"]:
        names[table["table_key"]] = mermaid_name(table["table_key"], used)
    for rel in relationships:
        for end in ("from", "to"):
            key = rel[end]["table"]
            if key not in names:
                names[key] = mermaid_name(key, used)
    lines += [f"%%   {name} = {mermaid_text(key)}" for key, name in names.items()]
    lines.append("erDiagram")
    pk_columns: dict[str, set[str]] = {}
    fk_columns: dict[str, set[str]] = {}
    uk_columns: dict[str, set[str]] = {}
    for table in profile["tables"]:
        for constraint in table.get("constraints", []):
            target = {
                "primary_key": pk_columns,
                "foreign_key": fk_columns,
                "unique": uk_columns,
            }.get(constraint["constraint_type"])
            if target is not None:
                target.setdefault(table["table_key"], set()).update(constraint["columns"])
    for table in profile["tables"]:
        entity = names[table["table_key"]]
        schema = table.get("schema") or {}
        top_nodes = schema.get("fields", [])
        if not top_nodes:
            lines.append(f"    {entity}")
            continue
        lines.append(f"    {entity} {{")
        attr_used: dict[str, str] = {}
        for node in top_nodes[:MAX_ERD_ATTRIBUTES]:
            path = node["display_path"]
            attr = mermaid_name(path, attr_used)
            keys = [
                label
                for label, source in (("PK", pk_columns), ("FK", fk_columns), ("UK", uk_columns))
                if path in source.get(table["table_key"], set())
            ]
            nullable = {True: "nullable", False: "not null", None: ""}[node.get("nullable")]
            comment = mermaid_text(
                " ".join(part for part in (path, node["type"]["physical_type"], nullable) if part)
            )
            key_text = f" {','.join(keys)}" if keys else ""
            lines.append(f'        {_r_erd_type(node)} {attr}{key_text} "{comment}"')
        lines.append("    }")
        if len(top_nodes) > MAX_ERD_ATTRIBUTES:
            lines.append(
                f"    %% {len(top_nodes) - MAX_ERD_ATTRIBUTES} more column(s) of {entity} not shown"
            )
    for key, name in names.items():
        if key not in {table["table_key"] for table in profile["tables"]}:
            lines.append(f"    {name}")
            lines.append(f"    %% {name} is referenced but was not profiled in this run")
    if not relationships:
        lines.append(
            "    %% No relationships are known (no visible FOREIGN KEY constraints and none "
            "provided); entities are shown without edges."
        )
    for rel in relationships:
        left = names[rel["from"]["table"]]
        right = names[rel["to"]["table"]]
        label = mermaid_text(", ".join(rel["from"]["columns"]))
        card = rel.get("cardinality")
        if card:
            lines.append(
                f'    {left} {ERD_LEFT[card["from"]]}..{ERD_RIGHT[card["to"]]} {right} : "{label}"'
            )
        else:
            lines.append(
                f"    %% not drawn ({mermaid_text(rel['origin'])}, cardinality not asserted): "
                f"{left} ({label}) references {right}"
            )
    return "\n".join(lines) + "\n"


def render_all(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Render every Markdown/Mermaid document, keyed by file name."""
    return {
        "overview.md": render_overview(profile, annotations),
        "data_dictionary.md": render_data_dictionary(profile, annotations),
        "quality_report.md": render_quality_report(profile, annotations),
        "relationships.md": render_relationships(profile, annotations),
        "erd.mmd": render_erd(profile, annotations),
    }
