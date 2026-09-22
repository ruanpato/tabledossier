"""Result package: documents derived from a profile, manifests and safe writing.

Part of the embedded runtime (standard library only). The notebook writes the
full package after a run; ``tabledossier render`` regenerates the documents
from a transferred ``profile.json`` with exactly the same functions.
"""

import hashlib
import os
from collections.abc import Mapping
from typing import Any

from tabledossier._version import __version__
from tabledossier.contract import PROFILE_SCHEMA_VERSION
from tabledossier.jsonutil import pretty_json
from tabledossier.quality import suggested_rules_document
from tabledossier.render import render_all

DOCUMENT_FILES = (
    "overview.md",
    "data_dictionary.md",
    "quality_report.md",
    "relationships.md",
    "erd.mmd",
    "suggested_rules.json",
)
PACKAGE_FILES = ("manifest.json", "profile.json", *DOCUMENT_FILES)
JOB_SUMMARY_KIND = "tabledossier.job_summary"
JOB_SUMMARY_VERSION = "1.0"


class OutputExistsError(FileExistsError):
    """Raised when writing would replace existing files without explicit consent."""


def build_documents(
    profile: Mapping[str, Any], annotations: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Return every derived document (Markdown, Mermaid, suggested rules)."""
    documents = render_all(profile, annotations)
    documents["suggested_rules.json"] = pretty_json(suggested_rules_document(profile))
    return documents


def file_digest(text: str) -> tuple[str, int]:
    """Return ``(sha256:<hex>, byte_length)`` of UTF-8 ``text``."""
    data = text.encode("utf-8")
    return "sha256:" + hashlib.sha256(data).hexdigest(), len(data)


def run_manifest(
    profile: Mapping[str, Any],
    files: Mapping[str, str],
    *,
    status: str,
    validation_errors: list[str] | None,
    generation_id: str | None,
) -> dict[str, Any]:
    """Return the run manifest describing a result package."""
    entries = []
    for name in sorted(files):
        digest, size = file_digest(files[name])
        entries.append({"path": name, "sha256": digest, "bytes": size})
    run = profile["run"]
    return {
        "kind": "tabledossier.run_manifest",
        "manifest_version": "1.0",
        "run_id": run["run_id"],
        "status": status,
        "tool_version": __version__,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "started_at": run["started_at"],
        "finished_at": run.get("finished_at"),
        "generation_id": generation_id,
        "files": entries,
        "profile_validation": {
            "checked": validation_errors is not None,
            "valid": None if validation_errors is None else not validation_errors,
            "errors": list(validation_errors or [])[:50],
        },
        "notes": [
            "profile.json is the source of every other file in this package.",
            "Documents can be regenerated offline with: tabledossier render --input profile.json "
            "--output <dir>",
        ],
    }


def job_summary(
    profile: Mapping[str, Any], run_dir: str, validation_errors: list[str] | None
) -> dict[str, Any]:
    """Return the job summary of a run (``tabledossier schema job_summary``).

    Counts only, with a size that does not grow with the number of tables: the
    notebook returns it with ``dbutils.notebook.exit`` (``jobs.exit_summary``)
    to the Job or notebook that ran it. ``run_dir`` holds the full result.
    """
    run = profile["run"]
    summary = profile["summary"]
    relationships = summary.get("relationships") or {}
    return {
        "kind": JOB_SUMMARY_KIND,
        "summary_version": JOB_SUMMARY_VERSION,
        "tool_version": __version__,
        "run_id": run["run_id"],
        "status": run["status"],
        "analysis_level": run["analysis_level"],
        "run_dir": run_dir,
        "profile_valid": not validation_errors,
        "tables": {
            "total": summary["tables_total"],
            "succeeded": summary["tables_succeeded"],
            "partial": summary["tables_partial"],
            "failed": summary["tables_failed"],
        },
        "checks": {
            name: summary["checks"][name] for name in ("pass", "fail", "not_evaluated", "error")
        },
        "relationships": {
            name: int(relationships.get(name, 0))
            for name in ("validated", "violated", "not_validated")
        },
        "relationship_hypotheses": int(summary.get("relationship_hypotheses") or 0),
    }


def running_manifest(run_id: str, started_at: str, generation_id: str | None) -> dict[str, Any]:
    """Return the manifest written before analysis starts (destination probe)."""
    return {
        "kind": "tabledossier.run_manifest",
        "manifest_version": "1.0",
        "run_id": run_id,
        "status": "running",
        "tool_version": __version__,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "started_at": started_at,
        "finished_at": None,
        "generation_id": generation_id,
        "files": [],
        "profile_validation": {"checked": False, "valid": None, "errors": []},
        "notes": ["Run in progress or interrupted before export; no profile has been written yet."],
    }


def write_files(
    directory: str,
    files: Mapping[str, str],
    *,
    overwrite: bool = False,
    replaceable: tuple[str, ...] = (),
) -> list[str]:
    """Write ``files`` into ``directory`` and return the written paths.

    Existing files are never replaced unless ``overwrite`` is true (or the
    name is listed in ``replaceable``, used for the run's own manifest).
    Nothing is written when a conflict is found.
    """
    os.makedirs(directory, exist_ok=True)
    targets = {name: os.path.join(directory, name) for name in files}
    for name, name_path in targets.items():
        if os.sep in name or name.startswith(".."):
            raise ValueError(f"invalid output file name: {name!r}")
        if os.path.exists(name_path) and not overwrite and name not in replaceable:
            raise OutputExistsError(
                f"{name_path} already exists; choose another output directory or allow "
                "overwriting explicitly"
            )
    written = []
    for name, name_path in targets.items():
        with open(name_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(files[name])
        written.append(name_path)
    return written
