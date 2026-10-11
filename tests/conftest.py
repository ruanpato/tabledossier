import copy
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "examples" / "demo"
DEMO_PROFILE = DEMO / "output" / "run" / "profile.json"
DEEP_PROFILE = DEMO / "output" / "deep" / "profile.json"
PROFILE_1_0 = ROOT / "tests" / "fixtures" / "profile-1.0.json"
PROFILE_1_1 = ROOT / "tests" / "fixtures" / "profile-1.1.json"
PROFILE_1_2 = ROOT / "tests" / "fixtures" / "profile-1.2.json"


@pytest.fixture(scope="session")
def demo_profile_data() -> dict:
    """Profile produced by executing the generated notebook on the synthetic demo tables."""
    return json.loads(DEMO_PROFILE.read_text(encoding="utf-8"))


@pytest.fixture
def demo_profile(demo_profile_data: dict) -> dict:
    return copy.deepcopy(demo_profile_data)


@pytest.fixture
def deep_profile() -> dict:
    """Deep-level profile (contract 1.3) produced by the same notebook on the demo tables."""
    return json.loads(DEEP_PROFILE.read_text(encoding="utf-8"))


@pytest.fixture
def profile_1_0() -> dict:
    """Profile written by TableDossier 0.1.0 (contract 1.0), kept to prove it is still read."""
    return json.loads(PROFILE_1_0.read_text(encoding="utf-8"))


@pytest.fixture
def profile_1_1() -> dict:
    """Deep profile written by TableDossier 0.2.0 (contract 1.1), kept to prove it is still read."""
    return json.loads(PROFILE_1_1.read_text(encoding="utf-8"))


@pytest.fixture
def profile_1_2() -> dict:
    """Deep profile written by TableDossier 0.4.0 (contract 1.2), kept to prove it is still read."""
    return json.loads(PROFILE_1_2.read_text(encoding="utf-8"))


# Labels the PostgreSQL connector gives the metrics it computes exactly (contract 1.3).
POSTGRES_DISTINCT_LABELS = {
    "accuracy": "exact",
    "method": "count(DISTINCT field) over non-null values (exact)",
    "details": {"relative_standard_deviation": None},
}
POSTGRES_QUANTILE_METHODS = {
    "quantiles": "percentile_disc over non-null (finite) values (exact; returns values from the "
    "data)",
    "length_quantiles": "percentile_disc of length over non-null values (exact; returns values "
    "from the data)",
}
_POSTGRES_TYPES = {
    "string": "text",
    "int": "integer",
    "double": "double precision",
    "timestamp": "timestamp with time zone",
    "timestamp_ntz": "timestamp without time zone",
    "array<string>": "text[]",
}


def _postgres_type(physical: str) -> str:
    if physical.startswith("decimal("):
        return "numeric" + physical[len("decimal") :]
    return _POSTGRES_TYPES.get(physical, physical)


def _postgres_metric(metric: dict, non_null: object) -> None:
    if metric.get("status") != "measured":
        return
    if metric["name"] == "approx_distinct_count":
        # An exact count never exceeds the non-null values (HyperLogLog++ can).
        if isinstance(non_null, int):
            metric["value"] = min(metric["value"], non_null)
        metric["accuracy"] = POSTGRES_DISTINCT_LABELS["accuracy"]
        metric["method"] = POSTGRES_DISTINCT_LABELS["method"]
        metric.get("details", {}).pop("relative_standard_deviation", None)
        if not metric.get("details"):
            metric.pop("details", None)
    if metric["name"] in POSTGRES_QUANTILE_METHODS:
        metric["accuracy"] = "exact"
        metric["method"] = POSTGRES_QUANTILE_METHODS[metric["name"]]
        metric["details"].pop("accuracy_parameter", None)


def build_postgres_profile(standard: dict) -> dict:
    """Return a synthetic PostgreSQL profile made from the committed standard Spark demo profile.

    It is changed the way the PostgreSQL connector will write it (contract 1.3): local CLI
    environment without Spark keys, transaction snapshot consistency, PostgreSQL types, catalog
    and size records, exact distinct counts and quantiles (labels), JSON validation through
    pg_input_is_valid, one enforced system_catalog constraint and one connect error. Roles,
    findings, checks and proposals are derived again by the shared modules from those labels.
    """
    from tabledossier.assemble import aggregate_metric, finalize_table
    from tabledossier.metrics import metric_value, not_measured
    from tabledossier.planning import iter_nodes

    profile = copy.deepcopy(standard)
    profile["schema_version"] = "1.3"
    run = profile["run"]
    run["environment"] = {
        "engine": "postgresql",
        "execution_context": "local_cli",
        "python_version": run["environment"]["python_version"],
        "postgresql_version": "17.11",
        "session_timezone": "UTC",
    }
    run["generation"] = {
        "generator_version": run["generation"]["generator_version"],
        "generation_id": None,
    }
    run["parameter_sources"] = {
        "precedence": ["built-in defaults", "configuration file", "command-line options"],
        "command_line": {"engine": "postgres", "output": "results"},
        "connection": {"source": "libpq environment"},
        "not_applicable": [],
    }
    run["capabilities"] = {
        "json_validation": {
            "available": True,
            "detail": "pg_input_is_valid(text, 'json') (PostgreSQL 16 or later)",
        },
        "tablesample": {
            "available": True,
            "detail": "TABLESAMPLE BERNOULLI ... REPEATABLE (base tables and materialized views)",
        },
    }
    config = run["effective_config"]
    for table in profile["tables"]:
        table["consistency"] = {
            "mode": "transaction_snapshot",
            "delta_version": None,
            "version_timestamp": run["started_at"],
            "guarantee": "All reads of this run (catalog metadata, sample and aggregation "
            "passes) used one READ ONLY REPEATABLE READ transaction snapshot taken at the start "
            "of the run; changes committed after it are not visible.",
            "notes": [
                "TRUNCATE and table-rewriting ALTER TABLE are not MVCC-safe: run concurrently, "
                "they can make a table look empty to this snapshot."
            ],
        }
        filtered = bool(table["scope"]["filters"])
        table["scope"]["scope_label"] = "filtered_snapshot" if filtered else "full_snapshot"
        source = table["source"]
        source.update(
            {
                "source_type": "table",
                "table_type": "BASE TABLE",
                "provider": "postgresql",
                "created_at": None,
                "last_modified": None,
                "notes": [
                    "Size (pg_total_relation_size) describes the physical size at capture, not "
                    "the snapshot."
                ],
            }
        )
        source["size_in_bytes"]["method"] = "pg_total_relation_size (physical size at capture)"
        source["file_count"] = not_measured(
            "file_count",
            "unsupported",
            "PostgreSQL does not expose a file count",
            scope="table_metadata",
            source="catalog_metadata",
            unit="files",
        )
        for node in iter_nodes(table["schema"]["fields"]):
            node["type"]["physical_type"] = _postgres_type(node["type"]["physical_type"])
        for field in table["field_profiles"]:
            field["physical_type"] = _postgres_type(field["physical_type"])
            non_null = metric_value(field["metrics"], "non_null_count")
            for index, metric in enumerate(field["metrics"]):
                _postgres_metric(metric, non_null)
                if metric["name"] == "json_invalid_count":
                    field["metrics"][index] = aggregate_metric(
                        {
                            "metric": "json_invalid_count",
                            "op": "count_json_invalid",
                            "params": {},
                            "labels": {
                                "method": "non-null values for which "
                                "pg_input_is_valid(value, 'json') is false"
                            },
                        },
                        0,
                        "string",
                        scope=metric["scope"],
                        denominators={"non_null": non_null},
                    )
        table["unsupported"] = []
        table["notes"] = []
        observed = {
            item["operation_id"]: item
            for item in table["operations"]["observed"]
            if item["operation_id"] not in ("op_catalog", "op_constraints")
        }
        planned = [
            {
                "operation_id": "op_catalog",
                "kind": "catalog_metadata",
                "description": "pg_catalog: relation, columns, comments and size",
                "reads_user_data": False,
            },
            {
                "operation_id": "op_constraints",
                "kind": "catalog_metadata",
                "description": "pg_catalog: key and check constraints",
                "reads_user_data": False,
            },
        ]
        for operation in table["operations"]["planned"]:
            if operation["kind"] == "sample_collect":
                operation["description"] = (
                    "projected prefix sample of string fields through a server-side cursor"
                )
                planned.append(operation)
            elif operation["kind"] == "aggregate_pass":
                operation["description"] = "one aggregate SELECT over the analysed scope"
                planned.append(operation)
        table["operations"]["planned"] = planned
        table["operations"]["observed"] = [
            observed.get(operation["operation_id"])
            or {
                "operation_id": operation["operation_id"],
                "status": "succeeded",
                "duration_ms": 1,
                "rows_returned": None,
                "detail": None,
            }
            for operation in planned
        ]
    customers = next(t for t in profile["tables"] if t["table_key"] == "analytics.customers")
    customers["constraints"] = [
        {
            "name": "customers_pkey",
            "constraint_type": "primary_key",
            "columns": ["customer_id"],
            "expression": None,
            "referenced": None,
            "enforcement": "enforced",
            "source": "system_catalog",
        }
    ]
    profile["tables"][-1]["errors"].append(
        {
            "stage": "connect",
            "error_class": "OperationalError",
            "condition": "08006",
            "message": "the connection to the server was lost",
        }
    )
    for table in profile["tables"]:
        finalize_table(table, config)
    statuses = [table["status"] for table in profile["tables"]]
    run["status"] = "partial" if "partial" in statuses else run["status"]
    checks = [check for table in profile["tables"] for check in table["quality_checks"]]
    findings = [finding for table in profile["tables"] for finding in table["findings"]]
    profile["summary"].update(
        {
            "tables_succeeded": statuses.count("succeeded"),
            "tables_partial": statuses.count("partial"),
            "tables_failed": statuses.count("failed"),
            "checks": {
                s: sum(1 for c in checks if c["status"] == s)
                for s in ("pass", "fail", "not_evaluated", "error")
            },
            "findings": {
                s: sum(1 for f in findings if f["severity"] == s) for s in ("info", "warning")
            },
            "suggested_rules": sum(len(t["suggested_rules"]) for t in profile["tables"]),
        }
    )
    return profile


@pytest.fixture
def postgres_profile(demo_profile_data: dict) -> dict:
    """Synthetic PostgreSQL profile (contract 1.3) derived from the standard demo profile."""
    return build_postgres_profile(demo_profile_data)


@pytest.fixture(scope="session")
def schemas() -> dict:
    from tabledossier.resources import SCHEMA_FILES, load_schema

    return {name: load_schema(name) for name in SCHEMA_FILES}


@pytest.fixture
def demo_annotations() -> dict:
    return json.loads((DEMO / "annotations.json").read_text(encoding="utf-8"))
