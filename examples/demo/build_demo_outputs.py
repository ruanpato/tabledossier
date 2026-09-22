"""Rebuild the synthetic demo outputs committed under ``examples/demo/output``.

Requires the development environment with PySpark and a Java runtime::

    python examples/demo/build_demo_outputs.py

Steps (all local, synthetic data only):

1. generate the notebook from ``demo.config.json`` with the real generator;
2. create the synthetic demo tables in a local Spark session;
3. execute the generated notebook file (only ``dbutils.widgets`` is simulated);
4. copy the result package written by the notebook to ``output/run``;
5. re-render the documents offline with ``annotations.json`` into ``output/annotated``;
6. execute the same notebook again with ``analysis_level = deep`` (default deep
   budgets) and copy that package to ``output/deep``.

The resulting profile honestly records ``execution_context: spark`` (local
Spark), not Databricks. When ``delta-spark`` is installed the demo tables are
created as Delta tables (as on Databricks); otherwise as Parquet tables.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from run_notebook_locally import (  # noqa: E402
    delta_available,
    load_demo_tables,
    local_spark,
    run_notebook,
)

from tabledossier.cli import main as cli  # noqa: E402

OUTPUT = HERE / "output"
# A fixed, generic path so that no machine-specific directory ends up in the committed profile.
RESULTS_DIR = Path("/tmp/tabledossier-demo/results")


def build() -> None:
    """Rebuild every committed demo artefact."""
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    notebook = OUTPUT / "notebook" / "profile_databricks.py"
    code = cli(["generate", "--config", str(HERE / "demo.config.json"), "--output", str(notebook)])
    if code != 0:
        raise SystemExit(code)
    config = json.loads((HERE / "demo.config.json").read_text(encoding="utf-8"))
    shutil.rmtree(RESULTS_DIR.parent, ignore_errors=True)
    with tempfile.TemporaryDirectory() as tmp:
        use_delta = delta_available()
        spark = local_spark(
            str(Path(tmp) / "warehouse"),
            "tabledossier-demo",
            delta=use_delta,
            delta_by_default=use_delta,
        )
        try:
            load_demo_tables(spark)
            runs = {}
            for level, target in (("standard", "run"), ("deep", "deep")):
                namespace = run_notebook(
                    notebook,
                    spark,
                    {
                        "tables_json": json.dumps(config["tables"]),
                        "analysis_level": level,
                        "output_dir": str(RESULTS_DIR),
                        "config_json": "{}",
                    },
                )
                if namespace["td_export"]["validation_errors"]:
                    raise SystemExit(f"{level} profile is invalid")
                runs[target] = Path(namespace["td_export"]["run_dir"])
        finally:
            spark.stop()
        for target, run_dir in runs.items():
            shutil.copytree(run_dir, OUTPUT / target)
    shutil.rmtree(RESULTS_DIR.parent, ignore_errors=True)
    code = cli(
        [
            "render",
            "--input",
            str(OUTPUT / "run" / "profile.json"),
            "--annotations",
            str(HERE / "annotations.json"),
            "--output",
            str(OUTPUT / "annotated"),
        ]
    )
    if code != 0:
        raise SystemExit(code)
    print(f"Demo outputs rebuilt under {OUTPUT}")


if __name__ == "__main__":
    build()
