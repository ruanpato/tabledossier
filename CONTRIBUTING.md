# Contributing to TableDossier

Thank you for helping. This project favours small, verifiable changes over broad abstractions.

## Ground rules

- Use **synthetic data only** in examples, tests and issues. Never paste real table names, values, paths,
  credentials or profiles from a private environment.
- Keep the MVP scope: see [roadmap](docs/roadmap.md) for what is planned and out of scope.
- Do not claim support you have not tested. Update [compatibility](docs/compatibility.md) with evidence.
- Do not copy code or rule catalogs from other projects without reviewing their license and attribution needs.

## Development setup

```bash
uv sync --group dev                     # or: python -m pip install -e . pytest ruff mypy types-jsonschema build
uv run pytest tests/unit
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Spark integration tests need Java 17, PySpark and delta-spark (whose JARs are fetched from Maven Central the first
time a session starts; the Delta tests are skipped when delta-spark is absent):

```bash
uv sync --group dev --group spark
JAVA_HOME=/path/to/jdk-17 uv run pytest tests/integration
```

Databricks shared access mode and serverless compute use Spark Connect. Run the same suite through a local Spark
Connect server (the fixture never falls back to classic mode):

```bash
uv sync --group dev --group spark --group connect
TD_TEST_SPARK_MODE=connect JAVA_HOME=/path/to/jdk-17 uv run pytest tests/integration
```

For Spark 4.0 use a separate environment, e.g. `python -m venv .venv-spark4` and
`pip install -e . pytest "pyspark[connect]>=4.0,<4.1"`. `TD_TEST_DELTA=0` starts the session without Delta.

## Rules for the embedded runtime

Modules listed in `tabledossier/notebook.py` (`PARAMETER_MODULES`, `RUNTIME_MODULES`) are copied into generated
notebooks. In those modules:

- import only the standard library (plus `pyspark` inside `tabledossier/runtime/`);
- import other TableDossier names with top-level `from tabledossier.<module> import name` (no aliases, no relative
  imports, no `__future__` imports), from modules listed *earlier*;
- keep top-level names unique across all embedded modules (prefix private helpers, e.g. `_sp_`, `_r_`);
- never add Python UDFs, caching, `toPandas()` on sources, writes to sources or per-column Spark actions.

`tests/unit/test_notebook.py` enforces these rules.

## After changing code

The committed demo artefacts must match the code (`tests/unit/test_examples.py`). Rebuild them with:

```bash
JAVA_HOME=/path/to/jdk-17 uv run python examples/demo/build_demo_outputs.py
```

## Changing the contract

Profile, configuration and annotation formats are defined by the JSON Schemas in `src/tabledossier/schemas/`.
Update the schema, `docs/contract.md` or `docs/configuration.md`, the invariants in `contract.py` if needed, and add
a CHANGELOG entry. Breaking changes require a new schema version.

## Branching and releases

The project follows gitflow:

- `develop` is the integration branch. Work happens on `feature/<topic>` branches cut from `develop` and returns to
  it through a pull request with green CI.
- A release is prepared on `release/X.Y.Z`, cut from `develop`, and a fix to a published release on
  `hotfix/X.Y.Z`, cut from `main`. Only these branches are merged into `main`, always through a pull request with a
  **merge commit** (no squash or rebase).
- Each release merged into `main` gets an annotated tag `vX.Y.Z` on the merge commit, and a GitHub release.
- After a release or hotfix reaches `main`, `main` is merged back into `develop` through a pull request, so the next
  release branch is up to date with `main`.

Repository rulesets enforce this:

| Ruleset | Target | Rules |
| --- | --- | --- |
| `main` | `main` | No deletion or force push; changes only through a pull request (merge commits only, review threads resolved, stale reviews dismissed) with all CI jobs green on a branch up to date with `main` |
| `develop` | `develop` | No deletion or force push; changes only through a pull request with all CI jobs green |
| `release-tags-immutable` | tags `v*` | A release tag is never moved or deleted |
| `release-tags-creation` | tags `v*` | Only repository administrators create release tags |

No ruleset has a bypass for pushes to `main` or `develop`: nobody pushes to them directly.

## Pull requests

Open pull requests against `develop` (or against `main` for `release/*` and `hotfix/*` branches). Describe the
behaviour change, the tests you ran (with versions) and any limitation you introduced or removed.
