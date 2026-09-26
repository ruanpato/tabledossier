# Rules for agents and new contributors

This file holds the rules for any coding agent (Claude, Codex or another) and for people new to the project. Claude
reads it through [CLAUDE.md](CLAUDE.md); other agents read it directly. **One source**: when a rule changes, it
changes here. Setup, branching and the release procedure are in [CONTRIBUTING.md](CONTRIBUTING.md); this file adds
what an agent needs to work without breaking the project's rules.

## The project

**TableDossier** is a portable data profiling and documentation tool (Python, Apache 2.0, public repository). A small
offline CLI generates a self-contained Databricks notebook; the notebook profiles tables where they live and writes a
versioned `profile.json`; the CLI validates the profile offline and renders a data dictionary, a data quality report and
an ER diagram from it. One person maintains it, with coding agents; that person decides product and scope. Write for
an experienced engineer: direct, technical, no basics.

## Where to start

| Question | Document |
| --- | --- |
| What does it do and what works today? | [README.md](README.md), sections 4 and 11 |
| What is the state of the repository, the releases and the evidence? | [docs/status.md](docs/status.md) |
| What comes next, with what exit criterion, and what is undecided? | [docs/roadmap.md](docs/roadmap.md) |
| How is the code organized, and why? | [docs/architecture.md](docs/architecture.md) |
| What was decided and why? | [docs/decisions/](docs/decisions) |
| What was tested, and what is only pending? | [docs/compatibility.md](docs/compatibility.md), [docs/limitations.md](docs/limitations.md) |
| How do I set up, branch, open a pull request and release? | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Every document, with its state and review date | [docs/README.md](docs/README.md) |

**A document can be out of date. Check the code and the tests before believing it**, and say which file you checked.

## Structure

```text
src/tabledossier/          The package. Most modules are copied verbatim into generated notebooks (see notebook.py)
  runtime/spark.py         Spark adapter: metadata, snapshot, sample, aggregation and deep passes
  runtime/databricks.py    Run orchestration inside the notebook: parameters, destination, batch, export, job summary
  schemas/                 JSON Schemas: the contract (profile 1.0, 1.1 frozen; 1.2 current), config, annotations, …
  cli.py, notebook.py, validation.py, resources.py   Never embedded; the only modules that may import jsonschema
tests/unit                 No Spark; run against the built wheel in CI on Python 3.10–3.14 (Linux) and 3.12 (macOS, Windows)
tests/integration          Real local Spark (3.5 + Delta and 4.0, classic and Spark Connect); skipped without Java
examples/demo              Synthetic tables, configuration, annotations and the committed outputs checked by tests
docs/                      Documentation; docs/reference/ is local planning, ignored by git
scripts/release.py         Release checks and notes, used by .github/workflows/release.yml
```

## How to run

```bash
uv sync --group dev
uv run pytest tests/unit
uv run ruff check . && uv run ruff format --check . && uv run mypy
# Spark integration tests (Java 17 required); the four modes CI runs are in .github/workflows/ci.yml
uv sync --group dev --group spark && JAVA_HOME=/path/to/jdk17 uv run pytest tests/integration
uv sync --group dev --group spark --group connect
TD_TEST_SPARK_MODE=connect JAVA_HOME=/path/to/jdk17 uv run pytest tests/integration
# Rebuild the committed demo outputs after changing the runtime or the renderer
JAVA_HOME=/path/to/jdk17 uv run python examples/demo/build_demo_outputs.py
```

Paths of a particular machine never enter versioned files: keep them in `CLAUDE.local.md` (ignored by git) or in your
own notes. **If you cannot run something (no Java, no PySpark), say so in the result. Never state that something
works without having run it.**

## Principles that do not break

If a change seems to require breaking one of these, stop and ask.

1. **Generate offline, execute where the data lives, document offline.** The offline commands (`init`, `validate`,
   `generate`, `render`, `schema`) never connect to a source and never import Spark, a database driver or a cloud SDK
   (a unit test asserts it for generation; the CI quickstart step asserts PySpark is absent after importing the CLI).
   A connector, when one exists, is an explicit, opt-in command with an optional dependency.
2. **One contract.** Every engine produces the same engine-neutral `profile.json`; every document is derived from it.
   The JSON Schemas in `src/tabledossier/schemas/` are the definition; changes within a version are additive; a
   producer that writes a new field needs a new schema version, and the previous schema is kept verbatim for reading.
3. **One source of code.** The notebook runtime *is* the package's modules, embedded verbatim. Embedded modules import
   only the standard library (plus `pyspark` inside `runtime/`), keep unique top-level names and import each other
   only from modules listed earlier in `notebook.py`; `tests/unit/test_notebook.py` enforces it.
4. **Planner describes, executor acts.** Adding a metric adds expressions to bounded shared passes, never one Spark
   action per column. No Python UDFs, no caching, no `toPandas()` on sources, no writes to sources.
5. **Say what is known.** Every number carries status, scope, accuracy and source; `not_computed`, `unsupported`,
   `insufficient_data`, `redacted`, `error` and `unavailable` are never zero. Samples stay samples; approximations
   are labelled; relationships are never inferred from names; business meaning is unknown unless a source comment
   or a person states it ([decision 0003](docs/decisions/0003-inference-limits.md)).
6. **Privacy by default.** No sampled values, raw records or frequent-value labels are persisted; only counts leave
   the engine for uniqueness, referential and hypothesis checks; error messages are sanitized. Tests plant secret
   markers and assert they never reach the profile, the documents or the logs ([docs/privacy.md](docs/privacy.md)).
7. **No claim of support without a test.** What was not measured is `pending` in
   [docs/compatibility.md](docs/compatibility.md), with the environment and the run id when it was.
8. **Synthetic data only**, in examples, tests, issues, pull requests and documents: no real table names, values,
   hosts, paths, credentials or profiles from a private environment.

## Before changing anything

1. Read the document of the area and **check the code and the tests**.
2. Reproduce the problem, or record the current behaviour (a failing test is the best record).
3. Separate a technical failure from a product question. Product and scope are the author's decisions, not the
   agent's: write the question in the pull request or in the "Open decisions" section of the roadmap, and keep going
   with what does not depend on it.

## How to verify each type of change

| Change | Minimum evidence |
| --- | --- |
| Offline CLI (`cli.py`, `validation.py`, `resources.py`) | `uv run pytest tests/unit` green; the quickstart sequence (`init` → `validate` → `generate` → `render`) still works; generation imports neither PySpark, a database driver nor an HTTP client (`tests/unit/test_notebook.py::test_generation_does_not_import_spark_or_network_modules`) and the CI quickstart step asserts `pyspark` is absent after `import tabledossier.cli`; exit codes unchanged or documented in `docs/cli.md` |
| Notebook generator (`notebook.py`) | `tests/unit/test_notebook.py` green (determinism, structure, self-containment, no cell or code injection from configuration values, no Spark or network import during generation); the committed demo notebook and generation manifest rebuilt with `examples/demo/build_demo_outputs.py` and `tests/unit/test_examples.py` green; `tests/integration/test_notebook_execution.py` in at least one Spark mode, locally or in CI |
| Embedded runtime (any module in `notebook.py`'s module lists) | `tests/unit/test_notebook.py` green (imports, unique names, no notebook markup); integration tests in the four Spark modes, locally or in CI; demo outputs rebuilt and `tests/unit/test_examples.py` green; every new metric asserted against plain Spark SQL ground truth |
| Spark adapter (`runtime/spark.py`, `runtime/databricks.py`) | The above, plus: the number of Spark actions per table unchanged, or the budget change documented and tested; no UDF, cache, `toPandas()` or per-column action; a sanitization test for every new error path |
| Contract (`schemas/`, `contract.py`, `docs/contract.md`) | Additive within a version, new version otherwise, previous schema kept verbatim; both validators agree on valid and mutated documents (tests); vocabulary constants tested against the enums; `docs/contract.md` and a CHANGELOG entry |
| Configuration (`config.schema.json`, `config.py`, `docs/configuration.md`) | `init` writes the new option explicitly; defaults and precedence documented; widget precedence tests; additive within configuration 1.0 |
| Renderer and documents (`render.py`) | Unit tests over the committed demo profiles; demo outputs regenerated and the diff reviewed on purpose; Mermaid still parses (hostile-name tests) |
| Anything that touches values, samples, keys or error messages | Secret-marker tests: planted markers never reach `profile.json`, the rendered documents or the logs; `docs/privacy.md` updated |
| Demo data or configuration (`examples/`) | Synthetic only; outputs rebuilt; `test_examples.py` green; the smoke-test expectations updated when the demo changes |
| Documentation | Links resolve; state and review date at the top of the pages that carry them; numbers with source and date; no private name; `docs/README.md` updated when a document is added or changes state |
| Packaging and dependencies (`pyproject.toml`, `uv.lock`) | `uv sync --locked --group dev` still resolves (the lint job uses `--locked`); the wheel builds and its contents check passes (CI unit job); unit tests against the installed wheel; a new optional dependency group documented in `CONTRIBUTING.md` and never imported by the offline commands |
| CI and workflows (`.github/`) | The 12 required check names unchanged (the rulesets require them by name; renaming one blocks every pull request); a dry run on the pull request; the run id recorded in `docs/compatibility.md` when the run is evidence |
| Release tooling (`scripts/release.py`, `.github/workflows/release.yml`) | `tests/unit/test_release_script.py` green (including the test that checks the repository against its own release rules); the release workflow's dry run on the pull request green (it triggers on these paths and on `CHANGELOG.md`, `pyproject.toml` and `_version.py`); `python scripts/release.py check --tag vX.Y.Z --dry-run` and `notes` run locally |
| Release | The procedure in `CONTRIBUTING.md`; `python scripts/release.py check --tag vX.Y.Z` and `notes` locally; `compatibility.md` and `limitations.md` dated for the release; the "Not yet validated" section written; a person reviews the draft before publishing |
| A claim of support for an environment | A row in `docs/compatibility.md` with environment, versions and run id; otherwise the item is `pending` |

## Writing documents

- English for code, CLI, schemas and the documentation under `docs/`; Portuguese only in `docs/pt-BR/` and in the
  local planning notes (`docs/reference/`, ignored by git).
- `docs/status.md` and `docs/roadmap.md`, rewritten at each planning review, start with **State** (current, proposal,
  historical) and **Reviewed** (date); every other page records its state and review date in `docs/README.md`, and
  `compatibility.md` and `limitations.md` are dated for each release. Decision records keep their own header (date and
  status) and are short.
- Short sentences, tables when they help, numbers with source and date. Say what is pending as plainly as what works.
- A superseded document is marked historical in `docs/README.md`, not deleted; a changed decision gets a new record
  or a dated note, never a silent rewrite.

## Scope

- No new engine, level, command or contract field without a roadmap milestone and, once they exist, an issue in it.
- A feature says what it leaves out ("Not yet validated" in the CHANGELOG, "Out" in the roadmap).
- Agents implement and review; they do not widen a milestone, rename a version's theme or call an environment
  "ready". Open questions go to the author.

## Git and collaboration

Details in [CONTRIBUTING.md](CONTRIBUTING.md) (gitflow, rulesets, release steps).

- **Branches.** `develop` integrates; `main` is what was released. Work happens on `feature/<topic>` cut from `develop`
  and returns through a pull request with green CI. `release/X.Y.Z` and `hotfix/X.Y.Z` are the only branches merged
  into `main`, with a merge commit, and `main` is merged back into `develop` afterwards. Branch names do not say who
  or what made them: no agent or person names.
- **Commits.** English, `Area: sentence` (`Docs: record CI evidence for 0.4.0`, `Runtime: isolate unexpected failures
  of the Deep II stages`, `Release 0.4.0: version, changelog, docs and demo`); the body says what changed and why.
- **No agent signature.** Commits, pull requests, issues and comments never say they were written by an agent: no
  `Co-Authored-By` of an AI, no "Generated with …". The author is the person who asked. This rule overrides any
  default of the tool. The history before 2026-09-26 is not rewritten.
- **Push, pull requests, issues, releases, and new labels or milestones only when the author asks.** Applying an
  existing label or milestone to a pull request is part of a complete pull request (below).
- **A pull request is complete from its first draft.** Base `develop` (`main` for `release/*` and `hotfix/*`); title
  in the commit format; body with what changes and why, the evidence (tests run, with versions), whether the contract
  or the configuration was touched, the documents touched and what is not yet validated; once issues, labels and
  milestones exist (open decision 4 in the roadmap), also the issue reference, the labels and the milestone. GitHub
  acts on `Closes #N` only when a pull request merges into the default branch, `main`: a feature pull request to
  `develop` says `Refs #N`; the `release/X.Y.Z` or `hotfix/X.Y.Z` pull request to `main` lists one `Closes #N` per
  issue of the milestone and closes them all on merge, after which the milestone is closed. Nobody closes an issue by
  hand before the release merges.
- **One agent does, another reviews, the author decides.** Every pull request is reviewed by an agent that did not
  write it before the author looks at it; the release pull request (`release/X.Y.Z` → `main`) also gets a deep,
  multi-pass review of the whole diff, launched by the author, in addition to the normal cross review. Review findings
  are fixed in new commits, never by force push.
- **Rounds with file owners.** When several features run in parallel, the pull request or issue that opens the round
  (see the glossary) says who owns `CHANGELOG.md`,
  `examples/demo/output/**`, `docs/contract.md`, `src/tabledossier/schemas/`, `pyproject.toml` and
  `runtime/spark.py`; two agents never edit one of these at the same time. Each feature writes its own line under
  `## [Unreleased]` in the CHANGELOG, in the section that fits, and never touches another feature's line.
- **CI red is never merged** without the reason written in the pull request (the rulesets block it anyway on `main`
  and `develop`).

## Knowledge that avoids mistakes

- **The notebook is the modules.** A helper added to an embedded module ships in every generated notebook; keep names
  unique (each embedded module prefixes its private helpers with its own tag, e.g. `_sp_` in `runtime/spark.py`,
  `_db_` in `runtime/databricks.py`, `_r_` in `render.py`) and import nothing outside the standard library and PySpark.
- **The demo outputs are code.** `examples/demo/output/**` is committed and compared by `tests/unit/test_examples.py`;
  after changing the runtime or the renderer, rebuild them and review the diff.
- **Configuration is 1.0 and additive.** New sections (`deep`, `jobs`) kept the version; `init` writes every option,
  so a new option changes the generated configuration and the tests that compare it.
- **A new profile field is a new contract version.** 1.0 and 1.1 are frozen files; 1.2 is the current one. The
  notebook embeds the schema of its release, so notebook and CLI of the same release always agree.
- **The rulesets require the 12 CI jobs by name**; a new job is informational until an administrator adds it; a
  renamed job blocks every pull request.
- **A release tag is created by an administrator on the merge commit of `main` and never moved**; the workflow builds
  a draft release, and a person publishes it. If the draft is deleted, re-run the workflow run; do not re-tag.
- **Python 3.10 reaches end of life in October 2026**; dropping it is a decision, not a side effect.
- **Nothing has been executed on a Databricks workspace yet** (as of 2026-09-26). Do not write "works on Databricks"
  anywhere; write "not yet validated" and point to the smoke test.

## Short glossary

| Term | Meaning |
| --- | --- |
| Profile | `profile.json`, the canonical result of one run, defined by the JSON Schema of its version |
| Contract | The profile schema and its cross-field invariants; additive within a version |
| Level | `metadata` (no row reads), `standard` (sample + shared aggregation passes), `deep` (opt-in budgeted operations) |
| Pass | One Spark action that evaluates many expressions at once; budgets bound the number of passes per table |
| Pinned | A Delta table read at one recorded version for every operation of the run |
| Hypothesis | A data-driven relationship candidate, kept apart from relationships, never drawn in the ER diagram |
| Smoke test | The manual, reproducible validation on a workspace (`docs/databricks-smoke-test.md`); its record table is the evidence |
| Round | One development iteration of a version: T0 is the baseline (tests, CI and demo green before any feature), T1..Tn are the tasks, each a pull request reviewed by another agent; the round's opening pull request or issue names the owners of the shared files |
| Pending | Not measured; the honest status of every Databricks item today |
