# Status

**State: current · Reviewed: 2026-09-26** (planning review before 0.5.0). What each release delivered and what stayed
pending, where a document and the repository disagree, and the state of git and releases. This page is rewritten at
each review; history stays in the [CHANGELOG](../CHANGELOG.md) and in git. Planned work and open decisions are in the
[roadmap](roadmap.md).

## One-minute summary

- Four releases, 0.1.0 to 0.4.0, on 2026-09-21 and 2026-09-22, all built and tested with **local Spark only**
  (PySpark 3.5 with Delta and 4.0, classic sessions and a local Spark Connect server) over synthetic data.
- **Nothing has been executed on a Databricks workspace yet.** Every Databricks item of
  [compatibility](compatibility.md) is `pending`; the record table of the [smoke test](databricks-smoke-test.md) is
  empty. 0.4.0 is called "Ready for Databricks" and its notes say honestly that no Databricks evidence exists.
- The 0.4.0 GitHub release exists only as a **draft**, created by the release workflow from tag `v0.4.0` on
  2026-09-22 and never published. 0.1.0, 0.2.0 and 0.3.0 are published pre-releases.
- `develop` is 3 commits ahead of `main` (documentation only) and 0 behind; no open pull request; clean tree.
- **Zero issues, zero milestones, zero projects** on GitHub, and only the default labels. The scope of each
  version lived in local planning notes (`docs/reference/`, ignored by git): the public repository knew only the
  roadmap's theme per version beforehand and the CHANGELOG after the fact; the detailed scope was never public. This
  review ran on a machine where the previous
  planning notes were absent and reconstructed the 0.5.0 scope from the roadmap, which is the cost of that choice.
- Checks run for this review (Linux, Python 3.14.2 through `uv`; CI covers 3.10 to 3.14): **295 unit tests pass**; `ruff check`,
  `ruff format --check` and `mypy` are clean. The Spark integration tests were not run here (no Java runtime on this
  machine); CI ran them green for 0.4.0 (see [compatibility](compatibility.md)).

## What each release delivered and what stayed pending

| Release | Date | Delivered and tested | Still pending after it |
| --- | --- | --- | --- |
| 0.1.0 | 2026-09-21 | Offline CLI (`init`, `validate`, `generate`, `render`, `schema`); self-contained notebook; `metadata` and `standard` levels; profile contract 1.0; renderer; synthetic demo | Execution on Databricks |
| 0.2.0 | 2026-09-22 | Deep level part I (array and map elements, JSON paths); contract 1.1; Spark Connect test baseline (local server) | Execution on Databricks; variant functions on 15.4 and 16.4 LTS |
| 0.3.0 | 2026-09-22 | Deep level part II (exact uniqueness, referential validation, relationship hypotheses); contract 1.2; gitflow and rulesets documented | Execution on Databricks; Unity Catalog constraints (only a simulated `information_schema`) |
| 0.4.0 | 2026-09-22 | Job summary through `dbutils.notebook.exit`; Unity Catalog constraints assembled by a pure function and tested end to end with a simulated `information_schema`; release workflow (draft release from a tag, proven by the real `v0.4.0` run); smoke-test steps for Jobs | Everything that needs a workspace: import, widgets, Jobs, Volumes, shared and serverless compute (Spark Connect restrictions), `information_schema`, deep cost on large tables. Publishing the draft release |

## Where documents and the repository disagree

| Where | What it says | What is true on 2026-09-26 | Action |
| --- | --- | --- | --- |
| Notes of the draft release `v0.4.0` on GitHub, "Not yet validated" | "The release workflow's publish job: the dry run proves the build, checks, notes and checksums; the draft release is created only by a real tag" | The publish job ran on the real tag (run 35740789902) and created this very draft | The author edits the note before publishing (open decision 2 in the [roadmap](roadmap.md#open-decisions)) |
| CHANGELOG, section 0.4.0, same bullet | Same text | Same | Released sections are history and are not rewritten; the `[Unreleased]` entry and [compatibility](compatibility.md) carry the correction |
| README, "Status" line | "early release (0.4.0) … not yet validated on a Databricks workspace" | Accurate | None |
| README §6, quickstart | "Install the tagged release from GitHub" | The tag `v0.4.0` exists (checked with `gh`), so the documented `pip install "tabledossier @ git+…@v0.4.0"` resolves to it; that install was not run in this review (no network); the wheel and checksums attached to the release are invisible to the public while the release is a draft | Publish the draft (open decision 2) |
| [roadmap](roadmap.md), before this review | Theme per version; "Nothing after it is available yet"; no exit criteria; no place for open questions | Accurate but incomplete: "0.4.0 Ready for Databricks" shipped with no evidence because no exit criterion asked for it | Rewritten in this review: exit criterion per version, a cross-cutting "Databricks evidence" milestone, an "Open decisions" section |
| [docs/README.md](README.md), before this review | Index without state or date | — | State and review date added in this review |
| [compatibility](compatibility.md), "Last reviewed" line | 2026-09-22 (release 0.4.0) | Reviewed again on 2026-09-26 without new measurements | Header dated 2026-09-26 with the note that nothing new was measured; every evidence row keeps its own date and run id |
| [Decision 0007](decisions/0007-release-automation.md), consequences | "The workflow itself can only be fully proven by a real tag" | Proven on 2026-09-22 (run 35740789902) | The record stays as written (true when written); the evidence is in compatibility.md |
| [CONTRIBUTING](../CONTRIBUTING.md), "Pull requests" | Asks for the tests run, with versions, and the limitations introduced | No pull request template asks for it; no issue template exists | Templates proposed (open decision 4) |
| Commit history | 14 of the last 30 commits on `develop` carry an agent `Co-Authored-By` trailer | The rule from now on is **no agent signature** in commits, pull requests, issues or comments ([AGENTS.md](../AGENTS.md)); history is not rewritten | None |
| Rulesets | CONTRIBUTING describes them | Confirmed: `main` allows merge commits only and requires the 12 CI jobs **by name**, up to date with `main`; `develop` requires the same 12 jobs by name; tags `v*` cannot be updated or deleted (no bypass), and their creation is blocked for everyone except repository administrators, the only bypass actor in the four rulesets. A new CI job is not required until an administrator edits the rulesets, and renaming a job blocks every pull request | Remember when adding PostgreSQL jobs (open decision 8) |

## Git and releases

| Item | State on 2026-09-26 |
| --- | --- |
| Branches on `origin` | `main`, `develop` |
| `main` | `76e2f57`, merge of PR #15 (`release/0.4.0`); tag `v0.4.0` on it |
| `develop` | `7e288c1`, merge of PR #17 (release-workflow evidence); 3 commits ahead of `main`, 0 behind |
| Open pull requests | None |
| Tags | `v0.1.0`, `v0.2.0`, `v0.3.0`, `v0.4.0` (annotated; immutable by ruleset) |
| Releases | 0.1.0, 0.2.0, 0.3.0: published pre-releases. 0.4.0: **draft**, created 2026-09-22 14:30 UTC by the release workflow, not published |
| Issues, milestones, projects | 0, 0, 0. Labels: GitHub defaults only |
| CI | Green on `develop` at `7e288c1` (merge of PR #17, run 35742224011) and on `main` at `76e2f57` (run 35740755133); release workflow green on tag `v0.4.0` (run 35740789902); all on 2026-09-22 |
| Contract | Profile 1.2 (the CLI reads 1.0, 1.1 and 1.2); configuration 1.0 with the `jobs` section; job summary 1.0 |
| Decision records | 0001 to 0007, all accepted |
| Version in `src/tabledossier/_version.py` | `0.4.0` (no release branch open) |

## Databricks evidence

None. The smoke test has never been executed for any release; every row of "Pending validation" in
[compatibility](compatibility.md) is open. Who runs it, on which workspace and when is open decision 1 in the
[roadmap](roadmap.md#open-decisions).
