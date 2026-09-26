# Documentation

Index of every document, with its state and the date it was last reviewed. **Current** is what holds today;
**proposal** is under discussion; **historical** records decisions and experiences that may be outdated. Decision
records carry their own status (`accepted`) and date.

## Start here

1. [status.md](status.md) — what the repository, the releases and the evidence look like today.
2. [roadmap.md](roadmap.md) — what comes next, with an exit criterion per version, and the open decisions.
3. [architecture.md](architecture.md) — how the code is organized and why.
4. [../AGENTS.md](../AGENTS.md) — rules for coding agents and new contributors, with the minimum evidence per type of
   change; [../CONTRIBUTING.md](../CONTRIBUTING.md) — setup, branches and releases.

## Documents

| Document | State | Reviewed | Content |
| --- | --- | --- | --- |
| [status.md](status.md) | Current | 2026-09-26 | What each release delivered and what stayed pending; where documents and the repository disagree; git, releases, rulesets |
| [roadmap.md](roadmap.md) | Current | 2026-09-26 | Milestones 0.5.0 to 1.0.0 and the cross-cutting "Databricks evidence", each with objective, scope and an observable exit criterion; open decisions, numbered |
| [architecture.md](architecture.md) | Current | 2026-09-22 | Principles, modules, notebook composition, execution per table at the standard and deep levels, testing against Spark Connect |
| [contract.md](contract.md) | Current | 2026-09-22 | `profile.json` fields (schema 1.2), metric semantics and encoding, manifests |
| [configuration.md](configuration.md) | Current | 2026-09-22 | Every configuration option, filters, checks, deep budgets, jobs, relationships |
| [cli.md](cli.md) | Current | 2026-09-22 | Commands and exit codes |
| [compatibility.md](compatibility.md) | Current | 2026-09-26 | Target runtimes, what was tested (with run ids), what is pending; no new measurement since 0.4.0 |
| [limitations.md](limitations.md) | Current | 2026-09-22 | Known limits of release 0.4.0 |
| [privacy.md](privacy.md) | Current | 2026-09-22 | Value exposure and controls |
| [databricks-jobs.md](databricks-jobs.md) | Current | 2026-09-22 | Running the notebook as a Job: parameters and the job summary (not validated on a workspace) |
| [databricks-smoke-test.md](databricks-smoke-test.md) | Current | 2026-09-22 | Reproducible manual validation on a workspace; record table (empty: never executed) |
| [offline-install.md](offline-install.md) | Current | 2026-09-21 | Wheel and wheelhouse installation |
| [decisions/](decisions) | Accepted | 2026-09-21 to 2026-09-22 | Decision records 0001 to 0007: self-contained notebook, canonical contract, inference limits, deep level parts I and II, job summary, release automation |
| [pt-BR/overview.md](pt-BR/overview.md), [pt-BR/quickstart.md](pt-BR/quickstart.md) | Current | 2026-09-22 | Overview and quickstart in Portuguese |

## Repository root

| Document | State | Reviewed | Content |
| --- | --- | --- | --- |
| [../README.md](../README.md) | Current | 2026-09-22 | What the tool does, what works today (§4), compatibility summary (§11), development (§12) |
| [../AGENTS.md](../AGENTS.md) | Current | 2026-09-26 | Rules for coding agents and new contributors; minimum evidence per type of change |
| [../CLAUDE.md](../CLAUDE.md) | Current | 2026-09-26 | Claude-specific notes; imports AGENTS.md |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Current | 2026-09-22 | Setup, embedded-runtime rules, branching, releases, rulesets |
| [../SECURITY.md](../SECURITY.md) | Current | 2026-09-21 | How to report a security issue |
| [../CHANGELOG.md](../CHANGELOG.md) | Current | with each release | Notable changes per release, with "Not yet validated" sections |

The folder `docs/reference/` holds local planning notes and session prompts; it is ignored by git and not published.
The public record of what a version will contain is the roadmap and, once created, the GitHub milestone and issues of
that version.
