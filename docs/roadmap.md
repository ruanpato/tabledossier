# Roadmap

**State: current · Reviewed: 2026-09-26.** Released: **0.1.0** (metadata and standard levels), **0.2.0** (deep level,
part I: array and map elements, JSON paths; Spark Connect test baseline), **0.3.0** (deep level, part II: exact
uniqueness, referential validation, relationship hypotheses) and **0.4.0** ("Ready for Databricks": Jobs integration,
Unity Catalog constraints, release automation). Nothing after 0.4.0 is available yet. The order was revised after
0.1.0 and reviewed again on 2026-09-26; it may still change. The state of the repository is in [status](status.md).

Each version below is a milestone with an objective, what goes in, what stays out and an **exit criterion that can be
observed** (a command, a file, a table row). "0.4.0 Ready for Databricks" shipped with no Databricks evidence because
nothing asked for it: this review proposes (open decision 3) that a version is never called ready for an environment
before the evidence exists, and the exit criteria below apply it. When GitHub milestones and issues exist (open
decision 4), each version links to its milestone and the issues are the scope; until then this page is.

## Milestones

| Milestone | Objective | Exit criterion (observable) |
| --- | --- | --- |
| [Databricks evidence](#databricks-evidence-cross-cutting) | Know how the notebook behaves on a workspace | At least one filled row in the record table of the smoke test |
| [0.5.0](#050--postgresql-connector) | PostgreSQL connector from the local CLI | The synthetic demo profiled from PostgreSQL in CI, validated, equal to SQL ground truth, credential-free |
| [0.6.0](#060--remote-databricks-execution) | Remote Databricks execution from the local CLI | The remote profile of the demo equals the notebook's, recorded in compatibility.md |
| [0.7.0](#070--distribution) | `.ipynb`, thin notebook + wheel, profiles in Delta tables | Each mode reproduces the demo profile, with evidence per mode |
| [0.8.0](#080--profile-comparison) | Profile comparison that respects scope and method | `diff` of the committed demo profiles with incomparable pairs refused |
| [1.0.0](#100--stabilization) | Contract frozen, package published, evidence recorded | Contract policy written in contract.md; `pip install tabledossier==1.0.0` from PyPI passes in CI; no `pending` row in compatibility.md without an owner or an explicit "out of scope" (definition of done proposed; open decision 5) |

### Databricks evidence (cross-cutting)

- **Objective.** Learn what only a workspace can show: import and widgets, Jobs and the job summary, Volumes, shared
  and serverless compute (Spark Connect restrictions), Unity Catalog `information_schema`, deep cost on large tables.
- **In.** The [smoke test](databricks-smoke-test.md), steps 1 to 13, on at least one runtime and access mode; the
  record table filled; fixes shipped as `0.4.x` hotfixes from `main`; the rows of
  [compatibility](compatibility.md) moved from `pending` to tested, or to a known limitation, with the evidence.
- **Out.** New features. Nothing in 0.5.0 depends on this milestone; 0.6.0 does.
- **Exit criterion.** At least one row in the record table of the smoke test with date, TableDossier version,
  runtime, access mode and the steps that passed; every failed step has an issue or a documented limitation.
- **Depends on.** A workspace (open decision 1). The Databricks Free Edition would cover serverless compute only,
  which is the mode with most unknowns; to confirm.

### 0.5.0 — PostgreSQL connector

- **Objective.** The same profile contract produced from a PostgreSQL database by an explicit, opt-in CLI command,
  with the driver as an optional dependency. The offline commands (`init`, `validate`, `generate`, `render`, `schema`)
  stay offline and never import a driver.
- **In.** Catalog metadata from `information_schema` and the system catalogs (statistics labelled `as_recorded`);
  the `standard` level: one bounded sample and shared aggregation passes compiled from the same planned specs as the
  Spark adapter; bound parameters for every value and quoted identifiers for every name; one read-only transaction
  per table with the consistency guarantee stated; credentials outside versioned configuration and outside the
  profile; contract 1.3, additive (at least the new `engine`, `execution_context` and `consistency.mode` values; see
  open decision 7 for the full list); PostgreSQL integration tests against a real server in CI; a PostgreSQL twin of
  the synthetic demo and its committed profile.
- **Out.** Remote Databricks execution (0.6.0); `.ipynb` (0.7.0); deep level part I on PostgreSQL (arrays, JSON
  paths); relationship hypotheses. Deep level part II (exact uniqueness, referential validation) in 0.5.0 or 0.5.x is
  open decision 7.
- **Exit criterion.** In CI, on the PostgreSQL versions chosen (open decision 7), the connector profiles the synthetic
  demo tables and produces a result package that `tabledossier validate` accepts (schema 1.3), whose metrics equal
  plain-SQL ground truth in the integration tests, with no credential, host or user name anywhere in the package; the
  offline commands still never import a database driver (tested); [compatibility](compatibility.md) has the evidence
  rows with run ids.
- **Depends on.** Nothing outside the repository: PostgreSQL runs in CI as a service container and locally in Docker.

### 0.6.0 — Remote Databricks execution

- **Objective.** Run the profiling from the local CLI against a workspace, with an explicit choice between Databricks
  Connect and the SQL Connector; only aggregated results return; never on the offline path.
- **In.** The connector command of 0.5.0 gains a Databricks engine; authentication through the Databricks SDK or
  CLI profiles, never in configuration; the same contract.
- **Out.** Running anything the notebook does not already do; catalog scans.
- **Exit criterion.** On the synthetic demo tables of one workspace, the profile produced remotely and the profile
  produced by the notebook agree on every metric (same contract, same values), recorded in
  [compatibility](compatibility.md) with runtime and access mode.
- **Depends on.** The Databricks evidence milestone: without a workspace this version cannot be tested.

### 0.7.0 — Distribution

- **Objective.** More ways to run and keep the notebook: `.ipynb` export, a thin notebook that installs a wheel
  (decision [0001](decisions/0001-self-contained-notebook.md) left the door open), optional persistence of profiles
  in Delta tables.
- **In.** `generate --format ipynb`; the thin mode reusing the same modules; a `persist` option writing the profile to
  a Delta table with the same value policy.
- **Out.** Any new metric.
- **Exit criterion.** Each mode reproduces the committed demo profile (bytes of `profile.json` equal up to run
  identity) on local Spark, with tests; the `.ipynb` imports on a workspace (evidence row).
- **Depends on.** Databricks evidence for the workspace part; nothing for the local part.

### 0.8.0 — Profile comparison

- **Objective.** Compare two profiles while respecting scope, snapshot, sampling, method, rule versions and value
  policy: differences are reported only where the two measurements are comparable, and the report says why not
  otherwise.
- **In.** `tabledossier diff`; a comparison document; comparability rules in the contract documentation.
- **Out.** Trend storage, scheduling, alerts.
- **Exit criterion.** `diff` of the committed demo profiles (`run` against `deep`, and a profile against itself)
  produces the documented report; incomparable pairs (different scope, sampling or contract major) are refused with
  the reason, with tests.
- **Depends on.** The contract of 0.5.0 (engine-neutral fields settled).

### 1.0.0 — Stabilization

- **Objective.** A contract people can build on, a package people can install, and evidence people can read.
- **In.** The contract compatibility policy in [contract](contract.md); the PyPI publication job in the release
  workflow (trusted publishing); the evidence rows below; a review of every document for the release; the Python floor
  decision.
- **Proposed definition of done** (open decision 5):
  - the profile contract is frozen for the 1.x line with a written compatibility policy (readers accept every 1.x;
    additive changes only; a breaking change is 2.0);
  - the package is on PyPI through trusted publishing, still published by a person from the draft release;
  - evidence recorded in [compatibility](compatibility.md) for the primary Databricks target (16.4 LTS) in single-user
    and one Spark Connect mode (shared or serverless), and for two PostgreSQL major versions in CI; no `pending` row
    without an owner or an explicit "out of scope";
  - [limitations](limitations.md) and [privacy](privacy.md) reviewed for the release; the Python floor decided
    (3.10 reaches end of life in October 2026).
- **Out.** Everything under [Possible later work](#possible-later-work).
- **Depends on.** 0.5.0 to 0.8.0 released; the Databricks evidence milestone; open decisions 5 and 6.

## Possible later work

Validation on a real Databricks workspace runs in parallel with the versions above, through the
[smoke test](databricks-smoke-test.md); fixes it reveals ship as patch releases (`hotfix/X.Y.Z` from `main`).
Possible follow-ups of 0.4.0, if there is demand: setting the job summary as Jobs task values
(`dbutils.jobs.taskValues`) so that later tasks can reference it.

Possible later work, if there is demand: structure UML and rule exporters for external quality tools.

Out of scope: LLM-based business inference, universal PII detection, automatic discovery of all relationships,
whole-catalog scans, continuous scheduling, unrestricted correlations, automatic data repair and a universal
quality score.

## Open decisions

Questions only the author can answer, numbered and in the order in which they block work. Each one lists the options,
what it blocks and the recommendation of the review that raised it. A decision becomes a decision record in
[decisions/](decisions) when it changes how the code is built, or a dated line here otherwise. Decided items are kept
with their date, not deleted.

| # | Decision | Options | Blocks | Recommendation (2026-09-26) |
| --- | --- | --- | --- | --- |
| 1 | **Databricks evidence**: which workspace, who runs the smoke test and when; is the Free Edition worth trying | (a) a workspace the author has access to; (b) the Databricks Free Edition (serverless only, to confirm); (c) no evidence for now | The "Databricks evidence" milestone; 0.6.0; any claim of Databricks support | (b) if no workspace is at hand: serverless is the mode with most unknowns, and one filled row beats four releases with none |
| 2 | **Publish the 0.4.0 draft release** | (a) publish after editing the stale "Not yet validated" note about the publish job; (b) keep it as a draft | The quickstart's wheel and checksums; the honesty of "Releases" | (a) |
| 3 | **Freeze features until Databricks evidence, or proceed with 0.5.0** | (A) freeze: evidence first; (B) proceed: PostgreSQL is verifiable end to end without a workspace, evidence in parallel; (C) B, and call nothing "ready" without evidence | The start of 0.5.0 | (B) with (C): the two do not compete for the same resource, and the exit criteria above make (C) a rule |
| 4 | **Issues, milestones, labels and templates on GitHub** | (a) create now: one milestone per version plus "Databricks evidence", the 0.5.0 issues, area and type labels, issue and pull request templates; (b) later. Sub-question: GitHub closes issues only from pull requests into the default branch, `main`, so either the release pull request lists `Closes #N` for every issue of the milestone, or the default branch becomes `develop` | Public scope; the 0.5.0 work | (a), closing issues through the release pull request: the scope of a version must survive a machine and be visible before the CHANGELOG |
| 5 | **Definition of done for 1.0** | The proposal above, or a different set | 1.0.0 planning | The proposal above |
| 6 | **PyPI before 1.0** | (a) publish 0.x pre-releases on PyPI (trusted publishing; one more job in the release workflow) after checking that the name `tabledossier` is available; (b) wait for 1.0 | Installation friction; the package name | (a), once the name is checked (needs network; not done in this review) |
| 7 | **Connector decisions for 0.5.0**: driver, PostgreSQL versions tested, connection and credentials, deep part II in 0.5.0 or 0.5.x, the exact list of contract 1.3 additions | Driver: (a) psycopg 3 as the optional extra, (b) psycopg2, (c) a pure-Python driver. PostgreSQL versions in CI: (a) oldest supported and newest major, (b) one version. Connection and credentials: (a) only from the environment (libpq variables, `.pgpass`, service file, or an environment variable whose name is given on the command line), (b) a connection option in the configuration file. Deep part II: (a) last task of 0.5.0, cut to 0.5.1 if it runs long, (b) 0.5.x from the start. Contract 1.3: (a) the minimal list (`engine`, `execution_context`, `consistency.mode` values, an engine version field, a `connect` error stage), (b) a longer list decided in the first task | The first 0.5.0 task (the adapter) | (a) for every sub-question |
| 8 | **PostgreSQL CI jobs required in the rulesets** | (a) required from the first release that ships the connector (administrator edits both rulesets; the 12 existing names unchanged); (b) informational only | The 0.5.0 release pull request | (a) |
