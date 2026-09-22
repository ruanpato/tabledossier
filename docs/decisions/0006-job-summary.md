# 0006 — A small job summary returned with `dbutils.notebook.exit`

Date: 2026-09-22 · Status: accepted

## Context

Release 0.4.0 prepares the notebook to run as a task of a Databricks Job. A following task (or a notebook that
orchestrates this one with `dbutils.notebook.run`) needs to know how the run went without parsing the result package:
where it is, its status and a few counts. Databricks offers `dbutils.notebook.exit(value)`, which ends the notebook and
hands one string to the caller (`dbutils.notebook.run`, the Jobs API `runs/get-output`). The notebook is also used
interactively, and it must stay runnable where `dbutils.notebook` does not exist (the local harness, plain Python).

Options considered:

1. No summary: callers read `manifest.json` in the run directory, whose path they cannot know in advance.
2. `dbutils.jobs.taskValues`: made for passing values between tasks, but a separate API with its own limits and
   semantics outside Jobs; it can be added later.
3. `dbutils.notebook.exit` with a small JSON document in the last cell.

## Decision

Option 3, **on by default** (`jobs.exit_summary = true`), configurable through the configuration file or
`config_json`:

- The summary is built from the profile by a pure function (`package.job_summary`) and has its own versioned schema
  (`tabledossier schema job_summary`, `summary_version` 1.0): run id, status, level, run directory, whether the profile
  passed validation, and counts of tables by status, configured checks, relationships by validation status and
  hypotheses. Counts only: no values, no table names, bounded size whatever the number of tables.
- It is returned only after the result package was written and printed, from the **last cell**, and only when
  `dbutils.notebook.exit` exists. The call is not wrapped in `try`/`except`, because `exit` may be implemented by
  raising and a broad handler could swallow it.
- A run whose tables failed still ends normally: `status` tells the caller, the notebook does not fail the task.

## Consequences

- Interactive runs end with "Notebook exited" and the summary in the last cell; nothing is lost because every result
  was already written and printed. Users who dislike it set `jobs.exit_summary = false`.
- The local harness simulates `dbutils.notebook.exit` by recording the call, which tests the value and its schema but
  not Databricks itself: behaviour on a workspace (Jobs, `dbutils.notebook.run`, serverless) stays pending until the
  smoke test is run.
- Adding a configuration section keeps configuration version 1.0 (the change is additive, as in previous releases);
  the profile contract is unchanged.
