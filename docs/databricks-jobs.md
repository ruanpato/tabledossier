# Running the notebook as a Databricks Job

The generated notebook runs unchanged as a notebook task of a Databricks Job. This page describes how parameters
reach it and what it returns to the tasks that follow. **Running it as a Job has not been validated on a Databricks
workspace yet** (see [compatibility](compatibility.md)); what follows relies on the documented behaviour of widgets
and `dbutils.notebook.exit`, and on the local harness that simulates them.

## Parameters

The notebook reads four widgets: `tables_json`, `analysis_level`, `output_dir` and `config_json`
([parameters and precedence](configuration.md#precedence-at-run-time)). A Job passes values to them as notebook task
parameters (or job parameters with the same names); Databricks exposes them to the notebook as widget values, and the
notebook never recreates a widget that already exists, so the values passed by the Job are the ones used.

| Parameter | Example value | Notes |
| --- | --- | --- |
| `tables_json` | `["demo.analytics.orders", "demo.analytics.customers"]` | A JSON list, passed as one string. |
| `analysis_level` | `standard` | `metadata`, `standard` or `deep`. |
| `output_dir` | `/Volumes/demo/analytics/results/tabledossier` | The run creates `<output_dir>/<run_id>/`; the identity running the Job needs write access. |
| `config_json` | `{"sampling": {"method": "none"}, "jobs": {"exit_summary": true}}` | A JSON object merged over the generated configuration; never secrets. |

Parameters that are not passed keep the defaults written into the notebook at generation time. Precedence is the same
as interactively: built-in defaults < generated configuration < `config_json` < the three dedicated widgets.

## What the notebook returns

At the end of a run, after the result package was written, the last cell calls `dbutils.notebook.exit` with a small
JSON summary (counts only), when `jobs.exit_summary` is `true` (the default) and `dbutils.notebook` exists:

```json
{"analysis_level":"deep","checks":{"error":0,"fail":1,"not_evaluated":1,"pass":3},"kind":"tabledossier.job_summary","profile_valid":true,"relationship_hypotheses":1,"relationships":{"not_validated":0,"validated":1,"violated":2},"run_dir":"/Volumes/demo/analytics/results/tabledossier/20260922T120000Z-1a2b3c4d","run_id":"20260922T120000Z-1a2b3c4d","status":"partial","summary_version":"1.0","tables":{"failed":1,"partial":0,"succeeded":4,"total":5},"tool_version":"0.4.0"}
```

The format is defined by `tabledossier schema job_summary`. `status` is the run status (`succeeded`, `partial`,
`failed`): the notebook task itself succeeds even when some tables failed, so a following task decides what a partial
run means. `run_dir` points to the complete result package (`profile.json`, `manifest.json` and the documents).

How the value can be read (Databricks features, not validated with TableDossier yet):

- a notebook that runs this one with `dbutils.notebook.run(path, timeout, arguments)` receives the JSON text as the
  return value;
- the Jobs API (`runs/get-output`, field `notebook_output.result`) and the Databricks CLI
  (`databricks jobs get-run-output <task run id>`) return it for the run of the notebook task.

`dbutils.notebook.exit` stops the notebook, which is why it is the last cell. Interactively, the cell shows
"Notebook exited" with the summary, after every result was written and printed. Set
`config_json` = `{"jobs": {"exit_summary": false}}` (or `jobs.exit_summary = false` in the configuration file) to
disable it; without `dbutils.notebook` (for example when the file runs as plain Python) nothing is returned.

## Example job definition (not validated)

**This definition has not been run on a workspace.** It shows where the parameters go in a Jobs API 2.1 request
(`databricks jobs create --json @job.json`); adapt the notebook path, the compute and the parameter values.

```json
{
  "name": "tabledossier-profile",
  "tasks": [
    {
      "task_key": "profile",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/<you>/profile_databricks",
        "base_parameters": {
          "tables_json": "[\"demo.analytics.orders\", \"demo.analytics.customers\"]",
          "analysis_level": "standard",
          "output_dir": "/Volumes/demo/analytics/results/tabledossier",
          "config_json": "{}"
        }
      }
    }
  ]
}
```

The compute of the task is omitted: add the one your workspace uses (`existing_cluster_id`, `job_cluster_key` with a
`job_clusters` entry, or serverless compute for jobs). Each run writes to its own `<output_dir>/<run_id>/` directory,
so runs never overwrite each other.
