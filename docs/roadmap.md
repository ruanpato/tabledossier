# Roadmap

Released: **0.1.0** (metadata and standard levels) and **0.2.0** (deep level, part I: array and map elements, JSON
paths; Spark Connect test baseline). **0.3.0** (deep level, part II: exact uniqueness, referential validation,
relationship hypotheses) is this release. Nothing after it is available yet. The order was revised after 0.1.0 and
may still change.

| Version | Theme |
| --- | --- |
| 0.3.0 | **Deep II** (this release): exact uniqueness, targeted referential validation, data-driven relationship hypotheses kept separate from known relationships |
| 0.4.0 | **Ready for Databricks**: real evidence on 15.4, 16.4 and 17.3 LTS (including shared and serverless compute), integration with Jobs, Unity Catalog constraint validation, release automation |
| 0.5.0 | **PostgreSQL connector** (optional dependency): catalog metadata and aggregations executed in the database from the local CLI, bound parameters, read-only transactions, credentials outside versioned configuration, same profile contract |
| 0.6.0 | **Remote Databricks execution** from the local CLI, with an explicit choice between Databricks Connect and the SQL Connector; only aggregated results return; never on the offline path |
| 0.7.0 | **Distribution**: `.ipynb` export, a thin notebook + wheel mode, optional persistence of profiles in Delta tables |
| 0.8.0 | **Profile comparison** that respects scope, snapshot, sampling, method, rule versions and value policy |
| 1.0.0 | **Stabilization** of the contract and publication |

Validation on a real Databricks workspace runs in parallel with [the smoke test](databricks-smoke-test.md); fixes it
reveals ship as patch releases.

Possible later work, if there is demand: structure UML and rule exporters for external quality tools.

Out of scope: LLM-based business inference, universal PII detection, automatic discovery of all relationships,
whole-catalog scans, continuous scheduling, unrestricted correlations, automatic data repair and a universal
quality score.
