# Roadmap

Nothing below is available in 0.1.0. Items are ordered by priority and may change.

1. **Databricks validation** of the current notebook on 16.4, 15.4 and 17.3 LTS (see the
   [smoke test](databricks-smoke-test.md)).
2. **PostgreSQL connector** (optional dependency): catalog metadata and scalar aggregations executed in the database
   from the local CLI, bound parameters, read-only transactions, credentials outside versioned configuration, tests
   against an ephemeral database, same profile contract.
3. **Remote Databricks execution** from the local CLI, with an explicit choice between Databricks Connect (Spark
   APIs) and the SQL Connector (SQL execution); only aggregated results return to the local machine. Never installed
   on the offline path.
4. **`deep` level** with explicit read budgets: array elements and map entries, JSON path extraction, exact uniqueness
   and targeted referential validation, data-driven relationship hypotheses kept separate from known relationships.
5. **Distribution modes**: `.ipynb` export, a thin notebook + wheel mode, optional persistence of profiles in Delta
   tables (same contract).
6. **Profile comparison** that respects scope, snapshot, sampling, method, rule versions and value policy.
7. **Structure UML and rule exporters** for external quality tools, if there is demand.

Out of scope: LLM-based business inference, universal PII detection, automatic discovery of all relationships,
whole-catalog scans, continuous scheduling, unrestricted correlations, automatic data repair and a universal
quality score.
