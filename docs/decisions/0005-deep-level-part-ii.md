# 0005 — Deep level, part II: exact uniqueness, referential validation and hypotheses

Date: 2026-09-22 · Status: accepted

## Context

Release 0.3.0 makes the claims that 0.1 and 0.2 deliberately avoided: whether a key is unique, whether a known
relationship holds in the data, and which relationships the data suggests. Each of these needs more than the shared
aggregation passes: a grouped aggregation per key, a join per relationship. The data involved is sensitive by nature
(duplicated keys and orphan values are often identifiers), and a data-driven relationship is easy to present as more
than it is. The rules of the project still hold: opt-in operations with budgets, declared in the profile; no Spark
action per column; samples stay samples; nothing persisted by default; relationships are never inferred from names.

## Decision

- **Every check is opt-in and budgeted per table or per run.** `deep.uniqueness` (explicit keys, declared keys,
  identifier candidates; `max_keys`, `max_passes`), `deep.referential` (configured and declared relationships;
  `max_relationships`, `mode`) and `deep.relationship_hypotheses` (off by default; `max_pairs`, sample size,
  inclusion threshold). Each Spark action is a declared operation (`uniqueness_pass`, `referential_check`,
  `relationship_hypothesis_check`); what a budget prevents is recorded with a reason.
- **Keys share passes.** Several keys are checked in one action: every row becomes one entry per key (a NULL flag
  and typed slots), exploded once and grouped. The number of actions per table depends on `max_passes`, not on the
  number of keys or columns. NULL semantics are those of SQL UNIQUE (NULLs are not equal) and stated in the profile;
  the outcome says whether a PRIMARY KEY would hold.
- **One action per relationship, pinned to the recorded snapshots.** Referential checks run after every table was
  profiled and only between tables of the run: the target is grouped by its key, the source left-joined to it, both
  sides aggregated and collected once, reading both tables at the Delta versions recorded in their profiles. The
  source keeps its analysed scope; the target is read in full, because a reference is valid when the key exists
  anywhere in the target.
- **Status follows evidence.** `validated` requires a full-scope check without orphans; `violated` requires an orphan
  (a sample can prove it); everything else is `not_validated` with a reason, including a clean sample. Target
  uniqueness is recorded as evidence and never becomes a cardinality: only people provide cardinality, and the ER
  diagram rule is unchanged.
- **Hypotheses are data-driven and kept apart.** Targets are single-column keys measured exactly unique in the run;
  sources are chosen by type compatibility and by ranges already measured at the standard level, never by names.
  Known relationships are skipped. A hypothesis needs an inclusion threshold and an unique target; it lives in its own
  list, always marked `hypothesis`, without cardinality and without an ER edge.
- **Counts only.** Duplicated key values, orphan values and matching values never reach the driver; tests plant
  secret markers in them.
- **Contract 1.2 is additive.** The 1.1 schema is frozen as `profile-1.1.schema.json`; the CLI reads 1.0, 1.1 and 1.2.

## Consequences

- A deep run with every check enabled costs, per table, at most `max_passes` more actions, and per run at most
  `max_relationships + max_pairs` more; with the defaults nothing changes until a check is requested.
- Sample-mode referential checks can only find violations; users who need a validated relationship pay for a full
  scope check.
- Hypotheses can miss real relationships (composite keys, pairs beyond the budget, disjoint ranges) and can list
  coincidences (small integer ranges): they are prompts for a person, not documentation.
- Relationships whose tables are not both in the run stay `not_validated`: TableDossier never reads a table that was
  not requested.
