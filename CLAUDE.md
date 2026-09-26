@AGENTS.md

# Notes for Claude

The project rules are in [AGENTS.md](AGENTS.md), imported above; they apply to every agent. Only Claude-specific notes
live here.

## Talking to the author

- One maintainer, an experienced engineer. Portuguese in conversation and in the local planning notes
  (`docs/reference/`, ignored by git); English in code, commits, pull requests, issues and everything under `docs/`
  except `docs/pt-BR/` and the unversioned `docs/reference/`.
- When you cannot run something on this machine (no Java, no PySpark, no database), say so and say how the author can
  run it. Never report a result you did not observe.

## Finishing a task

- Update the document of the area and `docs/README.md` (state and review date) when the state of something changed;
  add the feature's own line under `## [Unreleased]` in `CHANGELOG.md`.
- Commit in the project format (`Area: sentence`), **without** `Co-Authored-By` and without "Generated with Claude
  Code": this project rule overrides the Claude Code default, in commits and in pull requests.
- Push, pull requests, issues, releases and new labels or milestones only when the author asks, and a pull request is
  complete from its first draft: the rule and the checklist are in AGENTS.md, "Git and collaboration".
- The deep review of the release pull request (`release/X.Y.Z` → `main`) that AGENTS.md requires is
  `/code-review ultra`, launched by the author.

## Machines

Paths and versions of a particular machine are not rules: keep them in `CLAUDE.local.md` (ignored by git). Machines
differ (Java present or not, a local PostgreSQL server or not); `AGENTS.md` says what to do when something cannot run.
