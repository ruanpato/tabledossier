# 0007 — Release automation: a draft GitHub release built from the tag

Date: 2026-09-22 · Status: accepted

## Context

Releases 0.1.0 to 0.3.0 were built by hand: a worktree of the tagged merge commit, `python -m build`, `shasum`, notes
written from the CHANGELOG, `gh release create`. Each step can drift (a wheel built from a dirty checkout, notes that
do not match the CHANGELOG, a tag that does not match the version). Tags `v*` are created only by administrators and
never moved (rulesets `release-tags-creation` and `release-tags-immutable`), and the package is not published on PyPI.

## Decision

- A workflow (`.github/workflows/release.yml`) runs on tags `v*`. Its **build** job, with a read-only token, checks
  that the tag equals `v` + `__version__`, that the CHANGELOG has a dated section and a tag link for the version, and
  that the tagged commit is on `main`; it builds the wheel and the sdist, runs the unit tests against the installed
  wheel, extracts the release notes from the CHANGELOG section and writes `SHA256SUMS`.
- Its **publish** job is the only one with `contents: write`, uses only `GITHUB_TOKEN`, and creates a **draft** release
  (a pre-release for 0.x versions) with the three files. A person reviews the notes and publishes the draft; the
  workflow never publishes and never touches PyPI.
- The checks and the notes live in `scripts/release.py`, a standard-library script covered by unit tests.
- Pull requests that change the release inputs (the workflow, the script, the CHANGELOG, the version, `pyproject.toml`)
  run the build job as a **dry run**: same build, tests, notes and checksums, with undated sections reported as
  warnings and no release created.

## Consequences

- A release needs: the release PR merged into `main`, an administrator tag on the merge commit, then reviewing and
  publishing the draft. The manual build is no longer needed; the procedure is in `CONTRIBUTING.md`.
- The workflow itself can only be fully proven by a real tag; the dry run proves everything before the release
  creation.
- If a draft is deleted, pushing the tag again is not possible (tags are immutable): re-run the workflow run instead.
