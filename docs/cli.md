# CLI reference

All commands run offline. `python -m tabledossier …` is equivalent to `tabledossier …`.

| Command | Purpose |
| --- | --- |
| `tabledossier init --output FILE [--tables T …] [--output-dir DIR] [--level metadata\|standard\|deep] [--force]` | Write a configuration with every default explicit. Refuses to replace a file without `--force`. |
| `tabledossier validate --config FILE` | Validate a configuration (formal schema + semantic checks). |
| `tabledossier validate --profile FILE [--fail-on-check-failures]` | Validate a profile (version, formal schema of that version, embedded validator, invariants) and report its run status. Contract versions 1.0, 1.1 and 1.2 are accepted. |
| `tabledossier validate --annotations FILE` | Validate a human annotations file. |
| `tabledossier generate --config FILE --output NOTEBOOK.py [--manifest FILE] [--overwrite]` | Generate the Databricks notebook and its generation manifest (default `<output>.generation.json`). |
| `tabledossier render --input profile.json --output DIR [--annotations FILE] [--overwrite]` | Render the documents from a profile. Never recomputes metrics; refuses invalid profiles and existing files unless `--overwrite`. |
| `tabledossier schema NAME [--output FILE]` | Print a packaged JSON Schema (`config`, `profile` (1.2), `profile-1.0`, `profile-1.1`, `annotations`, `suggested_rules`, `manifest`, `job_summary`). |
| `tabledossier --version` | Print the version. |

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | Unexpected internal error (please report it; never include data or credentials). |
| 2 | Usage error (invalid arguments). |
| 3 | Invalid input: configuration, profile or annotations failed validation, or the version is not supported. |
| 4 | I/O problem: input not readable, or output already exists without `--overwrite`/`--force`. |
| 5 | `validate --profile`: the profile is valid but the recorded run is `partial`. |
| 6 | `validate --profile`: the profile is valid but the recorded run `failed`. |
| 7 | `validate --profile --fail-on-check-failures`: configured quality checks failed (evaluated after 5/6). |

Heuristic findings never change the exit code. `render` succeeds for partial runs (the documents describe the
errors) and prints a note.
