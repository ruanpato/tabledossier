# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub private vulnerability reporting:
<https://github.com/ruanpato/tabledossier/security/advisories/new> (*Security* → *Report a vulnerability*).

Include the affected version, a description, and a reproduction that uses **synthetic data only**. Never send real
profiles, table contents, credentials or workspace details.

## Scope

Relevant reports include, for example:

- a configuration or annotation value that injects code or cells into a generated notebook;
- the notebook reading beyond the requested scope, writing outside its run directory or modifying a source;
- sampled values, credentials or storage locations leaking into profiles, documents or logs;
- Markdown/Mermaid output that executes active content when rendered.

## Design notes

- The generator and renderer never open network connections and never import engine SDKs.
- Configuration and annotations are validated against strict schemas; unknown keys are rejected.
- Filters are compiled with the DataFrame API and literals; identifiers are quoted with backticks. No `eval`, no
  SQL built from user values.
- Generated notebooks install nothing and download nothing.

## Supported versions

Only the latest release receives fixes while the project is pre-1.0.
