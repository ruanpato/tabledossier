# 0001 — Self-contained notebook generated from the package modules

Date: 2026-09-21 · Status: accepted

## Context

The primary user generates a notebook on a machine without access to Databricks and imports it into a workspace
that may have no internet access and no permission to install packages. Options considered:

1. A thin notebook that installs a TableDossier wheel (`%pip install`) — requires publishing or uploading a wheel
   and network or volume access; excluded from the MVP.
2. `%run` of helper notebooks — requires importing several files and keeping them in sync.
3. Embedding the runtime as an encoded payload executed at run time — not reviewable; rejected.
4. Embedding the runtime modules verbatim as readable cells.

## Decision

Option 4. The runtime lives in ordinary, tested modules of the package. The generator copies each module into a
cell in dependency order and removes only intra-package import statements. An AST-based composer enforces the
rules that make this safe (stdlib/PySpark imports only, unique top-level names, consistent import bindings, no
notebook markup). Schemas are embedded as the exact text of the shipped files and parsed with `json.loads`.

## Consequences

- One source of truth; no hand-maintained second implementation. Each cell states its source file and SHA-256.
- The notebook is large (~300 KB) but fully readable and diffable; the generation manifest lists every embedded
  module and hash.
- Embedded modules cannot use third-party libraries; the notebook validates with a stdlib schema interpreter
  (see 0002).
- A future "thin notebook + wheel" mode can coexist, reusing the same modules.
