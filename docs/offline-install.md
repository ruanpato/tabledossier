# Offline installation

TableDossier is not published on a package index yet. Installation needs the source (or a built wheel) and its
single runtime dependency, `jsonschema` (with `attrs`, `referencing`, `rpds-py` and `jsonschema-specifications`).
After installation, every command works without network access.

## From a checkout (with network)

```bash
python -m venv .venv
# Activate the environment for your OS.
python -m pip install .
```

## Air-gapped machine: build a wheelhouse elsewhere

On a machine with network access and **the same operating system, CPU architecture and Python minor version** as
the target (`rpds-py` ships platform-specific wheels):

```bash
python -m pip install build
python -m build --wheel                         # creates dist/tabledossier-<version>-py3-none-any.whl
python -m pip wheel dist/tabledossier-*.whl -w wheelhouse
```

For a different target platform, download binary wheels explicitly, for example:

```bash
python -m pip download "jsonschema>=4.18,<5" -d wheelhouse --only-binary=:all: \
  --platform manylinux2014_x86_64 --python-version 3.12
cp dist/tabledossier-*.whl wheelhouse/
```

Copy `wheelhouse/` to the target and install without any index:

```bash
python -m venv .venv
# Activate the environment for your OS.
python -m pip install --no-index --find-links wheelhouse tabledossier
tabledossier --version
```

The generated notebook needs nothing from this installation: it runs with the Python and PySpark of the
Databricks Runtime and downloads nothing.
