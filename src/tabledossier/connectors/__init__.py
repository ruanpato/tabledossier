"""Engine connectors run from the local CLI.

A connector profiles tables where they live (only counts and aggregates come back, plus the
bounded transient sample that the notebook also takes) and builds the same contract records
as the generated notebook, through the same pure modules. Connectors are never embedded in
generated notebooks, and they import their database driver only when a profile actually runs:
``import tabledossier`` and the offline commands never load a driver.
"""
