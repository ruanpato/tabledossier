"""TableDossier: portable data profiling, notebooks, and documentation.

Importing the package never imports PySpark or any database/cloud SDK. The
Spark adapter lives in :mod:`tabledossier.runtime.spark` and is only imported
explicitly (or embedded in generated notebooks).
"""

from tabledossier._version import __version__

__all__ = ["__version__"]
