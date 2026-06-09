from forze.application.contracts.deps import DepKey

from ...kernel.client import DuckDbClientPort

# ----------------------- #

DuckDbClientDepKey = DepKey[DuckDbClientPort]("duckdb_client")
"""Dependency key for the shared :class:`DuckDbClientPort`."""
