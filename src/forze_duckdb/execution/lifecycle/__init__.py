"""DuckDB lifecycle steps (client startup and shutdown)."""

from .pool import (
    DuckDbShutdownHook,
    DuckDbStartupHook,
    duckdb_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "DuckDbShutdownHook",
    "DuckDbStartupHook",
    "duckdb_lifecycle_step",
]
