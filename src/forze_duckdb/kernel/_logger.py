from forze.base.logging import Logger
from forze_duckdb._logging import ForzeDuckDBLogger

# ----------------------- #

logger = Logger(ForzeDuckDBLogger.KERNEL)
"""DuckDB kernel logger."""
