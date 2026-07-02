"""Constants for the forze_duckdb package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeDuckDBLogger(StrEnum):
    """Forze DuckDB logger names."""

    KERNEL = "forze_duckdb.kernel"


# ....................... #

FORZE_DUCKDB_LOGGER_NAMES: Final = list(map(str, ForzeDuckDBLogger))
