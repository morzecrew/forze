"""Constants for the forze_postgres package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzePostgresLogger(StrEnum):
    """Forze Postgres logger names."""

    ADAPTERS = "forze_postgres.adapters"
    EXECUTION = "forze_postgres.execution"
    KERNEL = "forze_postgres.kernel"


# ....................... #

FORZE_POSTGRES_LOGGER_NAMES: Final = list(map(str, ForzePostgresLogger))
