"""Constants for the forze_clickhouse package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeClickHouseLogger(StrEnum):
    """Forze ClickHouse logger names."""

    KERNEL = "forze_clickhouse.kernel"


# ....................... #

FORZE_CLICKHOUSE_LOGGER_NAMES: Final = list(map(str, ForzeClickHouseLogger))
