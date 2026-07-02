"""Constants for the forze_bigquery package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeBigQueryLogger(StrEnum):
    """Forze BigQuery logger names."""

    KERNEL = "forze_bigquery.kernel"


# ....................... #

FORZE_BIGQUERY_LOGGER_NAMES: Final = list(map(str, ForzeBigQueryLogger))
