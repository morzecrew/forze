"""Constants for the forze_meilisearch package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeMeilisearchLogger(StrEnum):
    """Forze Meilisearch logger names."""

    KERNEL = "forze_meilisearch.kernel"


# ....................... #

FORZE_MEILISEARCH_LOGGER_NAMES: Final = list(map(str, ForzeMeilisearchLogger))
