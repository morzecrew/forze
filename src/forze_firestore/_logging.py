"""Constants for the forze_firestore package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeFirestoreLogger(StrEnum):
    """Forze Firestore logger names."""

    ADAPTERS = "firestore.adapters"
    EXECUTION = "firestore.execution"
    KERNEL = "firestore.kernel"


# ....................... #

FORZE_FIRESTORE_LOGGER_NAMES: Final = list(map(str, ForzeFirestoreLogger))
