"""Constants for the forze_firestore package."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class ForzeFirestoreLogger(StrEnum):
    """Forze Firestore logger names."""

    ADAPTERS = "forze_firestore.adapters"
    EXECUTION = "forze_firestore.execution"
    KERNEL = "forze_firestore.kernel"
