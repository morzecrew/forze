"""Constants for the forze_gcs package."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class ForzeGcsLogger(StrEnum):
    """Forze GCS logger names."""

    KERNEL = "forze_gcs.kernel"
