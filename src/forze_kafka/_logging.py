"""Constants for the forze_kafka package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeKafkaLogger(StrEnum):
    """Forze Kafka logger names."""

    ADAPTERS = "forze_kafka.adapters"
    KERNEL = "forze_kafka.kernel"


# ....................... #

FORZE_KAFKA_LOGGER_NAMES: Final = list(map(str, ForzeKafkaLogger))
