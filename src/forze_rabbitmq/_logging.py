"""Constants for the forze_rabbitmq package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeRabbitMQLogger(StrEnum):
    """Forze RabbitMQ logger names."""

    ADAPTERS = "forze_rabbitmq.adapters"
    KERNEL = "forze_rabbitmq.kernel"


# ....................... #

FORZE_RABBITMQ_LOGGER_NAMES: Final = list(map(str, ForzeRabbitMQLogger))
