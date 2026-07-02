from forze.base.logging import Logger
from forze_rabbitmq._logging import ForzeRabbitMQLogger

# ----------------------- #

logger = Logger(ForzeRabbitMQLogger.ADAPTERS)
"""RabbitMQ adapters logger."""
