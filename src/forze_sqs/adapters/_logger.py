from forze.base.logging import Logger
from forze_sqs._logging import ForzeSQSLogger

# ----------------------- #

logger = Logger(ForzeSQSLogger.ADAPTERS)
"""SQS adapters logger."""
