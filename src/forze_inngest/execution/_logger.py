from forze.base.logging import Logger
from forze_inngest._logging import ForzeInngestLogger

# ----------------------- #

logger = Logger(ForzeInngestLogger.EXECUTION)
"""Inngest execution (durable-function registration/handlers) logger."""
