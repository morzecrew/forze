from forze.base.logging import Logger
from forze_kits._logging import ForzeKitsLogger

# ----------------------- #

logger = Logger(ForzeKitsLogger.INTEGRATIONS)
"""Kits integrations (outbox relay, consumer/inbox, notify) logger."""
