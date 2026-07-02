from forze.base.logging import Logger
from forze_temporal._logging import ForzeTemporalLogger

# ----------------------- #

logger = Logger(ForzeTemporalLogger.EXECUTION)
"""Temporal execution (saga/compensation, lifecycle) logger."""
