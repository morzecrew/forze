from forze.base.logging import Logger
from forze_postgres._constants import ForzePostgresLogger

# ----------------------- #

logger = Logger(str(ForzePostgresLogger.ADAPTERS))
"""Postgres adapters logger."""
