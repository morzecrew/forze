from forze.base.logging import Logger
from forze_postgres._logging import ForzePostgresLogger

# ----------------------- #

logger = Logger(str(ForzePostgresLogger.EXECUTION))
"""Postgres execution logger."""
