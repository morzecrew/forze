from forze.base.logging import Logger
from forze_clickhouse._logging import ForzeClickHouseLogger

# ----------------------- #

logger = Logger(ForzeClickHouseLogger.KERNEL)
"""ClickHouse kernel logger."""
