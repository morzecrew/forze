from forze.base.logging import Logger
from forze_bigquery._logging import ForzeBigQueryLogger

# ----------------------- #

logger = Logger(ForzeBigQueryLogger.KERNEL)
"""BigQuery kernel logger."""
