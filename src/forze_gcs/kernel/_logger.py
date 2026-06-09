from forze.base.logging import Logger
from forze_gcs._logging import ForzeGcsLogger

# ----------------------- #

logger = Logger(ForzeGcsLogger.KERNEL)
"""GCS kernel logger."""
