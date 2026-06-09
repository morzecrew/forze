from forze.base.logging import Logger
from forze_s3._logging import ForzeS3Logger

# ----------------------- #

logger = Logger(ForzeS3Logger.KERNEL)
"""S3 kernel logger."""
