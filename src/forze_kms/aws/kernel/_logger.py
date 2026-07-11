from forze.base.logging import Logger
from forze_kms.aws._logging import ForzeKmsAwsLogger

# ----------------------- #

logger = Logger(ForzeKmsAwsLogger.KERNEL)
"""AWS KMS kernel logger."""
