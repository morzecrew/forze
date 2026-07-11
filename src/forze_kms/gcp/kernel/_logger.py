from forze.base.logging import Logger
from forze_kms.gcp._logging import ForzeKmsGcpLogger

# ----------------------- #

logger = Logger(ForzeKmsGcpLogger.KERNEL)
"""GCP KMS kernel logger."""
