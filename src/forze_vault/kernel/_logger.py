"""Vault kernel logger."""

from forze.base.logging import Logger

from forze_vault._logging import ForzeVaultLogger

# ----------------------- #

logger = Logger(ForzeVaultLogger.KERNEL)
"""Vault kernel logger."""
