"""Authn logger."""

from forze.base.logging import Logger
from forze_identity._logging import ForzeIdentityLogger

# ----------------------- #

logger = Logger(ForzeIdentityLogger.AUTHN)
"""Authn logger."""
