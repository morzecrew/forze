"""Dependency keys for secrets resolution."""

from ..base import DepKey
from .ports import SecretsPort

# ----------------------- #

SecretsDepKey = DepKey[SecretsPort]("secrets")
"""Key used to register an :class:`SecretsPort` implementation."""
