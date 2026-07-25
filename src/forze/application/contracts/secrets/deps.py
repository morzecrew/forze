"""Dependency keys for secrets resolution."""

from ..deps import DepKey
from .admin import SecretsAdminPort
from .lease import DynamicSecretsPort
from .ports import SecretsPort

# ----------------------- #

SecretsDepKey = DepKey[SecretsPort]("secrets")
"""Key used to register an :class:`SecretsPort` implementation."""

SecretsAdminDepKey = DepKey[SecretsAdminPort]("secrets_admin")
"""Key used to register a :class:`SecretsAdminPort` implementation (control plane).
Separate from :data:`SecretsDepKey` so the data path never acquires write access."""

SecretsLeaseDepKey = DepKey[DynamicSecretsPort]("secrets_lease")
"""Key used to register a :class:`DynamicSecretsPort` implementation."""
