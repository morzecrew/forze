"""Dependency keys for secrets resolution."""

from ..deps import DepKey
from .admin import SecretsAdminPort
from .lease import DynamicSecretsPort
from .ports import SecretsPort
from .rotating import RotatingCredentialStorePort

# ----------------------- #

SecretsDepKey = DepKey[SecretsPort]("secrets")
"""Key used to register an :class:`SecretsPort` implementation."""

SecretsAdminDepKey = DepKey[SecretsAdminPort]("secrets_admin")
"""Key used to register a :class:`SecretsAdminPort` implementation (control plane).
Separate from :data:`SecretsDepKey` so the data path never acquires write access."""

SecretsLeaseDepKey = DepKey[DynamicSecretsPort]("secrets_lease")
"""Key used to register a :class:`DynamicSecretsPort` implementation."""

RotatingCredentialsDepKey = DepKey[RotatingCredentialStorePort]("rotating_credentials")
"""Key used to register a :class:`RotatingCredentialStorePort` implementation.

Separate from :data:`SecretsDepKey` because the plane is different in kind: these
credentials are written on the read path (a counterparty rotates them as we use them),
so the store is inherently read-write and must never be mistaken for a resolve-only
secrets backend."""
