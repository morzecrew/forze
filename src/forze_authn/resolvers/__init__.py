"""Reference :class:`PrincipalResolverPort` implementations.

* :class:`JwtNativeUuidResolver` — trust the assertion subject as an internal UUID.
  Default for first-party Forze flows.
* :class:`MappingTableResolver` — look up ``(issuer, subject)`` in a document-backed
  registry, optionally with just-in-time provisioning. Use for external IdPs when you want
  the freedom to merge or remap identities later.
* :class:`DeterministicUuidResolver` — derive a stable :class:`UUID` from
  ``(issuer, subject)`` without any storage. Use for prototypes or read-only flows.
"""

from .deterministic_uuid import DeterministicUuidResolver, derive_principal_id
from .jwt_native_uuid import JwtNativeUuidResolver
from .mapping_table import MappingTableResolver

# ----------------------- #

__all__ = [
    "DeterministicUuidResolver",
    "JwtNativeUuidResolver",
    "MappingTableResolver",
    "derive_principal_id",
]
