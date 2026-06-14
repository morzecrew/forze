"""Contracts for envelope encryption: key management, the cipher, and policy.

Re-exports the dependency-light primitives from :mod:`forze.base.crypto` for a
single import surface, and adds the async key-management port, the orchestrating
:class:`EnvelopeCipher`, dependency keys, and the ``required_encryption`` floor.
"""

from forze.base.crypto import (
    Aead,
    AesGcmAead,
    ChaCha20Poly1305Aead,
    EncryptedEnvelope,
    is_envelope,
    pack_envelope,
    unpack_envelope,
)

from .cipher import EnvelopeCipher
from .deps import (
    AeadDepKey,
    KeyDirectoryDepKey,
    KeyManagementDepKey,
    KeyringDepKey,
)
from .directory import (
    KeyDirectoryPort,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from .ports import BytesCipherPort, KeyManagementPort
from .value_objects import DataKey, KeyRef
from .wiring import (
    EncryptionTier,
    encryption_satisfies,
    validate_required_encryption,
)

# ----------------------- #

__all__ = [
    # primitives (re-exported from forze.base.crypto)
    "Aead",
    "AesGcmAead",
    "ChaCha20Poly1305Aead",
    "EncryptedEnvelope",
    "is_envelope",
    "pack_envelope",
    "unpack_envelope",
    # key management + cipher
    "KeyManagementPort",
    "KeyRef",
    "DataKey",
    "EnvelopeCipher",
    "BytesCipherPort",
    # key directory (tenant → key)
    "KeyDirectoryPort",
    "StaticKeyDirectory",
    "TenantTemplateKeyDirectory",
    # deps
    "KeyManagementDepKey",
    "AeadDepKey",
    "KeyDirectoryDepKey",
    "KeyringDepKey",
    # wiring policy
    "EncryptionTier",
    "encryption_satisfies",
    "validate_required_encryption",
]
