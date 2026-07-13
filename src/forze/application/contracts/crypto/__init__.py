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
    DeterministicCipherDepKey,
    KeyDirectoryDepKey,
    KeyManagementDepKey,
    KeyringDepKey,
    RequiredReachDepKey,
)
from .directory import (
    KeyDirectoryPort,
    KeyDirectoryWithPrevious,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from .field_encryption import FieldEncryption
from .payload_envelope import (
    ENCRYPTED_PAYLOAD_KEY,
    encrypted_payload_ciphertext,
    is_encrypted_payload,
    looks_encrypted_body,
    wrap_encrypted_payload,
)
from .ports import (
    BytesCipherPort,
    ChunkedStreamOpener,
    DeterministicFieldCipherPort,
    FieldCipherPort,
    KeyManagementPort,
    KeyringPort,
    StreamingBytesCipherPort,
)
from .value_objects import CryptoKeyringStats, DataKey, KeyRef
from .wiring import (
    EncryptionTier,
    encryption_satisfies,
    validate_required_encryption,
    validate_required_reach,
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
    "CryptoKeyringStats",
    "EnvelopeCipher",
    "BytesCipherPort",
    "FieldCipherPort",
    "ChunkedStreamOpener",
    "KeyringPort",
    "StreamingBytesCipherPort",
    "DeterministicFieldCipherPort",
    # key directory (tenant → key)
    "KeyDirectoryPort",
    "KeyDirectoryWithPrevious",
    "StaticKeyDirectory",
    "TenantTemplateKeyDirectory",
    # field-encryption policy
    "FieldEncryption",
    # deps
    "KeyManagementDepKey",
    "AeadDepKey",
    "KeyDirectoryDepKey",
    "KeyringDepKey",
    "DeterministicCipherDepKey",
    "RequiredReachDepKey",
    # wiring policy
    "EncryptionTier",
    "encryption_satisfies",
    "validate_required_encryption",
    "validate_required_reach",
    # whole-payload encrypted-message marker
    "ENCRYPTED_PAYLOAD_KEY",
    "wrap_encrypted_payload",
    "is_encrypted_payload",
    "encrypted_payload_ciphertext",
    "looks_encrypted_body",
]
