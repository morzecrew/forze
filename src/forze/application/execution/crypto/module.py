"""Deps module registering the envelope-encryption keyring as a singleton.

The production counterpart to the crypto wiring `forze_mock` does for tests: give
it a key backend (e.g. ``VaultTransitKeyManagement``) and a key directory
(single-key or per-tenant/BYOK), and it composes a process-wide :class:`Keyring`
and registers the whole crypto stack under its dep keys. Integrations that opt
into encryption (object storage, field codecs) resolve ``KeyringDepKey`` from here.
"""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import (
    AeadDepKey,
    AesGcmAead,
    DeterministicCipherDepKey,
    KeyDirectoryDepKey,
    KeyDirectoryPort,
    KeyManagementDepKey,
    KeyManagementPort,
    KeyringDepKey,
)
from forze.application.contracts.deps import DepKey
from forze.application.integrations.crypto import DeterministicFieldCipher, Keyring
from forze.base.crypto import Aead

from ..deps import Deps

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CryptoDepsModule:
    """Register the key manager, AEAD, key directory, and composed keyring."""

    kms: KeyManagementPort
    """Key backend that wraps/unwraps data keys (the KEK never leaves it)."""

    directory: KeyDirectoryPort
    """Resolves a tenant to its key-encryption-key reference (single-key or per-tenant)."""

    aead: Aead = attrs.field(factory=AesGcmAead)
    """Local authenticated cipher; defaults to AES-256-GCM."""

    max_dek_messages: int = 1 << 20
    """Data-key reuse bound passed to the keyring."""

    deterministic_root: bytes | None = attrs.field(default=None, repr=False)
    """Stable root secret (>= 32 bytes) enabling searchable (deterministic) fields.

    When set, a :class:`DeterministicFieldCipher` is registered under
    ``DeterministicCipherDepKey``. Long-lived: rotating it requires re-encrypting
    searchable fields. Load it from a secret store (e.g. Vault) at startup."""

    # ....................... #

    def __call__(self) -> Deps:
        keyring = Keyring(
            kms=self.kms,
            aead=self.aead,
            directory=self.directory,
            max_dek_messages=self.max_dek_messages,
        )

        deps: dict[DepKey[Any], Any] = {
            KeyManagementDepKey: self.kms,
            AeadDepKey: self.aead,
            KeyDirectoryDepKey: self.directory,
            KeyringDepKey: keyring,
        }

        if self.deterministic_root is not None:
            deps[DeterministicCipherDepKey] = DeterministicFieldCipher(
                root=self.deterministic_root,
            )

        return Deps.plain(deps)
