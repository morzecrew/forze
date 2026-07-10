"""Deps module registering the envelope-encryption keyring as a singleton.

The production counterpart to the crypto wiring `forze_mock` does for tests: give
it a key backend (e.g. ``VaultTransitKeyManagement``) and a key directory
(single-key or per-tenant/BYOK), and it composes a process-wide :class:`Keyring`
and registers the whole crypto stack under its dep keys. Integrations that opt
into encryption (object storage, field codecs) resolve ``KeyringDepKey`` from here.
"""

from typing import Any, final

import attrs

from forze.application.contracts.base import EncryptionReach
from forze.application.contracts.crypto import (
    AeadDepKey,
    AesGcmAead,
    DeterministicCipherDepKey,
    KeyDirectoryDepKey,
    KeyDirectoryPort,
    KeyManagementDepKey,
    KeyManagementPort,
    KeyringDepKey,
    RequiredReachDepKey,
)
from forze.application.contracts.deps import DepKey
from forze.application.integrations.crypto import DeterministicFieldCipher, Keyring
from forze.base.crypto import Aead

from forze.application.contracts.deps import Deps

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

    dek_ttl_seconds: float | None = None
    """Optional lifetime (seconds) for a cached plaintext data key on both the encrypt
    and decrypt paths, forwarded to the keyring. ``None`` (default) keeps a data key
    until LRU eviction or restart — so a KEK rotation/revocation only takes effect after
    a restart. Set a TTL to bound that window (see :class:`Keyring.dek_ttl_seconds`)."""

    decrypt_cache_max: int = 1024
    """Maximum unwrapped data keys the keyring keeps on the decrypt path (LRU)."""

    enc_cache_max: int = 1024
    """Maximum active data keys / tenant→key entries the keyring keeps (LRU). Bounds
    memory in deployments with many distinct tenants/keys; eviction just re-fetches."""

    deterministic_root: bytes | None = attrs.field(default=None, repr=False)
    """Stable root secret (>= 32 bytes) enabling searchable (deterministic) fields.

    When set, a :class:`DeterministicFieldCipher` is registered under
    ``DeterministicCipherDepKey``. Long-lived: rotating it requires re-encrypting
    searchable fields. Load it from a secret store (e.g. Vault) at startup."""

    deterministic_previous_root: bytes | None = attrs.field(default=None, repr=False)
    """Prior deterministic root, set only during a rotation overlap.

    While set, new writes use :attr:`deterministic_root` but reads and equality
    queries also match values written under this previous root. Run
    ``reencrypt_documents`` to re-index every searchable value under the new root,
    then drop this. Ignored unless :attr:`deterministic_root` is also set."""

    required_reach: EncryptionReach | None = None
    """Deployment-wide minimum encryption *reach* for messaging routes (``None`` = no floor).

    When set (``at_rest`` or ``end_to_end``), every outbox and direct-transport
    (queue/stream/pub-sub) route whose declared reach is weaker is refused at resolve, so a
    payload can never travel through a store or broker more exposed than the deployment
    allows. A transport has no ``at_rest`` level, so an ``at_rest`` floor forces it to
    ``end_to_end``."""

    # ....................... #

    def __call__(self) -> Deps:
        keyring = Keyring(
            kms=self.kms,
            aead=self.aead,
            directory=self.directory,
            max_dek_messages=self.max_dek_messages,
            decrypt_cache_max=self.decrypt_cache_max,
            enc_cache_max=self.enc_cache_max,
            dek_ttl_seconds=self.dek_ttl_seconds,
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
                previous_root=self.deterministic_previous_root,
            )

        if self.required_reach is not None:
            deps[RequiredReachDepKey] = self.required_reach

        return Deps.plain(deps)
