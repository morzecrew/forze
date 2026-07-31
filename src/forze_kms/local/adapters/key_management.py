""":class:`~forze.application.contracts.crypto.KeyManagementPort` adapter over operator-provided master keys.

Self-hosted key management: random 32-byte data keys are wrapped under a master
key-encryption key with AES-256-GCM, the key identity bound as AAD. No cloud
SDK, no Vault, no network — the master keys are raw bytes the application
supplies (loading them from an env var, file mount, or secret manager is the
application's concern, exactly like ``deterministic_root`` on
:class:`~forze.application.execution.CryptoDepsModule`).

**Threat model, honestly:** master keys live in process memory and in operator
configuration. There is no HSM, no non-exportability, no backend audit log or
access policy — compromise of the host or the configuration is compromise of
the keys. Suitable for self-hosted deployments where the operator controls the
host and accepts that boundary. Because the adapter implements the same port as
the cloud backends, moving to one later changes wiring only — envelopes wrapped
under a local master key still have to be re-encrypted under the new backend's
key (an overlap window plus a re-encryption sweep, the same procedure as any
key retirement).

Rotation works by holding **multiple** master keys, keyed by key id: envelopes
are self-describing (the keyring unwraps with the envelope's own ``key_id``),
so during an overlap the directory's ``key_ref`` points at the new key while
this adapter still unwraps envelopes sealed under the previous one. Wire it
with a keyring, e.g.::

    Keyring(
        kms=LocalKeyManagement({"k1": key_v1_bytes}),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="k1")),
    )

and rotate with an overlap — new writes seal under ``k2``, old envelopes stay
readable, a re-encryption sweep (or natural rewrites) drains ``k1``, then it
drops from both places::

    Keyring(
        kms=LocalKeyManagement({"k2": key_v2_bytes, "k1": key_v1_bytes}),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(
            KeyRef(key_id="k2"),
            previous_key_ref=KeyRef(key_id="k1"),
        ),
    )

**Wrap-count bound (why each wrap derives its own key):** AES-GCM with random
96-bit nonces is only safe for ~2^32 encryptions *under one key* (NIST SP
800-38D) — and DEKs are minted per stream, per TTL expiry, per tenant, and on
cache eviction, so a busy fleet under a single long-lived master key could
plausibly approach that ceiling, where a nonce collision is catastrophic.
Every wrap therefore seals under a one-shot subkey — HKDF-SHA256 of the master
key with a fresh random 256-bit salt stored in the envelope
(``0x02 || salt || nonce || ciphertext``) — so no key ever performs a second
GCM encryption and the ceiling never accrues against the master key. There is
consequently **no volume-driven rotation cadence**: rotate master keys on
policy and on suspicion of compromise (the multi-key overlap below), not on a
wrap counter. Envelopes sealed by earlier builds (``nonce || ciphertext``
directly under the master key) are still opened; new wraps always use the
salted form.

The wrap is pure in-process computation, so the adapter also implements
:class:`~forze.application.contracts.crypto.SyncKeyManagementPort` — the
documented legitimate case for that opt-in: a keyring over this backend fills
its data-key cache inline and the synchronous field path never dies cold.

In a fleet every replica must hold the same key map. Each instance logs its key
ids and a one-way :attr:`~LocalKeyManagement.fingerprint` at construction —
compare that value across replicas to spot key-map drift before it surfaces as
unwrap errors.
"""

import hashlib
from collections.abc import Mapping
from types import MappingProxyType
from typing import final

import attrs
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from forze.application.contracts.crypto import AesGcmAead, DataKey, KeyRef
from forze.base.exceptions import exc
from forze.base.logging import get_logger
from forze.base.primitives import secure_random_bytes

# ----------------------- #

logger = get_logger("forze_kms.local")

# ....................... #

_NONCE_SIZE = 12
_DEK_SIZE = 32
_MASTER_KEY_SIZE = 32
_TAG_SIZE = 16
_AAD_PREFIX = "forze-local-kms"

_FINGERPRINT_DOMAIN = b"forze-local-kms-fingerprint-v1"
_KEY_DIGEST_DOMAIN = b"forze-local-kms-key-digest-v1|"

# Per-wrap subkey derivation (the v2 envelope): each wrap seals under
# HKDF-SHA256(master, salt) with a fresh random 256-bit salt, so the master key
# itself never performs two GCM encryptions — the NIST ~2^32 random-nonce wrap
# bound applies per *derived* key (which seals exactly once), not per master key.
_WRAP_VERSION_V2 = 0x02
_WRAP_SALT_SIZE = 32
_WRAP_SUBKEY_INFO = b"forze-local-kms-wrap-subkey-v2"

_WRAPPED_SIZE_V1 = _NONCE_SIZE + _DEK_SIZE + _TAG_SIZE
"""The pre-salt envelope: ``nonce || ciphertext`` sealed directly under the master key.
Still opened (read-only) so envelopes sealed by earlier builds stay readable."""

_WRAPPED_SIZE_V2 = 1 + _WRAP_SALT_SIZE + _NONCE_SIZE + _DEK_SIZE + _TAG_SIZE
"""The current envelope: ``0x02 || salt || nonce || ciphertext``."""


# ....................... #


def _copy_keys(keys: Mapping[str, bytes]) -> Mapping[str, bytes]:
    return MappingProxyType(dict(keys))


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class LocalKeyManagement:
    """Wrap/unwrap data keys under local master keys (AES-256-GCM), by key id."""

    keys: Mapping[str, bytes] = attrs.field(repr=False, converter=_copy_keys)
    """Key id → raw 32-byte key-encryption key; never logged (``repr`` suppressed).
    Detached into a read-only view at construction, so neither mutating the source
    mapping nor writing through this attribute can change the validated key set.

    Holds the active key and, during a rotation overlap, the previous one(s) —
    which key seals a new envelope is decided by the directory-resolved
    ``KeyRef``, which key opens a stored envelope by the envelope's own id.
    """

    _aead: AesGcmAead = attrs.field(factory=AesGcmAead, init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.keys:
            raise exc.configuration(
                "Local KMS requires at least one master key",
                code="core.crypto.master_key_invalid",
            )

        for key_id, key in self.keys.items():
            if len(key) != _MASTER_KEY_SIZE:
                raise exc.configuration(
                    f"Local KMS master key {key_id!r} must be {_MASTER_KEY_SIZE} bytes, "
                    f"got {len(key)}",
                    code="core.crypto.master_key_invalid",
                    details={"key_id": key_id, "length": len(key)},
                )

        logger.info(
            "Local KMS configured with %d master key(s) %s, fingerprint %s",
            len(self.keys),
            sorted(self.keys),
            self.fingerprint,
        )

    # ....................... #

    @property
    def fingerprint(self) -> str:
        """One-way digest of the key map, for comparing replicas — never a secret itself.

        Stable across processes and insertion order: SHA-256 over the sorted key ids,
        each paired with a domain-separated digest of its key bytes. Two replicas agree
        exactly when they hold the same ids *and* the same material, so grouping a fleet
        by this value (a startup log line, a metric attribute) turns key-map drift into
        a visible diff — including the confusing skew where one node holds *different
        bytes under the same id* (whose only runtime symptom is a generic AEAD
        authentication failure).

        A plain hash is sound here because master keys are required to be CSPRNG-random
        32-byte material: at 256 bits of entropy no preimage search is feasible, so the
        digest reveals nothing. (A guessable key would be compromised with or without a
        fingerprint — the key itself is the search target then.)
        """

        digest = hashlib.sha256(_FINGERPRINT_DOMAIN)

        for key_id in sorted(self.keys):
            encoded = key_id.encode()
            digest.update(len(encoded).to_bytes(4, "big"))
            digest.update(encoded)
            digest.update(hashlib.sha256(_KEY_DIGEST_DOMAIN + self.keys[key_id]).digest())

        return digest.hexdigest()

    # ....................... #

    def _master_key(self, key_ref: KeyRef) -> bytes:
        key = self.keys.get(key_ref.key_id)

        if key is None:
            raise exc.not_found(
                f"Unknown local KMS key id {key_ref.key_id!r}; a rotated-away key "
                "must stay configured until every envelope sealed under it is "
                "re-encrypted",
                code="core.crypto.master_key_unknown",
            )

        return key

    # ....................... #

    @staticmethod
    def _aad(key_ref: KeyRef) -> bytes:
        return f"{_AAD_PREFIX}|{key_ref.key_id}|{key_ref.version or 'v1'}".encode()

    # ....................... #

    @staticmethod
    def _wrap_subkey(master: bytes, salt: bytes) -> bytes:
        """The per-wrap AES key: HKDF-SHA256 of the master key under a fresh salt.

        Each envelope's salt is CSPRNG-random and stored alongside it, so every
        wrap encrypts under a distinct derived key. That removes the master key's
        GCM random-nonce wrap-count ceiling: a nonce collision only matters under
        the *same* key, and a derived key seals exactly one envelope.
        """

        return HKDF(
            algorithm=SHA256(),
            length=_MASTER_KEY_SIZE,
            salt=salt,
            info=_WRAP_SUBKEY_INFO,
        ).derive(master)

    # ....................... #

    def generate_data_key_sync(self, key_ref: KeyRef) -> DataKey:
        plaintext = secure_random_bytes(_DEK_SIZE)
        salt = secure_random_bytes(_WRAP_SALT_SIZE)
        nonce, ciphertext = self._aead.seal(
            key=self._wrap_subkey(self._master_key(key_ref), salt),
            plaintext=plaintext,
            aad=self._aad(key_ref),
        )

        return DataKey(
            plaintext=plaintext,
            wrapped=bytes([_WRAP_VERSION_V2]) + salt + nonce + ciphertext,
            key_id=key_ref.key_id,
            key_version=key_ref.version or "v1",
        )

    # ....................... #

    def unwrap_data_key_sync(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        # The one legacy shape: ``nonce || ciphertext`` sealed directly under the
        # master key, always exactly 60 bytes (12 + 32 + 16) — disjoint from the
        # 93-byte v2 envelope, so the dispatch is unambiguous.
        if len(wrapped) == _WRAPPED_SIZE_V1:
            return self._aead.open(
                key=self._master_key(key_ref),
                nonce=wrapped[:_NONCE_SIZE],
                ciphertext=wrapped[_NONCE_SIZE:],
                aad=self._aad(key_ref),
            )

        if len(wrapped) != _WRAPPED_SIZE_V2 or wrapped[0] != _WRAP_VERSION_V2:
            raise exc.validation(
                "Wrapped data key is not a recognized local KMS envelope",
                code="core.crypto.wrapped_key_invalid",
                details={"length": len(wrapped)},
            )

        salt = wrapped[1 : 1 + _WRAP_SALT_SIZE]
        nonce = wrapped[1 + _WRAP_SALT_SIZE : 1 + _WRAP_SALT_SIZE + _NONCE_SIZE]
        ciphertext = wrapped[1 + _WRAP_SALT_SIZE + _NONCE_SIZE :]

        return self._aead.open(
            key=self._wrap_subkey(self._master_key(key_ref), salt),
            nonce=nonce,
            ciphertext=ciphertext,
            aad=self._aad(key_ref),
        )

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        return self.generate_data_key_sync(key_ref)

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return self.unwrap_data_key_sync(wrapped=wrapped, key_ref=key_ref)
