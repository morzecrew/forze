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

The wrap is pure in-process computation, so the adapter also implements
:class:`~forze.application.contracts.crypto.SyncKeyManagementPort` — the
documented legitimate case for that opt-in: a keyring over this backend fills
its data-key cache inline and the synchronous field path never dies cold.
"""

from collections.abc import Mapping
from typing import final

import attrs

from forze.application.contracts.crypto import AesGcmAead, DataKey, KeyRef
from forze.base.exceptions import exc
from forze.base.primitives import secure_random_bytes

# ----------------------- #

_NONCE_SIZE = 12
_DEK_SIZE = 32
_MASTER_KEY_SIZE = 32
_AAD_PREFIX = "forze-local-kms"


# ....................... #


def _copy_keys(keys: Mapping[str, bytes]) -> dict[str, bytes]:
    return dict(keys)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class LocalKeyManagement:
    """Wrap/unwrap data keys under local master keys (AES-256-GCM), by key id."""

    keys: Mapping[str, bytes] = attrs.field(repr=False, converter=_copy_keys)
    """Key id → raw 32-byte key-encryption key; never logged (``repr`` suppressed),
    copied at construction so mutating the source mapping cannot change the key set.

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

    def generate_data_key_sync(self, key_ref: KeyRef) -> DataKey:
        plaintext = secure_random_bytes(_DEK_SIZE)
        nonce, ciphertext = self._aead.seal(
            key=self._master_key(key_ref),
            plaintext=plaintext,
            aad=self._aad(key_ref),
        )

        return DataKey(
            plaintext=plaintext,
            wrapped=nonce + ciphertext,
            key_id=key_ref.key_id,
            key_version=key_ref.version or "v1",
        )

    # ....................... #

    def unwrap_data_key_sync(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        if len(wrapped) <= _NONCE_SIZE:
            raise exc.validation(
                "Wrapped data key is too short to contain a nonce and ciphertext",
                code="core.crypto.wrapped_key_invalid",
                details={"length": len(wrapped)},
            )

        return self._aead.open(
            key=self._master_key(key_ref),
            nonce=wrapped[:_NONCE_SIZE],
            ciphertext=wrapped[_NONCE_SIZE:],
            aad=self._aad(key_ref),
        )

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        return self.generate_data_key_sync(key_ref)

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return self.unwrap_data_key_sync(wrapped=wrapped, key_ref=key_ref)
