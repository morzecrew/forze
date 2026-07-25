"""Unit tests for :class:`~forze_kms.local.LocalKeyManagement`.

The adapter is pure in-process computation, so — unlike the cloud providers —
these tests exercise the real cryptography end to end, including the keyring
overlap-rotation story and the synchronous field path (no async pre-pass
needed: the :class:`SyncKeyManagementPort` payoff).
"""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
)
from forze.application.execution import CryptoDepsModule
from forze.application.integrations.crypto import Keyring
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException
from forze_kms.local import LocalKeyManagement
from tests.support.execution_context import context_from_modules

# ----------------------- #

MASTER = bytes(range(32))
MASTER_V2 = bytes(range(1, 33))

KEY_REF = KeyRef(key_id="local-v1")
KEY_REF_V2 = KeyRef(key_id="local-v2")


def _keyring(kms: LocalKeyManagement, directory: StaticKeyDirectory) -> Keyring:
    return Keyring(kms=kms, aead=AesGcmAead(), directory=directory)


# ....................... #
# Wrap/unwrap


def test_wrap_unwrap_roundtrip() -> None:
    kms = LocalKeyManagement({"local-v1": MASTER})
    dek = kms.generate_data_key_sync(KEY_REF)

    assert len(dek.plaintext) == 32
    # 12-byte nonce + 32-byte DEK ciphertext + 16-byte GCM tag.
    assert len(dek.wrapped) == 12 + 32 + 16
    assert dek.wrapped != dek.plaintext
    assert dek.key_id == "local-v1"
    assert dek.key_version == "v1"
    assert kms.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF) == dek.plaintext


async def test_async_twins_match_sync() -> None:
    kms = LocalKeyManagement({"local-v1": MASTER})

    dek = await kms.generate_data_key(KEY_REF)
    assert await kms.unwrap_data_key(wrapped=dek.wrapped, key_ref=KEY_REF) == dek.plaintext

    # The surfaces are interchangeable: async-wrapped opens sync and vice versa.
    assert kms.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF) == dek.plaintext
    sync_dek = kms.generate_data_key_sync(KEY_REF)
    assert await kms.unwrap_data_key(wrapped=sync_dek.wrapped, key_ref=KEY_REF) == sync_dek.plaintext


# ....................... #
# Rotation overlap


def test_rotation_overlap_unwraps_old_envelopes() -> None:
    # Sealed under v1 while it was active.
    old_kms = LocalKeyManagement({"local-v1": MASTER})
    dek = old_kms.generate_data_key_sync(KEY_REF)

    # After rotation: v2 is active, v1 kept for the overlap.
    rotated = LocalKeyManagement({"local-v2": MASTER_V2, "local-v1": MASTER})

    assert rotated.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF) == dek.plaintext

    # New material seals under the new key and round-trips.
    fresh = rotated.generate_data_key_sync(KEY_REF_V2)
    assert fresh.key_id == "local-v2"
    assert rotated.unwrap_data_key_sync(wrapped=fresh.wrapped, key_ref=KEY_REF_V2) == fresh.plaintext


# ....................... #
# Fail closed


def test_unknown_key_id_fails_closed() -> None:
    kms = LocalKeyManagement({"local-v2": MASTER_V2})
    dek = LocalKeyManagement({"local-v1": MASTER}).generate_data_key_sync(KEY_REF)

    with pytest.raises(CoreException) as excinfo:
        kms.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF)

    assert excinfo.value.code == "core.crypto.master_key_unknown"
    # The message is part of the contract: it must tell the operator to keep a
    # rotated-away key configured until its envelopes are re-encrypted.
    assert "rotated-away" in str(excinfo.value)


def test_wrong_master_key_fails_authentication() -> None:
    dek = LocalKeyManagement({"local-v1": MASTER}).generate_data_key_sync(KEY_REF)
    other = LocalKeyManagement({"local-v1": MASTER_V2})

    with pytest.raises(CoreException) as excinfo:
        other.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF)

    assert excinfo.value.code == "core.crypto.aead_auth_failed"


def test_wrong_key_ref_fails_authentication() -> None:
    """A blob presented under another known id fails the AAD binding, not just the key."""

    kms = LocalKeyManagement({"local-v1": MASTER, "other": MASTER_V2})
    dek = kms.generate_data_key_sync(KEY_REF)

    with pytest.raises(CoreException) as excinfo:
        kms.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KeyRef(key_id="other"))

    assert excinfo.value.code == "core.crypto.aead_auth_failed"


def test_wrong_key_version_fails_authentication() -> None:
    """The version rides the AAD, so the same key id under another version refuses."""

    kms = LocalKeyManagement({"local-v1": MASTER})
    dek = kms.generate_data_key_sync(KEY_REF)

    with pytest.raises(CoreException) as excinfo:
        kms.unwrap_data_key_sync(
            wrapped=dek.wrapped,
            key_ref=KeyRef(key_id="local-v1", version="v2"),
        )

    assert excinfo.value.code == "core.crypto.aead_auth_failed"


def test_truncated_blob_is_rejected() -> None:
    kms = LocalKeyManagement({"local-v1": MASTER})

    with pytest.raises(CoreException) as excinfo:
        kms.unwrap_data_key_sync(wrapped=b"\x00" * 12, key_ref=KEY_REF)

    assert excinfo.value.code == "core.crypto.wrapped_key_invalid"


# ....................... #
# Construction


def test_empty_key_map_is_rejected() -> None:
    with pytest.raises(CoreException) as excinfo:
        LocalKeyManagement({})

    assert excinfo.value.code == "core.crypto.master_key_invalid"


@pytest.mark.parametrize("bad_key", [b"short", bytes(31), bytes(33)])
def test_master_key_size_is_enforced(bad_key: bytes) -> None:
    with pytest.raises(CoreException) as excinfo:
        LocalKeyManagement({"local-v1": bad_key})

    assert excinfo.value.code == "core.crypto.master_key_invalid"


def test_source_mapping_is_copied_at_construction() -> None:
    source = {"local-v1": MASTER}
    kms = LocalKeyManagement(source)
    dek = kms.generate_data_key_sync(KEY_REF)

    source.clear()

    assert kms.unwrap_data_key_sync(wrapped=dek.wrapped, key_ref=KEY_REF) == dek.plaintext


def test_key_map_is_read_only_after_construction() -> None:
    """The exposed mapping refuses writes — the validated key set cannot be
    extended, replaced, or shrunk behind the frozen instance's back."""

    kms = LocalKeyManagement({"local-v1": MASTER})

    with pytest.raises(TypeError):
        kms.keys["local-v2"] = MASTER_V2  # type: ignore[index]

    with pytest.raises((TypeError, AttributeError)):
        del kms.keys["local-v1"]  # type: ignore[attr-defined]


# ....................... #
# Fingerprint (fleet drift comparison)


def test_fingerprint_is_stable_and_order_independent() -> None:
    a = LocalKeyManagement({"local-v1": MASTER, "local-v2": MASTER_V2})
    b = LocalKeyManagement({"local-v2": MASTER_V2, "local-v1": MASTER})

    assert a.fingerprint == b.fingerprint
    assert a.fingerprint != LocalKeyManagement({"local-v1": MASTER}).fingerprint


def test_fingerprint_distinguishes_same_id_different_bytes() -> None:
    """The skew whose only runtime symptom is a generic AEAD auth failure —
    the fingerprint must make it visible."""

    assert (
        LocalKeyManagement({"local-v1": MASTER}).fingerprint
        != LocalKeyManagement({"local-v1": MASTER_V2}).fingerprint
    )


def test_fingerprint_encoding_is_pinned() -> None:
    """Operators compare fingerprints across nodes that may run different forze
    versions — changing the encoding must be a visible decision, never an accident."""

    assert LocalKeyManagement({"local-v1": MASTER}).fingerprint == (
        "edfe3539b9dd909f3567997202e364d06becd611bdef82a388a3b396530ac193"
    )


def test_fingerprint_and_repr_carry_no_key_material() -> None:
    kms = LocalKeyManagement({"local-v1": MASTER})

    assert len(kms.fingerprint) == 64  # full SHA-256 hex digest
    assert int(kms.fingerprint, 16) >= 0  # hex digest, not material
    assert repr(kms) == "LocalKeyManagement()"  # the keys field is repr-suppressed


# ....................... #
# Keyring integration (the overlap-rotation story, end to end)


async def test_keyring_overlap_rotation_across_epochs() -> None:
    """Epoch 1 encrypts under v1; epoch 2 (v2 active, v1 previous) still reads it,
    and its new envelopes carry the new key id."""

    epoch1 = context_from_modules(
        CryptoDepsModule(
            kms=LocalKeyManagement({"local-v1": MASTER}),
            directory=StaticKeyDirectory(KEY_REF),
        )
    ).deps.provide(KeyringDepKey)

    blob = await epoch1.encrypt(b"pre-rotation secret", tenant=None)
    assert unpack_envelope(blob).key_id == "local-v1"

    epoch2 = context_from_modules(
        CryptoDepsModule(
            kms=LocalKeyManagement({"local-v2": MASTER_V2, "local-v1": MASTER}),
            directory=StaticKeyDirectory(KEY_REF_V2, previous_key_ref=KEY_REF),
        )
    ).deps.provide(KeyringDepKey)

    assert await epoch2.decrypt(blob) == b"pre-rotation secret"

    fresh = await epoch2.encrypt(b"post-rotation secret", tenant=None)
    assert unpack_envelope(fresh).key_id == "local-v2"
    assert await epoch2.decrypt(fresh) == b"post-rotation secret"


def test_dropping_the_previous_key_closes_the_overlap() -> None:
    """Once v1 leaves the map, its envelopes refuse with the rotated-away message —
    the fail-closed end state the docs' rotation procedure leads to."""

    writer = _keyring(
        LocalKeyManagement({"local-v1": MASTER}),
        StaticKeyDirectory(KEY_REF),
    )
    blob = writer.encrypt_sync(b"stale secret", tenant=None)
    envelope = unpack_envelope(blob)

    closed = _keyring(
        LocalKeyManagement({"local-v2": MASTER_V2}),
        StaticKeyDirectory(KEY_REF_V2),
    )

    with pytest.raises(CoreException) as excinfo:
        closed.ensure_unwrapped_sync([envelope])

    assert excinfo.value.code == "core.crypto.master_key_unknown"


# ....................... #
# The SyncKeyManagementPort payoff: the sync field path needs no async pre-pass


def test_sync_path_works_without_an_async_pre_pass() -> None:
    """A cold keyring over the local backend encrypts synchronously (inline fill),
    and a cold reader only needs the *sync* pre-pass — no event loop anywhere."""

    writer = _keyring(
        LocalKeyManagement({"local-v1": MASTER}),
        StaticKeyDirectory(KEY_REF),
    )

    blob = writer.encrypt_sync(b"field value", tenant=None)  # no warm(), no cipher_not_warm
    assert writer.decrypt_sync(blob) == b"field value"

    reader = _keyring(  # cold cache, as in another process
        LocalKeyManagement({"local-v1": MASTER}),
        StaticKeyDirectory(KEY_REF),
    )
    reader.ensure_unwrapped_sync([unpack_envelope(blob)])
    assert reader.decrypt_sync(blob) == b"field value"
