"""Tests for record-id AAD binding in :class:`EncryptingModelCodec`."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import default_model_codec
from forze_mock import MockKeyManagement

# ----------------------- #


class _Profile(BaseModel):
    id: str
    name: str
    email: str


class _UpdateProfile(BaseModel):
    name: str | None = None
    email: str | None = None


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _codec(
    cipher: Keyring,
    *,
    record_id_field: str | None = "id",
    fields: frozenset[str] = frozenset({"email"}),
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=fields,
        tenant_provider=lambda: None,
        record_id_field=record_id_field,
    )


def _patch_codec(cipher: Keyring) -> EncryptingModelCodec[_UpdateProfile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_UpdateProfile),
        cipher=cipher,
        fields=frozenset({"email"}),
        tenant_provider=lambda: None,
        record_id_field="id",
    )


_A = _Profile(id="rec-a", name="Alice", email="a@example.com")
_B = _Profile(id="rec-b", name="Bob", email="b@example.com")


# ....................... #


async def test_bound_field_round_trips() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    stored = codec.encode_persistence_mapping(_A)
    assert stored["email"] != "a@example.com"  # ciphertext at rest

    back = codec.decode_mapping(stored)
    assert back.email == "a@example.com"


async def test_transplant_to_another_record_is_rejected() -> None:
    """A ciphertext bound to record A must not decrypt under record B's id."""

    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    a_stored = codec.encode_persistence_mapping(_A)

    # Move A's ciphertext into a row carrying B's id (an attacker with write access).
    transplanted = {"id": "rec-b", "name": "Bob", "email": a_stored["email"]}

    with pytest.raises(CoreException) as ei:
        codec.decode_mapping(transplanted)

    assert ei.value.kind is ExceptionKind.VALIDATION


async def test_unbound_codec_allows_transplant() -> None:
    """Without binding, the same ciphertext decrypts in any record (baseline)."""

    ring = _keyring()
    codec = _codec(ring, record_id_field=None)
    await codec.prepare_encrypt()

    a_stored = codec.encode_persistence_mapping(_A)
    transplanted = {"id": "rec-b", "name": "Bob", "email": a_stored["email"]}

    assert codec.decode_mapping(transplanted).email == "a@example.com"


async def test_decode_falls_back_to_legacy_aad_for_migration() -> None:
    """Ciphertext written before binding (unbound AAD) still reads once binding is on."""

    ring = _keyring()
    legacy = _codec(ring, record_id_field=None)
    await legacy.prepare_encrypt()
    legacy_stored = legacy.encode_persistence_mapping(_A)

    # Same keyring (warm cache), now reading through a binding-enabled codec.
    bound = _codec(ring)
    assert bound.decode_mapping(legacy_stored).email == "a@example.com"


async def test_decode_bound_field_without_record_id_is_clear_error() -> None:
    """An id-bound value read without its id column fails as a clear misconfiguration,
    not an opaque tamper error (e.g. a projection that omitted the id)."""

    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    stored = codec.encode_persistence_mapping(_A)
    # A projection that selected the encrypted field but not its id column.
    without_id = {"name": "Alice", "email": stored["email"]}

    with pytest.raises(CoreException) as ei:
        codec.decode_mapping(without_id)

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "core.crypto.record_id_required"


async def test_encode_patch_binds_threaded_record_id() -> None:
    """The patch path threads the pk; the resulting ciphertext is bound to it."""

    ring = _keyring()
    patch_codec = _patch_codec(ring)
    read_codec = _codec(ring)
    await patch_codec.prepare_encrypt()

    patch = patch_codec.encode_persistence_patch(
        _UpdateProfile(email="patched@example.com"),
        record_id="rec-a",
        exclude={"unset": True},
    )

    # Decrypts only when the row carries the threaded id...
    assert read_codec.decode_mapping(
        {"id": "rec-a", "name": "x", **patch}
    ).email == "patched@example.com"
    # ...and is rejected under any other id.
    with pytest.raises(CoreException):
        read_codec.decode_mapping({"id": "rec-b", "name": "x", **patch})


async def test_patch_without_id_rejects_bound_field() -> None:
    """A bulk-style patch (no record id) of a bound encrypted field fails loud."""

    ring = _keyring()
    patch_codec = _patch_codec(ring)
    await patch_codec.prepare_encrypt()

    with pytest.raises(CoreException) as ei:
        patch_codec.encode_persistence_patch(
            _UpdateProfile(email="nope@example.com"),
            record_id=None,
            exclude={"unset": True},
        )

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "core.crypto.record_id_required"


async def test_patch_without_id_allows_plaintext_only_fields() -> None:
    """A bulk patch touching no encrypted field needs no record id."""

    ring = _keyring()
    patch_codec = _patch_codec(ring)
    await patch_codec.prepare_encrypt()

    # Only ``email`` is encrypted; this patch sets only the plaintext ``name``.
    patch = patch_codec.encode_persistence_patch(
        _UpdateProfile(name="renamed"),
        record_id=None,
        exclude={"unset": True},
    )

    assert patch["name"] == "renamed"
    assert "email" not in patch


# ....................... #
# reject_plaintext (post-migration): the legacy id-less AAD downgrade is disabled


def _strict_codec(
    cipher: Keyring,
    *,
    record_id_field: str | None = "id",
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=frozenset({"email"}),
        tenant_provider=lambda: None,
        record_id_field=record_id_field,
        reject_plaintext=True,
    )


async def test_strict_mode_rejects_legacy_unbound_ciphertext() -> None:
    """With reject_plaintext on, a pre-binding (id-less AAD) ciphertext no longer reads."""

    ring = _keyring()
    legacy = _codec(ring, record_id_field=None)
    await legacy.prepare_encrypt()
    legacy_stored = legacy.encode_persistence_mapping(_A)

    strict = _strict_codec(ring)  # same warm keyring, binding + strict

    with pytest.raises(CoreException) as excinfo:
        strict.decode_mapping(legacy_stored)

    # No legacy downgrade: the id-bound AAD fails to authenticate the unbound ciphertext.
    assert excinfo.value.code == "core.crypto.aead_auth_failed"


async def test_strict_mode_still_reads_bound_ciphertext() -> None:
    ring = _keyring()
    codec = _strict_codec(ring)
    await codec.prepare_encrypt()

    stored = codec.encode_persistence_mapping(_A)
    assert codec.decode_mapping(stored).email == "a@example.com"


async def test_strict_mode_missing_record_id_requires_it() -> None:
    """Strict binding makes the id mandatory — no id-less fallback attempt."""

    ring = _keyring()
    codec = _strict_codec(ring)
    await codec.prepare_encrypt()

    stored = codec.encode_persistence_mapping(_A)
    without_id = {"name": "Alice", "email": stored["email"]}

    with pytest.raises(CoreException) as excinfo:
        codec.decode_mapping(without_id)

    assert excinfo.value.code == "core.crypto.record_id_required"
