"""Tests for the field-level :class:`EncryptingModelCodec`."""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization.model_codec import ModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


class _Profile(BaseModel):
    id: str
    name: str
    email: str
    prefs: dict[str, str] = {}


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _codec(
    cipher: Keyring,
    *,
    fields: frozenset[str] = frozenset({"email", "prefs"}),
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=fields,
        tenant_provider=lambda: None,
    )


_PROFILE = _Profile(
    id="1",
    name="Alice",
    email="alice@example.com",
    prefs={"theme": "dark"},
)


# ....................... #


async def test_encode_encrypts_only_marked_fields() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)

    # Plaintext, queryable fields untouched.
    assert mapping["id"] == "1"
    assert mapping["name"] == "Alice"
    # Marked fields are base64 envelopes, not the original values.
    assert mapping["email"] != "alice@example.com"
    assert is_envelope(base64.b64decode(mapping["email"]))
    assert is_envelope(base64.b64decode(mapping["prefs"]))


# ....................... #


async def test_persistence_round_trip() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)
    restored = codec.decode_mapping(mapping)

    assert restored == _PROFILE  # email + dict prefs decrypted and re-typed


# ....................... #


async def test_decode_cross_process_needs_pre_pass() -> None:
    writer = _codec(_keyring())
    await writer.prepare_encrypt()
    mapping = writer.encode_persistence_mapping(_PROFILE)

    reader = _codec(_keyring())  # cold keyring, like another process

    with pytest.raises(CoreException) as excinfo:
        reader.decode_mapping(mapping)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"

    await reader.prepare_decrypt([mapping])
    assert reader.decode_mapping(mapping) == _PROFILE


# ....................... #


async def test_encode_without_warm_raises() -> None:
    codec = _codec(_keyring())

    with pytest.raises(CoreException) as excinfo:
        codec.encode_persistence_mapping(_PROFILE)

    assert excinfo.value.code == "core.crypto.cipher_not_warm"


# ....................... #


async def test_event_path_does_not_encrypt() -> None:
    """``encode_mapping`` (events/JSON) passes through — persistence-only by design."""

    codec = _codec(_keyring())

    mapping = codec.encode_mapping(_PROFILE)

    assert mapping["email"] == "alice@example.com"


# ....................... #


async def test_decode_tolerates_legacy_plaintext() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    # A row written before encryption was enabled: plaintext in the marked field.
    legacy = {"id": "2", "name": "Bob", "email": "bob@example.com", "prefs": {}}
    restored = codec.decode_mapping(legacy)

    assert restored.email == "bob@example.com"


# ....................... #


async def test_cross_tenant_aad_mismatch_fails() -> None:
    from uuid import uuid4

    from forze.application.contracts.crypto import TenantTemplateKeyDirectory
    from forze.application.contracts.tenancy import TenantIdentity

    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
    )
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    writer = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email"}),
        tenant_provider=lambda: tenant_a,
    )
    await writer.prepare_encrypt()
    mapping = writer.encode_persistence_mapping(_PROFILE)

    # Tenant B reads tenant A's row: even after unwrapping, the AAD (which binds
    # the tenant) does not match, so authentication fails.
    reader = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email"}),
        tenant_provider=lambda: tenant_b,
    )
    await reader.prepare_decrypt([mapping])

    with pytest.raises(CoreException) as excinfo:
        reader.decode_mapping(mapping)
    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


def test_is_a_model_codec() -> None:
    codec: ModelCodec[_Profile, object] = _codec(_keyring())
    assert codec.model_type is _Profile
    assert "email" in codec.stored_field_names()
