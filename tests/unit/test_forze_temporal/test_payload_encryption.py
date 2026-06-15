"""Temporal EncryptingPayloadCodec: seals payloads at rest, round-trips, passes plaintext."""

from __future__ import annotations

import pytest
from temporalio.api.common.v1 import Payload

from uuid import uuid4

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze_mock import MockKeyManagement
from forze_temporal import EncryptingPayloadCodec, encrypting_data_converter

# ----------------------- #


def _codec() -> EncryptingPayloadCodec:
    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    return EncryptingPayloadCodec(keyring)


def _payload(data: bytes) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=data)


# ....................... #


@pytest.mark.asyncio
async def test_encode_seals_then_decode_round_trips() -> None:
    codec = _codec()
    original = _payload(b'{"n": 42}')

    [sealed] = await codec.encode([original])

    # Sealed: the marker encoding + the plaintext is gone from the bytes.
    assert sealed.metadata["encoding"] == b"binary/forze-encrypted"
    assert b'"n": 42' not in sealed.data

    [restored] = await codec.decode([sealed])

    # The original payload (including its encoding metadata) survives the round-trip.
    assert restored.data == original.data
    assert restored.metadata["encoding"] == b"json/plain"


@pytest.mark.asyncio
async def test_decode_passes_through_unencrypted_payloads() -> None:
    codec = _codec()
    plain = _payload(b'{"legacy": true}')

    [out] = await codec.decode([plain])

    assert out.data == plain.data  # not ours → untouched


@pytest.mark.asyncio
async def test_encode_resolves_bound_tenant_per_call() -> None:
    """encode consults the tenant provider (per-tenant key); decode needs no tenant —
    the self-describing envelope resolves the key by id."""

    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
    )
    calls: list[TenantIdentity] = []

    def _provider() -> TenantIdentity:
        tenant = TenantIdentity(tenant_id=uuid4())
        calls.append(tenant)
        return tenant

    codec = EncryptingPayloadCodec(keyring, tenant_provider=_provider)

    [sealed] = await codec.encode([_payload(b'{"x": 1}')])
    assert len(calls) == 1  # the bound tenant is resolved at seal time

    [restored] = await codec.decode([sealed])
    assert restored.data == b'{"x": 1}'  # decoded via the envelope's key id, no tenant
    assert len(calls) == 1  # decode did not consult the provider


def test_encrypting_data_converter_installs_the_codec() -> None:
    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    converter = encrypting_data_converter(keyring)

    assert isinstance(converter.payload_codec, EncryptingPayloadCodec)
