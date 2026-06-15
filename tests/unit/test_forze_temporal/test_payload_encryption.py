"""Temporal EncryptingPayloadCodec: seals payloads at rest, round-trips, passes plaintext."""

from __future__ import annotations

import dataclasses

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec

from collections.abc import Sequence
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


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


class _MarkerCodec(PayloadCodec):
    """Records its invocation order and tags payload metadata so we can assert chaining."""

    def __init__(self, order: list[str]) -> None:
        self._order = order

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self._order.append("base.encode")
        return [
            Payload(metadata={**dict(p.metadata), "base": b"1"}, data=p.data) for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self._order.append("base.decode")
        return list(payloads)


@pytest.mark.asyncio
async def test_encrypting_data_converter_chains_existing_base_codec() -> None:
    """A base codec is preserved: it runs first on encode (sees plaintext) and last on
    decode, keeping encryption the outermost layer at rest."""

    from temporalio.converter import DataConverter

    order: list[str] = []
    base = dataclasses.replace(DataConverter(), payload_codec=_MarkerCodec(order))

    converter = encrypting_data_converter(_keyring(), base=base)
    codec = converter.payload_codec
    assert codec is not None

    [sealed] = await codec.encode([_payload(b'{"n": 7}')])

    # Base codec ran first (its metadata tag is gone because encryption wraps the whole
    # payload), and the result is sealed: encryption is the outermost layer.
    assert sealed.metadata["encoding"] == b"binary/forze-encrypted"
    assert order == ["base.encode"]

    [restored] = await codec.decode([sealed])

    # Decode reverses the order: decrypt (outer) first, then the base codec (inner).
    assert order == ["base.encode", "base.decode"]
    assert restored.data == b'{"n": 7}'
    assert restored.metadata["base"] == b"1"  # the base codec's encode tag survived the seal
