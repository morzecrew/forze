"""Temporal EncryptingPayloadCodec: seals payloads at rest, round-trips, passes plaintext."""

from __future__ import annotations

import pytest
from temporalio.api.common.v1 import Payload

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
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


def test_encrypting_data_converter_installs_the_codec() -> None:
    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    converter = encrypting_data_converter(keyring)

    assert isinstance(converter.payload_codec, EncryptingPayloadCodec)
