"""Unit tests for outbox whole-payload envelope encryption helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from pydantic import BaseModel

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.outbox import (
    decrypt_consumed_payload,
    decrypt_outbox_payload,
    encrypt_outbox_payload,
    is_encrypted_payload,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="outbox-cmk")),
    )


# ....................... #


async def test_encrypt_then_decrypt_round_trip() -> None:
    ring = _keyring()
    event_id = uuid4()
    payload = {"n": 7, "msg": "hello"}

    enc = await encrypt_outbox_payload(
        ring, payload, tenant_id=None, event_id=event_id
    )

    assert is_encrypted_payload(enc)
    assert "n" not in enc  # ciphertext only

    back = await decrypt_outbox_payload(
        ring, enc, tenant_id=None, event_id=event_id
    )
    assert back == payload


async def test_decrypt_passes_legacy_plaintext_through() -> None:
    ring = _keyring()
    plain = {"n": 7}

    assert not is_encrypted_payload(plain)

    out = await decrypt_outbox_payload(
        ring, plain, tenant_id=None, event_id=uuid4()
    )
    assert out == plain


async def test_decrypt_encrypted_without_keyring_fails_loud() -> None:
    ring = _keyring()
    enc = await encrypt_outbox_payload(
        ring, {"n": 1}, tenant_id=None, event_id=uuid4()
    )

    with pytest.raises(CoreException) as ei:
        await decrypt_outbox_payload(
            None, enc, tenant_id=None, event_id=uuid4()
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION


class _Model(BaseModel):
    n: int


async def test_consumed_payload_missing_event_id_header_is_clear_error() -> None:
    """A stripped event-id header fails with its own code, not aead_auth_failed."""

    ring = _keyring()
    codec = PydanticModelCodec(_Model)
    enc = await encrypt_outbox_payload(ring, {"n": 1}, tenant_id=None, event_id=uuid4())

    with pytest.raises(CoreException) as ei:
        # No HEADER_EVENT_ID → the AAD cannot be reconstructed.
        await decrypt_consumed_payload(ring, enc, codec=codec, headers={})

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert ei.value.code == "core.outbox.payload_header_missing"


async def test_consumed_payload_with_event_id_header_decrypts() -> None:
    ring = _keyring()
    codec = PydanticModelCodec(_Model)
    event_id = uuid4()
    enc = await encrypt_outbox_payload(ring, {"n": 1}, tenant_id=None, event_id=event_id)

    model = await decrypt_consumed_payload(
        ring, enc, codec=codec, headers={HEADER_EVENT_ID: str(event_id)}
    )

    assert model == _Model(n=1)


async def test_decrypt_rejects_invalid_base64_ciphertext() -> None:
    """A corrupted (non-base64) wrapper fails as validation, not a raw binascii error."""

    ring = _keyring()
    enc = await encrypt_outbox_payload(ring, {"n": 1}, tenant_id=None, event_id=uuid4())
    enc["__fz_enc__"] = "not valid base64!!"  # transport/broker corruption

    with pytest.raises(CoreException) as ei:
        await decrypt_outbox_payload(ring, enc, tenant_id=None, event_id=uuid4())

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert ei.value.code == "core.outbox.payload_base64_invalid"


async def test_aad_binds_tenant_and_event() -> None:
    """A ciphertext can't be decrypted under a different (tenant, event)."""

    from uuid import uuid4 as _uuid4

    ring = _keyring()
    tenant_id, event_id = _uuid4(), uuid4()
    enc = await encrypt_outbox_payload(
        ring, {"n": 1}, tenant_id=tenant_id, event_id=event_id
    )

    # Same (tenant, event) → succeeds.
    assert await decrypt_outbox_payload(
        ring, enc, tenant_id=tenant_id, event_id=event_id
    ) == {"n": 1}

    # Wrong event id → fails.
    with pytest.raises(CoreException):
        await decrypt_outbox_payload(
            ring, enc, tenant_id=tenant_id, event_id=uuid4()
        )

    # Wrong tenant → fails.
    with pytest.raises(CoreException):
        await decrypt_outbox_payload(
            ring, enc, tenant_id=uuid4(), event_id=event_id
        )
