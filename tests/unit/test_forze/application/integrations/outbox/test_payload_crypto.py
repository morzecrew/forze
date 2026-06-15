"""Unit tests for outbox whole-payload envelope encryption helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.outbox import (
    decrypt_outbox_payload,
    encrypt_outbox_payload,
    is_encrypted_payload,
)
from forze.base.exceptions import CoreException, ExceptionKind
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
    route, event_id = "events", uuid4()
    payload = {"n": 7, "msg": "hello"}

    enc = await encrypt_outbox_payload(
        ring, payload, route=route, tenant_id=None, event_id=event_id
    )

    assert is_encrypted_payload(enc)
    assert "n" not in enc  # ciphertext only

    back = await decrypt_outbox_payload(
        ring, enc, route=route, tenant_id=None, event_id=event_id
    )
    assert back == payload


async def test_decrypt_passes_legacy_plaintext_through() -> None:
    ring = _keyring()
    plain = {"n": 7}

    assert not is_encrypted_payload(plain)

    out = await decrypt_outbox_payload(
        ring, plain, route="events", tenant_id=None, event_id=uuid4()
    )
    assert out == plain


async def test_decrypt_encrypted_without_keyring_fails_loud() -> None:
    ring = _keyring()
    enc = await encrypt_outbox_payload(
        ring, {"n": 1}, route="events", tenant_id=None, event_id=uuid4()
    )

    with pytest.raises(CoreException) as ei:
        await decrypt_outbox_payload(
            None, enc, route="events", tenant_id=None, event_id=uuid4()
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION


async def test_aad_binds_route_tenant_event() -> None:
    """A ciphertext can't be decrypted under a different (route, tenant, event)."""

    ring = _keyring()
    event_id = uuid4()
    enc = await encrypt_outbox_payload(
        ring, {"n": 1}, route="events", tenant_id=None, event_id=event_id
    )

    with pytest.raises(CoreException):
        await decrypt_outbox_payload(
            ring, enc, route="OTHER", tenant_id=None, event_id=event_id
        )

    with pytest.raises(CoreException):
        await decrypt_outbox_payload(
            ring, enc, route="events", tenant_id=None, event_id=uuid4()
        )
