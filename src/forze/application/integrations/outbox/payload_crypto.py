"""Outbox facade over the shared whole-payload crypto primitive.

The transactional outbox encrypts a staged event's payload as one opaque envelope (tier
``at_rest`` decrypts at the relay; ``end_to_end`` at the consumer). The mechanism is the
transport-agnostic :mod:`forze.application.integrations.crypto.payload`; the outbox shares
the messaging-plane domain so its messages decrypt identically whether relayed from the
outbox or published directly to a queue/stream/pub-sub.
"""

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.integrations.crypto.payload import (
    MESSAGE_PAYLOAD_DOMAIN,
    decrypt_payload,
    encrypt_payload,
    is_encrypted_payload,
)
from forze.application.integrations.crypto.payload import (
    decrypt_consumed_payload as _decrypt_consumed_payload,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec

# ----------------------- #

# ``is_encrypted_payload`` is re-exported (relay / consumer runner import it from here).
__all__ = [
    "is_encrypted_payload",
    "encrypt_outbox_payload",
    "decrypt_outbox_payload",
    "decrypt_consumed_payload",
]


async def encrypt_outbox_payload(
    cipher: BytesCipherPort,
    payload: JsonDict,
    *,
    tenant_id: UUID | None,
    event_id: UUID,
) -> JsonDict:
    """Encrypt a staged payload into a one-key envelope wrapper."""

    return await encrypt_payload(
        cipher,
        payload,
        domain=MESSAGE_PAYLOAD_DOMAIN,
        tenant_id=tenant_id,
        record_id=event_id,
    )


async def decrypt_outbox_payload(
    cipher: BytesCipherPort | None,
    payload: JsonDict,
    *,
    tenant_id: UUID | None,
    event_id: UUID | None,
) -> JsonDict:
    """Decrypt a one-key envelope wrapper; pass legacy plaintext through unchanged."""

    return await decrypt_payload(
        cipher,
        payload,
        domain=MESSAGE_PAYLOAD_DOMAIN,
        tenant_id=tenant_id,
        record_id=event_id,
    )


async def decrypt_consumed_payload[M](
    cipher: BytesCipherPort | None,
    payload: M,
    *,
    codec: ModelCodec[M, Any],
    headers: Mapping[str, str],
) -> M:
    """Turn a consumed message payload into the typed model, decrypting e2e ciphertext."""

    return await _decrypt_consumed_payload(
        cipher,
        payload,
        domain=MESSAGE_PAYLOAD_DOMAIN,
        codec=codec,
        headers=headers,
    )
