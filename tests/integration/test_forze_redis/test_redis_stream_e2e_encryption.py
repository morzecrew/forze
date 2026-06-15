"""Integration test: end-to-end encrypted payload carried through a real Redis stream.

The producer encrypts (the outbox staging step), the ciphertext envelope rides the Redis
stream opaquely (the codec passthrough), and the consumer decrypts it — proving e2e works
over a real Redis stream, not just queues.
"""

from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.outbox import (
    decrypt_consumed_payload,
    encrypt_outbox_payload,
)
from forze.base.primitives import uuid7
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="stream-cmk")),
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_end_to_end_encrypted_stream_payload_round_trip(
    redis_stream,
    stream_payload_cls,
) -> None:
    keyring = _keyring()
    payload_codec = PydanticModelCodec(stream_payload_cls)
    stream = f"it-e2e-{uuid4().hex[:12]}"
    event_id = uuid7()

    # Producer: encrypt the serialized payload into the envelope wrapper (staging step).
    wrapper = await encrypt_outbox_payload(
        keyring,
        payload_codec.encode_mapping(stream_payload_cls(value="secret-cargo")),
        tenant_id=None,
        event_id=event_id,
    )

    # Append the ciphertext through the real stream (the codec forwards it opaquely).
    await redis_stream.append(
        stream,
        wrapper,  # type: ignore[arg-type]
        headers={HEADER_EVENT_ID: str(event_id)},
    )

    # Consume: the wrapper survives the stream round-trip as ciphertext...
    messages = await redis_stream.read({stream: "0"})
    assert len(messages) == 1
    assert is_encrypted_payload(messages[0].payload)

    # ...and the consumer decrypts it back to the plaintext model.
    model = await decrypt_consumed_payload(
        keyring,
        messages[0].payload,
        codec=payload_codec,
        headers=messages[0].headers,
    )
    assert model == stream_payload_cls(value="secret-cargo")
