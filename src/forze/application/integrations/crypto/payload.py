"""Whole-payload envelope encryption for opaque message/command payloads.

Transport-agnostic counterpart to field-level :class:`EncryptingModelCodec`: the
transactional outbox, direct queue/stream/pub-sub publishing, durable workflow/event
payloads, and cache bodies all seal a serialized payload as one opaque envelope
``{"<sentinel>": "<base64 envelope>"}`` (the shared ``contracts.crypto`` marker), so
plaintext and ciphertext are trivially distinguishable and legacy plaintext is tolerated
for a zero-downtime rollout.

Associated data binds each ciphertext to ``(domain, tenant, record_id)``: *domain* isolates
one contract's ciphertext from another's (a queue message can't be replayed as a cache
entry), while ``(tenant, record_id)`` are values that ride the transport headers / envelope
so a consumer reconstructs the same AAD and a ciphertext cannot be transplanted between
records. The whole messaging plane (outbox relay + direct publish) shares one
:data:`MESSAGE_PAYLOAD_DOMAIN` so its messages stay interchangeable to any consumer.
"""

import base64
import binascii
from collections.abc import Mapping
from typing import Any, cast
from uuid import UUID

import orjson

from forze.application.contracts.crypto import (
    BytesCipherPort,
    encrypted_payload_ciphertext,
    is_encrypted_payload,
    wrap_encrypted_payload,
)
from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, uuid7
from forze.base.serialization import ModelCodec

# ----------------------- #

MESSAGE_PAYLOAD_DOMAIN = "message"
"""Shared AAD domain for the messaging plane (outbox relay + direct queue/stream/pub-sub),
so a message encrypted by either path decrypts identically at any consumer."""

PAYLOAD_CIPHER_MISSING_CODE = "core.crypto.payload_cipher_missing"
PAYLOAD_BASE64_INVALID_CODE = "core.crypto.payload_base64_invalid"
PAYLOAD_HEADER_MISSING_CODE = "core.crypto.payload_header_missing"

__all__ = [
    "MESSAGE_PAYLOAD_DOMAIN",
    "PAYLOAD_CIPHER_MISSING_CODE",
    "PAYLOAD_BASE64_INVALID_CODE",
    "PAYLOAD_HEADER_MISSING_CODE",
    "is_encrypted_payload",
    "payload_aad",
    "header_uuid",
    "encrypt_payload",
    "decrypt_payload",
    "seal_message_payload",
    "decrypt_consumed_payload",
]


def payload_aad(domain: str, tenant_id: UUID | None, record_id: UUID | str | None) -> bytes:
    """AAD binding a ciphertext to its ``(domain, tenant, record_id)``.

    *record_id* accepts a ``str`` as well as a ``UUID`` so non-message contexts (e.g. a
    cache entry keyed by a document pk) can anchor it; ``str(uuid)`` formats identically,
    so a UUID and its string render the same AAD.
    """

    return f"forze.{domain}|tenant={tenant_id}|id={record_id}".encode()


def header_uuid(headers: Mapping[str, str], key: str) -> UUID | None:
    """Parse a UUID from a transport header, or ``None`` if absent/malformed."""

    value = headers.get(key)

    if not isinstance(value, str):
        return None

    try:
        return UUID(value)
    except ValueError:
        return None


def _tenant(tenant_id: UUID | None) -> TenantIdentity | None:
    return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)


# ....................... #


async def encrypt_payload(
    cipher: BytesCipherPort,
    payload: JsonDict,
    *,
    domain: str,
    tenant_id: UUID | None,
    record_id: UUID | str | None,
) -> JsonDict:
    """Encrypt a serialized payload into a one-key envelope wrapper."""

    blob = await cipher.encrypt(
        orjson.dumps(payload),
        tenant=_tenant(tenant_id),
        aad=payload_aad(domain, tenant_id, record_id),
    )

    return wrap_encrypted_payload(base64.b64encode(blob).decode("ascii"))


# ....................... #


async def seal_message_payload[M](
    cipher: BytesCipherPort,
    codec: ModelCodec[M, Any],
    payload: M,
    *,
    domain: str,
    tenant_id: UUID | None,
    headers: Mapping[str, str] | None,
) -> tuple[JsonDict, dict[str, str]]:
    """Seal a model payload and build the headers a consumer rebuilds its AAD from.

    Shared by every direct-messaging command decorator (queue/stream/pub-sub): mints a
    fresh record (event) id, encrypts the encoded payload under it, and returns the
    one-key wrapper plus headers carrying the event id (always) and tenant id (when bound),
    so the consume side reconstructs the same ``(domain, tenant, id)`` AAD.

    An already-sealed envelope wrapper (a dead-letter or other forwarding re-produce)
    passes through unchanged with the headers it arrived with: its AAD is bound to the
    ``(tenant, id)`` those headers carry, so re-sealing would mint a new binding the
    forwarded copy's headers contradict — undecryptable at the consumer.
    """

    if is_encrypted_payload(payload):
        return cast(JsonDict, payload), dict(headers or {})

    record_id = uuid7()

    wrapper = await encrypt_payload(
        cipher,
        codec.encode_mapping(payload),
        domain=domain,
        tenant_id=tenant_id,
        record_id=record_id,
    )

    sealed_headers = dict(headers or {})
    sealed_headers[HEADER_EVENT_ID] = str(record_id)
    if tenant_id is not None:
        sealed_headers[HEADER_TENANT_ID] = str(tenant_id)
    else:
        # A forwarded stale tenant header would contradict the AAD minted just above
        # (sealed under no tenant), so the consumer could never reconstruct it.
        sealed_headers.pop(HEADER_TENANT_ID, None)

    return wrapper, sealed_headers


# ....................... #


async def decrypt_payload(
    cipher: BytesCipherPort | None,
    payload: JsonDict,
    *,
    domain: str,
    tenant_id: UUID | None,
    record_id: UUID | str | None,
) -> JsonDict:
    """Decrypt a one-key envelope wrapper; pass legacy plaintext through unchanged.

    Fails loud when an encrypted payload is met but no keyring is wired — a
    misconfiguration, never a per-record poison: decryption needs the key here.
    """

    if not is_encrypted_payload(payload):
        return payload

    if cipher is None:
        raise exc.configuration(
            "Payload is encrypted but no keyring is wired to decrypt it. Register a "
            "CryptoDepsModule in this process or disable encryption for this route.",
            code=PAYLOAD_CIPHER_MISSING_CODE,
        )

    try:
        blob = base64.b64decode(encrypted_payload_ciphertext(payload), validate=True)
    except (binascii.Error, ValueError) as error:
        raise exc.validation(
            "Encrypted payload ciphertext is not valid base64",
            code=PAYLOAD_BASE64_INVALID_CODE,
        ) from error

    raw = await cipher.decrypt(blob, aad=payload_aad(domain, tenant_id, record_id))

    return orjson.loads(raw)


# ....................... #


async def decrypt_consumed_payload[M](
    cipher: BytesCipherPort | None,
    payload: M,
    *,
    domain: str,
    codec: ModelCodec[M, Any],
    headers: Mapping[str, str],
) -> M:
    """Turn a consumed message payload into the typed model, decrypting e2e ciphertext.

    The transport-agnostic consumer counterpart of staging encryption, for any consume
    path (the queue runner, or an app-driven stream/pub-sub loop): a plaintext payload is
    already the model and returned as-is; a one-key envelope wrapper is decrypted (AAD
    rebuilt from the ``event_id``/``tenant`` envelope headers) and decoded via *codec*.
    """

    if not is_encrypted_payload(payload):
        return payload

    # The producer always forwards the record (event) id (the tenant id only when
    # present), and the AAD is rebuilt from them. A missing/garbled id header means the
    # AAD cannot be reconstructed — surface that as its own error rather than letting the
    # AEAD fail with the same ``aead_auth_failed`` code as genuine tampering (a stripped
    # header / non-Forze producer). A missing tenant header is legitimate (sealed None).
    record_id = header_uuid(headers, HEADER_EVENT_ID)

    if record_id is None:
        raise exc.validation(
            "Encrypted message is missing the event-id header required to reconstruct "
            "its decryption AAD (stripped header or non-Forze producer).",
            code=PAYLOAD_HEADER_MISSING_CODE,
        )

    plaintext = await decrypt_payload(
        cipher,
        cast(JsonDict, payload),
        domain=domain,
        tenant_id=header_uuid(headers, HEADER_TENANT_ID),
        record_id=record_id,
    )

    return codec.decode_mapping(plaintext)
