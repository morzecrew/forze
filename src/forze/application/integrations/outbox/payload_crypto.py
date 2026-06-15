"""Whole-payload envelope encryption for the transactional outbox (at-rest + e2e).

An ``OutboxSpec(encryption=...)`` seam that encrypts a staged event's whole serialized
payload as one opaque envelope at staging. Where it is decrypted depends on the tier:

- ``at_rest`` — the relay decrypts before publish (store-only protection).
- ``end_to_end`` — the ciphertext travels through the broker and the **consumer**
  decrypts it.

Both sides use the same helpers here. The encrypted payload is a one-key wrapper
``{"<sentinel>": "<base64 envelope>"}`` so plaintext and ciphertext are trivially
distinguishable: relays and consumers tolerate legacy plaintext (no sentinel) for a
zero-downtime rollout. Associated data binds the ciphertext to its ``(tenant, event_id)``
— the two identifiers that travel in transport headers, so an ``end_to_end`` consumer can
reconstruct the AAD and a ciphertext cannot be transplanted between events.
"""

import base64
from uuid import UUID

import orjson

from forze.application.contracts.crypto import (
    BytesCipherPort,
    encrypted_payload_ciphertext,
    is_encrypted_payload,
    wrap_encrypted_payload,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

# ``is_encrypted_payload`` is re-exported (relay / consumer runner import it from here).
__all__ = [
    "is_encrypted_payload",
    "encrypt_outbox_payload",
    "decrypt_outbox_payload",
]


def _aad(tenant_id: UUID | None, event_id: UUID | None) -> bytes:
    # Only tenant + event id — both ride transport headers, so an end-to-end consumer
    # reconstructs the same AAD from the message it received.
    return f"forze.outbox|tenant={tenant_id}|event={event_id}".encode()


def _tenant(tenant_id: UUID | None) -> TenantIdentity | None:
    return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)


# ....................... #


async def encrypt_outbox_payload(
    cipher: BytesCipherPort,
    payload: JsonDict,
    *,
    tenant_id: UUID | None,
    event_id: UUID,
) -> JsonDict:
    """Encrypt a serialized payload into a one-key envelope wrapper."""

    blob = await cipher.encrypt(
        orjson.dumps(payload),
        tenant=_tenant(tenant_id),
        aad=_aad(tenant_id, event_id),
    )

    return wrap_encrypted_payload(base64.b64encode(blob).decode("ascii"))


# ....................... #


async def decrypt_outbox_payload(
    cipher: BytesCipherPort | None,
    payload: JsonDict,
    *,
    tenant_id: UUID | None,
    event_id: UUID | None,
) -> JsonDict:
    """Decrypt a whole-envelope payload; pass plaintext (legacy) payloads through unchanged.

    Fails loud when an encrypted payload is met but no keyring is wired (relay or
    consumer) — a misconfiguration, never a per-row poison: decryption needs the key here.
    """

    if not is_encrypted_payload(payload):
        return payload

    if cipher is None:
        raise exc.configuration(
            "Outbox payload is encrypted but no keyring is wired to decrypt it. "
            "Register a CryptoDepsModule in this process or lower OutboxSpec(encryption=...).",
            code="core.outbox.payload_cipher_missing",
        )

    raw = await cipher.decrypt(
        base64.b64decode(encrypted_payload_ciphertext(payload)),
        aad=_aad(tenant_id, event_id),
    )

    return orjson.loads(raw)
