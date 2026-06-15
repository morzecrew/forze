"""Whole-envelope encryption for outbox payloads (at-rest in the outbox store).

An opt-in (``OutboxSpec(encrypt=True)``) seam that encrypts a staged event's whole
serialized payload as one opaque envelope before it is persisted, and decrypts it in
the relay before the payload is decoded and published. The outbox table holds only
ciphertext; the relay, transports and consumers are unchanged (they see plaintext) —
this protects the outbox store at rest, not the broker hop.

The encrypted payload is a one-key wrapper ``{"<sentinel>": "<base64 envelope>"}`` so a
plaintext column and an encrypted column are trivially distinguishable: a relay tolerates
legacy plaintext rows (no sentinel) for a zero-downtime rollout. Associated data binds the
ciphertext to its ``(route, tenant, event_id)`` so it cannot be transplanted between rows.
"""

import base64
from uuid import UUID

import orjson

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

_ENVELOPE_KEY = "__fz_outbox_envelope__"
"""Single key marking a whole-payload encrypted outbox row."""


def _aad(route: str, tenant_id: UUID | None, event_id: UUID) -> bytes:
    return f"forze.outbox|route={route}|tenant={tenant_id}|event={event_id}".encode()


def _tenant(tenant_id: UUID | None) -> TenantIdentity | None:
    return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)


# ....................... #


def is_encrypted_payload(payload: JsonDict) -> bool:
    """Return whether *payload* is a whole-envelope wrapper (vs legacy plaintext)."""

    return len(payload) == 1 and _ENVELOPE_KEY in payload


# ....................... #


async def encrypt_outbox_payload(
    cipher: BytesCipherPort,
    payload: JsonDict,
    *,
    route: str,
    tenant_id: UUID | None,
    event_id: UUID,
) -> JsonDict:
    """Encrypt a serialized payload into a one-key envelope wrapper."""

    blob = await cipher.encrypt(
        orjson.dumps(payload),
        tenant=_tenant(tenant_id),
        aad=_aad(route, tenant_id, event_id),
    )

    return {_ENVELOPE_KEY: base64.b64encode(blob).decode("ascii")}


# ....................... #


async def decrypt_outbox_payload(
    cipher: BytesCipherPort | None,
    payload: JsonDict,
    *,
    route: str,
    tenant_id: UUID | None,
    event_id: UUID,
) -> JsonDict:
    """Decrypt a whole-envelope payload; pass plaintext (legacy) rows through unchanged.

    Fails loud when an encrypted payload is met but no keyring is wired in the relay —
    a misconfiguration, never a per-row poison: encrypted-at-rest needs the key here.
    """

    if not is_encrypted_payload(payload):
        return payload

    if cipher is None:
        raise exc.configuration(
            "Outbox payload is encrypted but no keyring is wired in the relay. "
            "Register a CryptoDepsModule in the relay process or disable "
            "OutboxSpec(encrypt=...).",
            code="core.outbox.payload_cipher_missing",
        )

    raw = await cipher.decrypt(
        base64.b64decode(payload[_ENVELOPE_KEY]),
        aad=_aad(route, tenant_id, event_id),
    )

    return orjson.loads(raw)
