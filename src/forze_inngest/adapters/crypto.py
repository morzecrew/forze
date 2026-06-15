"""Whole-payload encryption for Inngest events (seal on send / open on receive).

Inngest has no native payload-codec hook, so encryption is applied at the adapter seams:
the event command adapter seals the payload before the ``_forze`` context envelope is
merged (the envelope stays plaintext for routing and context binding), and the function
handler opens it after splitting the envelope, before validating the typed args — so the
handler never sees ciphertext.

Per-tenant BYOK: the send side seals under the request-scope tenant's key (recorded in the
self-describing envelope); the receive side rebuilds the tenant from the ``_forze`` envelope
for the AAD and lets the envelope resolve the key. Tenant isolation comes from the per-tenant
key, so the AAD binds ``(domain, tenant)`` only — no reliable per-event id exists here.
"""

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import decrypt_payload, encrypt_payload
from forze.base.primitives import JsonDict

# ----------------------- #

DURABLE_PAYLOAD_DOMAIN = "durable"
"""AAD domain isolating durable-event ciphertext from other contexts."""


async def seal_event_payload(
    cipher: BytesCipherPort, data: JsonDict, *, tenant: TenantIdentity | None
) -> JsonDict:
    """Seal an event payload into a one-key envelope under *tenant*'s key."""

    return await encrypt_payload(
        cipher,
        data,
        domain=DURABLE_PAYLOAD_DOMAIN,
        tenant_id=None if tenant is None else tenant.tenant_id,
        record_id=None,
    )


async def open_event_payload(
    cipher: BytesCipherPort | None,
    payload: JsonDict,
    *,
    tenant: TenantIdentity | None,
) -> JsonDict:
    """Decrypt a sealed event payload; pass legacy plaintext through unchanged."""

    return await decrypt_payload(
        cipher,
        payload,
        domain=DURABLE_PAYLOAD_DOMAIN,
        tenant_id=None if tenant is None else tenant.tenant_id,
        record_id=None,
    )
