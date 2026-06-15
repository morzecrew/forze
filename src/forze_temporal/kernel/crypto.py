"""Encrypting Temporal ``PayloadCodec`` — seals workflow/activity payloads at rest.

Temporal's :class:`PayloadCodec` is the native seam for payload encryption: the SDK runs
``encode`` before a payload leaves the client and ``decode`` after it arrives, so workflow
inputs/outputs, signals, queries, and activity args are sealed in the Temporal datastore
and on the wire while staying transparent to handlers.

**Per-tenant BYOK.** ``encode`` runs in the request scope (a handler calling
``start_workflow``/signals), so it resolves the bound tenant via *tenant_provider* and
seals under that tenant's key — consistent with every other encryption seam. ``decode``
runs context-free on the worker, but needs no tenant: the self-describing
:class:`EncryptedEnvelope` records the ``key_id``, so the keyring resolves the right
per-tenant key straight from the envelope. Tenant isolation therefore comes from the
per-tenant *key* (recorded in the envelope), which is why the AAD stays tenant-independent
(domain-only) — the worker decode never depends on ambient context. The whole inner
``Payload`` is sealed so its original encoding survives; a payload we did not encrypt
(legacy / non-Forze producer) passes through untouched. Without a *tenant_provider* it
falls back to the deployment's default key (``tenant=None``).
"""

import dataclasses
from collections.abc import Callable, Sequence

from temporalio.api.common.v1 import Payload
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter, PayloadCodec

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #

_ENCODING = b"binary/forze-encrypted"
"""Marker on a sealed payload's ``encoding`` metadata so ``decode`` recognizes ours."""

_DURABLE_AAD = b"forze.durable"
"""Tenant-independent associated data binding the ciphertext to the durable domain.

Kept tenant-free so the context-free worker ``decode`` never needs ambient context; the
per-tenant *key* (self-described in the envelope) provides cross-tenant isolation."""


class EncryptingPayloadCodec(PayloadCodec):
    """Seals each Temporal ``Payload`` with the keyring; decodes our own back."""

    def __init__(
        self,
        cipher: BytesCipherPort,
        *,
        tenant_provider: Callable[[], TenantIdentity | None] | None = None,
    ) -> None:
        self._cipher = cipher
        self._tenant_provider = tenant_provider

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        # Resolve the bound tenant in the request scope so the payload seals under that
        # tenant's key (the envelope records which one, so decode needs no tenant).
        tenant = self._tenant_provider() if self._tenant_provider is not None else None

        out: list[Payload] = []

        for payload in payloads:
            sealed = await self._cipher.encrypt(
                payload.SerializeToString(), tenant=tenant, aad=_DURABLE_AAD
            )
            out.append(Payload(metadata={"encoding": _ENCODING}, data=sealed))

        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []

        for payload in payloads:
            if payload.metadata.get("encoding") != _ENCODING:
                # Not ours (plaintext / legacy / non-Forze producer): pass through.
                out.append(payload)
                continue

            raw = await self._cipher.decrypt(payload.data, aad=_DURABLE_AAD)
            inner = Payload()
            inner.ParseFromString(raw)
            out.append(inner)

        return out


# ....................... #


def encrypting_data_converter(
    cipher: BytesCipherPort,
    *,
    tenant_provider: Callable[[], TenantIdentity | None] | None = None,
    base: DataConverter | None = None,
) -> DataConverter:
    """A ``DataConverter`` that seals payloads via *cipher*, composed over *base*.

    *tenant_provider* (typically ``ctx.inv_ctx.get_tenant``) resolves the bound tenant at
    encode time for per-tenant keys. *base* defaults to the pydantic data converter; pass a
    custom converter to keep its payload/failure conversion while adding encryption.
    """

    return dataclasses.replace(
        base or pydantic_data_converter,
        payload_codec=EncryptingPayloadCodec(cipher, tenant_provider=tenant_provider),
    )
