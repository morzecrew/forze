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
from typing import Callable, Sequence, final

import attrs
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

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EncryptingPayloadCodec(PayloadCodec):
    """Seals each Temporal ``Payload`` with the keyring; decodes our own back."""

    cipher: BytesCipherPort
    tenant_provider: Callable[[], TenantIdentity | None] | None = None

    # ....................... #

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        # Resolve the bound tenant in the request scope so the payload seals under that
        # tenant's key (the envelope records which one, so decode needs no tenant).
        tenant = self.tenant_provider() if self.tenant_provider is not None else None

        out: list[Payload] = []

        for payload in payloads:
            sealed = await self.cipher.encrypt(
                payload.SerializeToString(), tenant=tenant, aad=_DURABLE_AAD
            )
            out.append(Payload(metadata={"encoding": _ENCODING}, data=sealed))

        return out

    # ....................... #

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []

        for payload in payloads:
            if payload.metadata.get("encoding") != _ENCODING:
                # Not ours (plaintext / legacy / non-Forze producer): pass through.
                out.append(payload)
                continue

            raw = await self.cipher.decrypt(payload.data, aad=_DURABLE_AAD)
            inner = Payload()
            inner.ParseFromString(raw)
            out.append(inner)

        return out


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _ChainedPayloadCodec(PayloadCodec):
    """Runs an *inner* codec under an *outer* one, keeping encryption outermost at rest.

    ``encode`` applies *inner* first then *outer* (e.g. compress, then encrypt the result);
    ``decode`` reverses it — *outer* first then *inner* (decrypt, then decompress). This
    keeps the encrypting codec the outermost layer at rest, so a base codec only ever sees
    plaintext and our seal wraps its output."""

    inner: PayloadCodec
    outer: PayloadCodec

    # ....................... #

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return await self.outer.encode(await self.inner.encode(payloads))

    # ....................... #

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return await self.inner.decode(await self.outer.decode(payloads))


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

    If *base* already carries a ``payload_codec`` (e.g. a compression codec), it is
    preserved by chaining: the base codec runs first on encode (so it compresses plaintext)
    and last on decode, keeping encryption the outermost layer at rest.
    """

    base_converter = base or pydantic_data_converter

    codec: PayloadCodec = EncryptingPayloadCodec(
        cipher=cipher,
        tenant_provider=tenant_provider,
    )

    if base_converter.payload_codec is not None:
        codec = _ChainedPayloadCodec(
            inner=base_converter.payload_codec,
            outer=codec,
        )

    return dataclasses.replace(base_converter, payload_codec=codec)
