"""Application-layer encrypting decorator for direct queue publishing.

Wraps any :class:`QueueCommandPort` so that, when a :class:`QueueSpec` declares
``encryption="end_to_end"``, each published payload is sealed into a one-key envelope
(the shared messaging-plane domain) before it reaches the backend. The backend's queue
codec already forwards the wrapper opaquely, and a consumer decrypts it from the
``event_id``/``tenant`` headers — the same path the outbox relay uses, so direct-published
and outbox-relayed messages are interchangeable.

Backend-agnostic: every backend's write factory wraps its adapter with
:func:`encrypting_queue_command`; the adapters themselves stay unaware of encryption.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import cast, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.queue import QueueCommandPort, QueueSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.integrations.crypto import (
    MESSAGE_PAYLOAD_DOMAIN,
    seal_message_payload,
)
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptingQueueCommand[M](QueueCommandPort[M]):
    """Seals each payload before delegating to the wrapped queue command."""

    inner: QueueCommandPort[M]
    spec: QueueSpec[M]
    cipher: BytesCipherPort = attrs.field(repr=False)
    tenant_provider: TenantProviderPort

    # ....................... #

    async def _seal(
        self, payload: M, headers: Mapping[str, str] | None
    ) -> tuple[M, dict[str, str]]:
        tenant = self.tenant_provider()
        wrapper, sealed_headers = await seal_message_payload(
            self.cipher,
            self.spec.codec,
            payload,
            domain=MESSAGE_PAYLOAD_DOMAIN,
            tenant_id=None if tenant is None else tenant.tenant_id,
            headers=headers,
        )
        return cast(M, wrapper), sealed_headers

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        sealed_payload, sealed_headers = await self._seal(payload, headers)
        return await self.inner.enqueue(
            queue,
            sealed_payload,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=delay,
            not_before=not_before,
            headers=sealed_headers,
        )

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[str]:
        # Each message needs its own event id + AAD, so a batch is published one message
        # at a time (encryption trades the single backend round-trip for per-message AAD).
        ids: list[str] = []
        for payload in payloads:
            ids.append(
                await self.enqueue(
                    queue,
                    payload,
                    type=type,
                    key=key,
                    enqueued_at=enqueued_at,
                    delay=delay,
                    not_before=not_before,
                    headers=headers,
                )
            )
        return ids


# ....................... #


def encrypting_queue_command[M](
    inner: QueueCommandPort[M],
    spec: QueueSpec[M],
    *,
    cipher: BytesCipherPort | None,
    tenant_provider: TenantProviderPort,
) -> QueueCommandPort[M]:
    """Wrap *inner* with payload encryption when *spec* declares it; else return it as-is.

    Fail-closed: a route that declares ``encryption`` but resolves no keyring (*cipher* is
    ``None``) raises rather than silently publishing plaintext.
    """

    if not spec.encrypts:
        return inner

    if cipher is None:
        raise exc.configuration(
            f"Queue route {spec.name!r} declares encryption but no keyring is wired to "
            "seal its payloads. Register a CryptoDepsModule or set encryption='none'.",
            code="core.queue.encryption_wiring",
        )

    return EncryptingQueueCommand(
        inner=inner, spec=spec, cipher=cipher, tenant_provider=tenant_provider
    )
