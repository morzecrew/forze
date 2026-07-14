from forze.base.primitives import JsonDict
from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from datetime import UTC, datetime
from typing import final

import attrs
import inngest
from pydantic import BaseModel

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandPort,
    DurableFunctionEventSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ..kernel.client import InngestClientPort
from .context import merge_envelope
from .crypto import seal_event_payload

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestEventCommandAdapter[M: BaseModel](DurableFunctionEventCommandPort[M]):
    """Inngest-backed implementation of :class:`DurableFunctionEventCommandPort`."""

    client: InngestClientPort
    """Inngest client used to send events."""

    spec: DurableFunctionEventSpec[M]
    """Event specification (name and payload codec)."""

    execution_ctx: ExecutionContext | None = attrs.field(default=None)
    """Execution context captured when the adapter is resolved (for envelope)."""

    include_execution_context: bool = attrs.field(default=True)
    """When ``True``, embed ``ExecutionContext`` identity on the event payload."""

    cipher: BytesCipherPort | None = attrs.field(default=None, repr=False)
    """Keyring for sealing the payload when ``spec.encrypt`` is set (else ``None``)."""

    # ....................... #

    async def send(
        self,
        payload: M,
        *,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> str:
        # ``mode="json"`` because an Inngest event's ``data`` **is** JSON — the SDK's own
        # ``Event`` model types it as a JSON-value union and then posts it over HTTP. The
        # codec's default Python encode keeps a ``UUID`` / ``datetime`` / ``Decimal`` as an
        # object, which that model rejects outright, so an event payload carrying any of them
        # could not be sent at all.
        data: JsonDict = dict(self.spec.codec.encode_mapping(payload, mode="json"))
        tenant = self.execution_ctx.inv_ctx.get_tenant() if self.execution_ctx is not None else None

        if self.spec.encrypt:
            if self.cipher is None:
                raise exc.configuration(
                    f"Durable event {self.spec.name!r} declares encrypt=True but no keyring "
                    "is wired to seal it. Register a CryptoDepsModule or set encrypt=False.",
                    code="core.durable.encryption_wiring",
                )
            # Seal the payload before the context envelope is merged — the ``_forze``
            # envelope must stay plaintext for routing and context binding.
            data = await seal_event_payload(self.cipher, data, tenant=tenant)

        if self.include_execution_context and self.execution_ctx is not None:
            ctx = self.execution_ctx

            data = merge_envelope(
                data,
                metadata=ctx.inv_ctx.get_metadata(),
                authn=ctx.inv_ctx.get_authn(),
                tenant=tenant,
            )
        elif self.spec.encrypt and tenant is not None:
            # Even with the execution-context envelope suppressed, a tenant-sealed payload
            # binds the tenant into its AAD (and resolves its key from it). The receive side
            # rebuilds both from the ``_forze`` envelope, so the tenant must always travel
            # with the ciphertext — otherwise decryption fails closed with ``aead_auth_failed``.
            data = merge_envelope(data, tenant=tenant)

        ts_ms = 0

        if occurred_at is not None:
            instant = occurred_at.astimezone(UTC)
            ts_ms = int(instant.timestamp() * 1000)

        event = inngest.Event(
            name=str(self.spec.name),
            data=data,
            id=event_id or "",
            ts=ts_ms,
        )

        ids = await self.client.send(event)

        if not ids:
            raise RuntimeError("Inngest send returned no event ids")

        return ids[0]
