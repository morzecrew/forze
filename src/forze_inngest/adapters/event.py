from forze.base.primitives import JsonDict
from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from datetime import datetime, timezone
from typing import final

import attrs
import inngest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandPort,
    DurableFunctionEventSpec,
)
from forze.application.execution import ExecutionContext

from ..kernel.platform import InngestClientPort
from .context import merge_envelope

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

    # ....................... #

    async def send(
        self,
        payload: M,
        *,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> str:
        data: JsonDict = dict(self.spec.codec.encode_mapping(payload))

        if self.include_execution_context and self.execution_ctx is not None:
            ctx = self.execution_ctx

            data = merge_envelope(
                data,
                metadata=ctx.inv_ctx.get_metadata(),
                authn=ctx.inv_ctx.get_authn(),
                tenant=ctx.inv_ctx.get_tenant(),
            )

        ts_ms = 0

        if occurred_at is not None:
            instant = occurred_at.astimezone(timezone.utc)
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
