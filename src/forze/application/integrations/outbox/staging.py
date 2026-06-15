"""Buffer integration events and delegate durable flush to stores."""

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.application.contracts.outbox.staging_context import OutboxStagingContext
from forze.base.exceptions import exc

from .enrichment import OutboxEventEnricher
from .payload_crypto import encrypt_outbox_payload

# ----------------------- #

FlushRowsFn = Callable[[Sequence[StagedOutboxEntry]], Awaitable[int]]

# ....................... #


@attrs.define(slots=True, kw_only=True)
class OutboxStaging[M: BaseModel]:
    """Request-scoped staging buffer with store-specific flush."""

    staging: OutboxStagingContext
    """Per-request buffer and flush flag."""

    spec: OutboxSpec[M]
    """Outbox route specification."""

    enricher: OutboxEventEnricher
    """Builds integration events with invocation envelope fields."""

    flush_rows: FlushRowsFn
    """Persist buffered rows; invoked by :meth:`flush`."""

    payload_cipher: BytesCipherPort | None = None
    """Keyring for whole-payload encryption when ``spec.encrypt`` is set (else ``None``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Fail closed at construction (mirrors the decrypt-side
        # ``payload_cipher_missing``): a route that declares encryption but has no
        # keyring would otherwise stage sensitive payloads silently as plaintext.
        if self.spec.encrypts and self.payload_cipher is None:
            raise exc.configuration(
                f"Outbox route {self._route!r} declares encryption "
                f"(OutboxSpec.encryption={self.spec.encryption!r}) but no keyring is wired "
                "to encrypt its payloads. Register a CryptoDepsModule or lower the tier.",
                code="core.outbox.payload_cipher_missing",
            )

    # ....................... #

    @property
    def _route(self) -> str:
        return str(self.spec.name)

    # ....................... #

    async def _to_entry(self, event: IntegrationEvent[M]) -> StagedOutboxEntry:
        payload_json = self.spec.codec.encode_mapping(event.payload)

        if self.spec.encrypts and self.payload_cipher is not None:
            payload_json = await encrypt_outbox_payload(
                self.payload_cipher,
                payload_json,
                tenant_id=event.tenant_id,
                event_id=event.event_id,
            )

        return StagedOutboxEntry(
            outbox_route=self._route,
            event=event,
            payload_json=payload_json,
        )

    # ....................... #

    async def stage(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
        ordering_key: str | None = None,
    ) -> None:
        """Buffer an integration event.

        *ordering_key* partitions delivery on capable transports (SQS FIFO
        ``MessageGroupId``, stream partition key); same-key events relay in
        ``created_at`` order on the happy path, and a retrying/failed row
        never stalls later rows of its key.
        """

        event = self.enricher.enrich(
            event_type,
            payload,
            event_id=event_id,
            occurred_at=occurred_at,
            ordering_key=ordering_key,
        )
        await self.stage_event(event)

    # ....................... #

    async def stage_many(
        self,
        events: Sequence[tuple[str, M]],
        *,
        event_ids: Sequence[UUID] | None = None,
    ) -> None:
        """Buffer multiple integration events."""

        if event_ids is not None and len(event_ids) != len(events):
            raise exc.precondition("event_ids length must match events length")

        for index, (event_type, payload) in enumerate(events):
            eid = event_ids[index] if event_ids is not None else None
            await self.stage(event_type, payload, event_id=eid)

    # ....................... #

    async def stage_event(self, event: IntegrationEvent[M]) -> None:
        """Buffer a fully built integration event into this spec's route."""

        route = self._route

        if self.staging.flushed_for(route):
            raise exc.internal("Cannot stage outbox events after flush")

        self.staging.buffer_for(route).push([await self._to_entry(event)])

    # ....................... #

    async def flush(self) -> int:
        """Persist events buffered for this spec's route only."""

        route = self._route

        if self.staging.flushed_for(route):
            return 0

        rows = self.staging.buffer_for(route).pop()

        if not rows:
            self.staging.set_flushed(route, True)
            return 0

        written = await self.flush_rows(rows)
        self.staging.set_flushed(route, True)
        return written
