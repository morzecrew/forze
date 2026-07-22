"""Durable search-index maintenance through a dedicated transactional outbox route.

The after-commit sync in :mod:`.sync` is at-most-once: an index call that exhausts its
in-place retries leaves the store and the index divergent until the row's next write.
This module is the opt-in stronger delivery: each committed write stages an
**index-maintenance marker** on a dedicated outbox route **in the same transaction** as
the write, the standard relay carries it at-least-once to a queue, and a small consumer
applies it to the ``SearchCommandPort`` with inbox dedup — a transient index failure now
retries via broker redelivery instead of being dropped.

The marker carries **identity only** (the document id), never row data: the consumer
**re-reads the row's current committed state** and applies *that* (upsert a live row,
delete a missing or soft-deleted one). Re-reading makes application idempotent and
order-insensitive — the outbox relay deliberately does not preserve per-``ordering_key``
order across failures/retries, so a stale upsert marker relayed *after* a later delete
re-reads the deleted state and still deletes; a ghost can never be resurrected. It also
keeps field-encrypted values out of the outbox row: the event payload is one id.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import (
    LifecycleStep,
    OnSuccess,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.integrations.search import assert_search_encryption_parity
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.document.dto import written_read_model
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD
from forze_kits.integrations.outbox import RelayBinding

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution.context import ExecutionContext
    from forze_kits.integrations.consumer import QueueConsumer

# ----------------------- #

SEARCH_SYNC_EVENT_TYPE: Final[str] = "search_index.sync"
"""Integration-event type staged for every index-maintenance marker."""

_STAGE_STEP_ID: Final[StrKey] = "search_sync_stage"

# ....................... #


class SearchSyncMarker(BaseModel):
    """One staged index-maintenance marker: the row's identity, nothing else.

    Deliberately payload-free beyond the id — the consumer re-reads the row's current
    committed state and applies that, so the marker never carries (possibly sensitive)
    row data and never encodes a stale upsert-vs-delete decision.
    """

    document_id: str
    """The document's primary key, as a string."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class OutboxSearchSync:
    """Opt-in durable delivery for a kit's search-index maintenance.

    Passed as ``AggregateKit(search=…, search_delivery=OutboxSearchSync())``: the
    after-commit best-effort sync is replaced by in-tx staged markers on a dedicated
    outbox route, relayed at-least-once and applied by re-read (see the module docs).
    The deps module must wire the outbox, queue, and inbox backends under
    :meth:`resolved_route` (``backend_requirements`` reports it).
    """

    route: StrKey | None = None
    """Route name shared by the derived outbox, queue, and inbox specs
    (default ``<search-name>_sync``)."""

    relay: RelayBinding | None = attrs.field(factory=RelayBinding)
    """In-process background relay config; its target is always the derived sync queue
    (``queue_spec`` / ``transport`` on the binding are overridden). ``None`` when the
    relay runs out-of-process — drive it there over :attr:`SearchSyncOutboxWiring.outbox_spec`."""

    consume: bool = True
    """Run the in-process consumer lifecycle step. ``False`` when the consumer runs
    elsewhere — build it there via :meth:`SearchSyncOutboxWiring.queue_consumer`."""

    max_deliveries: int | None = None
    """Optional poison-parking ceiling forwarded to the consumer."""

    bind_tenant_from_headers: bool = False
    """Bind the tenant from the relayed headers for the consumer's re-read + apply
    (opt-in — headers are untrusted; enable only for brokers where every producer is
    trusted to assert tenancy)."""

    consumer_restart_backoff: timedelta = timedelta(seconds=5)
    """Backoff before the consumer loop restarts after a consume-stream crash."""

    # ....................... #

    def resolved_route(self, search: SearchSpec[Any]) -> StrKey:
        """The effective route name for the derived outbox / queue / inbox specs."""

        return self.route if self.route is not None else f"{search.name}_sync"


# ....................... #


@final
@attrs.frozen(kw_only=True)
class SearchSyncOutboxWiring:
    """The composed durable index-maintenance wiring for one document + search pair.

    Emitted as separate artifacts, like the other kit wirings: the in-tx staging steps
    (attach to the write ops' plans), the derived :attr:`outbox_spec` / :attr:`queue_spec`
    / :attr:`inbox_spec` (wire their backends in the deps module), and the relay +
    consumer lifecycle steps (register on the runtime). Build via
    :func:`bind_search_sync_outbox`.
    """

    document: DocumentSpec[Any, Any, Any, Any]
    """The document aggregate whose committed state the consumer re-reads."""

    search: SearchSpec[Any]
    """The external index the markers maintain."""

    config: OutboxSearchSync
    """The delivery declaration this wiring was built from."""

    outbox_spec: OutboxSpec[SearchSyncMarker]
    """The dedicated outbox route markers are staged on (destination = the sync queue)."""

    queue_spec: QueueSpec[SearchSyncMarker]
    """The queue the relay publishes markers to and the consumer drains."""

    inbox_spec: InboxSpec
    """Consumer-side dedup store (event-id dedup → exactly-once effect per marker)."""

    # ....................... #

    def stage_on_write(self, *, step_id: StrKey = _STAGE_STEP_ID) -> OnSuccessStep:
        """In-tx staging step reading the id off the written read model (CREATE / UPDATE)."""

        return OnSuccessStep(
            id=step_id,
            factory=self._stage_factory(
                lambda args, result: str(written_read_model(result).id),
            ),
        )

    # ....................... #

    def stage_on_target(self, *, step_id: StrKey = _STAGE_STEP_ID) -> OnSuccessStep:
        """In-tx staging step reading the id off the request args (KILL, soft DELETE/RESTORE)."""

        return OnSuccessStep(
            id=step_id,
            factory=self._stage_factory(lambda args, result: str(args.id)),
        )

    # ....................... #

    def apply_handler(
        self, ctx: ExecutionContext
    ) -> Callable[[QueueMessage[SearchSyncMarker]], Awaitable[None]]:
        """The consumer handler: re-read the row's committed state and apply it to the index.

        A live row is upserted; a missing (killed) or soft-deleted row is deleted from the
        index. Idempotent and order-insensitive by construction — every application reflects
        the state *now*, not the state at staging time — so at-least-once redelivery and
        relay reordering both converge. A raising index call fails the handler, rolling the
        inbox mark back with its transaction, and broker redelivery retries it.
        """

        document = self.document
        search = self.search

        async def _apply(message: QueueMessage[SearchSyncMarker]) -> None:
            document_id = message.payload.document_id
            command = ctx.search.command(search)

            try:
                row = await ctx.doc.query(document).get(pk=UUID(document_id))

            except CoreException as error:
                if error.kind is not ExceptionKind.NOT_FOUND:
                    raise

                await command.delete([document_id])
                return

            if getattr(row, SOFT_DELETE_FIELD, False):
                await command.delete([document_id])
                return

            await command.upsert([row])

        return _apply

    # ....................... #

    def queue_consumer(
        self,
        ctx: ExecutionContext,
        *,
        tx_route: StrKey = "default",
    ) -> QueueConsumer[SearchSyncMarker]:
        """A configured :class:`~forze_kits.integrations.consumer.QueueConsumer` over the sync queue.

        The inbox mark and the apply run in one transaction on *tx_route*; use it directly
        for one-shot drains or out-of-process consumers (the lifecycle step builds the same).
        """

        # Local import: the consumer integration imports broadly; importing it at module
        # load would widen this module's import graph for a path only durable delivery hits.
        from forze_kits.integrations.consumer import QueueConsumer

        return QueueConsumer(
            queue=str(self.queue_spec.name),
            queue_spec=self.queue_spec,
            handler=self.apply_handler(ctx),
            inbox_spec=self.inbox_spec,
            tx_route=tx_route,
            max_deliveries=self.config.max_deliveries,
            bind_tenant_from_headers=self.config.bind_tenant_from_headers,
        )

    # ....................... #

    def lifecycle_steps(self, *, tx_route: StrKey = "default") -> tuple[LifecycleStep, ...]:
        """The background relay and consumer steps (per the config's ``relay`` / ``consume``)."""

        route = str(self.queue_spec.name)
        steps: list[LifecycleStep] = []

        if self.config.relay is not None:
            relay = attrs.evolve(
                self.config.relay,
                transport="queue",
                queue_spec=self.queue_spec,
                stream_spec=None,
                pubsub_spec=None,
            )
            steps.append(
                relay.as_lifecycle_step(self.outbox_spec, step_id=f"search_sync_relay:{route}")
            )

        if self.config.consume:
            from forze_kits.integrations.consumer import (
                queue_consumer_factory_background_lifecycle_step,
            )

            steps.append(
                queue_consumer_factory_background_lifecycle_step(
                    queue=route,
                    consumer_factory=lambda ctx: self.queue_consumer(ctx, tx_route=tx_route),
                    restart_backoff=self.config.consumer_restart_backoff,
                    step_id=f"search_sync_consumer:{route}",
                )
            )

        return tuple(steps)

    # ....................... #

    def _stage_factory(self, document_id_of: Callable[[Any, Any], str]) -> OnSuccessFactory:
        """An in-tx ``on_success`` factory staging + flushing one marker for the written row.

        Runs inside the write's transaction (attach with ``bind_tx``), so a rolled-back
        write stages nothing. The document id doubles as the ordering key — a grouping
        hint for capable transports, never an ordering guarantee (the re-read consumer
        does not need one).
        """

        outbox_spec = self.outbox_spec
        route = str(outbox_spec.name)

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            command = ctx.outbox.command(outbox_spec)

            async def _hook(args: Any, result: Any) -> None:
                document_id = document_id_of(args, result)

                # The sync route is dedicated: each write stages exactly one marker and
                # flushes it within its own transaction. Re-open the per-task flushed flag
                # first — it guards against post-flush staging on *shared* event routes,
                # but here a task's next write must stage (and flush) its own marker.
                ctx.outbox_staging.set_flushed(route, False)
                await command.stage(
                    SEARCH_SYNC_EVENT_TYPE,
                    SearchSyncMarker(document_id=document_id),
                    ordering_key=document_id,
                )
                await command.flush()

            return _hook

        return _factory


# ....................... #


def bind_search_sync_outbox(
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    search: SearchSpec[Any],
    config: OutboxSearchSync,
) -> SearchSyncOutboxWiring:
    """Compose the durable index-maintenance wiring for *document* + *search*.

    Derives the dedicated outbox / queue / inbox specs under one route name
    (``config.resolved_route``) with a marker codec, so staging, relay, and consumption
    all speak :class:`SearchSyncMarker`. The backend configs for those routes stay the
    author's (deps module), preserving the app/backend split.

    The two specs must declare the same field encryption: the consumer re-reads the row and
    applies the **decrypted** read model to the index, so a field sealed on the document but
    omitted on the search spec reaches the index in clear (see
    :func:`assert_search_encryption_parity`) — durable delivery does not change that.

    The derived outbox route declares ``require_transaction=True``: staging a marker
    atomically with the write is the guarantee this route exists to provide, so flushing one
    outside a transaction is refused (``core.outbox.flush_outside_transaction``) rather than
    silently downgraded to a dual-write. Attach :meth:`~SearchSyncOutboxWiring.stage_on_write`
    / :meth:`~SearchSyncOutboxWiring.stage_on_target` on a **tx-bound** operation
    (``bind_tx()``), as :class:`~forze_kits.aggregates.AggregateKit` does.
    """

    assert_search_encryption_parity(document=document, search=search)

    route = config.resolved_route(search)
    codec = PydanticModelCodec(SearchSyncMarker)

    return SearchSyncOutboxWiring(
        document=document,
        search=search,
        config=config,
        outbox_spec=OutboxSpec(
            name=route,
            codec=codec,
            destination=OutboxDestination.queue(route=route, channel=str(route)),
            # Atomic in-tx staging *is* this route's reason to exist: it is the whole
            # difference from the best-effort after-commit sync. Declaring the precondition
            # makes it checked — a marker staged outside the write's transaction is a
            # dual-write (the row commits, the marker rolls back or vice versa) that leaves
            # the index silently diverged with nothing to alert on. Without this a
            # misattached hook degrades to exactly the delivery guarantee the route was
            # chosen to replace, and looks identical while doing it.
            require_transaction=True,
        ),
        queue_spec=QueueSpec(name=route, codec=codec),
        inbox_spec=InboxSpec(name=route),
    )
