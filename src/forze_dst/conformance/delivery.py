"""Outbox → relay → inbox delivery semantics under a crash, as a backend-agnostic scenario.

The outbox promises **at-least-once** delivery and, via consumer-side inbox dedup, **exactly-once
effect** — with deliberately *no* fence token (a slow relay's rows can be reclaimed and re-published,
and the inbox absorbs the duplicate). The classic failure window is a crash *after* the broker publish
but *before* the row is marked published: the row stays ``processing``, a restart reclaims it, and the
event is delivered a second time.

:func:`run_crash_recovery_delivery` drives exactly that window through the real ports (stage + flush →
claim → publish → **crash** → reclaim → re-claim → re-publish → mark → consume), so the same scenario
runs against the in-memory mock and, via the differential leg, real Postgres. The durable boundary is
the store itself (the ``MockState`` / the Postgres table survives the crash), so a fresh relay sees the
un-marked rows. Asserting the same :class:`DeliveryOutcome` on both backends turns "the mock's outbox
survives a crash" into "the mock's outbox matches the engine that actually persists the row" — the
crash-path counterpart to the isolation battery. (The outbox/inbox mock journals write-through rather
than through MVCC — see the ``outbox-inbox-write-through`` mechanism divergence — but atomicity holds
on the crash path, so this positive property is expected to agree on both engines.)

The scenario is parameterized by ``dedup``: with the inbox on, the duplicate delivery collapses to a
single effect (exactly-once); with it off, the duplicate applies twice — proving the redelivery is
real and the inbox is doing the work, not that the crash was a no-op.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze.testing import Conductor, Gate

# ----------------------- #


class DeliveryPayload(BaseModel):
    """The featureless integration-event body the delivery scenario stages."""

    label: str


# The shared spec pair both legs wire (same name → same route, same codec), so the differential
# compares one scenario across backends. The inbox route is the spec name, matching the outbox route.
DELIVERY_OUTBOX: OutboxSpec[DeliveryPayload] = OutboxSpec(
    name="conformance_delivery",
    codec=PydanticModelCodec(DeliveryPayload),
)
DELIVERY_INBOX: InboxSpec = InboxSpec(name="conformance_delivery")

DELIVERY_EVENTS: tuple[tuple[str, DeliveryPayload], ...] = tuple(
    (f"conformance.evt.{i}", DeliveryPayload(label=f"v{i}")) for i in range(5)
)
"""Five integration events staged in one transaction (fresh ``event_id`` per stage)."""


# ....................... #


@attrs.frozen(kw_only=True)
class DeliveryOutcome:
    """The observable result of the crash-recovery delivery scenario, compared across backends."""

    staged: int
    """Rows the flush inserted (the events that became durable)."""

    delivered: int
    """Broker deliveries across both relay rounds — ``2 × staged`` (each event published twice)."""

    reclaimed: int
    """Rows the restart reset from ``processing`` back to ``pending`` (the crashed round's claims)."""

    applied: int
    """Effects applied at the consumer (``staged`` with the inbox on; ``delivered`` with it off)."""

    distinct_applied: int
    """Distinct event ids applied — always ``staged`` (no event lost, none conjured)."""


# ....................... #


async def run_crash_recovery_delivery(
    ctx: ExecutionContext,
    *,
    tx_scope: str,
    dedup: bool,
    outbox_spec: OutboxSpec[DeliveryPayload] = DELIVERY_OUTBOX,
    inbox_spec: InboxSpec = DELIVERY_INBOX,
    events: Sequence[tuple[str, DeliveryPayload]] = DELIVERY_EVENTS,
) -> DeliveryOutcome:
    """Stage → publish → crash → reclaim → re-publish → consume, over *ctx*'s real ports.

    *ctx* is a live context whose outbox/inbox resolve to the backend under test (mock or Postgres);
    *tx_scope* is the transaction route the flush commits on. The relay/claim/mark calls run outside a
    transaction (at-least-once, no fence) exactly as a real relay does. Returns the observable
    :class:`DeliveryOutcome` — identical on every backend that honors the delivery contract.
    """

    command = ctx.outbox.command(outbox_spec)
    query = ctx.outbox.query(outbox_spec)
    inbox = ctx.inbox(inbox_spec)

    # 1. Producer stages the events and commits them atomically (the outbox flush rides the business
    #    transaction — rows are durable, or the whole unit rolls back).
    async with ctx.tx_ctx.scope(tx_scope):
        for event_type, payload in events:
            await command.stage(event_type, payload)
        staged = await command.flush()

    broker: list[str] = []  # the transport: event ids the relay has published

    # 2. Relay round one: claim the pending rows (→ processing) and publish them to the broker, then
    #    CRASH before marking them published. The rows stay `processing`; the broker already has them.
    for claim in await query.claim_pending():
        broker.append(str(claim.event_id))
    # ---- crash: the process dies here; `mark_published` never runs. ----

    # 3. Restart: a fresh relay reclaims the stuck `processing` rows (a future `older_than` forces the
    #    reclaim deterministically, standing in for "processing longer than the stale threshold"),
    #    re-claims them, re-publishes (the duplicate delivery), and this time marks them published.
    reclaimed = await query.reclaim_stale_processing(older_than=_far_future())
    republished_ids: list[UUID] = []
    for claim in await query.claim_pending():
        broker.append(str(claim.event_id))
        republished_ids.append(claim.event_id)
    await query.mark_published(republished_ids)

    # 4. Consumer drains the broker, optionally deduping on the event id via the inbox before applying
    #    the effect (the exactly-once-effect mechanism). Same event id across both rounds → deduped.
    applied: list[str] = []
    for event_id in broker:
        if not dedup or await inbox.mark_if_unseen(inbox_spec.name, event_id):
            applied.append(event_id)

    return DeliveryOutcome(
        staged=staged,
        delivered=len(broker),
        reclaimed=reclaimed,
        applied=len(applied),
        distinct_applied=len(set(applied)),
    )


# ....................... #


def _far_future() -> datetime:
    """A timestamp far enough ahead that every currently-``processing`` row counts as stale — the
    deterministic way to force the crashed round's claims to be reclaimed without sleeping."""

    return utcnow() + timedelta(days=1)


# ....................... #


async def observe_uncommitted_outbox_visibility(
    producer: ExecutionContext,
    relay: ExecutionContext,
    *,
    tx_scope: str,
    outbox_spec: OutboxSpec[DeliveryPayload] = DELIVERY_OUTBOX,
) -> bool:
    """Whether a concurrent relay can claim an outbox row a producer has flushed but not committed.

    Pins the ``outbox-inbox-write-through`` mechanism divergence. The producer stages + flushes one
    event and *holds its transaction open*; a concurrent relay (a separate session over the same
    store) then claims pending rows. The mock's outbox journals **write-through** (no MVCC), so the
    relay sees — and would publish — the still-uncommitted row (``True``: over-visibility, a phantom
    event if the producer later rolls back). Real Postgres at READ COMMITTED does not (``False``).
    This is a *documented, expected* divergence: the two backends deliberately disagree here, and a
    test asserts each side so the catalog entry stays a checked fact, not a forward-looking note.

    *producer* and *relay* are two independent sessions over one shared store (the mock: two contexts
    over one ``MockState``; Postgres: two contexts over one pooled client, so each holds its own
    connection). Returns ``True`` iff the relay claimed the producer's uncommitted row.
    """

    claimed = 0

    async def stage_and_hold(gate: Gate) -> None:
        async with producer.tx_ctx.scope(tx_scope):
            command = producer.outbox.command(outbox_spec)
            await command.stage("conformance.uncommitted", DeliveryPayload(label="dirty"))
            await command.flush()
            await gate.checkpoint()  # flushed, NOT committed — hold the transaction open
        # on release the scope exits and the row commits

    async def claim_concurrently(gate: Gate) -> None:
        await gate.checkpoint()  # start gate: claim only after the producer has flushed-and-parked
        nonlocal claimed
        claimed = len(await relay.outbox.query(outbox_spec).claim_pending())

    # Release order: the relay claims while the producer is still parked (uncommitted), then the
    # producer commits. The relay's claim never blocks — an invisible row is skipped, not waited on.
    await Conductor(schedule=("relay", "producer")).run(
        {"producer": stage_and_hold, "relay": claim_concurrently}
    )

    return claimed > 0
