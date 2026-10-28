"""The durable half of realtime delivery — one copy for every consumer of the stream.

A durable, principal-addressed signal reaches an offline recipient only through the mailbox,
and what decides whether it gets there is the same whoever consumes the stream: admission
against the catalog, the tenancy gate, then the dedup mark and the mailbox store committed in
one transaction. :class:`RealtimeDelivery` is that decision. The Socket.IO gateway runs every
signal through it with its live emit; :class:`~.writer.RealtimeMailboxWriter` runs it with
none, for the SSE and WebSocket routes. Two writers of one mailbox with two copies of these
rules would drift, and the mailbox is the delivery guarantee.
"""

import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import Literal, Protocol, final
from uuid import UUID

import attrs
from pydantic import ValidationError

from forze.application._logger import logger as _default_logger
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.realtime import (
    AudienceKind,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.realtime import RealtimeMailbox
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp, StrKey

from .observability import RealtimeGatewayStats

# ----------------------- #


class LiveEmit(Protocol):
    """A consumer's live delivery of one admitted signal.

    *recoverable* is true only after the signal's mailbox store has committed, so an emit
    may skip it (nobody present) knowing the reconnect replay delivers it.
    """

    async def __call__(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        *,
        event_id: str | None,
        recoverable: bool,
    ) -> None: ...  # pragma: no cover


# ....................... #


def _bind_tenant(
    ctx: ExecutionContext,
    tenant: UUID | None,
    *,
    enabled: bool,
) -> AbstractContextManager[None]:
    """Bind the per-signal header *tenant* so a tenant-aware mailbox scopes ambiently.

    **Opt-in** (``enabled``): the ``forze_tenant_id`` header is **untrusted** input —
    within a deployment the relay writes it, but any producer with broker access could
    forge it, so binding it is only safe on brokers where every producer is trusted to
    assert tenancy (the same posture as the inbox consumer's ``bind_tenant_from_headers``).
    Disabled, or untenanted, binds nothing — the mailbox runs under the ambient tenant.
    """

    if not enabled or tenant is None:
        return nullcontext()

    return ctx.inv_ctx.bind_identity(
        authn=ctx.inv_ctx.get_authn(), tenant=TenantIdentity(tenant_id=tenant)
    )


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GatewayDedup:
    """Inbox-based delivery dedup for durable signals at the gateway.

    A mailboxed (recoverable) signal is exactly-once — the mark and the mailbox store commit
    together, then the live emit is best-effort (recovery via reconnect-replay). A signal with
    no replay-safe store (topic, no mailbox, ``offline_delivery=False``) is at-least-once: the
    emit must succeed for the mark to stand, so a failed emit is reclaimed and re-delivered,
    and the client dedups any redelivery by the envelope id."""

    inbox_spec: InboxSpec
    """The inbox route that records already-emitted durable signals."""

    tx_route: StrKey
    """Transaction route the dedup mark commits on."""


# ....................... #


DedupInboxConsumer = Literal["RealtimeGateway", "RealtimeMailboxWriter"]
"""The two kinds of consumer that mark a dedup inbox."""

_inbox_holders: dict[str, dict[DedupInboxConsumer, int]] = {}
"""Running consumers per dedup inbox name in this process, by kind."""

_inbox_holders_lock = threading.Lock()


# ....................... #


@contextmanager
def claim_dedup_inbox(dedup: GatewayDedup, consumer: DedupInboxConsumer) -> Iterator[None]:
    """Hold *dedup*'s inbox for one running *consumer* until the block exits.

    A gateway and a mailbox writer must not share a dedup inbox: whichever marks a durable
    signal first owns it, so a gateway that loses the race skips its live emit and a
    writer that loses it never stores the offline row. Consumers of one kind may share an
    inbox (that is how several nodes split a group). This only sees the current process;
    across processes keeping them apart is the deployment's job.

    :param dedup: The consumer's dedup settings.
    :param consumer: Which kind of consumer is starting.
    :raises CoreException: ``configuration`` (``realtime_dedup_inbox_shared``) when the
        other kind already runs on the same inbox in this process.
    """

    name = str(dedup.inbox_spec.name)

    with _inbox_holders_lock:
        holders = _inbox_holders.setdefault(name, {})
        others = sorted(kind for kind, count in holders.items() if count and kind != consumer)

        if others:
            raise exc.configuration(
                f"A {consumer} cannot share the dedup inbox {name!r} with the {others[0]} "
                "already running in this process: whichever marks a durable signal first "
                "owns it, so the gateway would skip live emits or the writer would never "
                "store offline rows. Give each its own dedup inbox, or run only one of them.",
                code="realtime_dedup_inbox_shared",
                details={"inbox": name},
            )

        holders[consumer] = holders.get(consumer, 0) + 1

    try:
        yield

    finally:
        with _inbox_holders_lock:
            holders = _inbox_holders[name]
            holders[consumer] -= 1

            if not any(holders.values()):
                del _inbox_holders[name]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeDelivery:
    """Admit, gate on tenancy, then mark and store a durable signal in one transaction.

    The consumer that owns it supplies the settings it was built with; the fields mean
    exactly what the same-named fields of ``RealtimeGateway`` and
    :class:`~.writer.RealtimeMailboxWriter` document.
    """

    tenant_unbound: Callable[[str | None], CoreException]
    """Builds the per-signal error for a tenant-aware mailbox with no tenant bound.

    The consumer is the one place that knows *why* nothing is bound (and which of its
    settings fixes it), so it words the error.
    """

    dedup: GatewayDedup | None = None
    event_catalog: RealtimeEventCatalog | None = None
    bind_tenant_from_headers: bool = False
    require_tenant: bool | None = None
    stats: RealtimeGatewayStats | None = None

    logger: Logger = _default_logger
    """Where rejections, drops and failed live emits are logged."""

    # ....................... #

    @property
    def tenant_required(self) -> bool:
        """The resolved tenancy requirement: an explicit setting wins; unset follows
        :attr:`bind_tenant_from_headers` — a consumer that binds per-signal tenancy runs a
        tenanted deployment, and its untenanted signals are misrouted, not global."""

        if self.require_tenant is not None:
            return self.require_tenant

        return self.bind_tenant_from_headers

    # ....................... #

    async def deliver(
        self,
        ctx: ExecutionContext,
        mailbox: RealtimeMailbox | None,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
        *,
        emit: LiveEmit | None = None,
    ) -> None:
        """Run one decoded signal through admission, the tenancy gate and the durable store.

        With *emit*, the signal is also delivered live, placed relative to the commit so the
        dedup mark never stands before delivery is guaranteed. Without one (a store-only
        consumer) a signal with nothing replay-safe to store is left alone, unmarked.

        :param ctx: The consumer's run context.
        :param mailbox: The mailbox built at the consumer's run start, if any.
        :param signal: The decoded signal.
        :param tenant: The tenant resolved for it by the source.
        :param dedup_id: The durable event id; ``None`` for an ephemeral signal.
        :param hlc: Its mailbox ordering position.
        :param emit: The consumer's live delivery, if it has one.
        :raises CoreException: ``precondition`` from :attr:`tenant_unbound` when a
            tenant-aware mailbox has no tenant bound; anything the ports raise.
        """

        admitted = self.admit(signal)

        if admitted is None:
            # undeclared event, disallowed audience, or wrong payload shape — drop it (the
            # caller then acks, so it never reaches a client and never reclaim-loops).
            return

        signal = admitted  # emit/store the catalog-normalized payload, not the raw one

        if self.tenant_required and tenant is None:
            # The room name is the isolation boundary — an untenanted signal would land in
            # the global (unprefixed) room. A missing/malformed tenant header is a
            # deterministic property of the signal, so the drop is final: the caller acks,
            # and the loss is counted and logged instead of silently degrading isolation.
            if self.stats is not None:
                self.stats.untenanted_dropped += 1

            self.logger.critical(
                "Realtime signal dropped: no tenant resolved on a tenant-required gateway",
                realtime_event=signal.event,
                event_id=dedup_id,
            )
            return

        if self.dedup is None or dedup_id is None:
            # ephemeral, or durable with no dedup configured — emit directly
            if emit is not None:
                await emit(signal, tenant, event_id=dedup_id, recoverable=False)
            return

        store = mailbox if (mailbox is not None and self.should_mailbox(signal)) else None

        if store is None and emit is None:
            # Nothing replay-safe to store and no live leg here: marking it would only
            # claim a delivery this consumer never makes.
            return

        # durable: mark (+ store) inside one transaction. Where the live emit sits relative to
        # the commit depends on whether the signal is **replay-safe**, so the dedup mark never
        # becomes final until delivery is guaranteed:
        #
        #  - mailboxed (recoverable): the store commits with the mark, so the recipient gets
        #    the signal on reconnect even if the live emit fails. Emit best-effort AFTER the
        #    commit — a commit failure then can't double-emit (exactly-once, mailbox recovery).
        #  - not mailboxed (topic, no mailbox, offline opt-out): nothing persists it, so the
        #    emit must succeed for the mark to stand. Emit INSIDE the transaction — a failed
        #    emit rolls the mark back, the entry stays pending, and reclaim re-delivers instead
        #    of dropping the frame. A commit failure after a successful emit redelivers too; the
        #    client dedups by the envelope id (at-least-once).
        #
        # The header tenant is bound only when opted-in (it is untrusted, see _bind_tenant).
        with _bind_tenant(ctx, tenant, enabled=self.bind_tenant_from_headers):
            async with ctx.tx_ctx.scope(self.dedup.tx_route):
                inbox = ctx.inbox(self.dedup.inbox_spec)

                if not await inbox.mark_if_unseen(str(self.dedup.inbox_spec.name), dedup_id):
                    if self.stats is not None:
                        self.stats.dedup_skipped += 1
                    return

                if store is not None:
                    try:
                        await store.store(
                            principal=signal.audience.name,
                            event_id=dedup_id,
                            hlc=hlc,
                            signal=signal,
                        )

                        if self.stats is not None:
                            self.stats.mailboxed += 1
                    except CoreException as error:
                        # A tenant-aware mailbox fails closed with an opaque
                        # ``tenant_required`` when nothing is bound; the consumer is the
                        # only place that knows *why* nothing is bound, so it rewords it.
                        if error.code == "tenant_required":
                            raise self.tenant_unbound(dedup_id) from error
                        raise
                elif emit is not None:
                    # not replay-safe: emit inside the tx so a failed emit rolls the mark back
                    await emit(signal, tenant, event_id=dedup_id, recoverable=False)

            if store is not None and emit is not None:
                # replay-safe: mark + store committed, so the durable obligation is met and the
                # recipient gets the signal on reconnect. The live emit is best-effort — a
                # failure (Socket.IO/presence outage, timeout) must not propagate, or the caller
                # would leave the message pending forever (it never re-emits live once the mark
                # is committed, and reclaim may be disabled). Swallow it; the mailbox recovers.
                try:
                    await emit(signal, tenant, event_id=dedup_id, recoverable=True)
                except Exception as error:
                    if (
                        isinstance(error, CoreException)
                        and error.kind is ExceptionKind.CONFIGURATION
                    ):
                        raise  # a wiring error must fail fast, not be swallowed as best-effort

                    self.logger.critical_exception(
                        "Realtime live emit failed after commit (recoverable via mailbox)",
                        event_id=dedup_id,
                    )

    # ....................... #

    def admit(self, signal: RealtimeSignal) -> RealtimeSignal | None:
        """Admit *signal* on the declared surface, **normalized** — or ``None``.

        With a catalog set the emitted surface is **closed**: a signal whose event is
        undeclared, whose audience kind the event forbids, or whose payload does not match
        the declared :class:`RealtimeEvent` is rejected (logged + dropped) rather than
        emitted — so a raw ``RealtimeSignal.of(...)`` producer or a malformed stream row
        can't bypass the contract. An admitted signal is returned with its payload replaced
        by the **parsed model's JSON form** (defaults, aliases, and coercions applied — the
        same ``model_dump(mode="json")`` :meth:`RealtimeSignal.for_event` produces), so the
        client receives the declared shape, not the raw payload. No catalog means an open
        surface: the signal passes through unchanged.
        """

        if self.event_catalog is None:
            return signal

        event = self.event_catalog.get(signal.event)

        if event is None:
            self.logger.critical(
                "Realtime signal rejected: event not in catalog",
                realtime_event=signal.event,
            )
            self._count_rejection()
            return None

        if not event.accepts(signal.audience):
            self.logger.critical(
                "Realtime signal rejected: audience kind not allowed for event",
                realtime_event=signal.event,
                audience_kind=signal.audience.kind.value,
            )
            self._count_rejection()
            return None

        try:
            normalized = event.parse(signal.payload).model_dump(mode="json")
        except ValidationError:
            self.logger.critical_exception(
                "Realtime signal rejected: payload does not match catalog",
                realtime_event=signal.event,
            )
            self._count_rejection()
            return None

        return signal.model_copy(update={"payload": normalized})

    # ....................... #

    def _count_rejection(self) -> None:
        """Count an admission rejection on the delivery stats, when wired."""

        if self.stats is not None:
            self.stats.admission_rejected += 1

    # ....................... #

    def should_mailbox(self, signal: RealtimeSignal) -> bool:
        """Whether this durable signal is stored for offline replay (audience + opt-out)."""

        if signal.audience.kind is not AudienceKind.PRINCIPAL:
            return False

        if self.event_catalog is None:
            return True  # always-store default

        event = self.event_catalog.get(signal.event)

        return event is None or event.offline_delivery
