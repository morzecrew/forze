"""Envelope rebinding in :func:`process_with_inbox` (headers -> inv_ctx)."""

from __future__ import annotations

from collections.abc import Mapping
from uuid import UUID, uuid4

import attrs
from structlog.contextvars import get_contextvars

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_TENANT_ID,
)
from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.application.execution.context.invocation import (
    CORR_ID_KEY,
    EXEC_ID_KEY,
    SURFACE_KEY,
)
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import uuid7
from forze.domain.models import BaseDTO
from forze_kits.integrations.inbox import process_with_inbox
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #

_SPEC = InboxSpec(name="events")
_IDEMPOTENCY = IdempotencySpec(name="charges")


@attrs.define(slots=True, kw_only=True)
class _Msg:
    key: str | None = None
    id: str | None = None
    headers: Mapping[str, str] = attrs.field(factory=dict)


@attrs.define(slots=True)
class _Observed:
    metadata: InvocationMetadata | None = None
    tenant: TenantIdentity | None = None


def _observing_handler(ctx: ExecutionContext, observed: _Observed):
    async def handler(_msg: _Msg) -> None:
        observed.metadata = ctx.inv_ctx.get_metadata()
        observed.tenant = ctx.inv_ctx.get_tenant()

    return handler


# ----------------------- #


async def test_envelope_headers_rebind_correlation_and_causation() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    correlation_id = uuid7()
    event_id = uuid7()
    msg = _Msg(
        key=str(event_id),
        headers={
            HEADER_CORRELATION_ID: str(correlation_id),
            HEADER_EVENT_ID: str(event_id),
        },
    )

    processed = await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert processed is True
    assert observed.metadata is not None
    # Handler ran under the ORIGINAL correlation id...
    assert observed.metadata.correlation_id == correlation_id
    # ...and the consumed event CAUSES the handler's effects.
    assert observed.metadata.causation_id == event_id
    # The consumer names itself; the publisher's surface is not carried.
    assert observed.metadata.surface == "inbox"


async def test_causation_absent_without_event_id_header() -> None:
    # The ordering key is a grouping token, not an event identity — without
    # the event-id header there is no causing event id to bind.
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    correlation_id = uuid7()
    msg = _Msg(
        key=str(uuid7()),  # UUID-shaped ordering key must not masquerade
        id="d-1",
        headers={HEADER_CORRELATION_ID: str(correlation_id)},
    )

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert observed.metadata is not None
    assert observed.metadata.correlation_id == correlation_id
    assert observed.metadata.causation_id is None


async def test_a_delivery_under_consumer_metadata_gets_its_own_execution_id() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    consumer_metadata = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
    )
    correlation_id = uuid7()
    msg = _Msg(id="evt-key", headers={HEADER_CORRELATION_ID: str(correlation_id)})

    with ctx.inv_ctx.bind_metadata(metadata=consumer_metadata):
        await process_with_inbox(
            ctx,
            msg,
            inbox_spec=_SPEC,
            handler=_observing_handler(ctx, observed),
            tx_route="mock",
        )

        # Rebinding is scoped: the consumer's own metadata is restored.
        assert ctx.inv_ctx.get_metadata() == consumer_metadata

    assert observed.metadata is not None
    # The execution id owns the handler's claims, so a delivery never shares the consumer's.
    assert observed.metadata.execution_id != consumer_metadata.execution_id
    assert observed.metadata.correlation_id == correlation_id
    # No event-id header: the consumer's invocation is the cause.
    assert observed.metadata.causation_id == consumer_metadata.execution_id
    assert observed.metadata.surface == "inbox"


async def test_a_message_without_headers_still_gets_metadata() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    processed = await process_with_inbox(
        ctx,
        _Msg(id="evt-1"),
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert processed is True
    assert observed.metadata is not None
    assert observed.metadata.surface == "inbox"
    assert observed.metadata.causation_id is None
    assert observed.metadata.execution_id != observed.metadata.correlation_id
    assert observed.tenant is None
    # Scoped to the delivery.
    assert ctx.inv_ctx.get_metadata() is None


async def test_each_delivery_without_headers_is_an_invocation_of_its_own() -> None:
    ctx = context_from_modules(MockDepsModule())
    first, second = _Observed(), _Observed()

    for message_id, observed in (("evt-a", first), ("evt-b", second)):
        await process_with_inbox(
            ctx,
            _Msg(id=message_id),
            inbox_spec=_SPEC,
            handler=_observing_handler(ctx, observed),
            tx_route="mock",
        )

    assert first.metadata is not None and second.metadata is not None
    assert first.metadata.execution_id != second.metadata.execution_id
    assert first.metadata.correlation_id != second.metadata.correlation_id


async def test_deliveries_under_one_consumer_metadata_share_only_its_correlation_id() -> None:
    ctx = context_from_modules(MockDepsModule())
    first, second = _Observed(), _Observed()
    consumer_metadata = InvocationMetadata(execution_id=uuid7(), correlation_id=uuid7())

    with ctx.inv_ctx.bind_metadata(metadata=consumer_metadata):
        for message_id, observed in (("evt-c1", first), ("evt-c2", second)):
            await process_with_inbox(
                ctx,
                _Msg(id=message_id),
                inbox_spec=_SPEC,
                handler=_observing_handler(ctx, observed),
                tx_route="mock",
            )

    assert first.metadata is not None and second.metadata is not None
    # One owner per delivery, whatever the consumer bound around them.
    assert len(
        {first.metadata.execution_id, second.metadata.execution_id, consumer_metadata.execution_id}
    ) == 3
    assert (
        first.metadata.correlation_id
        == second.metadata.correlation_id
        == consumer_metadata.correlation_id
    )
    assert first.metadata.surface == second.metadata.surface == "inbox"


async def test_a_claim_begun_in_one_delivery_cannot_be_committed_by_another() -> None:
    ctx = context_from_modules(MockDepsModule())
    consumer_metadata = InvocationMetadata(execution_id=uuid7(), correlation_id=uuid7())
    outcome: dict[str, BaseException | None] = {}

    async def begin(_msg: _Msg) -> None:
        await ctx.idempotency(_IDEMPOTENCY).begin("charge", "k-1", "h-1")

    async def commit(_msg: _Msg) -> None:
        try:
            await ctx.idempotency(_IDEMPOTENCY).commit(
                "charge", "k-1", "h-1", IdempotencyRecord(result=b"r")
            )
            outcome["commit"] = None

        except CoreException as error:
            outcome["commit"] = error

    with ctx.inv_ctx.bind_metadata(metadata=consumer_metadata):
        await process_with_inbox(
            ctx, _Msg(id="evt-a"), inbox_spec=_SPEC, handler=begin, tx_route="mock"
        )
        await process_with_inbox(
            ctx, _Msg(id="evt-b"), inbox_spec=_SPEC, handler=commit, tx_route="mock"
        )

    error = outcome["commit"]
    assert isinstance(error, CoreException)
    assert error.kind is ExceptionKind.CONFLICT


async def test_surface_headers_are_never_read() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()
    msg = _Msg(
        id="evt-forged",
        headers={
            HEADER_CORRELATION_ID: str(uuid7()),
            "forze_surface": "cli",
            "invocation_surface": "cli",
        },
    )

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert observed.metadata is not None
    assert observed.metadata.surface == "inbox"


class _Shipped(BaseDTO):
    n: int


async def test_a_header_less_delivery_logs_and_publishes_its_ids() -> None:
    ctx = context_from_modules(MockDepsModule())
    seen: dict[str, object] = {}

    async def handler(_msg: _Msg) -> None:
        seen.update(get_contextvars())
        enricher = InvocationOutboxEnricher(inv=ctx.inv_ctx, clock=ctx.outbox_clock)
        seen["event"] = enricher.enrich("shipped", _Shipped(n=1))

    await process_with_inbox(ctx, _Msg(id="evt-p"), inbox_spec=_SPEC, handler=handler, tx_route="mock")

    event = seen["event"]
    assert seen[SURFACE_KEY] == "inbox"
    assert seen[EXEC_ID_KEY] == str(event.execution_id)  # type: ignore[attr-defined]
    assert seen[CORR_ID_KEY] == str(event.correlation_id)  # type: ignore[attr-defined]
    assert event.execution_id is not None  # type: ignore[attr-defined]


async def test_malformed_correlation_header_is_ignored() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    msg = _Msg(id="evt-1", headers={HEADER_CORRELATION_ID: "not-a-uuid"})

    processed = await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert processed is True
    # A fresh correlation id stands in for the malformed one.
    assert observed.metadata is not None
    assert isinstance(observed.metadata.correlation_id, UUID)
    assert observed.metadata.surface == "inbox"


async def test_duplicate_is_still_skipped_with_headers() -> None:
    ctx = context_from_modules(MockDepsModule())
    calls: list[str] = []

    async def handler(msg: _Msg) -> None:
        calls.append(msg.id or "")

    msg = _Msg(
        id="evt-dup",
        headers={HEADER_CORRELATION_ID: str(uuid7())},
    )

    assert (
        await process_with_inbox(
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        is False
    )
    assert calls == ["evt-dup"]


# ....................... #
# Tenant binding is opt-in.


async def test_tenant_header_ignored_by_default() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    msg = _Msg(
        id="evt-t1",
        headers={
            HEADER_CORRELATION_ID: str(uuid7()),
            HEADER_TENANT_ID: str(uuid4()),
        },
    )

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert observed.tenant is None


async def test_tenant_header_bound_when_opted_in() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    tenant_id = uuid4()
    msg = _Msg(
        id="evt-t2",
        headers={
            HEADER_CORRELATION_ID: str(uuid7()),
            HEADER_TENANT_ID: str(tenant_id),
        },
    )

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
        bind_tenant_from_headers=True,
    )

    assert observed.tenant == TenantIdentity(tenant_id=tenant_id)
    # Scoped: nothing leaks past the call.
    assert ctx.inv_ctx.get_tenant() is None


async def test_tenant_opt_in_without_tenant_header_binds_nothing() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    msg = _Msg(id="evt-t3", headers={HEADER_CORRELATION_ID: str(uuid7())})

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
        bind_tenant_from_headers=True,
    )

    assert observed.tenant is None


async def test_tenant_opt_in_works_without_correlation_header() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    tenant_id = uuid4()
    msg = _Msg(id="evt-t4", headers={HEADER_TENANT_ID: str(tenant_id)})

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
        bind_tenant_from_headers=True,
    )

    assert observed.metadata is not None
    assert observed.tenant is not None
    assert observed.tenant.tenant_id == tenant_id


async def test_message_without_headers_attribute_is_fine() -> None:
    ctx = context_from_modules(MockDepsModule())

    @attrs.define(slots=True, kw_only=True)
    class _Bare:
        id: str

    async def handler(_msg: _Bare) -> None: ...

    assert (
        await process_with_inbox(
            ctx,
            _Bare(id="bare-1"),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )


async def test_uuid_typed_assertion_helper() -> None:
    # Guard: HEADER values round-trip as proper UUIDs end to end.
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()
    correlation_id = uuid7()

    await process_with_inbox(
        ctx,
        _Msg(id=str(uuid7()), headers={HEADER_CORRELATION_ID: str(correlation_id)}),
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert observed.metadata is not None
    assert isinstance(observed.metadata.correlation_id, UUID)
    assert isinstance(observed.metadata.execution_id, UUID)
