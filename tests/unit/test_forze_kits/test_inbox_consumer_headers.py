"""Envelope rebinding in :func:`process_with_inbox` (headers -> inv_ctx)."""

from __future__ import annotations

from typing import Mapping
from uuid import UUID, uuid4

import attrs

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_TENANT_ID,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import uuid7
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.inbox import process_with_inbox
from forze_mock import MockDepsModule

# ----------------------- #

_SPEC = InboxSpec(name="events")


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


async def test_causation_falls_back_to_message_key_without_event_id_header() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    correlation_id = uuid7()
    event_id = uuid7()
    msg = _Msg(
        key=str(event_id),
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
    assert observed.metadata.causation_id == event_id


async def test_existing_consumer_execution_id_is_kept() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    consumer_metadata = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
    )
    correlation_id = uuid7()
    msg = _Msg(key="evt-key", headers={HEADER_CORRELATION_ID: str(correlation_id)})

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
    assert observed.metadata.execution_id == consumer_metadata.execution_id
    assert observed.metadata.correlation_id == correlation_id


async def test_without_headers_no_rebind_current_behavior() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    processed = await process_with_inbox(
        ctx,
        _Msg(key="evt-1"),
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert processed is True
    assert observed.metadata is None
    assert observed.tenant is None


async def test_malformed_correlation_header_is_ignored() -> None:
    ctx = context_from_modules(MockDepsModule())
    observed = _Observed()

    msg = _Msg(key="evt-1", headers={HEADER_CORRELATION_ID: "not-a-uuid"})

    processed = await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert processed is True
    assert observed.metadata is None


async def test_duplicate_is_still_skipped_with_headers() -> None:
    ctx = context_from_modules(MockDepsModule())
    calls: list[str] = []

    async def handler(msg: _Msg) -> None:
        calls.append(msg.key or "")

    msg = _Msg(
        key="evt-dup",
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
        key="evt-t1",
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
        key="evt-t2",
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

    msg = _Msg(key="evt-t3", headers={HEADER_CORRELATION_ID: str(uuid7())})

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
    msg = _Msg(key="evt-t4", headers={HEADER_TENANT_ID: str(tenant_id)})

    await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
        bind_tenant_from_headers=True,
    )

    assert observed.metadata is None
    assert observed.tenant is not None
    assert observed.tenant.tenant_id == tenant_id


async def test_message_without_headers_attribute_is_fine() -> None:
    ctx = context_from_modules(MockDepsModule())

    @attrs.define(slots=True, kw_only=True)
    class _Bare:
        key: str

    async def handler(_msg: _Bare) -> None: ...

    assert (
        await process_with_inbox(
            ctx,
            _Bare(key="bare-1"),
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
        _Msg(key=str(uuid7()), headers={HEADER_CORRELATION_ID: str(correlation_id)}),
        inbox_spec=_SPEC,
        handler=_observing_handler(ctx, observed),
        tx_route="mock",
    )

    assert observed.metadata is not None
    assert isinstance(observed.metadata.correlation_id, UUID)
    assert isinstance(observed.metadata.execution_id, UUID)
