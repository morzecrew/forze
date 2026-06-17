"""Tests for the inbox consumer dedup helper (process_with_inbox)."""

from __future__ import annotations

from typing import Mapping

import attrs
import pytest

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_HLC
from forze.application.contracts.inbox import InboxSpec
from forze.application.execution.outbox.clock import outbox_clock, set_outbox_clock
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp, HybridLogicalClock, uuid7
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


async def test_first_message_processed_then_duplicate_skipped() -> None:
    ctx = context_from_modules(MockDepsModule())
    calls: list[str] = []

    async def handler(msg: _Msg) -> None:
        calls.append(msg.key or "")

    msg = _Msg(key="evt-1")

    first = await process_with_inbox(
        ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
    )
    second = await process_with_inbox(
        ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
    )

    assert first is True
    assert second is False  # redelivery skipped
    assert calls == ["evt-1"]  # handler ran exactly once


async def test_duplicate_does_not_advance_the_hlc_clock() -> None:
    # The causal merge runs only after the dedup mark succeeds, so a replayed
    # message cannot advance (or be used to skew) the process-global clock.
    saved = outbox_clock()
    set_outbox_clock(HybridLogicalClock())

    try:
        ctx = context_from_modules(MockDepsModule())

        async def handler(_msg: _Msg) -> None: ...

        ahead = HlcTimestamp(outbox_clock().now().physical_ms + 1, 0)
        msg = _Msg(key="evt-hlc", headers={HEADER_HLC: ahead.encode()})

        await process_with_inbox(
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        after_first = outbox_clock().last

        await process_with_inbox(  # duplicate
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )

        assert outbox_clock().last == after_first  # duplicate did not advance it

    finally:
        set_outbox_clock(saved)


async def test_prefers_key_over_id() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    # Same key, different id -> still a duplicate (dedup on key).
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="k", id="a"),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="k", id="b"),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is False
    )


async def test_falls_back_to_id_when_no_key() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    assert (
        await process_with_inbox(
            ctx, _Msg(id="only-id"), inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        is True
    )


# ....................... #
# Dedup-id priority: explicit extractor > forze_event_id header > key > id.


async def test_event_id_header_beats_key() -> None:
    """Two DIFFERENT events sharing an ordering key must both process."""

    ctx = context_from_modules(MockDepsModule())
    calls: list[str] = []

    async def handler(msg: _Msg) -> None:
        calls.append(msg.headers[HEADER_EVENT_ID])

    event_a, event_b = str(uuid7()), str(uuid7())

    # The relay publishes key=ordering_key: same key, distinct event ids.
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="order-1", id="d1", headers={HEADER_EVENT_ID: event_a}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="order-1", id="d2", headers={HEADER_EVENT_ID: event_b}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )
    assert calls == [event_a, event_b]


async def test_redelivery_with_same_event_id_header_is_skipped() -> None:
    """Same event id header, fresh broker id/delivery -> still a duplicate."""

    ctx = context_from_modules(MockDepsModule())
    calls: list[str] = []

    async def handler(msg: _Msg) -> None:
        calls.append(msg.id or "")

    event_id = str(uuid7())

    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="order-1", id="delivery-1", headers={HEADER_EVENT_ID: event_id}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="order-1", id="delivery-2", headers={HEADER_EVENT_ID: event_id}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is False
    )
    assert calls == ["delivery-1"]


async def test_explicit_extractor_beats_event_id_header() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    def build(event_id: str) -> _Msg:
        return _Msg(key="k", id="i", headers={HEADER_EVENT_ID: event_id})

    # Distinct headers, but the extractor pins both to one dedup id.
    assert (
        await process_with_inbox(
            ctx,
            build(str(uuid7())),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
            message_id=lambda _m: "pinned",
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx,
            build(str(uuid7())),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
            message_id=lambda _m: "pinned",
        )
        is False
    )


async def test_empty_event_id_header_falls_back_to_key() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="k-empty", headers={HEADER_EVENT_ID: ""}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is True
    )
    # Dedup happened on the key, not on the empty header value.
    assert (
        await process_with_inbox(
            ctx,
            _Msg(key="k-empty", headers={HEADER_EVENT_ID: ""}),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        is False
    )


async def test_caller_extractor_override() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    msg = _Msg(key="ignored")
    result = await process_with_inbox(
        ctx,
        msg,
        inbox_spec=_SPEC,
        handler=handler,
        tx_route="mock",
        message_id=lambda _m: "custom-id",
    )
    assert result is True


async def test_missing_dedup_id_raises() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    with pytest.raises(CoreException, match="deduplicate"):
        await process_with_inbox(
            ctx, _Msg(), inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
