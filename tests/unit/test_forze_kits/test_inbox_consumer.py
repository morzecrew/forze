"""Tests for the inbox consumer dedup helper (process_with_inbox)."""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.inbox import InboxSpec
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.inbox import process_with_inbox
from forze_mock import MockDepsModule

# ----------------------- #

_SPEC = InboxSpec(name="events")


@attrs.define(slots=True, kw_only=True)
class _Msg:
    key: str | None = None
    id: str | None = None


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


async def test_prefers_key_over_id() -> None:
    ctx = context_from_modules(MockDepsModule())

    async def handler(_msg: _Msg) -> None: ...

    # Same key, different id -> still a duplicate (dedup on key).
    assert (
        await process_with_inbox(
            ctx, _Msg(key="k", id="a"), inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        is True
    )
    assert (
        await process_with_inbox(
            ctx, _Msg(key="k", id="b"), inbox_spec=_SPEC, handler=handler, tx_route="mock"
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
