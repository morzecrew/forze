"""The mock leg of the capped-replay boundary battery.

The oracle runs the same scenario the real document stores do, which is the point: the
mailbox is store-agnostic kit code, so a boundary it gets right against the mock and wrong
against Postgres is a boundary nothing was comparing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from uuid import UUID

import pytest
import pytest_asyncio

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_kits.integrations.realtime.conformance import REPLAY_CAP, MailboxScope
from forze_mock.execution import MockDepsModule, MockRouteConfig
from tests.support.realtime_cursor_conformance import (
    CURSOR_REPLAY_BATTERY,
    Check,
    CursorReplayHarness,
)

# ----------------------- #

UNCAPPED = 10**6
"""The observer's cap: high enough that its window is always the whole store."""


@pytest_asyncio.fixture
async def harness() -> AsyncIterator[CursorReplayHarness]:
    # Both collections are tenant-aware: the adapter scopes every row by the bound
    # tenant, which is what the tenant-collision probe needs to be a real probe.
    routes = {
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
    }
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        @contextmanager
        def _scoped(tenant: UUID) -> Iterator[MailboxScope]:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                yield MailboxScope(
                    mailbox=build_realtime_mailbox(ctx, cap=REPLAY_CAP, replay_page_size=2),
                    cursors=build_realtime_cursors(ctx),
                    observer=build_realtime_mailbox(ctx, cap=UNCAPPED),
                )

        yield CursorReplayHarness(scoped=_scoped, backend="mock")


@pytest.mark.conformance(plane="realtime_cursor", engine="mock")
@pytest.mark.parametrize("check", CURSOR_REPLAY_BATTERY, ids=lambda check: check.__name__)
async def test_cursor_replay_battery(check: Check, harness: CursorReplayHarness) -> None:
    await check(harness)
