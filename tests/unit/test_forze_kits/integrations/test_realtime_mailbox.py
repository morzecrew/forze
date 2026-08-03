"""Document-backed mailbox + cursors over a tenant-aware mock document store.

Tenancy is the store's concern: the collections are wired ``tenant_aware`` and the
adapter scopes every row by the ambient (bound) tenant — the kit carries zero tenant
code. The mailbox/cursors are materialized via the build factories (resolved ports).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import asyncio
from unittest.mock import patch
from datetime import timedelta
from uuid import UUID

import pytest

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    MailboxRetention,
    MailboxStats,
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock.execution import MockDepsModule, MockRouteConfig
from tests.support.realtime_retention import UNSWEPT

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _runtime() -> ExecutionRuntime:
    # the mailbox + cursor collections are tenant-aware: the adapter scopes them
    routes = {
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
    }
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID = _T1) -> AbstractContextManager[None]:
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _eid(n: int) -> str:
    """event_id is the durable forze_event_id — always a UUID string."""

    return str(UUID(int=n))


# ----------------------- #
# mailbox


async def test_store_read_since_ordered_and_idempotent() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            await mb.store(principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))  # idempotent

            everything = await mb.read_since(principal="u1", since=None)
            after_e1 = await mb.read_since(principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == [_eid(1), _eid(2)]
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == [_eid(2)]


async def test_replay_since_streams_in_order_across_pages() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT, replay_page_size=2)
            for n in range(1, 6):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
                )

            streamed = [
                e.event_id async for e in mb.replay_since(principal="u1", since=None)
            ]
            after = [
                e.event_id async for e in mb.replay_since(principal="u1", since=_hlc(3))
            ]

    # 5 entries streamed oldest-first across 3 keyset pages of size 2.
    assert streamed == [_eid(1), _eid(2), _eid(3), _eid(4), _eid(5)]
    assert after == [_eid(4), _eid(5)]
    assert mb.stats().replayed == 7  # 5 + 2, counted per yielded entry


async def test_replay_since_bounded_by_cap_keeps_newest_window() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT, cap=3, replay_page_size=2)
            for n in range(1, 6):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
                )

            streamed = [
                e.event_id async for e in mb.replay_since(principal="u1", since=None)
            ]

    # The cap is a newest-first retention bound: an overflowing backlog loses its
    # OLDEST entries and the stream is a complete suffix — never a truncated prefix,
    # which would let a later cumulative ack skip (then trim) the undelivered middle.
    assert streamed == [_eid(3), _eid(4), _eid(5)]
    assert mb.stats().replayed == 3
    assert mb.stats().overflowed == 1


async def test_backlog_exactly_at_cap_is_not_counted_as_overflow() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT, cap=3, replay_page_size=2)
            for n in range(1, 4):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
                )

            streamed = [e.event_id async for e in mb.replay_since(principal="u1", since=None)]

    # a backlog that fills the window exactly loses nothing — no false loss counted
    assert streamed == [_eid(1), _eid(2), _eid(3)]
    assert mb.stats().overflowed == 0


async def test_stored_counter_tracks_real_writes_not_redeliveries() -> None:
    # a redelivered signal (same event_id) is idempotent and must NOT recount
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))

    assert mb.stats().stored == 1  # one real write, despite two store() calls


async def test_mailbox_is_tenant_isolated_by_the_store() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx, _T1):
            mb1 = build_realtime_mailbox(ctx, retention=UNSWEPT)
            await mb1.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            same = await mb1.read_since(principal="u1", since=None)
            other_principal = await mb1.read_since(principal="u2", since=None)
        with _bind(ctx, _T2):
            mb2 = build_realtime_mailbox(ctx, retention=UNSWEPT)
            other_tenant = await mb2.read_since(principal="u1", since=None)

    assert [e.event_id for e in same] == [_eid(1)]
    assert other_principal == []
    assert other_tenant == []  # the adapter scopes by the ambient tenant — no kit tenant code


async def test_position_of_and_trim() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            for i in (1, 2, 3):
                await mb.store(principal="u1", event_id=_eid(i), hlc=_hlc(i), signal=_signal(str(i)))

            assert await mb.position_of(principal="u1", event_id=_eid(2)) == _hlc(2)
            assert await mb.position_of(principal="u1", event_id=str(UUID(int=999))) is None

            await mb.trim(principal="u1", before=_hlc(2))
            rows = await mb.read_since(principal="u1", since=None)

    assert [e.event_id for e in rows] == [_eid(3)]


async def test_build_refused_in_read_only_operation() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_read_only(), pytest.raises(CoreException, match="read-only"):
            build_realtime_mailbox(ctx, retention=UNSWEPT)


# ----------------------- #
# cursors


async def test_cursor_monotonic_get_and_min() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            cursors = build_realtime_cursors(ctx)
            assert await cursors.get(principal="u1", client_key="d1") is None

            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(5))
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
            assert await cursors.get(principal="u1", client_key="d1") == _hlc(5)

            await cursors.advance(principal="u1", client_key="d2", up_to=_hlc(2))
            assert await cursors.min_cursor(principal="u1") == _hlc(2)  # slowest device


async def test_shared_stats_count_store_replay_trim_ack() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            cursors = build_realtime_cursors(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            replayed = await mb.read_since(principal="u1", since=None)
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(2))
            await mb.trim(principal="u1", before=_hlc(1))

    assert len(replayed) == 2
    assert mb.stats() == MailboxStats(stored=2, replayed=2, trimmed=1)
    assert cursors.stats() == MailboxStats(acked=1)


# ----------------------- #
# retention backstop: age-based entry sweep + stale-cursor pruning


async def test_sweep_older_than_deletes_across_principals() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u2", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            await mb.store(principal="u1", event_id=_eid(3), hlc=_hlc(5000), signal=_signal("c"))

            deleted = await mb.sweep_older_than(cutoff=_hlc(3000))

            u1 = await mb.read_since(principal="u1", since=None)
            u2 = await mb.read_since(principal="u2", since=None)

    # entries older than the cutoff die for EVERY principal (no cursor floor consulted)
    assert deleted == 2
    assert [e.event_id for e in u1] == [_eid(3)]
    assert u2 == []
    assert mb.stats().trimmed == 2


async def test_prune_stale_cursors_unfreezes_the_trim_floor() -> None:
    from datetime import timedelta

    from forze.base.primitives import utcnow

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            cursors = build_realtime_cursors(ctx)
            # two per-connection fallback keys: without pruning these rows are immortal
            # and the lower one freezes the all-device trim floor forever
            await cursors.advance(principal="u1", client_key="conn-1", up_to=_hlc(1))
            await cursors.advance(principal="u1", client_key="conn-2", up_to=_hlc(9))

            untouched = await cursors.prune_stale(idle_since=utcnow() - timedelta(days=1))
            assert untouched == 0  # both rows advanced just now — not stale
            assert await cursors.min_cursor(principal="u1") == _hlc(1)

            pruned = await cursors.prune_stale(idle_since=utcnow() + timedelta(days=1))
            assert pruned == 2  # idle past the window: the registry forgets them
            assert await cursors.min_cursor(principal="u1") is None


async def test_retention_step_tick_sweeps_entries_and_keeps_fresh_cursors() -> None:
    from datetime import timedelta

    from forze.base.primitives import utcnow
    from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

    now_ms = int(utcnow().timestamp() * 1000)
    step = realtime_mailbox_retention_lifecycle_step(
        max_age=timedelta(hours=1), tenants=lambda: [_T1]
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
            cursors = build_realtime_cursors(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("old"))
            await mb.store(
                principal="u1", event_id=_eid(2), hlc=_hlc(now_ms), signal=_signal("new")
            )
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(now_ms))

        # the tick binds each assigned tenant itself (tenant-aware collections)
        await step.startup._sweep_tick(ctx, [_T1])  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]

        with _bind(ctx):
            remaining = await mb.read_since(principal="u1", since=None)
            cursor = await cursors.get(principal="u1", client_key="d1")

    assert [e.event_id for e in remaining] == [_eid(2)]  # ancient entry swept by age
    assert cursor == _hlc(now_ms)  # a freshly-advanced cursor survives the prune


def test_retention_step_refuses_incoherent_windows() -> None:
    from datetime import timedelta

    from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

    with pytest.raises(CoreException, match="max_age must be positive"):
        realtime_mailbox_retention_lifecycle_step(max_age=timedelta(0))

    # a cursor pruned before its acked entries expire re-offers confirmed deliveries
    with pytest.raises(CoreException, match="cursor_max_age"):
        realtime_mailbox_retention_lifecycle_step(
            max_age=timedelta(hours=2), cursor_max_age=timedelta(hours=1)
        )

    with pytest.raises(CoreException, match="interval must be positive"):
        realtime_mailbox_retention_lifecycle_step(
            max_age=timedelta(hours=1), interval=timedelta(0)
        )


# ----------------------- #
# sealing the stored signal bodies


def test_mailbox_spec_encryption_passthrough() -> None:
    from forze.application.contracts.crypto import FieldEncryption

    policy = FieldEncryption(encrypted={"payload", "event"})
    spec = realtime_mailbox_spec(encryption=policy)

    assert spec.encryption is policy


def test_mailbox_spec_refuses_sealing_the_replay_index() -> None:
    from forze.application.contracts.crypto import FieldEncryption

    # principal/event_id/hlc are filtered and sorted by replay, ack resolution, and
    # trimming — sealed they would fail at query time, so the build refuses them
    for field in ("principal", "event_id", "hlc"):
        with pytest.raises(CoreException) as caught:
            realtime_mailbox_spec(encryption=FieldEncryption(encrypted={field}))

        assert caught.value.code == "realtime_mailbox_sealed_index"


async def test_replay_pages_through_an_equal_hlc_run_without_skipping() -> None:
    # the wall-clock fallback stamps a whole burst with ONE hlc: a page boundary
    # inside the tie run must not skip the rest (an `hlc > cursor` keyset would) —
    # the composite (hlc, id) cursor resumes inside the run
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT, replay_page_size=2)
            for n in range(1, 6):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(7), signal=_signal(f"s{n}")
                )

            streamed = [e.event_id async for e in mb.replay_since(principal="u1", since=None)]

    assert streamed == [_eid(n) for n in range(1, 6)]  # all five, in id order


async def test_overflow_window_inside_an_equal_hlc_group_keeps_the_newest() -> None:
    # cap boundary inside an equal-HLC group: an HLC-only floor would match the whole
    # group, and the cap-limited ascending read would deliver the group's OLDER
    # entries and drop the newest — the composite (hlc, id) floor keeps exactly the
    # newest-cap window
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, retention=UNSWEPT, cap=5, replay_page_size=2)
            for n in (1, 2, 3):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(10), signal=_signal(f"s{n}")
                )
            for n in (4, 5, 6):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(20), signal=_signal(f"s{n}")
                )

            streamed = [e.event_id async for e in mb.replay_since(principal="u1", since=None)]

    # the newest five in (hlc, id) order — id 1 (oldest of the hlc-10 group) is the loss
    assert streamed == [_eid(n) for n in (2, 3, 4, 5, 6)]
    assert mb.stats().overflowed == 1


# ----------------------- #
# no unbounded-mailbox default


class TestRetentionIsPaired:
    """The mailbox has no delete path of its own, so an unswept one grows until the disk
    does. Neither half of the pairing is a default: the build refuses to guess, and a
    declared window that nothing sweeps is refused separately, because the two are
    different mistakes and a single error message for both would name the wrong fix."""

    def test_a_bounded_declaration_needs_positive_ages(self) -> None:
        with pytest.raises(CoreException) as ei:
            MailboxRetention(max_age=timedelta(0))

        assert ei.value.code == "realtime_mailbox_retention_invalid"

    def test_a_cursor_window_shorter_than_the_entry_window_is_refused(self) -> None:
        # Pruning a device cursor while its acked prefix is still retained re-offers
        # confirmed deliveries on the device's next connect.
        with pytest.raises(CoreException) as ei:
            MailboxRetention(max_age=timedelta(days=2), cursor_max_age=timedelta(days=1))

        assert ei.value.code == "realtime_mailbox_retention_invalid"

    def test_unbounded_must_carry_a_reason(self) -> None:
        with pytest.raises(CoreException) as ei:
            MailboxRetention(max_age=None)

        assert ei.value.code == "realtime_mailbox_unbounded_without_reason"

        # ...and the same object built through the named constructor is fine.
        assert MailboxRetention.unbounded(reason="ephemeral dev gateway").is_bounded is False

    def test_a_declaration_cannot_be_both(self) -> None:
        with pytest.raises(CoreException) as ei:
            MailboxRetention(max_age=timedelta(days=1), unbounded_reason="belt and braces")

        assert ei.value.code == "realtime_mailbox_retention_contradiction"

    async def test_a_declared_window_with_no_sweeper_is_refused_at_build(self) -> None:
        # The failure this whole pairing exists for: wiring that *looks* bounded. The
        # window is declared, nothing enforces it, and without this the mailbox is
        # exactly as unbounded as declaring nothing — only harder to notice.
        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            with _bind(ctx), pytest.raises(CoreException) as ei:
                build_realtime_mailbox(
                    ctx, retention=MailboxRetention(max_age=timedelta(days=7))
                )

        assert ei.value.code == "realtime_mailbox_retention_unwired"

    async def test_a_running_sweeper_satisfies_the_declaration(self) -> None:
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        step = realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=7))
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                with _bind(ctx):
                    mb = build_realtime_mailbox(
                        ctx, retention=MailboxRetention(max_age=timedelta(days=7))
                    )

                assert mb is not None
            finally:
                await step.shutdown(ctx)

            # ...and once the sweeper stops, the claim stops being true with it.
            with _bind(ctx), pytest.raises(CoreException) as ei:
                build_realtime_mailbox(ctx, retention=MailboxRetention(max_age=timedelta(days=7)))

        assert ei.value.code == "realtime_mailbox_retention_unwired"

    async def test_a_sweep_that_never_starts_does_not_vouch(self) -> None:
        # The marker is published before the task is created, so the sweep's own first
        # tick can build the mailbox it sweeps. That ordering has a cost: if task creation
        # or drainable registration fails, nothing sweeps and the marker would outlive the
        # sweeper — later builds passing a coverage check with no coverage behind it. The
        # loop's own exits are covered by the `finally` that retracts it.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        window = timedelta(days=7)
        step = realtime_mailbox_retention_lifecycle_step(max_age=window)
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()

            with (
                patch("asyncio.create_task", side_effect=RuntimeError("no loop")),
                pytest.raises(RuntimeError),
            ):
                await step.startup(ctx)

            with _bind(ctx), pytest.raises(CoreException) as ei:
                build_realtime_mailbox(ctx, retention=MailboxRetention(max_age=window))

        assert ei.value.code == "realtime_mailbox_retention_unwired"

    async def test_a_sweep_whose_registration_fails_is_cancelled_not_orphaned(self) -> None:
        # The other half of the same window, and the worse one. When *task creation* fails
        # there is nothing to clean up; when registration fails the task is already
        # running, and retracting only the marker leaves a sweep that no drainable can
        # stop — shutdown never learns of it — deleting on an interval nothing claims.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        window = timedelta(days=7)
        step = realtime_mailbox_retention_lifecycle_step(max_age=window)
        runtime = _runtime()
        created: list[asyncio.Task[None]] = []
        real_create_task = asyncio.create_task

        def _capturing_create_task(coro, **kwargs):  # type: ignore[no-untyped-def]
            task = real_create_task(coro, **kwargs)
            created.append(task)

            return task

        async with runtime.scope():
            ctx = runtime.get_context()

            with (
                patch("asyncio.create_task", _capturing_create_task),
                patch.object(
                    type(ctx.drainables), "register", side_effect=RuntimeError("registry full")
                ),
                pytest.raises(RuntimeError),
            ):
                await step.startup(ctx)

            assert created, "the task was never created; this test would prove nothing"

            # Let the cancellation the handler requested actually land.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            assert created[0].cancelled(), "the orphaned sweep kept running"
            assert step.startup.task is None  # type: ignore[attr-defined]

            with _bind(ctx), pytest.raises(CoreException) as ei:
                build_realtime_mailbox(ctx, retention=MailboxRetention(max_age=window))

        assert ei.value.code == "realtime_mailbox_retention_unwired"

    async def test_a_sweeper_running_a_different_window_does_not_vouch(self) -> None:
        # The subtler half of the pairing: a sweeper exists for this mailbox, so a
        # presence-only check would call it covered — while the declaration promises one
        # hour and the sweep deletes at seven days. The mailbox then retains data six days
        # and twenty-three hours past the window it claims, which is the same lie the
        # unwired case tells, one level quieter.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        step = realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=7))
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                with _bind(ctx), pytest.raises(CoreException) as ei:
                    build_realtime_mailbox(
                        ctx, retention=MailboxRetention(max_age=timedelta(hours=1))
                    )
            finally:
                await step.shutdown(ctx)

        assert ei.value.code == "realtime_mailbox_retention_mismatch"

    async def test_an_omitted_cursor_window_matches_the_step_that_resolves_it(self) -> None:
        # MailboxRetention(max_age=X) and a step configured (max_age=X, cursor_max_age=X)
        # are the same promise — the step resolves an omitted cursor window to max_age —
        # so they must key the same marker. Keying the raw fields made them disagree.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        window = timedelta(days=3)
        step = realtime_mailbox_retention_lifecycle_step(
            max_age=window, cursor_max_age=window
        )
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                with _bind(ctx):
                    assert build_realtime_mailbox(
                        ctx, retention=MailboxRetention(max_age=window)
                    )
            finally:
                await step.shutdown(ctx)

    @pytest.mark.parametrize(
        ("declared", "swept"),
        [
            (timedelta(milliseconds=1200), timedelta(milliseconds=1900)),
            (timedelta(milliseconds=500), timedelta(milliseconds=900)),
        ],
        ids=["same-whole-second", "both-under-a-second"],
    )
    async def test_windows_inside_one_second_are_still_different_windows(
        self, declared: timedelta, swept: timedelta
    ) -> None:
        # The marker keyed whole seconds, so every window inside a second collided: a
        # mailbox promising 1.2s was vouched for by a sweep deleting at 1.9s, and anything
        # sub-second collapsed onto 0 and was covered by any other sub-second sweep. The
        # window is only evidence if the comparison keeps the value it compares.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        step = realtime_mailbox_retention_lifecycle_step(max_age=swept)
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                with _bind(ctx), pytest.raises(CoreException) as ei:
                    build_realtime_mailbox(ctx, retention=MailboxRetention(max_age=declared))

            finally:
                await step.shutdown(ctx)

        assert ei.value.code == "realtime_mailbox_retention_mismatch"

    async def test_a_sweeper_vouches_only_for_the_mailbox_it_sweeps(self) -> None:
        # Two channels, one sweeper: the marker is keyed on the spec, so the unswept
        # channel is not covered by its neighbour's step.
        from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

        swept = realtime_mailbox_spec("swept")
        other = realtime_mailbox_spec("other")
        step = realtime_mailbox_retention_lifecycle_step(
            max_age=timedelta(days=7), mailbox_spec=swept
        )
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                with _bind(ctx):
                    build_realtime_mailbox(
                        ctx, spec=swept, retention=MailboxRetention(max_age=timedelta(days=7))
                    )

                    with pytest.raises(CoreException) as ei:
                        build_realtime_mailbox(
                            ctx,
                            spec=other,
                            retention=MailboxRetention(max_age=timedelta(days=7)),
                        )
            finally:
                await step.shutdown(ctx)

        assert ei.value.code == "realtime_mailbox_retention_unwired"
