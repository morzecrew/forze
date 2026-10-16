"""Integration tests for the Postgres outbox's consume storage.

A relay pass runs in one transaction: the claim locks its rows without updating them, a
published row is deleted at its mark, and a pass that fails rolls back whole.

# covers: TransactionalOutboxPasses.pass_scope
# covers: OutboxQueryPort.claim_pending
# covers: OutboxQueryPort.mark_published
# covers: OutboxQueryPort.mark_retry
# covers: OutboxQueryPort.mark_failed
# covers: OutboxQueryPort.requeue_failed
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from itertools import pairwise
from typing import Any, Literal
from unittest.mock import MagicMock, patch
from urllib.parse import quote
from uuid import uuid4

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxClaim,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay, outbox_relay_background_lifecycle_step
from forze_kits.integrations.outbox._relay_core import relay_outbox_claims
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_mock import MockDepsModule
from forze_mock.adapters import MockState
from forze_mock.adapters.queue import MockQueueAdapter
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresOutboxConfig
from forze_postgres.kernel.client import PostgresClient, PostgresConfig

# ----------------------- #

_ROUTE = "consume"


class _Payload(BaseModel):
    label: str


_CODEC = PydanticModelCodec(_Payload)
_SPEC: OutboxSpec[_Payload] = OutboxSpec(name=_ROUTE, codec=_CODEC)
_QUEUE = QueueSpec(name="relay", codec=_CODEC)


@pytest.fixture
async def outbox_table(pg_client: PostgresClient) -> AsyncIterator[str]:
    """A fresh outbox table with the documented unique key and claim index."""

    table = f"outbox_consume_{uuid4().hex[:8]}"
    ident = sql.Identifier("public", table)

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {t} (
                id UUID PRIMARY KEY,
                outbox_route TEXT NOT NULL,
                event_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                tenant_id UUID,
                execution_id UUID,
                correlation_id UUID,
                causation_id UUID,
                occurred_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                published_at TIMESTAMPTZ,
                processing_at TIMESTAMPTZ,
                last_error TEXT,
                attempts INT NOT NULL DEFAULT 0,
                available_at TIMESTAMPTZ,
                ordering_key TEXT,
                UNIQUE (outbox_route, event_id)
            )
            """
        ).format(t=ident)
    )
    await pg_client.execute(
        sql.SQL("CREATE INDEX ON {t} (outbox_route, status, available_at, created_at)").format(
            t=ident
        )
    )

    yield table

    await pg_client.execute(sql.SQL("DROP TABLE IF EXISTS {t}").format(t=ident))


def _runtime(
    client: PostgresClient,
    table: str,
    state: MockState | None = None,
    *,
    storage: Literal["retain", "consume"] = "consume",
    **config: Any,
) -> ExecutionRuntime:
    module = PostgresDepsModule(
        client=client,
        tx={"default"},
        outboxes={
            _ROUTE: PostgresOutboxConfig(relation=("public", table), storage=storage, **config)
        },
    )
    registry = DepsRegistry.from_modules(module)

    if state is not None:
        # The mock serves only what Postgres does not: the queue the relay publishes to.
        registry = registry.with_modules(MockDepsModule(state=state))

    return ExecutionRuntime(deps=registry.freeze())


async def _stage(ctx: ExecutionContext, labels: Sequence[str]) -> None:
    """Persist each event on its own, so its ``created_at`` orders it after the last one."""

    store = ctx.outbox.query(_SPEC)

    for label in labels:
        event = IntegrationEvent(
            event_type="demo.created", payload=_Payload(label=label), event_id=uuid4()
        )
        entry = StagedOutboxEntry(outbox_route=_ROUTE, event=event, payload_json={"label": label})
        assert await store.persist_rows([entry]) == 1


async def _rows(client: PostgresClient, table: str) -> dict[str, dict[str, Any]]:
    rows = await client.fetch_all(
        sql.SQL(
            "SELECT payload->>'label' AS label, status, attempts, available_at, last_error FROM {t}"
        ).format(t=sql.Identifier("public", table))
    )
    return {str(row["label"]): row for row in rows}


def _queued(state: MockState) -> list[str]:
    return [entry.message.payload.label for entry in state.queues.get("relay", {}).get("relay", [])]


async def _publish_ok(claim: OutboxClaim, payload: Any) -> None:
    del claim, payload


async def _relay(ctx: ExecutionContext, publish_one: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("reclaim_stale_after", None)
    return await relay_outbox_claims(ctx, outbox_spec=_SPEC, publish_one=publish_one, **kwargs)


@asynccontextmanager
async def _single_connection_client(
    postgres_container: Any, session: dict[str, str] | None = None
) -> AsyncIterator[PostgresClient]:
    """One pooled connection; *session* settings go in the DSN's ``options``, as a role's would."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")

    if session:
        url += "?options=" + quote(
            " ".join(f"-c {name}={value}" for name, value in session.items())
        )

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=1))

    try:
        yield client

    finally:
        await client.close()


# ....................... #


@pytest.mark.integration
class TestConsumeWrites:
    async def test_a_first_try_publish_inserts_and_deletes_and_never_updates(
        self, postgres_container: Any, outbox_table: str
    ) -> None:
        labels = [f"e{i:02d}" for i in range(12)]
        state = MockState()

        # One connection, so one backend holds every count and flushes them on request.
        async with _single_connection_client(postgres_container) as client:
            runtime = _runtime(client, outbox_table, state)

            async with runtime.scope():
                ctx = runtime.get_context()
                await _stage(ctx, labels)
                result = await OutboxRelay(outbox_spec=_SPEC, reclaim_stale_after=None).to_queue(
                    ctx, _QUEUE
                )

            await client.execute("SELECT pg_stat_force_next_flush()")
            stats = await client.fetch_one(
                "SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables "
                "WHERE relname = %s",
                (outbox_table,),
            )

        assert result.published == len(labels)
        assert stats == {"n_tup_ins": 12, "n_tup_upd": 0, "n_tup_del": 12}
        assert _queued(state) == labels

    async def test_each_mark_matches_exactly_the_rows_it_was_given(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["published", "retried", "parked", "untouched"])
            store = ctx.outbox.query(_SPEC)

            async with store.pass_scope():
                by_label = {
                    claim.payload["label"]: claim.id for claim in await store.claim_pending()
                }
                marks = (
                    await store.mark_published([by_label["published"]]),
                    await store.mark_retry(
                        [by_label["retried"]],
                        attempts=1,
                        available_at=utcnow() + timedelta(hours=1),
                        error="broker down",
                    ),
                    await store.mark_failed([by_label["parked"]], error="poison"),
                )

            rows = await _rows(pg_client, outbox_table)

        assert marks == (1, 1, 1)
        assert "published" not in rows
        assert rows["retried"]["status"] == "pending" and rows["retried"]["attempts"] == 1
        assert rows["parked"]["status"] == "failed" and rows["parked"]["last_error"] == "poison"
        assert rows["untouched"]["attempts"] == 0 and rows["untouched"]["last_error"] is None

    async def test_a_claim_outside_a_transaction_is_refused(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["a"])

            with pytest.raises(CoreException) as caught:
                await ctx.outbox.query(_SPEC).claim_pending()

            rows = await _rows(pg_client, outbox_table)

        assert caught.value.code == "outbox_claim_outside_transaction"
        assert rows["a"]["status"] == "pending"


# ....................... #


@pytest.mark.integration
class TestConsumePerRowFailures:
    async def test_one_row_failing_never_aborts_its_batch(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.label == "raises":
                raise RuntimeError("broker down")

            if payload.label == "slow":
                await asyncio.sleep(30)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["ok-1", "raises", "slow", "ok-2"])
            # A payload the codec cannot decode is poison.
            await pg_client.execute(
                sql.SQL("UPDATE {t} SET payload = %s WHERE payload->>'label' = 'ok-2'").format(
                    t=sql.Identifier("public", outbox_table)
                ),
                (Jsonb({"label": 7, "shape": "wrong"}),),
            )
            result = await asyncio.wait_for(
                _relay(ctx, publish, publish_timeout=timedelta(milliseconds=200)),
                timeout=20,
            )
            rows = await _rows(pg_client, outbox_table)

        assert (result.published, result.retried, result.failed) == (1, 2, 1)
        assert "ok-1" not in rows
        for label in ("raises", "slow"):
            assert rows[label]["status"] == "pending"
            assert rows[label]["attempts"] == 1
            assert rows[label]["available_at"] is not None
        # Both share one retry slot, so they share its error: "broker down (+1 more)".
        assert rows["slow"]["last_error"] == rows["raises"]["last_error"]
        poison = next(row for label, row in rows.items() if label not in ("raises", "slow"))
        assert poison["status"] == "failed" and poison["attempts"] == 0

    async def test_a_row_failing_every_publish_is_parked_at_max_attempts(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            raise RuntimeError("broker down")

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["poison"])
            passes = 0

            # Bounded: a row that never parks would otherwise loop here for the whole timeout.
            for _ in range(5):
                passes += 1
                result = await _relay(
                    ctx,
                    publish,
                    max_attempts=3,
                    retry_base_delay=timedelta(milliseconds=5),
                    retry_max_backoff=timedelta(milliseconds=10),
                )

                if result.failed:
                    break

                await asyncio.sleep(0.03)

            rows = await _rows(pg_client, outbox_table)

        assert passes == 3
        assert rows["poison"]["status"] == "failed"
        assert rows["poison"]["attempts"] == 2  # parking does not bump the counter


# ....................... #


_TIMEOUTS = ("statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout")


@pytest.mark.integration
class TestConsumePassBounds:
    @pytest.mark.parametrize(
        ("session", "in_pass"),
        [
            # Stricter than the pass (1 s, 1 s, 5 s + 1 s): the session's own values hold.
            (("300ms", "200ms", "2s"), ("300ms", "200ms", "2s")),
            # Looser, or unlimited: the pass's values apply.
            (("1min", "1min", "1h"), ("1s", "1s", "6s")),
            (("0", "0", "0"), ("1s", "1s", "6s")),
        ],
    )
    async def test_a_pass_tightens_the_session_timeouts_and_never_loosens_them(
        self,
        postgres_container: Any,
        outbox_table: str,
        session: tuple[str, str, str],
        in_pass: tuple[str, str, str],
    ) -> None:
        settings = dict(zip(_TIMEOUTS, session, strict=True))

        async with _single_connection_client(postgres_container, settings) as client:
            runtime = _runtime(client, outbox_table)

            async with runtime.scope():
                store = runtime.get_context().outbox.query(_SPEC)

                async with store.pass_scope():
                    shown = tuple([await client.fetch_value(f"SHOW {name}") for name in _TIMEOUTS])

                after = tuple([await client.fetch_value(f"SHOW {name}") for name in _TIMEOUTS])

        assert shown == in_pass
        assert after == session  # transaction-local: the session's values are back

    async def test_a_stricter_session_statement_timeout_still_cuts_a_pass_statement(
        self, postgres_container: Any, outbox_table: str
    ) -> None:
        async with _single_connection_client(
            postgres_container, {"statement_timeout": "300ms"}
        ) as client:
            runtime = _runtime(client, outbox_table)

            async with runtime.scope():
                store = runtime.get_context().outbox.query(_SPEC)

                with pytest.raises(CoreException) as caught:
                    async with store.pass_scope():
                        await client.execute("SELECT pg_sleep(0.8)")

        # The pass alone allows 1 s, which a 0.8 s sleep fits, so the statement is cut only
        # because the session's stricter 300 ms still holds. Asserted on the cancel itself,
        # not on elapsed time, which a loaded runner stretches.
        assert "query canceled" in str(caught.value)

    async def test_a_pass_stops_at_its_deadline_and_commits_what_it_published(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table, pass_deadline=timedelta(milliseconds=500))

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.label == "e1":
                await asyncio.sleep(30)  # a destination slower than the pass allows

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["e0", "e1", "e2", "e3"])
            started = time.monotonic()
            result = await asyncio.wait_for(_relay(ctx, publish), timeout=20)
            elapsed = time.monotonic() - started
            rows = await _rows(pg_client, outbox_table)

        assert elapsed < 5
        assert (result.published, result.retried) == (1, 1)
        assert "e0" not in rows
        assert rows["e1"]["attempts"] == 1
        assert rows["e1"]["last_error"].startswith("publish timed out")
        for label in ("e2", "e3"):
            assert rows[label]["status"] == "pending"
            assert rows[label]["attempts"] == 0 and rows[label]["last_error"] is None

    async def test_a_mark_blocked_on_a_lock_fails_at_the_statement_timeout(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(
            pg_client, outbox_table, pass_statement_timeout=timedelta(milliseconds=300)
        )
        locked = asyncio.Event()
        release = asyncio.Event()

        async def hold_share_lock() -> None:
            # Its own root transaction on its own connection, not the pass's.
            async with pg_client.detached(), pg_client.transaction():
                await pg_client.execute(
                    sql.SQL("LOCK TABLE {t} IN SHARE MODE").format(
                        t=sql.Identifier("public", outbox_table)
                    )
                )
                locked.set()
                await release.wait()

        holder: asyncio.Task[None] | None = None

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            nonlocal holder

            if holder is None:
                holder = asyncio.create_task(hold_share_lock())
                await locked.wait()

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["e0", "e1"])
            started = time.monotonic()

            try:
                with pytest.raises(CoreException):
                    await asyncio.wait_for(_relay(ctx, publish), timeout=20)

                elapsed = time.monotonic() - started

            finally:
                release.set()

                if holder is not None:
                    await holder

            rows = await _rows(pg_client, outbox_table)

        # The delete waited for the lock no longer than the timeout, then the pass rolled back.
        assert elapsed < 3
        assert [rows[label]["status"] for label in ("e0", "e1")] == ["pending", "pending"]

    async def test_a_stalled_pass_is_cut_off_by_the_idle_timeout(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(
            pg_client,
            outbox_table,
            pass_deadline=timedelta(milliseconds=300),
            pass_statement_timeout=timedelta(milliseconds=200),
        )
        pids: list[int] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            pids.append(int(await pg_client.fetch_value("SELECT pg_backend_pid()")))
            time.sleep(1.5)  # noqa: ASYNC251 — a blocked loop: nothing yields, nothing commits

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["e0", "e1"])

            with pytest.raises(CoreException):
                await asyncio.wait_for(_relay(ctx, publish), timeout=20)

            alive = await pg_client.fetch_value(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (pids[0],)
            )
            rows = await _rows(pg_client, outbox_table)

        # The server ended the session, so the pass's locks and delete went with it.
        assert alive == 0
        assert [rows[label]["status"] for label in ("e0", "e1")] == ["pending", "pending"]


# ....................... #


@pytest.mark.integration
class TestConsumeCrashesAndConcurrency:
    async def test_a_relay_killed_mid_pass_leaves_its_rows_for_the_next(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)
        in_pass = asyncio.Event()
        pids: list[int] = []

        async def hang(claim: OutboxClaim, payload: Any) -> None:
            if not pids:
                pids.append(int(await pg_client.fetch_value("SELECT pg_backend_pid()")))
                in_pass.set()

            await asyncio.Event().wait()

        published: list[str] = []

        async def record(claim: OutboxClaim, payload: Any) -> None:
            published.append(payload.label)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["e0", "e1", "e2"])
            dying = asyncio.create_task(_relay(ctx, hang))
            await asyncio.wait_for(in_pass.wait(), timeout=10)

            # The process dies between its claim and its commit: its connection drops.
            async with pg_client.detached():
                killed = await pg_client.fetch_value("SELECT pg_terminate_backend(%s)", (pids[0],))

            dying.cancel()

            with suppress(asyncio.CancelledError, Exception):
                await dying

            pending = await _rows(pg_client, outbox_table)
            result = await _relay(ctx, record)

        assert killed is True
        assert dying.cancelled() or dying.exception() is not None
        assert [pending[label]["status"] for label in ("e0", "e1", "e2")] == ["pending"] * 3
        assert result.published == 3
        assert published == ["e0", "e1", "e2"]

    async def test_two_relays_take_disjoint_batches_and_each_keeps_creation_order(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)
        labels = [f"e{i}" for i in range(6)]
        first_claimed = asyncio.Event()
        second_done = asyncio.Event()
        seen: dict[str, list[str]] = {"first": [], "second": []}

        async def first(claim: OutboxClaim, payload: Any) -> None:
            seen["first"].append(payload.label)
            first_claimed.set()
            await second_done.wait()  # hold the pass, and its row locks, open

        async def second(claim: OutboxClaim, payload: Any) -> None:
            seen["second"].append(payload.label)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, labels)
            held = asyncio.create_task(_relay(ctx, first, limit=3))
            await asyncio.wait_for(first_claimed.wait(), timeout=10)

            try:
                other = await asyncio.wait_for(_relay(ctx, second, limit=3), timeout=10)

            finally:
                second_done.set()

            held_result = await asyncio.wait_for(held, timeout=10)
            rows = await _rows(pg_client, outbox_table)

        assert (held_result.published, other.published) == (3, 3)
        assert set(seen["first"]).isdisjoint(seen["second"])
        assert seen["first"] == labels[:3] and seen["second"] == labels[3:]
        assert rows == {}

    async def test_an_outer_transaction_that_rolls_back_leaves_every_row_pending(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        runtime = _runtime(pg_client, outbox_table)

        class _FenceLost(Exception):
            pass

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, ["e0", "e1"])
            timeouts: list[Any] = []

            with pytest.raises(_FenceLost):
                async with ctx.tx_ctx.scope("default"):
                    await pg_client.execute("SET LOCAL statement_timeout = 7000")
                    result = await _relay(ctx, _publish_ok)
                    # The nested pass set nothing: the caller's own limit is still in force.
                    timeouts.append(await pg_client.fetch_value("SHOW statement_timeout"))
                    raise _FenceLost

            rows = await _rows(pg_client, outbox_table)

        assert result.published == 2
        assert timeouts == ["7s"]
        assert [rows[label]["status"] for label in ("e0", "e1")] == ["pending", "pending"]


# ....................... #


@pytest.mark.integration
class TestConsumeBackgroundStep:
    async def test_a_failed_pass_ends_the_tick_and_the_next_waits_out_the_backoff(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        # Every delete fails, as a missing delivery-log partition would fail it.
        ident = sql.Identifier("public", outbox_table)
        function = sql.Identifier("public", f"{outbox_table}_refuse")
        await pg_client.execute(
            sql.SQL(
                "CREATE FUNCTION {f}() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN RAISE EXCEPTION 'delete refused'; END $$"
            ).format(f=function)
        )
        await pg_client.execute(
            sql.SQL(
                "CREATE TRIGGER refuse BEFORE DELETE ON {t} FOR EACH ROW EXECUTE FUNCTION {f}()"
            ).format(t=ident, f=function)
        )

        state = MockState()
        runtime = _runtime(pg_client, outbox_table, state)
        batch = ["e0", "e1", "e2"]
        passes: list[float] = []
        relay_to_queue = OutboxRelay.to_queue

        async def counting(self: OutboxRelay, *args: Any, **kwargs: Any) -> Any:
            passes.append(time.monotonic())
            return await relay_to_queue(self, *args, **kwargs)

        step = outbox_relay_background_lifecycle_step(
            outbox_spec=_SPEC,
            queue_spec=_QUEUE,
            interval=timedelta(milliseconds=200),
            jitter=0.0,
            reclaim_stale_after=None,
            limit=len(batch),  # a full batch, so a tolerant tick would claim it again
            max_batches_per_tick=10,
            retry_base_delay=timedelta(milliseconds=10),
            retry_max_backoff=timedelta(milliseconds=800),
        )

        try:
            async with runtime.scope():
                await _stage(runtime.get_context(), batch)

                # Quiet loggers: rendering each failed pass's traceback takes longer than a tick.
                with (
                    patch.object(OutboxRelay, "to_queue", counting),
                    patch("forze_kits.integrations.outbox.lifecycle.logger", MagicMock()),
                    patch("forze_postgres.kernel.client.client.logger", MagicMock()),
                ):
                    ctx = runtime.get_context()
                    await step.startup(ctx)

                    try:
                        await asyncio.sleep(1.6)

                    finally:
                        await step.shutdown(ctx)

                rows = await _rows(pg_client, outbox_table)

        finally:
            await pg_client.execute(sql.SQL("DROP FUNCTION {f} CASCADE").format(f=function))

        gaps = [later - earlier for earlier, later in pairwise(passes)]
        # One pass per tick, waiting 0.2 s, 0.4 s, 0.8 s: four passes in 1.6 s, not forty.
        assert 3 <= len(passes) <= 5
        assert gaps[1] > gaps[0] * 1.5
        # Each pass published the batch once, then rolled back: nothing advanced.
        assert len(_queued(state)) == len(batch) * len(passes)
        assert all(row["status"] == "pending" and row["attempts"] == 0 for row in rows.values())


# ....................... #


@pytest.mark.integration
class TestConsumeShutdownDrain:
    async def test_a_drain_shorter_than_the_pass_deadline_still_commits_what_it_published(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        # The pass may publish for a second, and the drain has a second in all: a pass cut
        # at the drain's end would roll back, and every row it published would go again.
        state = MockState()
        runtime = _runtime(pg_client, outbox_table, state, pass_deadline=timedelta(seconds=1))
        step = outbox_relay_background_lifecycle_step(
            outbox_spec=_SPEC,
            queue_spec=_QUEUE,
            reclaim_stale_after=None,
            drain_on_shutdown=True,
            shutdown_drain_timeout=timedelta(seconds=1),
        )
        startup = step.startup
        assert isinstance(startup, _OutboxRelayBackgroundStartup)
        enqueue = MockQueueAdapter.enqueue

        async def slow_enqueue(self: MockQueueAdapter[Any], *args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.3)
            return await enqueue(self, *args, **kwargs)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _stage(ctx, [f"e{i}" for i in range(10)])
            # What startup would read from the store, without starting the poll loop.
            startup.scope_ctx = ctx
            startup.passes_roll_back = True

            with (
                patch.object(MockQueueAdapter, "enqueue", slow_enqueue),
                patch("forze_kits.integrations.outbox.lifecycle.logger", MagicMock()),
                patch("forze_postgres.kernel.client.client.logger", MagicMock()),
            ):
                completed = await startup._bounded_drain(
                    ctx, deadline=asyncio.get_running_loop().time() + 10, label="drain"
                )

            rows = await _rows(pg_client, outbox_table)

        published = _queued(state)
        assert published  # the drain did publish
        assert set(published).isdisjoint(rows)  # and everything it published was consumed
        assert completed


# ....................... #


@pytest.mark.integration
class TestConsumeMigration:
    async def test_a_retain_table_drains_and_then_runs_without_the_retain_columns(
        self, pg_client: PostgresClient, outbox_table: str
    ) -> None:
        ident = sql.Identifier("public", outbox_table)
        long_ago = utcnow() - timedelta(hours=1)

        retain = _runtime(pg_client, outbox_table, storage="retain")

        async with retain.scope():
            ctx = retain.get_context()
            await _stage(ctx, ["was-published", "was-processing", "pending"])
            # Leave the shapes a retain deployment leaves behind.
            await pg_client.execute(
                sql.SQL(
                    "UPDATE {t} SET status = 'published', published_at = %s "
                    "WHERE payload->>'label' = 'was-published'"
                ).format(t=ident),
                (long_ago,),
            )
            await pg_client.execute(
                sql.SQL(
                    "UPDATE {t} SET status = 'processing', processing_at = %s "
                    "WHERE payload->>'label' = 'was-processing'"
                ).format(t=ident),
                (long_ago,),
            )

        published: list[str] = []

        async def record(claim: OutboxClaim, payload: Any) -> None:
            published.append(payload.label)

        consume = _runtime(pg_client, outbox_table)

        async with consume.scope():
            ctx = consume.get_context()
            result = await _relay(ctx, record, reclaim_stale_after=timedelta(minutes=5))
            drained = await _rows(pg_client, outbox_table)

        assert result.reclaimed == 1
        assert sorted(published) == ["pending", "was-processing"]
        assert list(drained) == ["was-published"]  # left to the retention job

        # Reclaim off and the published history gone, the two columns can go.
        await pg_client.execute(sql.SQL("DELETE FROM {t}").format(t=ident))
        await pg_client.execute(
            sql.SQL("ALTER TABLE {t} DROP COLUMN processing_at, DROP COLUMN published_at").format(
                t=ident
            )
        )

        async def refuse_poison(claim: OutboxClaim, payload: Any) -> None:
            if payload.label == "flaky":
                raise RuntimeError("broker down")

        async with consume.scope():
            ctx = consume.get_context()
            await _stage(ctx, ["ok", "flaky", "parked"])
            store = ctx.outbox.query(_SPEC)
            await pg_client.execute(
                sql.SQL("UPDATE {t} SET payload = %s WHERE payload->>'label' = 'parked'").format(
                    t=ident
                ),
                (Jsonb({"label": 1}),),
            )
            first = await _relay(ctx, refuse_poison)
            parked = await pg_client.fetch_value(
                sql.SQL("SELECT id FROM {t} WHERE status = 'failed'").format(t=ident)
            )
            requeued = await store.requeue_failed([parked])
            after = await _rows(pg_client, outbox_table)

        assert (first.published, first.retried, first.failed) == (1, 1, 1)
        assert requeued == 1
        assert {label: row["status"] for label, row in after.items()} == {
            "flaky": "pending",
            "1": "pending",
        }
