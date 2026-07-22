"""Unit tests for the background commit-stream (offset-log) consumer lifecycle step."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import (
    OffsetReset,
    StreamMessage,
    StreamSpec,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import (
    CommitStreamGroupConsumer,
    commit_stream_consumer_background_lifecycle_step,
)
from forze_kits.integrations.consumer.commit_stream_lifecycle import (
    _CommitStreamConsumerBackgroundStartup,
)
from forze_mock import MockDepsModule
from forze_mock.adapters import (
    MockCommitStreamGroupAdminAdapter,
    MockState,
    MockStreamAdapter,
)

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_TOPIC = "orders"
_STREAM_SPEC = StreamSpec(name=_TOPIC, codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="events")


async def _noop_handler(message: StreamMessage[Any]) -> None:
    del message


def _step(**overrides: Any):
    kwargs: dict[str, Any] = {
        "topics": [_TOPIC],
        "group": "g",
        "consumer": "c",
        "stream_spec": _STREAM_SPEC,
        "handler": _noop_handler,
        "inbox_spec": _INBOX_SPEC,
        "tx_route": "default",
    }
    kwargs.update(overrides)

    return commit_stream_consumer_background_lifecycle_step(**kwargs)


def _runtime(state: MockState | None = None, *, strict_tx: bool = False):
    module = MockDepsModule(state=state or MockState(), strict_tx=strict_tx)

    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


# ----------------------- #


@pytest.mark.asyncio
async def test_background_lifecycle_starts_and_stops_task() -> None:
    step = _step()
    run_mock = AsyncMock()
    runtime = _runtime()

    # Patching the method with an AsyncMock does not bind self, so the call is
    # ``run(ctx, timeout=None)`` — no instance arg.
    with patch.object(CommitStreamGroupConsumer, "run", run_mock):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None

            await asyncio.sleep(0.05)
            await step.shutdown(ctx)

    run_mock.assert_called()
    # The background loop consumes forever: idle timeout disabled.
    assert run_mock.call_args.kwargs["timeout"] is None
    assert startup.task.done()


# ....................... #


@pytest.mark.asyncio
async def test_consume_crash_is_logged_and_restarts_after_backoff() -> None:
    calls = 0
    restarted = asyncio.Event()

    async def _crashy(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1

        if calls == 1:
            raise RuntimeError("broker down")

        restarted.set()
        await asyncio.Event().wait()  # behave like consume-forever

    step = _step(restart_backoff=timedelta(milliseconds=10))
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_crashy)),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            # Crash on the first run is logged and the consume restarts.
            await asyncio.wait_for(restarted.wait(), timeout=2.0)
            assert calls == 2

            await step.shutdown(ctx)

    logger_mock.exception.assert_called_once()

    startup = step.startup
    assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
    assert startup.task is not None and startup.task.done()


# ....................... #


@pytest.mark.asyncio
async def test_terminal_configuration_crash_stops_supervision() -> None:
    # A fault retrying cannot clear — a revoked or deleted KMS key, an unresolvable
    # route — must not be restarted: doing so hot-loops a critical log forever while
    # every liveness probe still reads the consumer as "running".
    calls = 0

    async def _revoked(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1
        raise exc.configuration("KMS access denied")

    step = _step(restart_backoff=timedelta(milliseconds=10))
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_revoked)),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None
            await asyncio.wait_for(startup.task, timeout=2.0)

            await step.shutdown(ctx)

    assert calls == 1  # ran once, never restarted
    logger_mock.critical.assert_called_once()


# ....................... #


@pytest.mark.asyncio
async def test_crash_ceiling_stops_a_hot_loop() -> None:
    # A transient-looking fault that never clears would otherwise restart every few
    # seconds indefinitely. The ceiling turns "quietly down forever" into one loud stop.
    calls = 0

    async def _always_crashes(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1
        raise RuntimeError("KMS unavailable")

    step = _step(
        restart_backoff=timedelta(milliseconds=1),
        max_crash_window=timedelta(milliseconds=20),
    )
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_always_crashes)),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None
            await asyncio.wait_for(startup.task, timeout=5.0)

            await step.shutdown(ctx)

    # It restarted at least once before giving up: the ceiling measures a *window* of
    # unbroken crashing, so a single crash can never trip it however tight the window.
    assert calls > 1
    logger_mock.critical.assert_called_once()


# ....................... #


@pytest.mark.asyncio
async def test_a_long_healthy_run_does_not_count_as_crash_looping() -> None:
    """The window measures time spent crashing, never time spent working.

    A consumer that runs healthily for hours and then hits one transient blip must restart.
    Dating the incident from the *run's* start instead of the failure booked all that
    healthy uptime as crash-loop time, so the very first crash of a long-lived consumer
    already exceeded the window and stopped supervision for good — inverting the healthy
    reset, which exists precisely to keep rare blips from accumulating.
    """

    calls = 0
    recovered = asyncio.Event()
    # Run 1 stays up longer than the whole crash window before failing.
    healthy_for = 0.08
    window = timedelta(seconds=0.05)

    async def _healthy_then_one_blip(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1

        if calls == 1:
            await asyncio.sleep(healthy_for)
            raise RuntimeError("broker connection reset")

        recovered.set()
        await asyncio.Event().wait()  # behave like consume-forever

    step = _step(restart_backoff=timedelta(milliseconds=1), max_crash_window=window)
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(
            CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_healthy_then_one_blip)
        ),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.HEALTHY_UPTIME_SECONDS",
            healthy_for / 2,
        ),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None
            await asyncio.wait_for(recovered.wait(), timeout=5.0)

            await step.shutdown(ctx)

    assert calls == 2  # restarted rather than giving up on its first crash
    logger_mock.critical.assert_not_called()


# ....................... #


@pytest.mark.asyncio
async def test_a_slow_clearing_fault_outlives_the_restart_count() -> None:
    """The ceiling must not stop a fault that needs many restarts to clear.

    A denial during IAM or key-policy propagation is retryable and does clear itself — but
    only after minutes of failing every single restart. While the ceiling counted restarts
    over a constant backoff, patience was ``count * restart_backoff``: the shipped default
    of ten gave up roughly a minute in, stranding an uncommitted record behind an outage
    that would have fixed itself. A window has no such ceiling on restarts.
    """

    crashes_before_recovery = 25
    calls = 0
    recovered = asyncio.Event()

    async def _crashes_then_recovers(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1

        if calls <= crashes_before_recovery:
            raise RuntimeError("KMS access denied — policy still propagating")

        recovered.set()
        await asyncio.Event().wait()  # behave like consume-forever

    step = _step(
        restart_backoff=timedelta(milliseconds=1),
        max_crash_window=timedelta(seconds=30),
    )
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(
            CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_crashes_then_recovers)
        ),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None
            await asyncio.wait_for(recovered.wait(), timeout=5.0)

            await step.shutdown(ctx)

    # Restarted well past the old ten-crash default and reached the healthy run.
    assert calls == crashes_before_recovery + 1
    logger_mock.critical.assert_not_called()


# ....................... #


@pytest.mark.asyncio
async def test_poison_pause_return_stops_supervision_without_restart() -> None:
    # A pause-and-alert poison makes run() *return* (failed > 0), not raise. Restarting
    # would re-fetch the same uncommitted record from the committed offset and pause
    # again forever, so the supervisor must stop — call run() exactly once, log an alert,
    # and let the task end. It must NOT rewind to committed (that is the crash path).
    from forze_kits.integrations.consumer.commit_stream_runner import (
        CommitStreamGroupConsumerRunResult,
    )

    calls = 0

    async def _paused(ctx: Any, **kwargs: Any) -> CommitStreamGroupConsumerRunResult:
        del ctx, kwargs
        nonlocal calls
        calls += 1

        return CommitStreamGroupConsumerRunResult(processed=2, failed=1)

    step = _step(restart_backoff=timedelta(milliseconds=10))
    logger_mock = MagicMock()
    runtime = _runtime()
    reset_mock = AsyncMock()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_paused)),
        patch.object(CommitStreamGroupConsumer, "reset_to_committed", reset_mock),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)
            assert startup.task is not None

            # The task ends on its own once the poison pause returns.
            await asyncio.wait_for(startup.task, timeout=2.0)

            await step.shutdown(ctx)

    # run() invoked once and never restarted (no re-fetch of the same poison).
    assert calls == 1
    # The pause is a rewind-free alert, distinct from the crash path.
    reset_mock.assert_not_called()
    logger_mock.error.assert_called_once()
    logger_mock.exception.assert_not_called()


# ....................... #


@pytest.mark.asyncio
async def test_crash_restart_rewinds_to_committed() -> None:
    # BUG 3 (loss-free restart): a crashed run rewinds the group to its committed
    # offset before restarting, so the reader does not resume past uncommitted
    # records. We observe the rewind by patching reset_to_committed.
    calls = 0
    restarted = asyncio.Event()
    reset = asyncio.Event()

    async def _crashy(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("broker down")
        restarted.set()
        await asyncio.Event().wait()

    async def _reset(ctx: Any) -> None:
        del ctx
        reset.set()

    step = _step(restart_backoff=timedelta(milliseconds=10))
    runtime = _runtime()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_crashy)),
        patch.object(
            CommitStreamGroupConsumer,
            "reset_to_committed",
            AsyncMock(side_effect=_reset),
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            await asyncio.wait_for(reset.wait(), timeout=2.0)
            await asyncio.wait_for(restarted.wait(), timeout=2.0)

            await step.shutdown(ctx)

    assert reset.is_set()


# ....................... #


@pytest.mark.asyncio
async def test_lifecycle_consumes_from_the_real_mock_offset_log() -> None:
    state = MockState()
    runtime = _runtime(state, strict_tx=True)

    handled = asyncio.Event()
    values: list[str] = []

    async def handler(message: StreamMessage[_Payload]) -> None:
        values.append(message.payload.value)
        handled.set()

    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    step = _step(handler=handler)

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        await producer.append(_TOPIC, _Payload(value="bg"), key="k")

        await asyncio.wait_for(handled.wait(), timeout=2.0)
        await step.shutdown(ctx)

    assert values == ["bg"]


# ....................... #


@pytest.mark.asyncio
async def test_duplicate_startup_is_ignored_and_does_not_orphan_the_task() -> None:
    # A second startup call while the first task still runs must warn and keep the original
    # task, not spawn (and leak) a second consumer.
    async def _forever(ctx: Any, **kwargs: Any) -> None:
        del ctx, kwargs
        await asyncio.Event().wait()

    step = _step()
    logger_mock = MagicMock()
    runtime = _runtime()

    with (
        patch.object(CommitStreamGroupConsumer, "run", AsyncMock(side_effect=_forever)),
        patch(
            "forze_kits.integrations.consumer.commit_stream_lifecycle.logger",
            logger_mock,
        ),
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            startup = step.startup
            assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)

            await startup(ctx)
            first_task = startup.task
            assert first_task is not None and not first_task.done()

            await startup(ctx)  # duplicate while the first task is still running
            logger_mock.warning.assert_called_once()
            assert startup.task is first_task  # unchanged — no second task spawned

            await step.shutdown(ctx)


@pytest.mark.asyncio
async def test_shutdown_without_startup_is_a_noop() -> None:
    step = _step()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        # Shutdown before any startup: startup.task is None, so it must return cleanly.
        await step.shutdown(ctx)


# ....................... #


def test_default_step_id_derives_from_group() -> None:
    assert _step(group="orders").id == "commit_stream_consumer:orders"
    assert _step(step_id="my_consumer").id == "my_consumer"


# ....................... #


def test_lifecycle_step_rejects_invalid_options() -> None:
    with pytest.raises(CoreException, match="backoff"):
        _step(restart_backoff=timedelta(0))

    with pytest.raises(CoreException, match="max_attempts"):
        _step(max_attempts=0)

    with pytest.raises(CoreException, match="Crash window"):
        _step(max_crash_window=timedelta(0))


# ....................... #


@pytest.mark.asyncio
async def test_stop_commits_the_messages_it_already_processed() -> None:
    """A stop must not lose the offsets of work the consumer has already committed.

    The unit of work used to be the whole batch, and ``batch_limit=None`` — the default — reads
    the entire uncommitted tail. So a batch routinely outlasts the shutdown grace, the loop is
    cancelled part-way through, and the offsets of every message it had already handled are
    lost: each of those had its *business effect* committed in its own inbox transaction, but
    the single ``commit`` of their offsets never ran. The inbox dedups the redelivery, so the
    effect stays exactly-once — but not redelivering them at all is the entire point of the
    stop signal.

    Here the batch (10 x 0.1s) cannot possibly finish inside the 0.25s budget, so this passes
    only if the consumer stops *between messages* and commits what it has.
    """

    state = MockState()
    runtime = _runtime(state, strict_tx=True)

    values: list[str] = []
    second = asyncio.Event()

    async def handler(message: StreamMessage[_Payload]) -> None:
        await asyncio.sleep(0.1)
        values.append(message.payload.value)

        if len(values) == 2:
            second.set()

    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    for index in range(10):
        await producer.append(_TOPIC, _Payload(value=f"m{index}"), key="k")

    step = _step(handler=handler)

    async with runtime.scope():
        ctx = runtime.get_context()
        startup = step.startup
        assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)

        await startup(ctx)
        await asyncio.wait_for(second.wait(), timeout=5.0)

        clock = asyncio.get_running_loop()
        graceful = await startup.stop(deadline=clock.time() + 0.25)

    committed = sum(nxt for key, nxt in state.commit_stream_offsets.items() if key[1] == "g")

    assert graceful, "the loop was cancelled instead of reaching a stopping point"
    assert 0 < len(values) < 10, "the run must stop mid-batch for this test to be about anything"
    assert committed == len(values), (
        f"{len(values)} messages were processed but only {committed} offsets committed — "
        f"the rest are redelivered on restart"
    )


# ....................... #


@pytest.mark.asyncio
async def test_a_cancel_mid_handler_still_commits_what_the_batch_finished() -> None:
    """The stopping point is the message — but a handler can outlast the grace anyway.

    Then the loop is cancelled *inside* it, which is the backstop working as designed. What is
    not by design is what the cancel used to take with it: the messages before that handler had
    already committed their effects, each in its own inbox transaction, and their offsets were
    accumulated for the single ``commit`` at the end of the batch — the very call a cancel skips.
    So a hard stop redelivered work it had demonstrably already done.

    Here the third handler never returns, so no grace can reach a boundary; this passes only if
    the cancellation path pays the offsets it owes for the two that finished.
    """

    state = MockState()
    runtime = _runtime(state, strict_tx=True)

    completed: list[str] = []
    wedged = asyncio.Event()

    async def handler(message: StreamMessage[_Payload]) -> None:
        if message.payload.value == "m2":
            wedged.set()
            await asyncio.sleep(30)  # outlasts any grace; the loop must be cancelled out of it

        completed.append(message.payload.value)

    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)

    for index in range(6):
        await producer.append(_TOPIC, _Payload(value=f"m{index}"), key="k")

    step = _step(handler=handler)

    async with runtime.scope():
        ctx = runtime.get_context()
        startup = step.startup
        assert isinstance(startup, _CommitStreamConsumerBackgroundStartup)

        await startup(ctx)
        await asyncio.wait_for(wedged.wait(), timeout=5.0)

        clock = asyncio.get_running_loop()
        graceful = await startup.stop(deadline=clock.time() + 0.1)

    committed = sum(nxt for key, nxt in state.commit_stream_offsets.items() if key[1] == "g")

    assert graceful is False, "a loop wedged in a handler must be reported as cancelled"
    assert completed == ["m0", "m1"], "the wedged handler must not have completed"
    assert committed == 2, (
        f"2 messages committed their effects but only {committed} offsets were committed — "
        f"the cancel dropped work that was already done"
    )
