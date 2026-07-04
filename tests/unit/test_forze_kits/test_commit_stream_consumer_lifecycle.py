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
from forze.base.exceptions import CoreException
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
