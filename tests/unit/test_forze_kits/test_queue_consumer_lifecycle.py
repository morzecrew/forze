"""Unit tests for the background queue-consumer lifecycle step."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import (
    QueueConsumer,
    queue_consumer_background_lifecycle_step,
)
from forze_kits.integrations.consumer.lifecycle import _QueueConsumerBackgroundStartup
from forze_mock import MockDepsModule
from forze_mock.adapters import MockQueueAdapter, MockState

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_QUEUE_SPEC = QueueSpec(name="jobs", codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="events")


async def _noop_handler(message: QueueMessage[Any]) -> None:
    del message


def _step(**overrides: Any):
    kwargs: dict[str, Any] = {
        "queue": "jobs",
        "queue_spec": _QUEUE_SPEC,
        "handler": _noop_handler,
        "inbox_spec": _INBOX_SPEC,
        "tx_route": "mock",
    }
    kwargs.update(overrides)

    return queue_consumer_background_lifecycle_step(**kwargs)


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
    with patch.object(QueueConsumer, "run", run_mock):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            startup = step.startup
            assert isinstance(startup, _QueueConsumerBackgroundStartup)
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
        patch.object(QueueConsumer, "run", AsyncMock(side_effect=_crashy)),
        patch("forze_kits.integrations.consumer.lifecycle.logger", logger_mock),
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
    assert isinstance(startup, _QueueConsumerBackgroundStartup)
    assert startup.task is not None and startup.task.done()


# ....................... #


@pytest.mark.asyncio
async def test_lifecycle_consumes_from_the_real_mock_queue() -> None:
    state = MockState()
    runtime = _runtime(state, strict_tx=True)

    handled = asyncio.Event()
    values: list[str] = []

    async def handler(message: QueueMessage[_Payload]) -> None:
        values.append(message.payload.value)
        handled.set()

    step = _step(handler=handler)

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        adapter = MockQueueAdapter[_Payload](
            state=state,
            namespace="jobs",
            codec=_CODEC,
        )
        await adapter.enqueue("jobs", _Payload(value="bg"), key="evt-bg")

        await asyncio.wait_for(handled.wait(), timeout=2.0)
        await step.shutdown(ctx)

    assert values == ["bg"]
    # Acked by the background loop: nothing in-flight, nothing receivable.
    assert state.queue_pending.get("jobs", {}).get("jobs", {}) == {}


# ....................... #


def test_default_step_id_derives_from_queue() -> None:
    assert _step(queue="orders").id == "queue_consumer:orders"
    assert _step(step_id="my_consumer").id == "my_consumer"


# ....................... #


def test_lifecycle_step_rejects_invalid_options() -> None:
    with pytest.raises(CoreException, match="backoff"):
        _step(restart_backoff=timedelta(0))

    with pytest.raises(CoreException, match="max_deliveries"):
        _step(max_deliveries=0)
