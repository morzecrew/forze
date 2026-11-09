"""The invocation surface reaches every log line of an operation (RFC 0079 §5.5).

The key is ``invocation_surface``, not bare ``surface``: that is the port-call log key, and an
event's own key wins over a bound one, so both must survive on a port-call line. An operation
run with no metadata bound still logs a surface: ``unknown``.
"""

from __future__ import annotations

import asyncio
import io
import json
from typing import Any
from uuid import uuid4

import attrs
import pytest
from structlog.contextvars import get_contextvars

from forze._logging import ForzeLogger
from forze.application._logger import logger
from forze.application.contracts.execution import Handler
from forze.application.execution import InvocationMetadata, run_traced_operation
from forze.application.execution.context.invocation import SURFACE_KEY
from forze.application.execution.interception import LoggingInterceptor
from forze.application.execution.interception.protocol import PortCall
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.logging import configure_logging
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules
from tests.support.logging import reset_forze_stdlib_loggers

# ----------------------- #


@pytest.fixture(autouse=True)
def _reset_logging():
    yield
    reset_forze_stdlib_loggers()


def _records(stream: io.StringIO) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if line.strip().startswith("{")
    ]


def _configured() -> io.StringIO:
    stream = io.StringIO()
    configure_logging(level="trace", render_mode="json", logger_names=[ForzeLogger], stream=stream)
    return stream


async def _port_result(_call: PortCall) -> str:
    return "ok"


@attrs.define(slots=True)
class _Logs(Handler[None, None]):
    async def __call__(self, _args: None) -> None:
        logger.info("inside the operation")
        call = PortCall(surface="document_command", route="orders", op="create", args=(), kwargs={})
        await LoggingInterceptor().around(call, _port_result)


def _registry() -> Any:
    return OperationRegistry(handlers={"op": lambda _ctx: _Logs()}).freeze()


def _line(records: list[dict[str, Any]], event: str) -> dict[str, Any]:
    (record,) = [r for r in records if r.get("event") == event]
    return record


# ....................... #


async def test_every_line_inside_an_operation_carries_the_bound_surface() -> None:
    stream = _configured()
    ctx = context_from_modules(MockDepsModule())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4(), surface="cli")

    with ctx.inv_ctx.bind_metadata(metadata=metadata):
        await run_operation(_registry(), "op", None, ctx)

    records = _records(stream)
    assert _line(records, "inside the operation")[SURFACE_KEY] == "cli"
    port_call = _line(records, "port call")
    # Both meanings of "surface" survive on one line, under two keys.
    assert port_call["surface"] == "document_command"
    assert port_call[SURFACE_KEY] == "cli"


async def test_an_operation_with_no_metadata_logs_unknown() -> None:
    stream = _configured()
    ctx = context_from_modules(MockDepsModule())

    await run_traced_operation(_registry(), "op", None, ctx)

    records = _records(stream)
    assert _line(records, "inside the operation")[SURFACE_KEY] == "unknown"
    assert _line(records, "port call")[SURFACE_KEY] == "unknown"
    # Bound for the call only.
    assert SURFACE_KEY not in get_contextvars()


# ....................... #


@attrs.define(slots=True)
class _Seen(Handler[None, None]):
    seen: list[dict[str, Any]]
    started: asyncio.Event | None = None

    async def __call__(self, _args: None) -> None:
        self.seen.append(get_contextvars())

        if self.started is None:
            raise RuntimeError("boom")

        self.started.set()
        await asyncio.Event().wait()


async def test_the_unknown_surface_is_unbound_after_a_failed_operation() -> None:
    ctx = context_from_modules(MockDepsModule())
    seen: list[dict[str, Any]] = []
    registry = OperationRegistry(handlers={"op": lambda _ctx: _Seen(seen)}).freeze()

    with pytest.raises(RuntimeError, match="boom"):
        await run_operation(registry, "op", None, ctx)

    assert seen == [{SURFACE_KEY: "unknown"}]
    assert SURFACE_KEY not in get_contextvars()


async def test_the_unknown_surface_is_unbound_after_a_cancelled_operation() -> None:
    ctx = context_from_modules(MockDepsModule())
    seen: list[dict[str, Any]] = []
    started = asyncio.Event()
    registry = OperationRegistry(handlers={"op": lambda _ctx: _Seen(seen, started)}).freeze()

    async def _driver() -> dict[str, Any]:
        # The same task runs the operation and then reads its own logging context.
        try:
            await run_operation(registry, "op", None, ctx)
        except asyncio.CancelledError:
            return get_contextvars()

        raise AssertionError("the operation was not cancelled")

    task = asyncio.create_task(_driver())
    await started.wait()
    task.cancel()

    assert seen == [{SURFACE_KEY: "unknown"}]
    assert SURFACE_KEY not in await task
