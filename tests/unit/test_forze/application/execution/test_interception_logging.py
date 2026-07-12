"""Unit tests for the production port LoggingInterceptor."""

import io
import json

import pytest

from forze._logging import ForzeLogger
from forze.application.execution.interception import LoggingInterceptor
from forze.application.execution.interception.protocol import PortCall
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger, configure_logging
from tests.support.logging import reset_forze_stdlib_loggers


@pytest.fixture(autouse=True)
def _reset_logging():
    yield
    # configure_logging leaves handlers + propagate=False on forze* stdlib loggers;
    # clear them so state does not leak into later tests.
    reset_forze_stdlib_loggers()


def _records(stream: io.StringIO) -> list[dict]:
    return [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if line.strip().startswith("{")
    ]


def _configure(stream: io.StringIO, level: str = "trace") -> None:
    configure_logging(
        level=level, render_mode="json", logger_names=[ForzeLogger], stream=stream
    )


def _call() -> PortCall:
    return PortCall(
        surface="document_command", route="orders", op="create", args=(), kwargs={}
    )


async def _ok(_call: PortCall) -> str:
    return "RESULT"


async def _core(_call: PortCall):
    raise exc.not_found("nope")


async def _boom(_call: PortCall):
    raise RuntimeError("kaboom")


class TestLoggingInterceptor:
    async def test_success_logs_at_trace_under_domain_logger(self) -> None:
        stream = io.StringIO()
        _configure(stream)

        result = await LoggingInterceptor().around(_call(), _ok)

        assert result == "RESULT"
        (record,) = _records(stream)
        assert record["event"] == "port call"
        assert record["level"] == "trace"
        assert record["logger"] == "forze.integrations.document"
        assert record["surface"] == "document_command"
        assert record["op"] == "create"

    async def test_success_is_silent_at_info(self) -> None:
        stream = io.StringIO()
        _configure(stream, level="info")

        await LoggingInterceptor().around(_call(), _ok)

        assert _records(stream) == []

    async def test_core_exception_logs_at_debug_and_reraises(self) -> None:
        stream = io.StringIO()
        _configure(stream, level="debug")

        with pytest.raises(CoreException):
            await LoggingInterceptor().around(_call(), _core)

        (record,) = _records(stream)
        assert record["event"] == "port call failed"
        assert record["level"] == "debug"
        assert record["error_kind"] == "not_found"

    async def test_unexpected_exception_logs_at_warning_with_stack(self) -> None:
        stream = io.StringIO()
        _configure(stream, level="info")

        with pytest.raises(RuntimeError):
            await LoggingInterceptor().around(_call(), _boom)

        (record,) = _records(stream)
        assert record["event"] == "port call raised"
        assert record["level"] == "warning"
        assert record["error.type"] == "RuntimeError"

    async def test_logger_override_wins(self) -> None:
        stream = io.StringIO()
        configure_logging(
            level="trace",
            render_mode="json",
            logger_names=["forze_postgres.adapters"],
            stream=stream,
        )

        await LoggingInterceptor(logger=Logger("forze_postgres.adapters")).around(
            _call(), _ok
        )

        (record,) = _records(stream)
        assert record["logger"] == "forze_postgres.adapters"


async def _ok_stream(_call: PortCall):
    yield 1
    yield 2


async def _core_stream(_call: PortCall):
    yield 1
    raise exc.not_found("nope")


async def _boom_stream(_call: PortCall):
    yield 1
    raise RuntimeError("kaboom")


class TestLoggingInterceptorStream:
    """``around_stream`` times the whole stream and logs its item count once — success at
    ``trace``, a mid-stream ``CoreException`` at ``debug``, an unexpected raise at ``warning``."""

    async def test_stream_success_logs_once_at_trace_with_item_count(self) -> None:
        stream = io.StringIO()
        _configure(stream)

        got = [i async for i in LoggingInterceptor().around_stream(_call(), _ok_stream)]

        assert got == [1, 2]
        (record,) = _records(stream)
        assert record["event"] == "port stream"
        assert record["level"] == "trace"
        assert record["items"] == 2

    async def test_stream_is_silent_at_info(self) -> None:
        stream = io.StringIO()
        _configure(stream, level="info")

        got = [i async for i in LoggingInterceptor().around_stream(_call(), _ok_stream)]

        assert got == [1, 2]
        assert _records(stream) == []

    async def test_stream_core_exception_logs_at_debug_with_items_and_reraises(
        self,
    ) -> None:
        stream = io.StringIO()
        _configure(stream, level="debug")

        got: list[int] = []
        with pytest.raises(CoreException):
            async for i in LoggingInterceptor().around_stream(_call(), _core_stream):
                got.append(i)

        assert got == [1]  # one item delivered before the mid-stream failure
        (record,) = _records(stream)
        assert record["event"] == "port stream failed"
        assert record["level"] == "debug"
        assert record["items"] == 1
        assert record["error_kind"] == "not_found"

    async def test_stream_unexpected_exception_logs_at_warning_and_reraises(
        self,
    ) -> None:
        stream = io.StringIO()
        _configure(stream, level="info")

        with pytest.raises(RuntimeError):
            async for _ in LoggingInterceptor().around_stream(_call(), _boom_stream):
                pass

        (record,) = _records(stream)
        assert record["event"] == "port stream raised"
        assert record["level"] == "warning"

    async def test_stream_consumer_aclose_closes_inner_and_logs_nothing(self) -> None:
        # Closing the logged stream closes the inner stream at that moment (before the
        # consumer's scope exits, not at GC), and an abandoned stream is not an error —
        # nothing is logged.
        stream = io.StringIO()
        _configure(stream)
        events: list[str] = []

        async def _cursor(_call: PortCall):
            try:
                yield 1
                yield 2
            finally:
                events.append("inner:closed")

        agen = LoggingInterceptor().around_stream(_call(), _cursor)
        assert await anext(agen) == 1
        await agen.aclose()
        events.append("scope:exit")

        assert events == ["inner:closed", "scope:exit"]
        assert _records(stream) == []
