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
