"""Unit tests for :mod:`forze.base.logging`."""

import io
import json
import logging

import pytest
import structlog
from structlog.contextvars import bound_contextvars

from forze.base.logging import Logger, configure_logging
from forze.base.logging.renderers import ForzeConsoleRenderer

# ----------------------- #
# Helpers


def _cleanup_logging() -> None:
    structlog.reset_defaults()
    for name in (
        "forze.test",
        "test.module",
        "root",
        "forze.application.execution",
        "forze.base.utils",
        "forze.application",
        "forze.base",
        "forze.redis.adapters.cache",
        "forze.application.usecase",
        "forze_postgres.kernel",
    ):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True


def _json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


# ----------------------- #
# Fixtures


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    """Reset logging after each test to avoid cross-test leakage."""
    yield
    _cleanup_logging()


# ----------------------- #
# Logger and basic logging


class TestLogger:
    """Tests for :class:`Logger` and basic log emission."""

    def test_logger_has_name(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["test.module"], stream=buf, render_mode="json"
        )
        log = Logger("test.module")
        assert log.name == "test.module"

    def test_logger_named_root(self) -> None:
        buf = io.StringIO()
        configure_logging(level="info", logger_names=["root"], stream=buf, render_mode="json")
        log = Logger("root")
        assert log.name == "root"

    def test_logger_with_bound_scope(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["test.module"], stream=buf, render_mode="json"
        )
        log = Logger("test.module").bind(scope="usecase")
        log.info("hello")
        records = _json_records(buf)
        assert records[-1]["event"] == "hello"
        assert records[-1].get("scope") == "usecase"

    def test_log_emits_to_stream(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="debug", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("hello world")
        records = _json_records(buf)
        assert records[-1]["event"] == "hello world"
        assert records[-1]["level"] == "info"

    def test_debug_filtered_at_info_level(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.debug("should not appear")
        assert _json_records(buf) == []

    def test_debug_emitted_at_debug_level(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="debug", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.debug("debug message")
        records = _json_records(buf)
        assert records[-1]["event"] == "debug message"
        assert records[-1]["level"] == "debug"

    def test_trace_filtered_at_info_level(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.trace("should not appear")
        assert _json_records(buf) == []

    def test_trace_filtered_at_debug_level(self) -> None:
        """Trace is sent through the debug API but ranked below debug; it is dropped here."""
        buf = io.StringIO()
        configure_logging(
            level="debug", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.trace("should not appear")
        assert _json_records(buf) == []

    def test_trace_emitted_at_trace_level(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="trace", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.trace("trace message")
        records = _json_records(buf)
        assert records[-1]["event"] == "trace message"
        assert records[-1]["level"] == "trace"


# ----------------------- #
# Context vars (structlog)


class TestBoundContext:
    """Correlation / request context via structlog contextvars."""

    def test_bound_contextvars_adds_to_log_output(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        with bound_contextvars(correlation_id="req-123"):
            log.info("inside request")
        records = _json_records(buf)
        assert records[-1]["event"] == "inside request"
        assert records[-1].get("correlation_id") == "req-123"


# ----------------------- #
# bind and scope


class TestBindAndScope:
    """Tests for :meth:`Logger.bind` and fields in output."""

    def test_bind_scope_appears_in_output(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test").bind(scope="usecase")
        log.info("message")
        records = _json_records(buf)
        assert records[-1]["scope"] == "usecase"
        assert records[-1]["event"] == "message"


# ----------------------- #
# Event string and kwargs (structlog extras)


class TestLogExtras:
    """Tests for structured fields attached to log events."""

    def test_kwargs_appear_as_json_fields(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("User {user_id} logged in", user_id=123, request_id="x")
        records = _json_records(buf)
        row = records[-1]
        assert row["event"] == "User {user_id} logged in"
        assert row["user_id"] == 123
        assert row["request_id"] == "x"

    def test_partial_keys_only_bound_fields_in_output(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("User {user_id} from {region}", region="EU")
        records = _json_records(buf)
        row = records[-1]
        assert row["event"] == "User {user_id} from {region}"
        assert row["region"] == "EU"

    def test_simple_message_with_extra(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("Something happened", detail=42)
        records = _json_records(buf)
        row = records[-1]
        assert row["event"] == "Something happened"
        assert row["detail"] == 42

    def test_critical_exception_logs_with_error_fields(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        try:
            raise ValueError("test unhandled")
        except ValueError as e:
            log.critical_exception(
                "Unhandled failure",
                exc_type=type(e).__name__,
                message=str(e),
                exc=e,
            )
        records = _json_records(buf)
        row = records[-1]
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled failure"
        assert row["error.type"] == "ValueError"
        assert row["error.message"] == "test unhandled"
        assert "ValueError" in row["error.stack"]


# ----------------------- #
# Nested values (JSON render)


class TestJsonRender:
    """Structured values in JSON log output."""

    def test_nested_dict_extra_is_json_serializable(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("mapping", exclude={"unset": True, "defaults": True})
        records = _json_records(buf)
        row = records[-1]
        assert row["event"] == "mapping"
        assert row["exclude"] == {"unset": True, "defaults": True}

    def test_multiple_primitive_extras(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("config", a=1, b=2, c=3, d=4)
        records = _json_records(buf)
        row = records[-1]
        assert row["a"] == 1 and row["d"] == 4

    def test_simple_string_and_int_extras(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info", logger_names=["forze.test"], stream=buf, render_mode="json"
        )
        log = Logger("forze.test")
        log.info("step", n=1, mode="python")
        records = _json_records(buf)
        row = records[-1]
        assert row["n"] == 1
        assert row["mode"] == "python"


# ----------------------- #
# Console renderer


class TestForzeConsoleRenderer:
    """Layout and ID shortening for :class:`ForzeConsoleRenderer`."""

    def test_layout_timestamp_level_logger_event_and_extra(self) -> None:
        render = ForzeConsoleRenderer(colors=False)
        line = render(
            None,  # type: ignore[arg-type]
            "info",
            {
                "timestamp": "2026-03-22T12:00:00Z",
                "level": "info",
                "logger": "forze.test",
                "event": "started",
                "detail": 1,
            },
        )
        assert line.startswith(
            "2026-03-22T12:00:00Z  info  [forze.test]  started  |  detail=1"
        )

    def test_shortens_correlation_execution_causation_ids(self) -> None:
        render = ForzeConsoleRenderer(colors=False)
        line = render(
            None,  # type: ignore[arg-type]
            "info",
            {
                "timestamp": "t",
                "level": "info",
                "logger": "x",
                "event": "e",
                "correlation_id": "prefix-ABCDEF",
                "execution_id": "run-uuid-XYZZYX",
                "causation_id": "short",
            },
        )
        assert "corr=ABCDEF" in line
        assert "exec=XYZZYX" in line
        assert "caus=short" in line
        assert "correlation_id" not in line
        assert "execution_id" not in line
        assert "causation_id" not in line

    def test_colors_level_and_extras(self) -> None:
        render = ForzeConsoleRenderer(colors=True)
        line = render(
            None,  # type: ignore[arg-type]
            "info",
            {
                "timestamp": "t",
                "level": "info",
                "logger": "pkg.mod",
                "event": "done",
                "x": 1,
            },
        )
        assert "\033[2mt\033[0m" in line
        assert "\033[32minfo    \033[0m" in line
        assert "\033[2m[pkg.mod]             \033[0m" in line
        assert "\033[1m" in line and "done" in line
        assert "\033[36mx\033[0m=\033[35m1\033[0m" in line

    def test_trace_level_uses_dim_not_green(self) -> None:
        render = ForzeConsoleRenderer(colors=True)
        line = render(
            None,  # type: ignore[arg-type]
            "debug",
            {
                "timestamp": "t",
                "level": "trace",
                "logger": "x",
                "event": "e",
            },
        )
        assert "\033[2mtrace   \033[0m" in line

    def test_normalized_error_fields_plain(self) -> None:
        render = ForzeConsoleRenderer(colors=False)
        stack = "Traceback (most recent call last):\n  File \"t.py\", line 1, in <module>\nValueError: test unhandled"
        line = render(
            None,  # type: ignore[arg-type]
            "critical",
            {
                "timestamp": "t",
                "level": "critical",
                "logger": "forze.test",
                "event": "Unhandled failure",
                "error.type": "ValueError",
                "error.message": "test unhandled",
                "error.stack": stack,
            },
        )
        assert "Unhandled failure" in line
        assert "ValueError: test unhandled" in line
        assert "Traceback (most recent call last)" in line
        assert "error.type" not in line

    def test_normalized_error_fields_rich(self) -> None:
        render = ForzeConsoleRenderer(colors=True)
        stack = "Traceback (most recent call last):\n  File \"t.py\", line 1, in <module>\nValueError: x\n"
        line = render(
            None,  # type: ignore[arg-type]
            "error",
            {
                "timestamp": "t",
                "level": "error",
                "logger": "x",
                "event": "boom",
                "error.type": "RuntimeError",
                "error.message": "oops",
                "error.stack": stack,
            },
        )
        assert "RuntimeError: oops" in line
        assert "\x1b" in line
        assert "Traceback" in line

    def test_configure_console_uses_renderer(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info",
            logger_names=["forze.test"],
            stream=buf,
            render_mode="console",
            custom_console_renderer=ForzeConsoleRenderer(colors=False),
        )
        log = Logger("forze.test")
        log.info("hello", foo="bar")
        out = buf.getvalue().strip()
        assert "[forze.test]" in out
        assert "hello" in out
        assert "foo=bar" in out

    def test_configure_console_critical_exception_uses_normalized_error(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info",
            logger_names=["forze.test"],
            stream=buf,
            render_mode="console",
            custom_console_renderer=ForzeConsoleRenderer(colors=False),
        )
        log = Logger("forze.test")
        try:
            raise ValueError("pipe")
        except ValueError as e:
            log.critical_exception("fail", exc=e)
        out = buf.getvalue()
        assert "fail" in out
        assert "ValueError: pipe" in out
        assert "Traceback" in out
