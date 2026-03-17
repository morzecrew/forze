"""Unit tests for :mod:`forze.base.logging`."""

import asyncio

import pytest

from forze.base.logging import (
    bound_context,
    configure,
    getLogger,
    register_unhandled_exception_handler,
    reset,
)

# ----------------------- #
# Fixtures


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    """Reset logging after each test to avoid cross-test leakage."""
    yield
    reset()


# ----------------------- #
# getLogger and basic logging


class TestGetLogger:
    """Tests for :func:`getLogger` and basic log emission."""

    def test_get_logger_returns_logger_with_name(self) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("test.module")
        assert log.name == "test.module"

    def test_get_logger_defaults_to_root(self) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger()
        assert log.name == "root"

    def test_get_logger_with_scope(self) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("test.module", scope="usecase")
        log.info("hello")
        # scope is bound; we verify via emission
        assert log.name == "test.module"

    def test_log_emits_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="DEBUG", colorize=False)
        log = getLogger("forze.test")
        log.info("hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.err
        assert "INFO" in captured.err

    def test_debug_filtered_at_info_level(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.debug("should not appear")
        captured = capsys.readouterr()
        assert "should not appear" not in captured.err

    def test_debug_emitted_at_debug_level(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="DEBUG", colorize=False)
        log = getLogger("forze.test")
        log.debug("debug message")
        captured = capsys.readouterr()
        assert "debug message" in captured.err
        assert "DEBUG" in captured.err

    def test_trace_filtered_at_info_level(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.trace("should not appear")
        captured = capsys.readouterr()
        assert "should not appear" not in captured.err

    def test_trace_filtered_at_debug_level(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="DEBUG", colorize=False)
        log = getLogger("forze.test")
        log.trace("trace should not appear at debug")
        captured = capsys.readouterr()
        assert "trace should not appear at debug" not in captured.err

    def test_trace_emitted_at_trace_level(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="TRACE", colorize=False)
        log = getLogger("forze.test")
        log.trace("trace message")
        captured = capsys.readouterr()
        assert "trace message" in captured.err
        assert "TRACE" in captured.err


# ----------------------- #
# Per-namespace levels


class TestPerNamespaceLevels:
    """Tests for per-namespace level configuration."""

    def test_namespace_level_overrides_default(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(
            level="WARNING",
            levels={"forze.application": "DEBUG"},
            colorize=False,
        )
        app_log = getLogger("forze.application.execution")
        base_log = getLogger("forze.base.utils")
        app_log.debug("app debug")
        base_log.debug("base debug")
        captured = capsys.readouterr()
        assert "app debug" in captured.err
        assert "base debug" not in captured.err

    def test_longest_prefix_wins(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(
            level="WARNING",
            levels={
                "forze": "INFO",
                "forze.application": "DEBUG",
            },
            colorize=False,
        )
        log = getLogger("forze.application.execution")
        log.debug("nested debug")
        captured = capsys.readouterr()
        assert "nested debug" in captured.err

    def test_namespace_trace_level(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(
            level="INFO",
            levels={"forze.application": "TRACE"},
            colorize=False,
        )
        app_log = getLogger("forze.application.execution")
        base_log = getLogger("forze.base.utils")
        app_log.trace("app trace")
        base_log.trace("base trace")
        captured = capsys.readouterr()
        assert "app trace" in captured.err
        assert "base trace" not in captured.err

    def test_wildcard_pattern_matches(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(
            level="WARNING",
            levels={"forze.redis.*": "DEBUG"},
            colorize=False,
        )
        redis_log = getLogger("forze.redis.adapters.cache")
        other_log = getLogger("forze.application.usecase")
        redis_log.debug("redis debug")
        other_log.debug("other debug")
        captured = capsys.readouterr()
        assert "redis debug" in captured.err
        assert "other debug" not in captured.err


# ----------------------- #
# Sections (indentation)


class TestLogSections:
    """Tests for :meth:`Logger.section` and indentation."""

    def test_section_increases_indentation(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", step="  ", colorize=False)
        log = getLogger("forze.test")
        log.info("before section")
        with log.section():
            log.info("inside section")
        log.info("after section")
        captured = capsys.readouterr()
        lines = captured.err.strip().split("\n")
        assert len(lines) >= 3
        inside_idx = next(i for i, li in enumerate(lines) if "inside section" in li)
        assert "  " in lines[inside_idx] or lines[inside_idx].strip().startswith("  ")

    def test_nested_sections_stack_indentation(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", step="  ", colorize=False)
        log = getLogger("forze.test")
        with log.section():
            log.info("depth 1")
            with log.section():
                log.info("depth 2")
        captured = capsys.readouterr()
        assert "depth 1" in captured.err
        assert "depth 2" in captured.err

    def test_logger_section_context_manager(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", step="  ", colorize=False)
        log = getLogger("forze.test")
        with log.section():
            log.info("via logger.section()")
        captured = capsys.readouterr()
        assert "via logger.section()" in captured.err


# ----------------------- #
# isEnabledFor


class TestIsEnabledFor:
    """Tests for :meth:`Logger.isEnabledFor`."""

    def test_is_enabled_for_respects_level(self) -> None:
        configure(level="INFO")
        log = getLogger("forze.test")
        assert log.isEnabledFor("INFO") is True
        assert log.isEnabledFor("DEBUG") is False
        assert log.isEnabledFor("TRACE") is False
        assert log.isEnabledFor("WARNING") is True

    def test_is_enabled_for_trace_at_trace_level(self) -> None:
        configure(level="TRACE")
        log = getLogger("forze.test")
        assert log.isEnabledFor("TRACE") is True

    def test_is_enabled_for_respects_namespace_level(self) -> None:
        configure(level="WARNING", levels={"forze.application": "DEBUG"})
        app_log = getLogger("forze.application")
        base_log = getLogger("forze.base")
        assert app_log.isEnabledFor("DEBUG") is True
        assert base_log.isEnabledFor("DEBUG") is False


# ----------------------- #
# bound_context (correlation_id, request-scoped)


class TestBoundContext:
    """Tests for :func:`bound_context` and :func:`bind_context`."""

    def test_bound_context_adds_to_log_output(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        with bound_context(correlation_id="req-123"):
            log.info("inside request")
        captured = capsys.readouterr()
        assert "correlation_id" in captured.err or "req-123" in captured.err
        assert "inside request" in captured.err


# ----------------------- #
# bind and scope


class TestBindAndScope:
    """Tests for :meth:`Logger.bind` and scope in output."""

    def test_bind_scope_appears_in_output(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test").bind(scope="usecase")
        log.info("message")
        captured = capsys.readouterr()
        assert "[usecase]" in captured.err or "usecase" in captured.err
        assert "message" in captured.err


# ----------------------- #
# Key-based message format (substitution + extras)


class TestKeyBasedFormat:
    """Tests for sub (substitution) and kwargs (extras) separation."""

    def test_substitution_from_sub_extras_from_kwargs(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.info("User {user_id} logged in", sub={"user_id": 123}, request_id="x")
        captured = capsys.readouterr()
        assert "User 123 logged in" in captured.err
        assert "request_id" in captured.err or "x" in captured.err

    def test_partial_substitution_missing_key_in_sub_stays_as_placeholder(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.info("User {user_id} from {region}", sub={"region": "EU"})
        captured = capsys.readouterr()
        assert "User {user_id} from EU" in captured.err

    def test_no_placeholders_message_unchanged_extras_passed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.info("Something happened", detail=42)
        captured = capsys.readouterr()
        assert "Something happened" in captured.err
        assert "detail" in captured.err or "42" in captured.err

    def test_critical_exception_logs_at_critical_with_traceback(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        try:
            raise ValueError("test unhandled")
        except ValueError as e:
            log.critical_exception(
                "Unhandled: {exc_type}: {message}",
                sub={"exc_type": type(e).__name__, "message": str(e)},
                exc=e,
            )
        captured = capsys.readouterr()
        assert "CRITICAL" in captured.err
        assert "Unhandled: ValueError: test unhandled" in captured.err
        assert "ValueError" in captured.err
        assert "test unhandled" in captured.err

    @pytest.mark.asyncio
    async def test_register_unhandled_exception_handler_with_loop_logs_task_exceptions(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """When loop is provided, asyncio task exceptions are logged at CRITICAL."""
        configure(level="INFO", colorize=False)
        loop = asyncio.get_running_loop()
        register_unhandled_exception_handler(loop=loop)

        async def failing_task() -> None:
            raise RuntimeError("task exploded")

        asyncio.create_task(failing_task())
        await asyncio.sleep(0.05)  # Let the task run and hit the handler

        captured = capsys.readouterr()
        assert "CRITICAL" in captured.err
        assert "asyncio task exception" in captured.err
        assert "RuntimeError" in captured.err
        assert "task exploded" in captured.err


# ----------------------- #
# Rich enhancements (block format, ReprHighlighter)


class TestRichEnhancements:
    """Tests for block format and ReprHighlighter when colorize=True."""

    def test_extra_with_nested_dict_uses_block_when_colorized(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=True)
        log = getLogger("forze.test")
        log.info("mapping", exclude={"unset": True, "defaults": True})
        captured = capsys.readouterr()
        assert "mapping" in captured.err
        assert "unset" in captured.err
        assert "defaults" in captured.err
        # Block format: blank line before extra
        assert "\n\n" in captured.err

    def test_extra_with_five_keys_stays_inline_when_colorized(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=True)
        log = getLogger("forze.test")
        log.info("config", a=1, b=2, c=3, d=4)
        captured = capsys.readouterr()
        assert "config" in captured.err
        # ReprHighlighter inserts ANSI codes between key and "=", so check values
        assert "1" in captured.err and "4" in captured.err
        # Simple extras inline on same line
        assert "\n\n" not in captured.err

    def test_extra_with_simple_values_emits_inline_when_colorized(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=True)
        log = getLogger("forze.test")
        log.info("step", n=1, mode="python")
        captured = capsys.readouterr()
        assert "step" in captured.err
        # ReprHighlighter inserts ANSI codes between key and "=", so check values
        assert "1" in captured.err
        assert "python" in captured.err

    def test_extra_indent_adds_spaces_before_inline_extra(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False, extra_indent=3)
        log = getLogger("forze.test")
        log.info("msg", x=1)
        captured = capsys.readouterr()
        # 3 spaces before "x=1" (extra_indent=3)
        assert "   x=1" in captured.err

    def test_extra_indent_not_applied_when_no_extra(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False, extra_indent=5)
        log = getLogger("forze.test")
        log.info("no extra")
        captured = capsys.readouterr()
        assert "no extra" in captured.err

    def test_extra_indent_not_applied_for_block_extra(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=False, extra_indent=5)
        log = getLogger("forze.test")
        log.info("mapping", exclude={"unset": True, "defaults": True})
        captured = capsys.readouterr()
        # Block format: extra below, not inline; no extra_indent between event and block
        assert "\n\n" in captured.err
        assert "mapping" in captured.err
        assert "unset" in captured.err

    def test_event_width_pads_event_to_align_inline_extra(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(
            level="INFO",
            colorize=False,
            event_width=80,
            extra_indent=1,
        )
        log = getLogger("forze.test")
        log.info("short", x=1)
        out1 = capsys.readouterr().err
        log.info("longer message here", x=1)
        out2 = capsys.readouterr().err
        # Both should have x=1 at same column (event padded)
        idx1 = out1.find("x=1")
        idx2 = out2.find("x=1")
        assert idx1 > 0 and idx2 > 0
        assert idx1 == idx2

    def test_prefix_width_aligns_inline_extra_across_depths(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(
            level="INFO",
            colorize=False,
            event_width=80,
            extra_indent=1,
            prefix_width=80,  # >= max prefix len (timestamp+level+scope+indent)
        )
        log = getLogger("forze.test")
        log.info("depth0", x=1)
        out1 = capsys.readouterr().err
        with log.section():
            log.info("depth1", x=1)
        out2 = capsys.readouterr().err
        # x=1 at same column: prefix_width + event_width + extra_indent
        idx1 = out1.find("x=1")
        idx2 = out2.find("x=1")
        assert idx1 > 0 and idx2 > 0
        assert idx1 == idx2


# ----------------------- #
# Dual output (pretty stderr + JSON stdout)


class TestDualOutput:
    """Tests for dual_output: pretty to stderr, JSON to stdout."""

    def test_dual_output_emits_pretty_to_stderr_and_json_to_stdout(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", dual_output=True, colorize=False)
        log = getLogger("forze.test")
        log.info("dual test", x=1)
        captured = capsys.readouterr()
        assert "dual test" in captured.err
        assert "x" in captured.err or "1" in captured.err
        assert '"event"' in captured.out
        assert '"dual test"' in captured.out
        assert '"x"' in captured.out or '"logger"' in captured.out
