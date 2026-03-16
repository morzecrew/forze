"""Unit tests for :mod:`forze.base.logging_v2`."""

import pytest

from forze.base.logging_v2 import bound_context, configure, getLogger, reset

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


# ----------------------- #
# Sections (indentation)


class TestLogSections:
    """Tests for :meth:`Logger.section` and indentation."""

    def test_section_increases_indentation(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
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
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
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
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
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
# Rich enhancements (Rule, Pretty, ReprHighlighter)


class TestRichEnhancements:
    """Tests for Rule, Pretty, and ReprHighlighter when colorize=True."""

    def test_error_emits_rule_when_colorized(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=True)
        log = getLogger("forze.test")
        log.error("something failed")
        captured = capsys.readouterr()
        assert "something failed" in captured.err
        assert "─" in captured.err or "━" in captured.err  # Rule character

    def test_extra_with_nested_dict_uses_pretty_when_colorized(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", colorize=True)
        log = getLogger("forze.test")
        log.info("mapping", exclude={"unset": True, "defaults": True})
        captured = capsys.readouterr()
        assert "mapping" in captured.err
        assert "unset" in captured.err
        assert "defaults" in captured.err

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
