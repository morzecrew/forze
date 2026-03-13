"""Unit tests for :mod:`forze.base.logging`.

Run with ``pytest -s tests/unit/test_forze/base/test_logging*.py`` to preview
logging output (sections, levels, formatting) in the terminal.
"""

import pytest

from forze.base.logging import configure, getLogger, log_section, reset, setup_default

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

    def test_log_emits_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="DEBUG", colorize=False)
        log = getLogger("forze.test")
        log.info("hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.err
        assert "INFO" in captured.err
        assert "forze" in captured.err

    def test_debug_filtered_at_info_level(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="INFO", colorize=False)
        log = getLogger("forze.test")
        log.debug("should not appear")
        captured = capsys.readouterr()
        assert "should not appear" not in captured.err

    def test_debug_emitted_at_debug_level(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="DEBUG", colorize=False)
        log = getLogger("forze.test")
        log.debug("debug message")
        captured = capsys.readouterr()
        assert "debug message" in captured.err
        assert "DEBUG" in captured.err


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


# ----------------------- #
# Sections (indentation)


class TestLogSections:
    """Tests for :func:`log_section` and indentation."""

    def test_section_increases_indentation(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
        log = getLogger("forze.test")
        log.info("before section")
        with log_section():
            log.info("inside section")
        log.info("after section")
        captured = capsys.readouterr()
        lines = captured.err.strip().split("\n")
        assert len(lines) >= 3
        # Inside section should have extra indentation (step)
        inside_idx = next(i for i, l in enumerate(lines) if "inside section" in l)
        assert "  " in lines[inside_idx] or lines[inside_idx].strip().startswith("  ")

    def test_nested_sections_stack_indentation(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
        log = getLogger("forze.test")
        with log_section():
            log.info("depth 1")
            with log_section():
                log.info("depth 2")
        captured = capsys.readouterr()
        assert "depth 1" in captured.err
        assert "depth 2" in captured.err

    def test_logger_section_context_manager(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(level="INFO", step="  ", prefixes=("forze",), colorize=False)
        log = getLogger("forze.test")
        with log.section():
            log.info("via logger.section()")
        captured = capsys.readouterr()
        assert "via logger.section()" in captured.err


# ----------------------- #
# Formatting (name truncation, root aliases)


class TestFormatting:
    """Tests for logger name formatting and display."""

    def test_keep_sections_truncates_name(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(
            level="INFO",
            keep_sections={"forze": 2},
            colorize=False,
        )
        log = getLogger("forze.application.execution.usecase")
        log.info("truncated")
        captured = capsys.readouterr()
        # Should show forze.application, not full path
        assert "forze.application" in captured.err
        assert "usecase" not in captured.err

    def test_root_aliases_replace_prefix(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure(
            level="INFO",
            root_aliases={"forze": "fz"},
            colorize=False,
        )
        log = getLogger("forze.application")
        log.info("aliased")
        captured = capsys.readouterr()
        assert "fz" in captured.err


# ----------------------- #
# isEnabledFor


class TestIsEnabledFor:
    """Tests for :meth:`Logger.isEnabledFor`."""

    def test_is_enabled_for_respects_level(self) -> None:
        configure(level="INFO")
        log = getLogger("forze.test")
        assert log.isEnabledFor("INFO") is True
        assert log.isEnabledFor("DEBUG") is False
        assert log.isEnabledFor("WARNING") is True

    def test_is_enabled_for_respects_namespace_level(self) -> None:
        configure(level="WARNING", levels={"forze.application": "DEBUG"})
        app_log = getLogger("forze.application")
        base_log = getLogger("forze.base")
        assert app_log.isEnabledFor("DEBUG") is True
        assert base_log.isEnabledFor("DEBUG") is False


# ----------------------- #
# setup_default


class TestSetupDefault:
    """Tests for :func:`setup_default`."""

    def test_setup_default_configures_sink(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_default()
        log = getLogger("forze.test")
        log.info("after setup_default")
        captured = capsys.readouterr()
        assert "after setup_default" in captured.err
