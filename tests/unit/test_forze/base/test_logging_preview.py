"""Preview tests for the logging strategy.

Run with ``pytest -s tests/unit/test_forze/base/test_logging_preview.py`` to see
formatted output in the terminal. These tests exercise sections, per-namespace
levels, and formatting options.
"""

import pytest

from forze.base.logging import configure, getLogger, reset

# ----------------------- #
# Fixtures


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    """Reset logging after each test."""
    yield
    reset()


# ----------------------- #
# Preview: default config with sections


def test_preview_default_with_sections() -> None:
    """Preview: default INFO level, nested sections, forze prefix indentation.

    Run: ``pytest -s tests/unit/test_forze/base/test_logging_preview.py::test_preview_default_with_sections``
    """
    configure(level="INFO", colorize=False)
    log = getLogger("forze.application.execution")
    log.info("Starting execution plan")
    with log.section():
        log.info("Loading registry")
        with log.section():
            log.debug("Registry lookup (filtered at INFO)")
            log.info("Registry loaded")
        log.info("Executing steps")
        with log.section():
            log.info("Step 1: validate")
            log.info("Step 2: execute")
    log.info("Execution complete")


# ----------------------- #
# Preview: DEBUG level with sections


def test_preview_debug_with_sections() -> None:
    """Preview: DEBUG level shows nested debug messages.

    Run: ``pytest -s ...::test_preview_debug_with_sections``
    """
    configure(level="DEBUG", colorize=False)
    log = getLogger("forze.application.execution")
    log.info("Plan started")
    with log.section():
        log.debug("Entering section")
        log.info("Processing")
        with log.section():
            log.debug("Nested debug visible")
        log.debug("Exiting section")
    log.info("Plan finished")


# ----------------------- #
# Preview: per-namespace levels


def test_preview_per_namespace_levels() -> None:
    """Preview: different levels per namespace (app=DEBUG, base=WARNING).

    Run: ``pytest -s ...::test_preview_per_namespace_levels``
    """
    configure(
        level="WARNING",
        levels={
            "forze.application": "DEBUG",
            "forze.domain": "INFO",
        },
        colorize=False,
    )
    app_log = getLogger("forze.application.execution")
    domain_log = getLogger("forze.domain.validation")
    base_log = getLogger("forze.base.utils")
    app_log.debug("App debug (visible)")
    app_log.info("App info (visible)")
    domain_log.info("Domain info (visible)")
    domain_log.debug("Domain debug (filtered)")
    base_log.warning("Base warning (visible)")
    base_log.info("Base info (filtered)")


# ----------------------- #
# Preview: name truncation and root aliases


def test_preview_formatting_options() -> None:
    """Preview: keep_sections truncation and root_aliases.

    Run: ``pytest -s ...::test_preview_formatting_options``
    """
    configure(
        level="INFO",
        keep_sections={"forze": 2},
        root_aliases={"forze": "fz"},
        colorize=False,
    )
    log = getLogger("forze.application.execution.usecase")
    log.info("Logger name shown as fz.application (truncated + aliased)")


# ----------------------- #
# Preview: full workflow simulation


def test_preview_full_workflow() -> None:
    """Preview: realistic workflow with sections and mixed levels.

    Run: ``pytest -s ...::test_preview_full_workflow``
    """
    configure(
        level="INFO",
        levels={"forze.application": "DEBUG"},
        colorize=False,
    )
    exec_log = getLogger("forze.application.execution")
    val_log = getLogger("forze.domain.validation")
    exec_log.info("Execution plan started")
    with exec_log.section():
        exec_log.debug("Loading adapters")
        exec_log.info("Adapters ready")
        with exec_log.section():
            exec_log.debug("Validating input")
            val_log.info("Validation passed")
            exec_log.info("Executing use case")
    exec_log.info("Execution plan completed")
