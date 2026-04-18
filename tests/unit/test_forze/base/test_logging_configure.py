"""Focused unit tests for logging configuration helpers."""

import io
import json
import logging

import pytest
import structlog

from forze.base.logging.configure import (
    OpenTelemetryConfig,
    attach_foreign_loggers,
    build_common_processors,
    build_foreign_formatter,
    build_renderer,
    configure_logging,
)
from forze.base.logging.logger import Logger
from forze.base.logging.processors import (
    ExceptionInfoFormatter,
    OpenTelemetryContextInjector,
    RedundantKeysDropper,
)
from forze.base.logging.renderers import ForzeConsoleRenderer


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    yield
    structlog.reset_defaults()
    for name in ("foreign.test", "foreign.keep", "forze.test", "forze.other"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True


def _json_records(stream: io.StringIO) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in stream.getvalue().splitlines():
        if line.strip().startswith("{"):
            rows.append(json.loads(line))
    return rows


class TestBuildRenderer:
    def test_json_mode_uses_json_renderer_even_with_custom(self) -> None:
        custom = object()

        renderer = build_renderer("json", custom_console_renderer=custom)

        assert type(renderer).__name__ == "JSONRenderer"

    def test_console_mode_uses_custom_renderer(self) -> None:
        custom = ForzeConsoleRenderer(colors=False)

        renderer = build_renderer("console", custom_console_renderer=custom)

        assert renderer is custom

    def test_console_mode_defaults_to_forze_console_renderer(self) -> None:
        renderer = build_renderer("console")

        assert isinstance(renderer, ForzeConsoleRenderer)


class TestBuildCommonProcessors:
    def test_default_includes_otel_and_exception_formatter(self) -> None:
        processors = build_common_processors("json")

        assert any(isinstance(p, OpenTelemetryContextInjector) for p in processors)
        assert any(isinstance(p, ExceptionInfoFormatter) for p in processors)

    def test_can_disable_otel_injection(self) -> None:
        processors = build_common_processors("json", otel_config={"enable": False})

        assert not any(isinstance(p, OpenTelemetryContextInjector) for p in processors)

    def test_custom_otel_keys_are_applied(self) -> None:
        config: OpenTelemetryConfig = {
            "enable": True,
            "trace_key": "trace_custom",
            "span_key": "span_custom",
        }

        processors = build_common_processors("json", otel_config=config)
        injector = next(p for p in processors if isinstance(p, OpenTelemetryContextInjector))

        assert injector.trace_key == "trace_custom"
        assert injector.span_key == "span_custom"


class TestForeignLoggerAttachment:
    def test_build_foreign_formatter_includes_dropper(self) -> None:
        formatter = build_foreign_formatter("json", drop_keys=["secret", "skip"])

        dropper = next(
            p
            for p in formatter.foreign_pre_chain
            if isinstance(p, RedundantKeysDropper)
        )

        assert dropper.keys == ["secret", "skip"]

    def test_attach_foreign_loggers_replace_handlers_true(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("foreign.test")
        logger.addHandler(logging.NullHandler())

        attach_foreign_loggers(
            ["foreign.test"],
            level="debug",
            render_mode="json",
            stream=stream,
            replace_handlers=True,
            propagate=False,
        )

        assert len(logger.handlers) == 1
        assert logger.propagate is False
        logger.info("hello", extra={"request_id": "x-1"})

        rows = _json_records(stream)
        assert rows[-1]["event"] == "hello"

    def test_attach_foreign_loggers_replace_handlers_false_keeps_existing(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("foreign.keep")
        logger.addHandler(logging.NullHandler())

        attach_foreign_loggers(
            ["foreign.keep"],
            level="info",
            render_mode="json",
            stream=stream,
            replace_handlers=False,
            propagate=True,
        )

        assert len(logger.handlers) == 2
        assert logger.propagate is True

    def test_foreign_formatter_drops_requested_keys(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("foreign.test")
        logger.handlers.clear()
        logger.propagate = False

        handler = logging.StreamHandler(stream)
        handler.setFormatter(build_foreign_formatter("json", drop_keys=["secret"]))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        logger.info("foreign-message", extra={"keep": 1, "secret": "token"})

        rows = _json_records(stream)
        assert rows[-1]["event"] == "foreign-message"
        assert "secret" not in rows[-1]


class TestConfigureLogging:
    def test_configure_logging_clears_existing_handlers_for_targets(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("forze.test")
        logger.addHandler(logging.NullHandler())

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        assert len(logger.handlers) == 1
        logger.info("configured")
        assert _json_records(stream)[-1]["event"] == "configured"

    def test_configure_logging_applies_level_rank_even_when_unknown(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info", logger_names=["forze.other"], render_mode="json", stream=stream
        )

        logger = logging.getLogger("forze.other")
        assert logger.level == logging.INFO

    def test_configure_logging_json_mode_serializes_exception_fields(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        try:
            raise RuntimeError("explode")
        except RuntimeError:
            logger.exception("failed")

        rows = _json_records(stream)
        row = rows[-1]
        assert row["event"] == "failed"
        assert row["error.type"] == "RuntimeError"
        assert row["error.message"] == "explode"
