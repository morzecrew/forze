"""Focused unit tests for logging configuration helpers."""

import io
import json
import logging
import warnings
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from forze.base.logging.configure import (
    OpenTelemetryConfig,
    _UnnamedRedactionHandler,
    attach_foreign_loggers,
    bootstrap_logging,
    build_common_processors,
    build_foreign_formatter,
    build_renderer,
    configure_logging,
)
from forze.base.logging.logger import Logger
from forze.base.logging.processors import (
    ExceptionFieldsSanitizer,
    ExceptionInfoFormatter,
    OpenTelemetryContextInjector,
    RedundantKeysDropper,
)
from forze.base.logging.renderers import ForzeConsoleRenderer
from forze.base.scrubbing import SECRET_PLACEHOLDER
from tests.support.logging import reset_forze_stdlib_loggers


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    yield
    reset_forze_stdlib_loggers(
        "foreign.test", "foreign.keep", "app", "vendor.client", "vendor.access", "unlisted.app"
    )


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
    def test_configure_logging_without_names_configures_root(self) -> None:
        """Omitting ``logger_names`` configures the root logger, so a logger the caller
        never listed still reaches the stream instead of being silently dropped."""

        stream = io.StringIO()
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level

        try:
            configure_logging(
                level="info",
                render_mode="json",
                stream=stream,
                logger_names=None,
            )

            assert len(root.handlers) == 1

            logging.getLogger("some.unlisted.logger").info("hello-unlisted")

            assert "hello-unlisted" in stream.getvalue()

        finally:
            root.handlers[:] = saved_handlers
            root.setLevel(saved_level)

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

    def test_configure_logging_scrubs_exception_message_and_stack(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        try:
            raise RuntimeError("password=fixture-7chr")
        except RuntimeError:
            logger.exception("failed")

        row = _json_records(stream)[-1]
        assert SECRET_PLACEHOLDER in row["error.message"]
        assert "fixture-7chr" not in row["error.message"]
        assert SECRET_PLACEHOLDER in row["error.stack"]
        assert "fixture-7chr" not in row["error.stack"]

    def test_configure_logging_omits_stack_when_disabled(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
            include_exception_stack=False,
        )

        logger = Logger("forze.test")
        try:
            raise RuntimeError("explode")
        except RuntimeError:
            logger.exception("failed")

        row = _json_records(stream)[-1]
        assert row["error.type"] == "RuntimeError"
        assert "error.stack" not in row

    def test_configure_logging_sanitize_logs_disabled_keeps_raw_exception(
        self,
    ) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
            sanitize_logs=False,
        )

        logger = Logger("forze.test")
        try:
            raise RuntimeError("password=fixture-7chr")
        except RuntimeError:
            logger.exception("failed")

        row = _json_records(stream)[-1]
        assert row["error.message"] == "password=fixture-7chr"

    def test_event_sanitizer_includes_exception_fields_scrubber(self) -> None:
        from forze.base.logging.configure import _event_sanitizer_processors

        processors = _event_sanitizer_processors(sanitize_logs=True, text_scrub=True)
        assert any(isinstance(p, ExceptionFieldsSanitizer) for p in processors)

    def test_configure_logging_scrubs_sensitive_extras(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        logger.info("login", password="fixture-7chr")

        row = _json_records(stream)[-1]
        assert row["event"] == "login"
        assert row["password"] == "**********"

    def test_configure_logging_scrubs_event_message_text(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        logger.info("authorization header was Bearer abc.def.ghi")

        row = _json_records(stream)[-1]
        assert "abc.def.ghi" not in str(row["event"])
        assert SECRET_PLACEHOLDER in str(row["event"])

    def test_configure_logging_scrubs_dsn_in_event_message(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        logger.info("connecting to postgresql://user:fixture-7chr@db.host/app")

        row = _json_records(stream)[-1]
        assert "fixture-7chr" not in str(row["event"])
        assert SECRET_PLACEHOLDER in str(row["event"])

    def test_configure_logging_scrubs_positional_args_in_event_message(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
        )

        logger = Logger("forze.test")
        logger.info("token=%s", "supersecretvalue")

        row = _json_records(stream)[-1]
        assert "supersecretvalue" not in str(row["event"])
        assert SECRET_PLACEHOLDER in str(row["event"])

    def test_configure_logging_event_message_untouched_when_sanitize_disabled(
        self,
    ) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
            sanitize_logs=False,
        )

        logger = Logger("forze.test")
        logger.info("token=fixture-7chr via Bearer abc.def.ghi")

        row = _json_records(stream)[-1]
        assert row["event"] == "token=fixture-7chr via Bearer abc.def.ghi"

    def test_configure_logging_event_message_untouched_when_text_scrub_disabled(
        self,
    ) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
            text_scrub=False,
        )

        logger = Logger("forze.test")
        logger.info("connecting via Bearer abc.def.ghi", password="fixture-7chr")

        row = _json_records(stream)[-1]
        # Message text is left alone without text scrubbing...
        assert row["event"] == "connecting via Bearer abc.def.ghi"
        # ...while key-based extras masking still applies.
        assert row["password"] == SECRET_PLACEHOLDER

    def test_configure_logging_sanitize_logs_disabled(self) -> None:
        stream = io.StringIO()

        configure_logging(
            level="info",
            render_mode="json",
            logger_names=["forze.test"],
            stream=stream,
            sanitize_logs=False,
        )

        logger = Logger("forze.test")
        logger.info("login", password="fixture-7chr")

        row = _json_records(stream)[-1]
        assert row["password"] == "fixture-7chr"


@contextmanager
def _bare_root() -> Iterator[logging.Logger]:
    """The root as a fresh process has it: no handlers, so Python's last resort is in play.

    pytest keeps capture handlers on the root, which would hide that fallback entirely.
    """

    root = logging.getLogger()
    saved = list(root.handlers)
    root.handlers.clear()

    try:
        yield root

    finally:
        root.handlers[:] = saved


def _redaction_handlers(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if isinstance(h, _UnnamedRedactionHandler)]


class TestRedactUnnamed:
    """Narrowing ``logger_names`` keeps the scrub on loggers outside the list."""

    def test_default_path_scrubs_an_unlisted_foreign_logger(self) -> None:
        stream = io.StringIO()
        root = logging.getLogger()
        saved_handlers, saved_level = list(root.handlers), root.level

        try:
            configure_logging(render_mode="json", stream=stream)
            logging.getLogger("vendor.client").warning("auth Bearer abc.def.ghi")

        finally:
            root.handlers[:] = saved_handlers
            root.setLevel(saved_level)

        row = _json_records(stream)[-1]
        assert "abc.def.ghi" not in str(row["event"])
        assert SECRET_PLACEHOLDER in str(row["event"])

    def test_last_resort_prints_warning_raw_and_drops_info(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """What a narrowed call left before: the configure.py comment's claim, measured."""

        stream = io.StringIO()
        vendor = logging.getLogger("vendor.client")
        vendor.setLevel(logging.INFO)  # past the root's level, so only a handler can drop it

        with _bare_root(), warnings.catch_warnings(record=True):
            configure_logging(
                render_mode="json", logger_names=["app"], stream=stream, redact_unnamed=False
            )
            vendor.info("info-line")
            vendor.warning("auth Bearer abc.def.ghi")

        err = capsys.readouterr().err
        assert "info-line" not in err
        assert "auth Bearer abc.def.ghi" in err
        assert stream.getvalue() == ""

    def test_narrowed_path_scrubs_an_unlisted_logger_message(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        stream = io.StringIO()
        vendor = logging.getLogger("vendor.client")
        vendor.setLevel(logging.INFO)

        with _bare_root():
            configure_logging(render_mode="json", logger_names=["app"], stream=stream)
            vendor.info("info-line")
            vendor.warning("auth Bearer abc.def.ghi, token=%s", "fixture-7chr")

        rows = _json_records(stream)
        assert [row["logger"] for row in rows] == ["vendor.client"]
        assert "abc.def.ghi" not in str(rows[0]["event"])
        assert "fixture-7chr" not in str(rows[0]["event"])
        assert SECRET_PLACEHOLDER in str(rows[0]["event"])
        # Nothing new: INFO stays dropped and the WARNING no longer reaches stderr raw.
        assert "info-line" not in stream.getvalue()
        assert capsys.readouterr().err == ""

    def test_narrowed_path_scrubs_an_unlisted_logger_extras(self) -> None:
        stream = io.StringIO()

        with _bare_root():
            configure_logging(render_mode="json", logger_names=["app"], stream=stream)
            Logger("unlisted.app").warning("login", password="fixture-7chr")

        row = _json_records(stream)[-1]
        assert row["event"] == "login"
        assert row["password"] == SECRET_PLACEHOLDER

    def test_named_logger_record_is_emitted_once(self) -> None:
        stream = io.StringIO()

        with _bare_root():
            configure_logging(render_mode="json", logger_names=["app"], stream=stream)
            logging.getLogger("app").warning("only-once")

        assert [row["event"] for row in _json_records(stream)] == ["only-once"]

    def test_opt_out_warns_once_and_leaves_the_root_alone(self) -> None:
        with _bare_root() as root, warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            configure_logging(logger_names=["app"], stream=io.StringIO(), redact_unnamed=False)
            handlers = list(root.handlers)

        assert handlers == []
        assert len(caught) == 1
        assert "unscrubbed" in str(caught[0].message)

    def test_root_handler_follows_a_higher_level_and_is_replaced(self) -> None:
        with _bare_root() as root:
            configure_logging(logger_names=["app"], stream=io.StringIO())
            configure_logging(level="error", logger_names=["app"], stream=io.StringIO())
            reconfigured = _redaction_handlers(root)

            with warnings.catch_warnings(record=True):
                configure_logging(
                    logger_names=["app"], stream=io.StringIO(), redact_unnamed=False
                )

            opted_out = _redaction_handlers(root)

        assert [h.level for h in reconfigured] == [logging.ERROR]
        assert opted_out == []

    def test_no_second_root_handler_when_root_is_named(self) -> None:
        root = logging.getLogger()
        saved_level = root.level

        with _bare_root():
            configure_logging(logger_names=["root", "app"], stream=io.StringIO())
            handlers = list(root.handlers)

        root.setLevel(saved_level)
        root.propagate = True
        assert len(handlers) == 1
        assert not isinstance(handlers[0], _UnnamedRedactionHandler)

    def test_sanitize_disabled_neither_attaches_nor_warns(self) -> None:
        with _bare_root() as root, warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            configure_logging(
                logger_names=["app"],
                stream=io.StringIO(),
                sanitize_logs=False,
                redact_unnamed=False,
            )
            handlers = list(root.handlers)

        assert handlers == []
        assert caught == []

    def test_non_propagating_logger_is_reached_only_by_naming_it(self) -> None:
        """The ``uvicorn.access`` shape: its own handler and ``propagate=False``."""

        own, stream = io.StringIO(), io.StringIO()
        access = logging.getLogger("vendor.access")
        access.addHandler(logging.StreamHandler(own))
        access.propagate = False

        with _bare_root():
            configure_logging(render_mode="json", logger_names=["app"], stream=stream)
            access.warning("GET /cb?access_token=fixture-7chr")
            attach_foreign_loggers(["vendor.access"], render_mode="json", stream=stream)
            access.warning("GET /cb?access_token=fixture-7chr")

        assert "fixture-7chr" in own.getvalue()
        rows = _json_records(stream)
        assert len(rows) == 1
        assert "fixture-7chr" not in str(rows[0]["event"])

    def test_bootstrap_logging_threads_the_opt_out(self) -> None:
        with _bare_root() as root:
            bootstrap_logging(stream=io.StringIO(), install_uncaught=False)
            attached = _redaction_handlers(root)

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                bootstrap_logging(
                    stream=io.StringIO(), install_uncaught=False, redact_unnamed=False
                )

            declined = _redaction_handlers(root)

        assert len(attached) == 1
        assert declined == []
        assert len(caught) == 1
