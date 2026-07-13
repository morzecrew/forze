"""Configure logging for the application.

Some code taken from: https://gist.github.com/nymous/f138c7f06062b7c43c060bf03759c29e.
"""

import logging
import sys
from collections.abc import Callable, Sequence
from enum import StrEnum
from typing import Any, Final, Literal, TextIO, TypedDict

import orjson
import structlog
from structlog.types import Processor

from .constants import (
    OTEL_DEFAULT_SPAN_ID_KEY,
    OTEL_DEFAULT_TRACE_ID_KEY,
    LogLevel,
    LogLevelToRank,
    RenderMode,
)
from .excepthook import install_excepthook
from .logger import set_configured_min_rank
from .processors import (
    EventDictSanitizer,
    ExceptionFieldsSanitizer,
    ExceptionInfoFormatter,
    OpenTelemetryContextInjector,
    RedundantKeysDropper,
    SamplingDeduplicator,
    TraceLevelResolver,
)
from .renderers import ForzeConsoleRenderer

# ----------------------- #


class OpenTelemetryConfig(TypedDict, total=False):
    """OpenTelemetry configuration."""

    enable: bool
    """Enable OpenTelemetry context injection."""

    trace_key: str
    """Key to inject the trace id into."""

    span_key: str
    """Key to inject the span id into."""


# ....................... #


def _orjson_serializer(
    obj: Any,
    *,
    default: Callable[[Any], Any] | None = None,
    **_: Any,
) -> str:
    """orjson-backed serializer for :class:`structlog.processors.JSONRenderer`.

    ``orjson.dumps`` returns UTF-8 ``bytes``; we decode to ``str`` because the
    configured :class:`structlog.stdlib.ProcessorFormatter` bridges into stdlib
    logging, which expects text. ``default`` is forwarded from structlog's JSON
    fallback handler. Output is equivalent compact JSON to ``json.dumps`` (no key
    sorting, no extra whitespace).
    """

    return orjson.dumps(obj, default=default).decode("utf-8")


# ....................... #


def build_renderer(
    render_mode: RenderMode,
    custom_console_renderer: structlog.types.Processor | None = None,
) -> structlog.types.Processor:
    if render_mode == "json":
        return structlog.processors.JSONRenderer(serializer=_orjson_serializer)

    elif custom_console_renderer:
        return custom_console_renderer

    else:
        return ForzeConsoleRenderer()


# ....................... #


def build_common_processors(
    render_mode: RenderMode,
    otel_config: OpenTelemetryConfig | None = None,
    *,
    include_exception_stack: bool = True,
) -> list[Processor]:
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
    ]

    otel_config = otel_config or {}

    if otel_config.get("enable", True):
        processors.append(
            OpenTelemetryContextInjector(
                trace_key=otel_config.get("trace_key", OTEL_DEFAULT_TRACE_ID_KEY),
                span_key=otel_config.get("span_key", OTEL_DEFAULT_SPAN_ID_KEY),
            )
        )

    processors.extend(
        [
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            ExceptionInfoFormatter(
                render_mode=render_mode,
                include_exception_stack=include_exception_stack,
            ),
        ]
    )

    return processors


# ....................... #


def _event_sanitizer_processors(
    *,
    sanitize_logs: bool,
    text_scrub: bool,
) -> list[Processor]:
    if not sanitize_logs:
        return []
    return [
        ExceptionFieldsSanitizer(),
        EventDictSanitizer(text_scrub=text_scrub),
    ]


# ....................... #


def build_structlog_processors(level: LogLevel) -> list[Processor]:
    return [
        TraceLevelResolver(configured_level=level),
        structlog.stdlib.PositionalArgumentsFormatter(remove_positional_args=True),
        structlog.stdlib.ExtraAdder(),
    ]


# ....................... #


def build_foreign_formatter(
    render_mode: RenderMode,
    custom_console_renderer: structlog.types.Processor | None = None,
    drop_keys: list[str] | None = None,
    otel_config: OpenTelemetryConfig | None = None,
    *,
    sanitize_logs: bool = True,
    text_scrub: bool = True,
    include_exception_stack: bool = True,
) -> structlog.stdlib.ProcessorFormatter:
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            *build_common_processors(
                render_mode,
                otel_config,
                include_exception_stack=include_exception_stack,
            ),
            *_event_sanitizer_processors(
                sanitize_logs=sanitize_logs,
                text_scrub=text_scrub,
            ),
            RedundantKeysDropper(keys=drop_keys or []),
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            build_renderer(render_mode, custom_console_renderer=custom_console_renderer),
        ],
    )


# ....................... #

_LoggerNames = Sequence[str | StrEnum | type[StrEnum]]


def _cast_logger_names(x: _LoggerNames) -> list[str]:
    output: list[str] = []

    for item in x:
        if isinstance(item, StrEnum):
            output.append(item.value)

        elif isinstance(item, type) and issubclass(item, StrEnum):  # pyright: ignore[reportUnnecessaryIsInstance]
            output.extend(list(map(str, item)))

        else:
            output.append(str(item))

    return output


def configure_logging(
    *,
    level: LogLevel = "info",
    render_mode: Literal["console", "json"] = "console",
    custom_console_renderer: structlog.types.Processor | None = None,
    logger_names: _LoggerNames | None = None,
    stream: TextIO = sys.stdout,
    otel_config: OpenTelemetryConfig | None = None,
    sanitize_logs: bool = True,
    text_scrub: bool = True,
    include_exception_stack: bool = True,
    enable_sampling: bool = True,
    dedup_window: float = 60.0,
) -> None:
    """Configure logging for the application.

    :param level: The logging level to use: "notset", "trace", "debug", "info", "warning", "error", "critical".
        Trace is opt-in: until ``configure_logging`` runs, the :meth:`~forze.base.logging.Logger.trace`
        gate defaults to the INFO rank, so unconfigured processes drop trace events
        entirely; pass ``level="trace"`` to emit them.
    :param render_mode: The render mode to use: "console", "json".
    :param custom_console_renderer: A custom console renderer to use for the console mode.
    :param logger_names: The logger names to attach handlers to (an explicit allowlist).
        When omitted or empty, the **root** logger is configured so no logger's output is
        silently dropped; pass a list only to deliberately narrow capture to those names.
    :param stream: The stream to use for logging (default: stdout).
    :param sanitize_logs: Scrub sensitive keys (and optionally text PII) from log event fields.
    :param text_scrub: Apply scrub to string values in log extras when ``sanitize_logs`` is true.
    :param include_exception_stack: When false, omit ``error.stack`` from structured logs.
    :param enable_sampling: Honor per-event ``_sample`` / ``_dedup_key`` controls to collapse
        high-volume events; a no-op for events that carry neither.
    :param dedup_window: Default dedup window in seconds for ``_dedup_key`` events.
    """

    set_configured_min_rank(level)

    wrapper_class = (
        structlog.make_filtering_bound_logger(level)
        if level != "trace"
        else structlog.make_filtering_bound_logger("debug")
    )

    sampling: list[Processor] = (
        [SamplingDeduplicator(default_window=dedup_window)] if enable_sampling else []
    )

    structlog.configure(
        processors=[
            *build_common_processors(
                render_mode,
                otel_config,
                include_exception_stack=include_exception_stack,
            ),
            *build_structlog_processors(level),
            *sampling,
            *_event_sanitizer_processors(
                sanitize_logs=sanitize_logs,
                text_scrub=text_scrub,
            ),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=wrapper_class,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            *build_common_processors(
                render_mode,
                otel_config,
                include_exception_stack=include_exception_stack,
            ),
            *_event_sanitizer_processors(
                sanitize_logs=sanitize_logs,
                text_scrub=text_scrub,
            ),
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            build_renderer(
                render_mode,
                custom_console_renderer=custom_console_renderer,
            ),
        ],
    )

    # With no explicit allowlist, configure the *root* logger so every logger in the
    # process is captured. Attaching handlers only to a caller-supplied list (the else
    # branch) silently drops INFO from any logger not on it — Python's WARNING-level
    # last-resort handler swallows them — so a caller who omits the list (or forgets a
    # name) would lose logs invisibly. An explicit list is still honored verbatim.
    names = _cast_logger_names(logger_names) if logger_names else [""]

    for name in names:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(LogLevelToRank.get(level, 0))
        logger.propagate = False

        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


# ....................... #


def attach_foreign_loggers(
    names: _LoggerNames,
    *,
    level: LogLevel = "info",
    render_mode: RenderMode = "console",
    custom_console_renderer: structlog.types.Processor | None = None,
    stream: TextIO = sys.stdout,
    replace_handlers: bool = True,
    propagate: bool = False,
    otel_config: OpenTelemetryConfig | None = None,
    sanitize_logs: bool = True,
    text_scrub: bool = True,
    include_exception_stack: bool = True,
) -> None:
    formatter = build_foreign_formatter(
        render_mode,
        custom_console_renderer=custom_console_renderer,
        otel_config=otel_config,
        sanitize_logs=sanitize_logs,
        text_scrub=text_scrub,
        include_exception_stack=include_exception_stack,
    )

    for name in _cast_logger_names(names):
        logger = logging.getLogger(name)

        if replace_handlers:
            logger.handlers.clear()

        logger.setLevel(LogLevelToRank.get(level, 0))
        logger.propagate = propagate

        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


# ....................... #

# The framework's own logger roots. Configuring a parent (e.g. ``forze.integrations``)
# captures its dynamic children (``forze.integrations.cache``, ``forze_kits.outbox``, …)
# through the stdlib logger hierarchy, so app authors need not enumerate every child.
CORE_LOGGER_NAMES: Final[tuple[str, ...]] = ("forze", "forze_kits")


def bootstrap_logging(
    *,
    level: LogLevel = "info",
    render_mode: RenderMode = "console",
    custom_console_renderer: structlog.types.Processor | None = None,
    logger_names: _LoggerNames = (),
    third_party: _LoggerNames = (),
    stream: TextIO = sys.stdout,
    otel_config: OpenTelemetryConfig | None = None,
    sanitize_logs: bool = True,
    text_scrub: bool = True,
    include_exception_stack: bool = True,
    enable_sampling: bool = True,
    dedup_window: float = 60.0,
    install_uncaught: bool = True,
) -> None:
    """One-call logging setup: framework loggers + integrations + foreign + excepthook.

    Wraps the three steps an app otherwise wires by hand — :func:`configure_logging`
    over the framework's own logger roots (plus any integration ``FORZE_*_LOGGER_NAMES``
    passed in ``logger_names``), :func:`attach_foreign_loggers` for third-party stdlib
    loggers (uvicorn, sqlalchemy, …), and :func:`install_excepthook`.

    :param logger_names: Integration logger names/enums to configure alongside the core,
        e.g. ``[FORZE_POSTGRES_LOGGER_NAMES, FORZE_REDIS_LOGGER_NAMES]``.
    :param third_party: Foreign stdlib logger names to route through the Forze formatter.
    :param install_uncaught: Install the ``forze.uncaught`` excepthook for unhandled errors.

    See :func:`configure_logging` for the shared rendering/sanitizing parameters.
    """

    names: list[str | StrEnum | type[StrEnum]] = [*CORE_LOGGER_NAMES, *logger_names]

    configure_logging(
        level=level,
        render_mode=render_mode,
        custom_console_renderer=custom_console_renderer,
        logger_names=names,
        stream=stream,
        otel_config=otel_config,
        sanitize_logs=sanitize_logs,
        text_scrub=text_scrub,
        include_exception_stack=include_exception_stack,
        enable_sampling=enable_sampling,
        dedup_window=dedup_window,
    )

    if third_party:
        attach_foreign_loggers(
            third_party,
            level=level,
            render_mode=render_mode,
            custom_console_renderer=custom_console_renderer,
            stream=stream,
            otel_config=otel_config,
            sanitize_logs=sanitize_logs,
            text_scrub=text_scrub,
            include_exception_stack=include_exception_stack,
        )

    if install_uncaught:
        install_excepthook()
