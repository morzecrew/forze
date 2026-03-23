"""Configure logging for the application.

Some code taken from: https://gist.github.com/nymous/f138c7f06062b7c43c060bf03759c29e.
"""

import logging
import sys
from typing import Final, Literal, Sequence, TextIO, TypedDict

import structlog
from structlog.types import Processor

from .constants import (
    OTEL_DEFAULT_SPAN_ID_KEY,
    OTEL_DEFAULT_TRACE_ID_KEY,
    LogLevel,
    LogLevelToRank,
    RenderMode,
)
from .processors import (
    ExceptionInfoFormatter,
    OpenTelemetryContextInjector,
    RedundantKeysDropper,
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


def build_renderer(
    render_mode: RenderMode,
    custom_console_renderer: structlog.types.Processor | None = None,
) -> structlog.types.Processor:
    if render_mode == "json":
        return structlog.processors.JSONRenderer()

    elif custom_console_renderer:
        return custom_console_renderer

    else:
        return ForzeConsoleRenderer()


# ....................... #


def build_common_processors(
    render_mode: RenderMode,
    otel_config: OpenTelemetryConfig | None = None,
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
            ExceptionInfoFormatter(render_mode=render_mode),
        ]
    )

    return processors


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
) -> structlog.stdlib.ProcessorFormatter:
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            *build_common_processors(render_mode, otel_config),
            RedundantKeysDropper(keys=drop_keys or []),
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            build_renderer(
                render_mode, custom_console_renderer=custom_console_renderer
            ),
        ],
    )


# ....................... #

DEFAULT_LOGGER_NAMES: Final[tuple[str, ...]] = (
    "forze",
    "forze.uncaught",
    "forze.application",
    "forze.domain",
    "forze.base",
)
"""Default logger names to configure logging for."""


def configure_logging(
    *,
    level: LogLevel = "info",
    render_mode: Literal["console", "json"] = "console",
    custom_console_renderer: structlog.types.Processor | None = None,
    logger_names: Sequence[str] = DEFAULT_LOGGER_NAMES,
    stream: TextIO = sys.stdout,
    otel_config: OpenTelemetryConfig | None = None,
) -> None:
    """Configure logging for the application.

    :param level: The logging level to use: "notset", "trace", "debug", "info", "warning", "error", "critical".
    :param render_mode: The render mode to use: "console", "json".
    :param custom_console_renderer: A custom console renderer to use for the console mode.
    :param logger_names: The logger names to configure logging for.
    :param stream: The stream to use for logging (default: stdout).
    """

    wrapper_class = (
        structlog.make_filtering_bound_logger(level)
        if level != "trace"
        else structlog.make_filtering_bound_logger("debug")
    )

    structlog.configure(
        processors=[
            *build_common_processors(render_mode, otel_config),
            *build_structlog_processors(level),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=wrapper_class,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            build_renderer(
                render_mode,
                custom_console_renderer=custom_console_renderer,
            ),
        ],
    )

    for name in logger_names:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(LogLevelToRank.get(level, 0))
        logger.propagate = False

        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


# ....................... #


def attach_foreign_loggers(
    names: Sequence[str],
    *,
    level: LogLevel = "info",
    render_mode: RenderMode = "console",
    custom_console_renderer: structlog.types.Processor | None = None,
    stream: TextIO = sys.stdout,
    replace_handlers: bool = True,
    propagate: bool = False,
    otel_config: OpenTelemetryConfig | None = None,
) -> None:
    formatter = build_foreign_formatter(
        render_mode,
        custom_console_renderer=custom_console_renderer,
        otel_config=otel_config,
    )

    for name in names:
        logger = logging.getLogger(name)

        if replace_handlers:
            logger.handlers.clear()

        logger.setLevel(LogLevelToRank.get(level, 0))
        logger.propagate = propagate

        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
