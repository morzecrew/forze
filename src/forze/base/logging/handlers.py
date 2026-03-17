"""Handler setup for single and dual output (pretty + JSON)."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from .config import DEBUG, LoggingConfig
from .processors import (
    add_forze_context,
    filter_by_level,
    maybe_rich_exc_info,
    resolve_forze_level,
)
from .renderers import ConsoleRenderer, build_json_renderer


def _shared_processors() -> list[Any]:
    """Processors shared by structlog and ProcessorFormatter foreign_pre_chain."""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        resolve_forze_level,
        structlog.processors.TimeStamper(fmt="iso"),
        maybe_rich_exc_info,
        structlog.processors.format_exc_info,
        add_forze_context,
        filter_by_level,
    ]


def _build_single_chain(config: LoggingConfig) -> list[Any]:
    """Processor chain for single output (PrintLoggerFactory)."""
    chain = _shared_processors()
    if config.render_json:
        chain.append(build_json_renderer())
    else:
        chain.append(
            ConsoleRenderer(
                step=config.step,
                width=config.width,
                colorize=config.colorize,
            )
        )
    return chain


def _build_dual_handlers(config: LoggingConfig) -> tuple[logging.Handler, ...]:
    """Create stderr (pretty) and stdout (JSON) handlers for dual output."""
    shared = _shared_processors()

    # Pretty: stderr, colorized
    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            ConsoleRenderer(  # type: ignore[list-item]
                step=config.step,
                width=config.width,
                colorize=config.colorize,
            ),
        ],
    )
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(console_formatter)

    # JSON: stdout
    json_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            build_json_renderer(),
        ],
    )
    json_handler = logging.StreamHandler(sys.stdout)
    json_handler.setFormatter(json_formatter)

    return console_handler, json_handler


def configure_logging(config: LoggingConfig) -> None:
    """Configure stdlib logging and structlog for the given config."""
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    if config.dual_output:
        # Dual: structlog -> stdlib -> two handlers
        structlog.configure(
            processors=_shared_processors()
            + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
            wrapper_class=structlog.make_filtering_bound_logger(DEBUG),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        console_handler, json_handler = _build_dual_handlers(config)
        root.addHandler(console_handler)
        root.addHandler(json_handler)
    else:
        # Single: structlog -> PrintLoggerFactory (direct to stderr)
        structlog.configure(
            processors=_build_single_chain(config),
            wrapper_class=structlog.make_filtering_bound_logger(DEBUG),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )
