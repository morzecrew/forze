"""Core implementation: config, processors, renderers, facade, context.

Consolidated to avoid unnecessary file fragmentation.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from typing import TYPE_CHECKING, Any, Iterator, Literal, Mapping, Optional, cast

if TYPE_CHECKING:
    from .logger import Logger

import structlog
from rich.console import Console
from rich.traceback import Traceback

from forze.base.logging.helpers import render_message, safe_preview

# ----------------------- #
# Constants and types

LogLevelName = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LogLevel = LogLevelName | int
LevelsMap = Optional[Mapping[str, LogLevelName]]

TRACE = 5
DEBUG = 10
INFO = 20
WARNING = 30
ERROR = 40
CRITICAL = 50

LEVEL_TO_NO: dict[str, int] = {
    "TRACE": TRACE,
    "DEBUG": DEBUG,
    "INFO": INFO,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}
NO_TO_LEVEL: dict[int, str] = {v: k for k, v in LEVEL_TO_NO.items()}

DEFAULT_LEVEL: LogLevelName = "INFO"
DEFAULT_PREFIXES: tuple[str, ...] = ("forze",)
DEFAULT_STEP = "  "
DEFAULT_WIDTH = 36

# ----------------------- #
# Config


@dataclass(slots=True, frozen=True)
class LoggingConfig:
    """Immutable logging configuration."""

    level: LogLevelName = DEFAULT_LEVEL
    levels: LevelsMap = None
    prefixes: tuple[str, ...] = DEFAULT_PREFIXES
    step: str = DEFAULT_STEP
    width: int = DEFAULT_WIDTH
    colorize: bool = False
    render_json: bool = False


_config: Optional[LoggingConfig] = None


def get_config() -> LoggingConfig:
    return _config or LoggingConfig()


def set_config(config: LoggingConfig) -> None:
    global _config
    _config = config


# ----------------------- #
# Helpers


def normalize_level(level: str | int) -> LogLevelName:
    if isinstance(level, int):
        if level not in NO_TO_LEVEL:
            raise ValueError(f"Unknown log level number: {level}")
        return cast(LogLevelName, NO_TO_LEVEL[level])
    upper = level.upper()
    if upper not in LEVEL_TO_NO:
        raise ValueError(f"Unknown log level name: {level}")
    return cast(LogLevelName, upper)


def level_no(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return LEVEL_TO_NO.get(level.upper(), 20)  # TRACE maps to 5, below DEBUG


def matches_namespace(name: str, prefixes: tuple[str, ...]) -> bool:
    return any(
        name == p or name.startswith(f"{p}.") or name.startswith(f"{p}_")
        for p in prefixes
    )


def effective_level_for_name(name: str) -> LogLevelName:
    config = get_config()
    if not config.levels:
        return config.level
    matched_prefix: str | None = None
    matched_level: str | None = None
    for prefix, level in config.levels.items():
        if (
            name == prefix
            or name.startswith(f"{prefix}.")
            or name.startswith(f"{prefix}_")
        ):
            if matched_prefix is None or len(prefix) > len(matched_prefix):
                matched_prefix = prefix
                matched_level = level

    return cast(LogLevelName, matched_level or config.level)  # type: ignore[redundant-cast]


# ----------------------- #
# Context (depth, log_section)

_log_depth: ContextVar[int] = ContextVar("forze_log_v2_depth", default=0)


def get_depth() -> int:
    return _log_depth.get()


@contextmanager
def log_section() -> Iterator[None]:
    """Increase indentation depth inside a logical logging section."""
    depth = _log_depth.get()
    token = _log_depth.set(depth + 1)
    try:
        yield
    finally:
        _log_depth.reset(token)


# ----------------------- #
# Correlation ID / request-scoped context
# structlog.contextvars provides bind_contextvars, bound_contextvars, clear_contextvars


def bind_context(**kwargs: Any) -> Any:
    """Bind key-value pairs to the context-local context (e.g. correlation_id).

    Use at the start of a request (e.g. FastAPI middleware) so all loggers
    in that request get the context. Returns tokens for reset_contextvars if needed.
    """
    return structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear the context-local context. Call at the start of each request."""
    structlog.contextvars.clear_contextvars()


@contextmanager
def bound_context(**kwargs: Any) -> Iterator[None]:
    """Context manager to bind key-value pairs for the duration of the block.

    Use for request-scoped context (correlation_id, request_id, etc.).
    """
    with structlog.contextvars.bound_contextvars(**kwargs):
        yield


# ----------------------- #
# Processors


_FORZE_LEVEL_KEY = "_forze_level"


def _resolve_forze_level(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Override structlog level when Logger.trace() passes _forze_level.

    structlog has no native TRACE level; trace() emits via debug() and this
    processor sets level to trace so _filter_by_level and renderers handle it.
    """
    del logger, method_name
    override = event_dict.pop(_FORZE_LEVEL_KEY, None)
    if override is not None:
        event_dict["level"] = override
    return event_dict


def _maybe_rich_exc_info(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Format exception with Rich when colorize; else let format_exc_info handle it."""
    del logger, method_name
    exc_info = event_dict.get("exc_info")
    if exc_info is None or not get_config().colorize:
        return event_dict
    if exc_info is True:
        exc_info = sys.exc_info()
    if exc_info and exc_info[0] is not None:
        exc_type, exc_value, exc_tb = exc_info
        if exc_type is not None and exc_value is not None:
            tb = Traceback.from_exception(exc_type, exc_value, exc_tb)
            buf = StringIO()
            Console(file=buf, force_terminal=True, color_system="auto").print(tb)
            event_dict["exception"] = buf.getvalue()
            event_dict.pop("exc_info", None)
    return event_dict


def _add_forze_context(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    del method_name
    event_dict["depth"] = get_depth()
    if "scope" not in event_dict or event_dict["scope"] is None:
        event_dict["scope"] = "root"
    return event_dict


def _filter_by_level(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    del method_name
    config = get_config()
    name = event_dict.get("logger", "root")
    levels = config.levels
    if levels:
        matched_prefix: str | None = None
        matched_level: str | None = None
        for prefix, level in levels.items():
            if (
                name == prefix
                or name.startswith(f"{prefix}.")
                or name.startswith(f"{prefix}_")
            ):
                if matched_prefix is None or len(prefix) > len(matched_prefix):
                    matched_prefix = prefix
                    matched_level = level
        effective = matched_level or config.level
    else:
        effective = config.level
    msg_level = event_dict.get("level", "INFO")
    msg_no = msg_level if isinstance(msg_level, int) else level_no(msg_level)
    if msg_no < level_no(effective):
        raise structlog.DropEvent
    return event_dict


def _indent_for_name(name: str) -> str:
    config = get_config()
    if not matches_namespace(name, config.prefixes):
        return ""
    return config.step * get_depth()


# ----------------------- #
# Renderers


def _level_display(level: Any) -> str:
    if isinstance(level, int):
        return NO_TO_LEVEL.get(level, "INFO")
    return str(level).upper()


def _console_renderer(logger: Any, method_name: str, event_dict: dict[str, Any]) -> str:
    del logger, method_name
    config = get_config()
    name = event_dict.get("logger", "root")
    indent = _indent_for_name(name)
    level = _level_display(event_dict.get("level", "INFO")).ljust(8)
    ts = event_dict.get("timestamp", "")
    time_str = _format_ts(ts) if ts else ""
    event = event_dict.get("event", "")
    scope = event_dict.get("scope", "root")
    scope_str = f"[{scope}]".ljust(config.width)
    standard_keys = {
        "event",
        "level",
        "timestamp",
        "logger",
        "scope",
        "source",
        "depth",
        "exception",
        "exc_info",
    }
    exception_str = event_dict.get("exception", "")
    extra = {
        k: v for k, v in event_dict.items() if k not in standard_keys and v is not None
    }
    extra_str = ""
    if extra:
        extra_str = " " + " ".join(f"{k}={v!r}" for k, v in sorted(extra.items()))
    dim = "\033[2m" if config.colorize else ""
    rst = "\033[0m" if config.colorize else ""
    colors = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    is_trace = level.strip() == "TRACE"
    if is_trace and config.colorize:
        # Dim entire TRACE record so it recedes visually
        line = f"{dim}{time_str}   {level}{scope_str}{indent}{event}{extra_str}{rst}\n"
    else:
        lvl_style = colors.get(level.strip(), "") if config.colorize else ""
        line = f"{dim}{time_str}{rst}   {lvl_style}{level}{rst}{dim}{scope_str}{rst}{indent}{event}{extra_str}"
    if exception_str:
        line += f"\n\n{indent}{exception_str}\n"
    return line


def _format_ts(ts: Any) -> str:
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return str(ts)


def _build_chain(*, json_mode: bool = False) -> list[Any]:
    chain: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        _resolve_forze_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _maybe_rich_exc_info,
        structlog.processors.format_exc_info,
        _add_forze_context,
        _filter_by_level,
    ]
    if json_mode or get_config().render_json:
        chain.append(structlog.processors.JSONRenderer())
    else:
        chain.append(_console_renderer)
    return chain


# ----------------------- #
# Facade


def getLogger(
    name: str | None = None,
    *,
    scope: str | None = None,
    source: str | None = None,
) -> Logger:
    from .logger import Logger

    logger_name = name or "root"
    initial: dict[str, object] = {"logger": logger_name}
    if scope is not None:
        initial["scope"] = scope
    if source is not None:
        initial["source"] = source
    bound = structlog.get_logger(logger_name).bind(**initial)
    return Logger(name=logger_name, backend=bound)


def configure(
    *,
    level: LogLevel = DEFAULT_LEVEL,
    levels: Optional[Mapping[str, LogLevel]] = None,
    prefixes: tuple[str, ...] = DEFAULT_PREFIXES,
    step: str = "  ",
    width: int = 36,
    colorize: bool = False,
    render_json: bool = False,
) -> None:
    normalized_levels: dict[str, LogLevelName] | None = None
    if levels is not None:
        normalized_levels = {p: normalize_level(lv) for p, lv in levels.items()}
    config = LoggingConfig(
        level=normalize_level(level),
        levels=normalized_levels,
        prefixes=prefixes,
        step=step,
        width=width,
        colorize=colorize,
        render_json=render_json,
    )
    set_config(config)
    structlog.configure(
        processors=_build_chain(json_mode=render_json),
        wrapper_class=structlog.make_filtering_bound_logger(DEBUG),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def reset() -> None:
    structlog.reset_defaults()


# Re-exports for external use
__all__ = [
    "LogLevel",
    "LogLevelName",
    "LoggingConfig",
    "bound_context",
    "bind_context",
    "clear_context",
    "configure",
    "effective_level_for_name",
    "getLogger",
    "get_config",
    "level_no",
    "log_section",
    "normalize_level",
    "render_message",
    "reset",
    "safe_preview",
    "set_config",
]
