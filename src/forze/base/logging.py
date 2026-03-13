"""Project logging facade built on top of loguru.

Provides a stdlib-like API surface for internal use while using loguru under
the hood. Supports indentation-aware nested debug logs via :func:`log_section`
and namespace-based level configuration.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Iterator, Literal, Mapping, cast

if TYPE_CHECKING:
    from loguru import Record

else:
    Record = dict[str, Any]

from loguru import logger as _base_logger
from loguru._logger import Logger as _LoguruLogger

# ----------------------- #

LogLevelName = Literal[
    "TRACE",
    "DEBUG",
    "INFO",
    "SUCCESS",
    "WARNING",
    "ERROR",
    "CRITICAL",
]
LogLevel = LogLevelName | int

TRACE: Final[int] = 5
DEBUG: Final[int] = 10
INFO: Final[int] = 20
SUCCESS: Final[int] = 25
WARNING: Final[int] = 30
ERROR: Final[int] = 40
CRITICAL: Final[int] = 50

_LEVEL_TO_NO: Final[dict[str, int]] = {
    "TRACE": TRACE,
    "DEBUG": DEBUG,
    "INFO": INFO,
    "SUCCESS": SUCCESS,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}

_NO_TO_LEVEL: Final[dict[int, str]] = {
    TRACE: "TRACE",
    DEBUG: "DEBUG",
    INFO: "INFO",
    SUCCESS: "SUCCESS",
    WARNING: "WARNING",
    ERROR: "ERROR",
    CRITICAL: "CRITICAL",
}

_DEFAULT_LEVEL: Final[LogLevelName] = "INFO"
_DEFAULT_PREFIXES: Final[tuple[str, ...]] = ("forze",)

# Register TRACE explicitly in case environment differs.
try:
    _base_logger.level("TRACE")

except ValueError:
    _base_logger.level("TRACE", no=TRACE, color="<cyan>", icon="✏️")

# ....................... #

_log_depth: ContextVar[int] = ContextVar("forze_log_depth", default=0)
"""Current indentation depth for the active execution context."""

# Active sink ids so reconfiguration cleanly replaces previous setup.
_sink_ids: list[int] = []

# ----------------------- #
# Config


@dataclass(slots=True, frozen=True)
class _LoggingConfig:
    level: LogLevelName = _DEFAULT_LEVEL
    levels: Mapping[str, LogLevelName] | None = None
    prefixes: tuple[str, ...] = _DEFAULT_PREFIXES
    step: str = "  "
    shorten_name: bool = True
    strip_prefix: str | None = None
    width: int = 36
    colorize: bool = True


_config: _LoggingConfig = _LoggingConfig()

# ----------------------- #
# Helpers


def _normalize_level(level: LogLevel) -> LogLevelName:
    """Normalize an integer or string level to a canonical level name."""

    if isinstance(level, int):
        try:
            return _NO_TO_LEVEL[level]  # type: ignore[return-value]
        except KeyError as exc:
            raise ValueError(f"Unknown log level number: {level}") from exc

    upper = level.upper()
    if upper not in _LEVEL_TO_NO:
        raise ValueError(f"Unknown log level name: {level}")

    return cast(LogLevelName, upper)


def _level_no(level: LogLevelName) -> int:
    return _LEVEL_TO_NO[level]


def _escape_loguru_braces(text: str) -> str:
    return text.replace("{", "{{").replace("}", "}}")


def _matches_namespace(name: str, prefixes: tuple[str, ...]) -> bool:
    """Return ``True`` if *name* belongs to one of the configured namespaces."""

    return any(
        name == prefix or name.startswith(f"{prefix}.") or name.startswith(f"{prefix}_")
        for prefix in prefixes
    )


def _normalize_name(
    name: str,
    *,
    shorten_name: bool,
    strip_prefix: str | None,
) -> str:
    """Normalize a logger name for rendering."""

    if strip_prefix:
        if name == strip_prefix:
            name = strip_prefix
        elif name.startswith(f"{strip_prefix}."):
            name = name[len(strip_prefix) + 1 :]

    if shorten_name and "." in name:
        name = name.rsplit(".", 1)[0]

    return name


def _effective_level_for_name(name: str) -> LogLevelName:
    """Return the effective configured level for a logger name."""

    levels = _config.levels
    if not levels:
        return _config.level

    matched_prefix: str | None = None
    matched_level: LogLevelName | None = None

    for prefix, level in levels.items():
        if (
            name == prefix
            or name.startswith(f"{prefix}.")
            or name.startswith(f"{prefix}_")
        ):
            if matched_prefix is None or len(prefix) > len(matched_prefix):
                matched_prefix = prefix
                matched_level = level

    return matched_level or _config.level


def _indent_for_name(name: str) -> str:
    """Return indentation prefix for a logger name in the current context."""

    if not _matches_namespace(name, _config.prefixes):
        return ""

    return _config.step * _log_depth.get()


def _render_message(message: Any, args: tuple[Any, ...]) -> str:
    """Render a stdlib-style `%` log message safely."""

    text = str(message)

    if not args:
        return text

    try:
        return text % args
    except Exception:
        rendered_args = ", ".join(repr(a) for a in args)
        return f"{text} | args=({rendered_args})"


def _record_name(record: Record) -> str:
    return cast(str, record["extra"].get("logger_name") or record["name"])


def _record_filter(record: Record) -> bool:
    name = _record_name(record)
    effective = _effective_level_for_name(name)

    return record["level"].no >= _level_no(effective)


def _record_format(record: Record) -> str:
    name = _record_name(record)
    shortname = _normalize_name(
        name,
        shorten_name=_config.shorten_name,
        strip_prefix=_config.strip_prefix,
    )
    indent = _indent_for_name(name)
    level = f"{record['level'].name:<8}"
    time = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    message = record["message"]

    return (
        f"<dim>{time}</dim> "
        f"<level>{level}</level> "
        f"<dim>{shortname:<{_config.width}}</dim> "
        f"{indent}{message}\n"
    )


# ----------------------- #
# Public API


@contextmanager
def log_section() -> Iterator[None]:
    """Increase indentation depth inside a logical logging section."""

    depth = _log_depth.get()
    token = _log_depth.set(depth + 1)

    try:
        yield
    finally:
        _log_depth.reset(token)


class Logger:
    """Small stdlib-like wrapper around a loguru bound logger."""

    __slots__ = ("_logger", "_name")

    def __init__(self, logger: _LoguruLogger, name: str) -> None:
        self._logger = logger
        self._name = name

    # ....................... #

    @property
    def name(self) -> str:
        return self._name

    # ....................... #

    def isEnabledFor(self, level: LogLevel) -> bool:
        """Return whether a message at *level* would be emitted."""

        want = _level_no(_normalize_level(level))
        have = _level_no(_effective_level_for_name(self._name))

        return want >= have

    # ....................... #

    def bind(self, **kwargs: Any) -> Logger:
        """Return a logger with additional bound context."""

        return Logger(self._logger.bind(**kwargs), self._name)  # type: ignore[no-untyped-call]

    # ....................... #

    def trace(self, message: Any, *args: Any) -> None:
        self._log("TRACE", message, *args)

    def debug(self, message: Any, *args: Any) -> None:
        self._log("DEBUG", message, *args)

    def info(self, message: Any, *args: Any) -> None:
        self._log("INFO", message, *args)

    def success(self, message: Any, *args: Any) -> None:
        self._log("SUCCESS", message, *args)

    def warning(self, message: Any, *args: Any) -> None:
        self._log("WARNING", message, *args)

    warn = warning

    def error(self, message: Any, *args: Any) -> None:
        self._log("ERROR", message, *args)

    def critical(self, message: Any, *args: Any) -> None:
        self._log("CRITICAL", message, *args)

    def exception(self, message: Any, *args: Any) -> None:
        rendered = _render_message(message, args)
        escaped = _escape_loguru_braces(rendered)
        self._logger.opt(exception=True).error(escaped)  # type: ignore[no-untyped-call]

    def log(self, level: LogLevel, message: Any, *args: Any) -> None:
        self._log(_normalize_level(level), message, *args)

    # ....................... #

    def _log(self, level: LogLevelName, message: Any, *args: Any) -> None:
        rendered = _render_message(message, args)
        escaped = _escape_loguru_braces(rendered)
        self._logger.log(level, escaped)  # type: ignore[no-untyped-call]


def getLogger(name: str | None = None) -> Logger:
    """Return a stdlib-like logger bound to *name*."""

    logger_name = name or "root"
    return Logger(_base_logger.bind(logger_name=logger_name), logger_name)  # type: ignore[arg-type]


def configure(
    *,
    level: LogLevel = _DEFAULT_LEVEL,
    levels: Mapping[str, LogLevel] | None = None,
    prefixes: tuple[str, ...] = _DEFAULT_PREFIXES,
    step: str = "  ",
    shorten_name: bool = True,
    strip_prefix: str | None = None,
    width: int = 36,
    colorize: bool = False,
) -> None:
    """Configure global logging.

    :param level: Default fallback level.
    :param levels: Optional per-prefix levels, e.g.
        ``{"forze.application": "DEBUG", "forze.base.serialization": "TRACE"}``.
    :param prefixes: Namespaces that should receive indentation.
    :param step: Indentation unit.
    :param shorten_name: Drop the last logger-name segment when rendering.
    :param strip_prefix: Optional prefix stripped from rendered logger names.
    :param width: Width of the rendered logger-name column.
    :param colorize: Whether to allow color output.
    """

    global _config, _sink_ids

    normalized_levels: dict[str, LogLevelName] | None = None

    if levels is not None:
        normalized_levels = {
            prefix: _normalize_level(prefix_level)
            for prefix, prefix_level in levels.items()
        }

    _config = _LoggingConfig(
        level=_normalize_level(level),
        levels=normalized_levels,
        prefixes=prefixes,
        step=step,
        shorten_name=shorten_name,
        strip_prefix=strip_prefix,
        width=width,
        colorize=colorize,
    )

    _base_logger.remove()
    _sink_ids = []

    # for sink_id in _sink_ids:
    #     try:
    #         _base_logger.remove(sink_id)
    #     except ValueError:
    #         pass

    _sink_ids = [
        _base_logger.add(
            sys.stderr,
            level="TRACE",
            format=_record_format,
            filter=_record_filter,
            colorize=colorize,
            backtrace=False,
            diagnose=False,
            enqueue=False,
        )
    ]


def reset() -> None:
    """Reset logging sinks configured through this facade."""

    global _sink_ids

    for sink_id in _sink_ids:
        try:
            _base_logger.remove(sink_id)
        except ValueError:
            pass

    _sink_ids = []


# Configure a sane default sink on first import.
configure()
