"""Thin Logger wrapper around structlog.

Strict API: ``log.info("User {user_id} logged in", sub={"user_id": 123}, request_id="x")``.
- sub: substitution for {key} placeholders; only keys in both message and sub are substituted.
- kwargs: all go to extras.
"""

import re
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Self

import attrs
import structlog.typing

from .config import LogLevel, effective_level_for_name, level_no, normalize_level
from .context import log_section

# ----------------------- #

# Keys that must not be passed as user extras (structlog reserved)
_STRUCTLOG_KEYS = frozenset({"event", "logger", "scope", "source"})

# ....................... #


def _format_event(  #! TODO: extract class names if objects are passed
    message: str,
    sub: Mapping[str, Any],
    **kwargs: Any,
) -> tuple[str, dict[str, Any]]:
    """Format message with {key} placeholders, return (event, extras) for structlog.

    sub: substitute only when message has {key} and sub provides key; otherwise skip.
    Values are str()'d for substitution.
    kwargs: all go to extras.
    """

    if "{" in message and "}" in message and sub:

        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            return str(sub[key]) if key in sub else match.group(0)

        event = re.sub(r"\{(\w+)\}", replace, message)

    else:
        event = message

    extras = {k: v for k, v in kwargs.items() if k not in _STRUCTLOG_KEYS}

    return event, extras


# ....................... #
#! TODO: outline difference between methods within the class


@attrs.define(slots=True, frozen=True, kw_only=True)
class Logger:
    """Logger bound to a name, scope, and optional source."""

    name: str
    """Logger name."""

    backend: structlog.typing.FilteringBoundLogger
    """Structlog backend."""

    # ....................... #

    def isEnabledFor(self, level: LogLevel) -> bool:
        want = level_no(normalize_level(level))
        have = level_no(effective_level_for_name(self.name))
        return want >= have

    # ....................... #

    def bind(self, **kwargs: Any) -> Self:
        return attrs.evolve(self, backend=self.backend.bind(**kwargs))

    # ....................... #

    @contextmanager
    def section(self) -> Iterator[None]:
        with log_section():
            yield

    # ....................... #

    def trace(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Log at TRACE level (below DEBUG). Use for noisy per-op details."""

        self._log("debug", message, sub, {**kwargs, "_forze_level": "trace"})

    # ....................... #

    def debug(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log("debug", message, sub, kwargs)

    # ....................... #

    def info(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log("info", message, sub, kwargs)

    # ....................... #

    def warning(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log("warning", message, sub, kwargs)

    warn = warning

    # ....................... #

    def error(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log("error", message, sub, kwargs)

    # ....................... #

    def critical(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log("critical", message, sub, kwargs)

    # ....................... #

    def critical_exception(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        exc: BaseException | None = None,
        **kwargs: Any,
    ) -> None:
        """Log unhandled exception at CRITICAL with full traceback (Rich when colorize)."""
        event, extras = _format_event(message, sub or {}, **kwargs)
        exc_info: bool | tuple[type[BaseException], BaseException, object] = (
            (type(exc), exc, exc.__traceback__) if exc is not None else True
        )
        self.backend.critical(event, exc_info=exc_info, **extras)

    # ....................... #

    def exception(
        self,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        event, extras = _format_event(message, sub or {}, **kwargs)
        self.backend.error(event, exc_info=True, **extras)

    # ....................... #

    def log(
        self,
        level: LogLevel,
        message: str,
        sub: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        method = normalize_level(level).lower()
        self._log(method, message, sub, kwargs)

    # ....................... #

    def _log(
        self,
        method: str,
        message: str,
        sub: Mapping[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> None:
        event, extras = _format_event(message, sub or {}, **kwargs)
        getattr(self.backend, method)(event, **extras)
