"""Stdlib-like Logger wrapper around structlog."""

from contextlib import contextmanager
from typing import Any, Iterator, Self

import attrs
import structlog.typing

from ._core import (
    LogLevel,
    effective_level_for_name,
    level_no,
    log_section,
    normalize_level,
    render_message,
)

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class Logger:
    """Stdlib-like logger bound to a name, scope, and optional source."""

    name: str
    backend: structlog.typing.FilteringBoundLogger

    def isEnabledFor(self, level: LogLevel) -> bool:
        want = level_no(normalize_level(level))
        have = level_no(effective_level_for_name(self.name))
        return want >= have

    def bind(self, **kwargs: Any) -> Self:
        return attrs.evolve(self, backend=self.backend.bind(**kwargs))

    @contextmanager
    def section(self) -> Iterator[None]:
        with log_section():
            yield

    def trace(self, message: Any, *args: Any, **kwargs: Any) -> None:
        """Log at TRACE level (below DEBUG). Use for noisy per-op details."""

        self._log("debug", message, args, {**kwargs, "_forze_level": "trace"})

    def debug(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._log("debug", message, args, kwargs)

    def info(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._log("info", message, args, kwargs)

    def warning(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._log("warning", message, args, kwargs)

    warn = warning

    def error(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._log("error", message, args, kwargs)

    def critical(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._log("critical", message, args, kwargs)

    def exception(self, message: Any, *args: Any, **kwargs: Any) -> None:
        rendered = render_message(message, args)
        self.backend.error(rendered, exc_info=True, **kwargs)

    def log(self, level: LogLevel, message: Any, *args: Any, **kwargs: Any) -> None:
        method = normalize_level(level).lower()
        self._log(method, message, args, kwargs)

    def _log(
        self,
        method: str,
        message: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        rendered = render_message(message, args)
        structlog_keys = {"event", "logger", "scope", "source"}
        user_kw = {k: v for k, v in kwargs.items() if k not in structlog_keys}
        getattr(self.backend, method)(rendered, **user_kw)
