from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Self

import attrs
from structlog.typing import FilteringBoundLogger

from .constants import FORZE_LEVEL_KEY
from .context import log_nested

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class Logger:
    """Logger bound to a name, scope, and optional source."""

    name: str
    """Logger name."""

    backend: FilteringBoundLogger
    """Structlog backend."""

    # ....................... #

    def bind(self, **kwargs: Any) -> Self:
        """Bind additional context to the logger."""

        return attrs.evolve(self, backend=self.backend.bind(**kwargs))

    # ....................... #

    @contextmanager
    def nested(self) -> Iterator[None]:
        """Log at nested depth level."""

        with log_nested():
            yield

    # ....................... #

    def trace(self, message: str, *sub: Any, **extras: Any) -> None:
        """Log at TRACE level (below DEBUG). Use for noisy per-op details."""

        extras = {**extras, FORZE_LEVEL_KEY: "TRACE"}

        self._log("debug", message, sub, extras)

    # ....................... #

    def debug(self, message: str, *sub: Any, **extras: Any) -> None:
        """Log at DEBUG level."""

        self._log("debug", message, sub, extras)

    # ....................... #

    def info(self, message: str, *sub: Any, **extras: Any) -> None:
        """Log at INFO level."""

        self._log("info", message, sub, extras)

    # ....................... #

    def warning(self, message: str, *sub: Any, **extras: Any) -> None:
        """Log at WARNING level."""

        self._log("warning", message, sub, extras)

    # ....................... #

    def _log(
        self,
        method: str,
        message: str,
        sub: tuple[Any, ...],
        extras: Mapping[str, Any],
    ) -> None:
        pass
