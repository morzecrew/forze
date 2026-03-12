import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

# ----------------------- #

__log_depth: ContextVar[int] = ContextVar("log_depth", default=0)

# ....................... #


def _current_log_indent(step: str = "  ") -> str:
    """Return the current log indent."""

    return step * __log_depth.get()


# ....................... #


@contextmanager
def log_section() -> Iterator[None]:
    """Context manager to keep track of the current log depth."""

    depth = __log_depth.get()
    token = __log_depth.set(depth + 1)

    try:
        yield

    finally:
        __log_depth.reset(token)


# ....................... #


class NamespaceIndentFilter(logging.Filter):
    """Inject identation into log records based on the namespace.

    Adds ``record.indent`` so formatters may use ``%(indent)s``.
    """

    def __init__(
        self,
        *,
        prefixes: tuple[str, ...] = ("forze", "forze_"),
        step: str = "  ",
    ) -> None:
        super().__init__()
        self._prefixes = prefixes
        self._step = step

    # ....................... #

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith(self._prefixes):
            record.indent = _current_log_indent(self._step)

        else:
            record.indent = ""

        return True


# ....................... #


class SafeNamespaceIndentFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "indent"):
            record.indent = ""

        return super().format(record)


# ....................... #


def enable_logging_indentation_for_handler(
    handler: logging.Handler,
    *,
    fmt: str = "%(levelname)s %(name)s %(indent)s%(message)s",
    prefixes: tuple[str, ...] = ("forze", "forze_"),
    step: str = "  ",
    apply_fmt: bool = False,
) -> None:
    """Enable logging indentation for a specific logging handler."""

    indent_filter = NamespaceIndentFilter(prefixes=prefixes, step=step)
    indent_formatter = SafeNamespaceIndentFormatter(fmt)

    handler.addFilter(indent_filter)

    if apply_fmt:
        handler.setFormatter(indent_formatter)


# ....................... #


def enable_logging_indentation(
    logger: Optional[logging.Logger] = None,
    *,
    fmt: str = "%(levelname)s %(name)s %(indent)s%(message)s",
    prefixes: tuple[str, ...] = ("forze", "forze_"),
    step: str = "  ",
    apply_fmt: bool = False,
) -> None:
    """Enable logging indentation for a specific logger."""

    if logger is None:
        logger = logging.getLogger()

    indent_filter = NamespaceIndentFilter(prefixes=prefixes, step=step)
    indent_formatter = SafeNamespaceIndentFormatter(fmt)

    for handler in logger.handlers:
        handler.addFilter(indent_filter)

        if apply_fmt:
            handler.setFormatter(indent_formatter)
