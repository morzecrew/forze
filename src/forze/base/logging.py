import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Final, Iterator, Mapping, Optional

# ----------------------- #

__log_depth: ContextVar[int] = ContextVar("log_depth", default=0)
"""Current indentation depth for the active execution context."""

DEFAULT_LOG_FORMAT: Final[str] = (
    "%(asctime)s %(levelname)-8s %(shortname)-36s %(indent)s%(message)s"
)
"""Default formatter for indentation-aware debug logging."""

# ....................... #


def _matches_namespace(name: str, prefixes: tuple[str, ...]) -> bool:
    """Return ``True`` if *name* belongs to one of the configured namespaces."""

    return any(
        name == prefix or name.startswith(f"{prefix}.") or name.startswith(f"{prefix}_")
        for prefix in prefixes
    )


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
        prefixes: tuple[str, ...] = ("forze",),
        step: str = "  ",
        shorten_name: bool = True,
        strip_prefix: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._prefixes = prefixes
        self._step = step
        self._shorten_name = shorten_name
        self._strip_prefix = strip_prefix

    # ....................... #

    def filter(self, record: logging.LogRecord) -> bool:
        record.indent = self._step * __log_depth.get()

        name = record.name

        if self._strip_prefix:
            if name == self._strip_prefix:
                name = self._strip_prefix
            elif name.startswith(f"{self._strip_prefix}."):
                name = name[len(self._strip_prefix) + 1 :]

        if self._shorten_name and "." in name:
            name = name.rsplit(".", 1)[0]

        record.shortname = name
        record.is_forze = _matches_namespace(record.name, self._prefixes)

        return True


# ....................... #


class SafeNamespaceIndentFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "indent"):
            record.indent = ""

        if not hasattr(record, "shortname"):
            record.shortname = record.name

        if not hasattr(record, "is_forze"):
            record.is_forze = False

        return super().format(record)


# ....................... #


def _iter_matching_loggers(prefixes: tuple[str, ...]) -> list[logging.Logger]:
    out: list[logging.Logger] = []

    manager = logging.root.manager
    for name, obj in manager.loggerDict.items():
        if not isinstance(obj, logging.Logger):
            continue

        if _matches_namespace(name, prefixes):
            out.append(obj)

    for prefix in prefixes:
        root_logger = logging.getLogger(prefix)
        if root_logger not in out:
            out.append(root_logger)

    return out


# ....................... #


def configure_log_levels(levels: Mapping[str, int | str]) -> None:
    """Set log level for matching existing loggers and namespace roots."""

    for prefix, level in levels.items():
        if isinstance(level, str):
            level = logging._nameToLevel.get(level.upper(), logging.INFO)  # type: ignore

        for logger in _iter_matching_loggers((prefix,)):
            logger.setLevel(level)


# ....................... #


def enable_logging_indentation(
    logger: Optional[logging.Logger] = None,
    *,
    fmt: str = DEFAULT_LOG_FORMAT,
    prefixes: tuple[str, ...] = ("forze",),
    step: str = "  ",
    apply_fmt: bool = False,
    levels: Mapping[str, int | str] | None = None,
    configure_matching_loggers: bool = False,
    shorten_name: bool = True,
    strip_prefix: str | None = None,
) -> None:
    """Enable indentation-aware formatting for handlers on a logger.

    :param logger: Logger whose handlers should be configured. Defaults to root logger.
    :param fmt: Formatter string.
    :param prefixes: Logger namespace prefixes considered part of the library.
    :param step: Indentation unit.
    :param apply_fmt: When true, replace handler formatters with a safe formatter.
    :param level: Optional log level to assign to matching loggers.
    :param configure_matching_loggers: When true, set ``level`` on existing matching loggers.
    :param shorten_name: When true, drop the last logger name segment into ``shortname``.
    :param strip_prefix: Optional prefix to strip from logger names in rendered output.
    """

    if logger is None:
        logger = logging.getLogger()

    indent_filter = NamespaceIndentFilter(
        prefixes=prefixes,
        step=step,
        shorten_name=shorten_name,
        strip_prefix=strip_prefix,
    )
    indent_formatter = SafeNamespaceIndentFormatter(fmt)

    for handler in logger.handlers:
        handler.addFilter(indent_filter)

        if apply_fmt:
            handler.setFormatter(indent_formatter)

    if levels is not None and configure_matching_loggers:
        configure_log_levels(levels)
