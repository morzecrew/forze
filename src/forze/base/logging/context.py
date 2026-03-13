"""Context-local indentation depth for nested log sections.

Uses a :class:`contextvars.ContextVar` so that concurrent tasks (e.g. async)
each have their own depth. :func:`log_section` increments on entry and
decrements on exit.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

# ----------------------- #

_log_depth: ContextVar[int] = ContextVar("forze_log_depth", default=0)
"""Current indentation depth for the active execution context."""


def get_depth() -> int:
    """Return the current indentation depth.

    Used by :func:`~.formatting.indent_for_name` to prefix log lines
    when the logger name matches configured prefixes.
    """
    return _log_depth.get()


# ....................... #


@contextmanager
def log_section() -> Iterator[None]:
    """Increase indentation depth inside a logical logging section.

    Use as a context manager around a block of code. All log lines
    emitted within the block (for matching namespaces) will be indented
    by one extra step. Depth is restored on exit, including on exception.

    Example::

        with log_section():
            logger.info("nested message")  # indented
    """
    depth = _log_depth.get()
    token = _log_depth.set(depth + 1)

    try:
        yield

    finally:
        _log_depth.reset(token)
