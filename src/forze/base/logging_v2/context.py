from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

# ----------------------- #
#! Mb rename to depth.py or so

_log_depth: ContextVar[int] = ContextVar("forze_log_v2_depth", default=0)

# ....................... #


def get_depth() -> int:
    """Get the current log depth."""

    return _log_depth.get()


# ....................... #


@contextmanager
def log_nested() -> Iterator[None]:
    """Increase indentation depth inside a logical logging section."""

    depth = _log_depth.get()
    token = _log_depth.set(depth + 1)

    try:
        yield

    finally:
        _log_depth.reset(token)
