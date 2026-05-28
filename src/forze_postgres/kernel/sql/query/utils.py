"""Psycopg query-building helpers."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import threading
from typing import Any

import attrs
from psycopg import sql

# ----------------------- #


@attrs.define(slots=True)
class PsycopgPositionalBinder:
    """Accumulates params as a list and returns '%s' placeholders.

    :meth:`add` and :meth:`values` are safe to call concurrently on the same
    instance. :meth:`values` returns a snapshot copy. Direct reads of
    :attr:`params` are not synchronized; use :meth:`values` when sharing an
    instance across threads.
    """

    params: list[Any] = attrs.field(factory=list)
    _lock: threading.Lock = attrs.field(
        factory=threading.Lock,
        init=False,
        repr=False,
        eq=False,
    )

    # ....................... #

    def add(self, value: Any) -> sql.Placeholder:
        with self._lock:
            self.params.append(value)

        return sql.Placeholder()  # -> %s

    # ....................... #

    def values(self) -> list[Any]:
        with self._lock:
            return list(self.params)
