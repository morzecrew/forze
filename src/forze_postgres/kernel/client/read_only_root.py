"""Per-task record of a read-only root transaction, and of writes that bypass it.

``BEGIN … READ ONLY`` makes Postgres refuse writes to this database inside the transaction.
It says nothing about writes the task commits elsewhere while the root is open: a
:meth:`~forze_postgres.kernel.client.client.PostgresClient.detached` statement autocommits,
and another client's root transaction commits on its own. Either one clears the record, so
the root is reported as enforced only when it was the only thing that could commit.

Shared by every client in the process (one record per task), so a bypass on any client
clears the open root's claim. It does not cover ``dblink`` or ``postgres_fdw``.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# ----------------------- #

_root: ContextVar[list[bool] | None] = ContextVar("forze_pg_read_only_root", default=None)
"""``[True]`` while the open root began ``READ ONLY`` and nothing bypassed it; a box, so a
bypass in a nested scope or a child task clears the root's own record."""


# ....................... #


@contextmanager
def read_only_root() -> Iterator[None]:
    """Record a root ``BEGIN … READ ONLY`` for the block."""

    token = _root.set([True])

    try:
        yield

    finally:
        try:
            _root.reset(token)

        except ValueError:
            # Finalized in another context (an abandoned generator): drop the record there.
            _root.set(None)


# ....................... #


def read_only_root_enforced() -> bool:
    """Whether the open root began ``READ ONLY`` and nothing has bypassed it since."""

    record = _root.get()

    return record is not None and record[0]


# ....................... #


def bypass_read_only_root() -> None:
    """Clear the open root's record: a write may commit outside it."""

    record = _root.get()

    if record is not None:
        record[0] = False
