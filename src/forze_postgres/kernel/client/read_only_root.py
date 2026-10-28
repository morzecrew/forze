"""Per-task record of a read-only root transaction, and of writes that bypass it.

``BEGIN … READ ONLY`` makes Postgres refuse writes to this database inside the transaction.
It says nothing about writes the task commits elsewhere while the root is open: a
:meth:`~forze_postgres.kernel.client.client.PostgresClient.detached` statement or another
client's statement outside a transaction autocommits, and another client's root transaction
commits on its own. Each clears the record, so the root is reported as enforced only when
it was the only thing that could commit; an autocommit read costs only that answer.

Shared by every client in the process (one record per task), so a bypass on any client
clears the open root's claim. It does not cover ``dblink`` or ``postgres_fdw``.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# ----------------------- #

_roots: ContextVar[tuple[list[bool], ...]] = ContextVar("forze_pg_read_only_root", default=())
"""One box per open read-only root, innermost last: ``[True]`` while nothing bypassed it. A
box, so a bypass in a nested scope or a child task clears the root's own record; a bypass
clears every open root's, since a write it lets through commits beside each of them."""


# ....................... #


@contextmanager
def read_only_root() -> Iterator[None]:
    """Record a root ``BEGIN … READ ONLY`` for the block."""

    box = [True]
    token = _roots.set((*_roots.get(), box))

    try:
        yield

    finally:
        try:
            _roots.reset(token)

        except ValueError:
            # Finalized in another context (an abandoned generator): drop the record there.
            _roots.set(tuple(other for other in _roots.get() if other is not box))


# ....................... #


def read_only_root_enforced() -> bool:
    """Whether the open root began ``READ ONLY`` and nothing has bypassed it since."""

    roots = _roots.get()

    return bool(roots) and roots[-1][0]


# ....................... #


def bypass_read_only_root() -> None:
    """Clear every open root's record: a write may commit outside them."""

    for box in _roots.get():
        box[0] = False
