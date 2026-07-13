"""Transaction journal — concurrency-preserving atomicity for participating mock stores.

A faithful mock transaction must be **atomic** (a failed operation leaves no partial writes)
*without* serializing concurrent transactions (or DST could never observe an interleaving).
A whole-store snapshot/restore can't do that — restoring would clobber a concurrent
transaction's committed writes — so it forces serialization (the strict manager's tradeoff).

Instead, each write to a participating store records an **undo thunk** (a zero-arg callable
that reverts exactly that write) into a per-task journal; a rollback replays the journal in
reverse, undoing *only this transaction's* writes. Concurrent transactions interleave freely;
document write-write conflicts are caught by the row ``rev`` (optimistic concurrency).
Visibility is write-through. A thunk works for any container — :class:`JournalingStore`
(document / dict stores) records key restores, while the outbox (``list``) and inbox
(``set``) adapters record their own element-level undos via :func:`record_undo`.

The journal ContextVar is owned by the transaction manager
(:class:`~forze_mock.adapters.tx.MockJournalTxManagerAdapter`); stores journal into it
automatically while it is set, and behave as plain containers when it is not.
"""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

# ----------------------- #

_MISSING: Any = object()

UndoThunk = Callable[[], None]
"""A zero-arg callable that reverts one write."""

_journal: ContextVar[list[UndoThunk] | None] = ContextVar("forze_mock_tx_journal", default=None)
"""The active transaction's undo journal, or ``None`` outside a transaction."""

# ....................... #


def record_undo(thunk: UndoThunk) -> None:
    """Record an undo *thunk* on the active transaction journal (a no-op outside a tx).

    The seam for non-dict participating stores (outbox ``list``, inbox ``set``): the adapter
    performs its write, then records how to revert exactly that write so a rollback leaves no
    trace, without disturbing concurrent transactions.
    """

    journal = _journal.get()

    if journal is not None:
        journal.append(thunk)


# ....................... #


def _restore_key(store: dict[Any, Any], key: Any, prior: Any) -> UndoThunk:
    """An undo thunk that restores *key* to *prior* (or removes it when *prior* is missing)."""

    if prior is _MISSING:

        def _undo() -> None:
            dict.pop(store, key, None)  # pyright: ignore[reportUnknownMemberType]

    else:

        def _undo() -> None:
            dict.__setitem__(  # pyright: ignore[reportUnknownMemberType]
                store, key, prior
            )

    return _undo


# ....................... #


class JournalingStore(dict[Any, Any]):
    """A store dict that records undo info on write while a transaction journal is active.

    Outside a transaction (no journal bound) it is an ordinary ``dict``. Inside one, each
    ``__setitem__`` / ``__delitem__`` records an undo thunk so a rollback reverts exactly
    this transaction's writes — never a whole-store restore — leaving concurrent
    transactions intact.
    """

    def __setitem__(self, key: Any, value: Any) -> None:
        journal = _journal.get()

        if journal is not None:
            journal.append(_restore_key(self, key, self.get(key, _MISSING)))

        super().__setitem__(key, value)

    # ....................... #

    def __delitem__(self, key: Any) -> None:
        journal = _journal.get()

        if journal is not None and key in self:
            journal.append(_restore_key(self, key, self[key]))

        super().__delitem__(key)


# ....................... #


def undo(journal: list[UndoThunk]) -> None:
    """Replay *journal* in reverse, running each undo thunk (bypassing re-journaling)."""

    for thunk in reversed(journal):
        thunk()
