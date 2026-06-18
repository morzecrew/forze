"""Transaction journal — concurrency-preserving atomicity for participating mock stores.

A faithful mock transaction must be **atomic** (a failed operation leaves no partial writes)
*without* serializing concurrent transactions (or DST could never observe an interleaving).
A whole-store snapshot/restore can't do that — restoring would clobber a concurrent
transaction's committed writes — so it forces serialization (the strict manager's tradeoff).

Instead, each write to a participating store records an **undo entry** ``(store, key, prior)``
into a per-task journal; a rollback replays the journal in reverse, undoing *only this
transaction's* keys. Concurrent transactions interleave freely; write-write conflicts are
caught by the document row ``rev`` (optimistic concurrency). Visibility is write-through.

The journal ContextVar is owned by the transaction manager
(:class:`~forze_mock.adapters.tx.MockJournalTxManagerAdapter`); stores journal into it
automatically while it is set, and behave as plain dicts when it is not.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

# ----------------------- #

_MISSING: Any = object()

JournalEntry = tuple[dict[Any, Any], Any, Any]
"""``(store, key, prior_value | _MISSING)`` — enough to undo one write."""

_journal: ContextVar[list[JournalEntry] | None] = ContextVar(
    "forze_mock_tx_journal", default=None
)
"""The active transaction's undo journal, or ``None`` outside a transaction."""

# ....................... #


class JournalingStore(dict[Any, Any]):
    """A store dict that records undo info on write while a transaction journal is active.

    Outside a transaction (no journal bound) it is an ordinary ``dict``. Inside one, each
    ``__setitem__`` / ``__delitem__`` appends an undo entry so a rollback can revert exactly
    this transaction's writes — never a whole-store restore — leaving concurrent
    transactions intact.
    """

    def __setitem__(self, key: Any, value: Any) -> None:
        journal = _journal.get()
        if journal is not None:
            journal.append((self, key, self.get(key, _MISSING)))
        super().__setitem__(key, value)

    # ....................... #

    def __delitem__(self, key: Any) -> None:
        journal = _journal.get()
        if journal is not None and key in self:
            journal.append((self, key, self[key]))
        super().__delitem__(key)


# ....................... #


def undo(journal: list[JournalEntry]) -> None:
    """Replay *journal* in reverse, undoing each write (bypassing re-journaling)."""

    for store, key, prior in reversed(journal):
        if prior is _MISSING:
            dict.pop(store, key, None)  # pyright: ignore[reportUnknownMemberType]

        else:
            dict.__setitem__(  # pyright: ignore[reportUnknownMemberType]
                store, key, prior
            )
