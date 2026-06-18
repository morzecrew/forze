"""Journal transaction manager — concurrency-preserving atomicity for the mock.

Unit coverage of the undo journal: create/update/delete rollback, and the key property that
undo is *per key* — so a rolled-back transaction never clobbers a concurrent committed one
(the reason for a journal over the strict manager's whole-store snapshot + serialization).
"""

from __future__ import annotations

from forze_mock.adapters._journal import JournalingStore, _journal, undo

# ----------------------- #


def test_undo_reverts_create_update_delete() -> None:
    store = JournalingStore({"keep": {"v": 0}})
    token = _journal.set([])
    try:
        store["new"] = {"v": 1}  # create
        store["keep"] = {"v": 99}  # update
        del store["keep"]  # delete
        journal = _journal.get()
    finally:
        _journal.reset(token)

    assert journal is not None
    undo(journal)
    assert "new" not in store  # created key removed
    assert store["keep"] == {"v": 0}  # updated-then-deleted key restored to original


def test_undo_is_per_key_not_whole_store() -> None:
    # Undoing one transaction must not clobber a concurrent transaction's committed write to
    # a *different* key — this is why a journal beats snapshot/restore (no serialization).
    store: JournalingStore = JournalingStore({})

    committed = _journal.set([])  # transaction A
    store["a"] = 1
    _journal.reset(committed)  # A commits (journal discarded)

    rolled_back = _journal.set([])  # transaction B
    store["b"] = 2
    journal_b = _journal.get()
    _journal.reset(rolled_back)

    assert journal_b is not None
    undo(journal_b)  # B rolls back
    assert store == {"a": 1}  # B's write undone; A's committed write intact


def test_no_journal_means_plain_dict() -> None:
    store = JournalingStore()
    store["x"] = 1  # no active journal → behaves as a plain dict
    del store["x"]
    assert store == {}
