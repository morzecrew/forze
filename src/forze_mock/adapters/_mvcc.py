"""MVCC overlay — snapshot & serializable isolation for the mock document store.

The default journal manager is read-committed (write-through + per-row ``rev`` OCC). Stronger
isolation needs a transaction to read a *consistent snapshot* rather than the live store, so
this layer gives snapshot / serializable transactions a **buffered overlay**:

* at begin, the transaction captures an as-of-begin snapshot of every document namespace
  (committed state — buffered transactions never write through, so the live store holds only
  committed rows);
* reads come from the transaction's own buffered writes (the overlay) falling back to that
  snapshot — so a concurrent transaction's writes are invisible (no non-repeatable reads /
  phantoms);
* writes go to the overlay, not the live store;
* at commit the overlay is validated against the write-sets of transactions that committed
  after this one began, then published to the live store:
  - **snapshot** rejects a *write-write* conflict (first-committer-wins — prevents lost
    update), while permitting write-skew; and
  - **serializable** additionally rejects a *read-write* conflict (a key this transaction
    read was written by a concurrent committer — prevents write-skew).

Conflicts raise ``exc.concurrency(code="serialization_failure")``, which aborts the
transaction (the overlay is discarded — nothing reached the live store). No global lock, so
concurrent transactions still interleave freely — the basis DST needs.

v1 limitations: the snapshot assumes the conflicting transactions are themselves
snapshot/serializable (a concurrent *read-committed* transaction writes through and would be
visible); the commit log is append-only (unbounded across a run); read-set tracking is by key
(a full scan records every visible key, so it conflicts with any concurrent write in that
namespace — sound, if coarse).
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Iterator

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_TOMBSTONE: Any = object()
"""Overlay marker for a key deleted within the transaction."""


# ....................... #


@attrs.define(slots=True)
class IsolatedStoreView:
    """A per-namespace dict-like view: reads from overlay→snapshot, writes buffer to overlay.

    Returned by :meth:`MvccTx.view` in place of the live store, so the document adapter's
    reads and writes route through the overlay with no call-site changes. A point read records
    its key in *reads*; a **scan** additionally marks the namespace in *scanned* — a predicate
    read — so a concurrent insert of a *new* matching key is caught as a phantom under
    serializable (key-level read tracking alone would miss it).
    """

    snapshot: dict[Any, Any]
    overlay: dict[Any, Any]
    reads: set[Any]
    ns: str = ""
    scanned: set[str] = attrs.field(factory=set)

    # ....................... #

    def __getitem__(self, key: Any) -> Any:
        self.reads.add(key)

        if key in self.overlay:
            value = self.overlay[key]

            if value is _TOMBSTONE:
                raise KeyError(key)

            return value

        return self.snapshot[key]

    def __contains__(self, key: Any) -> bool:
        self.reads.add(key)

        if key in self.overlay:
            return self.overlay[key] is not _TOMBSTONE

        return key in self.snapshot

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]

        except KeyError:
            return default

    def __setitem__(self, key: Any, value: Any) -> None:
        self.overlay[key] = value

    def __delitem__(self, key: Any) -> None:
        if key not in self:
            raise KeyError(key)

        self.overlay[key] = _TOMBSTONE

    # ....................... #

    def _merged(self) -> dict[Any, Any]:
        merged = dict(self.snapshot)

        for key, value in self.overlay.items():
            if value is _TOMBSTONE:
                merged.pop(key, None)
            else:
                merged[key] = value

        return merged

    def _scan(self) -> dict[Any, Any]:
        # A scan is a predicate read: record every visible key AND mark the namespace scanned,
        # so a concurrent insert of a new matching key conflicts under serializable (phantom).
        merged = self._merged()
        self.reads.update(merged.keys())
        self.scanned.add(self.ns)
        return merged

    def values(self) -> Any:
        return self._scan().values()

    def items(self) -> Any:
        return self._scan().items()

    def keys(self) -> Any:
        return self._scan().keys()

    def __iter__(self) -> Iterator[Any]:
        return iter(self._scan())

    def __len__(self) -> int:
        return len(self._scan())


# ....................... #


@attrs.define(slots=True)
class MvccTx:
    """One snapshot/serializable transaction's buffered overlay + read-set, by namespace."""

    begin_version: int
    serializable: bool
    snapshots: dict[str, dict[Any, Any]] = attrs.field(factory=dict)
    overlays: dict[str, dict[Any, Any]] = attrs.field(factory=dict)
    reads: dict[str, set[Any]] = attrs.field(factory=dict)
    scans: set[str] = attrs.field(factory=set)
    """Namespaces this transaction scanned (predicate reads) — for phantom detection."""

    # ....................... #

    @classmethod
    def begin(cls, state: Any, *, serializable: bool) -> MvccTx:
        # Eager as-of-begin snapshot of every document namespace (shallow: rows are replaced,
        # never mutated in place, so holding the prior row refs is a faithful snapshot).
        snapshots = {ns: dict(store) for ns, store in state.documents.items()}
        begin_version = state.mvcc_version
        state.mvcc_active.append(begin_version)

        return cls(
            begin_version=begin_version,
            serializable=serializable,
            snapshots=snapshots,
        )

    # ....................... #

    def finish(self, state: Any) -> None:
        """Deregister this transaction and prune the commit log below the oldest in-flight one.

        Called once on transaction end (commit or abort). An entry only matters to a
        transaction whose begin-version precedes it, so once no in-flight transaction began
        before an entry, the entry can never be consulted again and is dropped — keeping the
        log bounded and ``validate`` from degrading to O(commits) per call across a run.
        """

        state.mvcc_active.remove(self.begin_version)
        horizon = min(state.mvcc_active) if state.mvcc_active else state.mvcc_version
        state.mvcc_commit_log[:] = [
            entry for entry in state.mvcc_commit_log if entry[0] > horizon
        ]

    # ....................... #

    def view(self, ns: str, _live_store: Any) -> IsolatedStoreView:
        """Return the buffered view for namespace *ns* (shared per-namespace state)."""

        return IsolatedStoreView(
            snapshot=self.snapshots.setdefault(ns, {}),
            overlay=self.overlays.setdefault(ns, {}),
            reads=self.reads.setdefault(ns, set()),
            ns=ns,
            scanned=self.scans,
        )

    # ....................... #

    def _write_keys(self, ns: str) -> set[Any]:
        return set(self.overlays.get(ns, {}).keys())

    def validate(self, state: Any) -> None:
        """Raise on a conflict with any transaction committed after this one began.

        Snapshot rejects write-write (lost update); serializable also rejects read-write
        (write skew) and, for any namespace this transaction *scanned*, a concurrent write to
        that namespace — including the insert of a new matching key (phantom / write skew).
        """

        for version, write_sets in state.mvcc_commit_log:
            if version <= self.begin_version:
                continue

            for ns, committed_keys in write_sets.items():
                if self._write_keys(ns) & committed_keys:
                    raise exc.concurrency(
                        "Write-write conflict: a concurrent transaction modified a row "
                        "this transaction also wrote (lost update prevented)",
                        code="serialization_failure",
                    )

                if not self.serializable:
                    continue

                if ns in self.scans:
                    raise exc.concurrency(
                        "Phantom conflict: a concurrent transaction wrote to a namespace "
                        "this transaction scanned (phantom / write skew prevented)",
                        code="serialization_failure",
                    )

                if self.reads.get(ns, set()) & committed_keys:
                    raise exc.concurrency(
                        "Read-write conflict: a concurrent transaction modified a row "
                        "this transaction read (write skew prevented)",
                        code="serialization_failure",
                    )

    # ....................... #

    def commit(self, state: Any) -> None:
        """Publish the overlay to the live stores and record this transaction's write-set."""

        state.mvcc_version += 1

        if write_sets := {
            ns: frozenset(overlay.keys())
            for ns, overlay in self.overlays.items()
            if overlay
        }:
            state.mvcc_commit_log.append((state.mvcc_version, write_sets))

        for ns, overlay in self.overlays.items():
            if not overlay:
                continue

            live = state.documents.get(ns)

            if live is None:
                live = {}
                state.documents[ns] = live

            for key, value in overlay.items():
                # Bypass the journaling layer (no journal is active for an MVCC transaction;
                # these writes are the commit itself, not undoable work).
                if value is _TOMBSTONE:
                    dict.pop(  # pyright: ignore[reportUnknownMemberType]
                        live, key, None
                    )

                else:
                    dict.__setitem__(  # pyright: ignore[reportUnknownMemberType]
                        live, key, value
                    )


# ....................... #

_mvcc_tx: ContextVar[MvccTx | None] = ContextVar("forze_mock_mvcc_tx", default=None)
"""The active snapshot/serializable transaction's overlay, or ``None``."""


def current_mvcc_tx() -> MvccTx | None:
    """Return the active MVCC (snapshot/serializable) transaction, if any."""

    return _mvcc_tx.get()
