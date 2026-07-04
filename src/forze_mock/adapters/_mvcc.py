"""MVCC overlay — buffered isolation for the mock document store, at every level.

Every transaction on the document store gets a **buffered overlay**: writes go to the overlay,
never write through, so the live store holds only committed rows and a concurrent transaction can
never observe an uncommitted write (**no dirty reads, at any level**, including read-committed). The
level differs only in what reads fall back to and what commit validates:

* at begin, snapshot / serializable capture an as-of-begin snapshot of every document namespace
  (committed state); read-committed keeps no snapshot and reads through to the live store instead;
* reads come from the transaction's own buffered writes (the overlay) falling back to that snapshot
  (snapshot / serializable — so concurrent writes are invisible: no non-repeatable reads / phantoms)
  or to the live store (read-committed — so each statement sees the latest committed state);
* writes go to the overlay, not the live store;
* at commit the overlay is validated, then published to the live store:
  - a **create** (plain INSERT) whose id a concurrent committer already published is a *unique
    violation* — raised as ``exc.conflict`` (Postgres ``23505``) at **every** level, before any
    serialization check, so the KIND matches Postgres regardless of isolation. (``ensure`` / ``upsert``
    are ``ON CONFLICT DO NOTHING`` idempotent and are not tracked as creates.)
  - **read-committed** rejects a *write-write* conflict only for a *claimed* row (a rev-guarded write
    or a ``FOR UPDATE`` locked read) and only when the concurrent committer wrote it **after** this
    transaction read / claimed it — a windowed check anchored at the row's read version, not at begin.
    That matches Postgres read-committed: a statement re-reads the latest committed row, so an update
    that reads a value a concurrent transaction then does not touch commits cleanly, while one whose
    row changed since it was read fails. A *blind* (rev-less, unlocked) write is not claimed, so it
    silently loses, as read-committed permits;
  - **snapshot** rejects a *write-write* conflict on **every** write, begin-anchored
    (first-committer-wins — prevents lost update), while permitting write-skew;
  - **serializable** additionally rejects a *read-write* conflict (a key this transaction read was
    written by a concurrent committer) and a *phantom* (a write to a namespace it scanned).

Serialization conflicts raise ``exc.concurrency(code="serialization_failure")`` and unique
violations raise ``exc.conflict``; both abort the transaction (the overlay is discarded — nothing
reached the live store). No global lock, so concurrent transactions still interleave freely — the
basis DST needs.

v1 limitations: the snapshot assumes the conflicting transactions are themselves
snapshot/serializable; the commit log is append-only (unbounded across a run); serializable read-set
tracking is by key, and a scan records every visible key (so it conflicts with any concurrent write
in that namespace — sound, if coarse).
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
    """One transaction's buffered document overlay + read-set, by namespace.

    Buffering writes (never writing through) is what keeps an uncommitted write invisible to a
    concurrent transaction — i.e. **no dirty reads**, at *every* level. The level differs only in
    what reads fall back to and what commit validates:

    - **read-committed** — reads fall through to the **live store** (latest committed, re-read per
      statement); commit rejects a write-write conflict only for a *claimed* row (see
      :attr:`rev_guarded`), windowed at the row's read version, plus a unique violation for a
      duplicate ``create`` (see :attr:`created`).
    - **snapshot / serializable** — reads fall back to a frozen as-of-begin snapshot; commit
      rejects write-write (snapshot) and additionally read-write / phantom (serializable), plus the
      same duplicate-``create`` unique violation.
    """

    begin_version: int
    serializable: bool
    read_committed: bool = False
    snapshots: dict[str, dict[Any, Any]] = attrs.field(factory=dict)
    overlays: dict[str, dict[Any, Any]] = attrs.field(factory=dict)
    reads: dict[str, set[Any]] = attrs.field(factory=dict)
    scans: set[str] = attrs.field(factory=set)
    """Namespaces this transaction scanned (predicate reads) — for phantom detection."""
    rev_guarded: dict[str, dict[Any, int]] = attrs.field(factory=dict)
    """Rows this transaction holds a *claim* on, mapped to the commit-version at which the claim was
    taken (the version the row was read at). A claim is a rev-guarded write (``update``/``delete``/
    ``restore`` with an expected rev) or a ``FOR UPDATE`` locked read. Read-committed surfaces a
    write-write conflict for a claimed row only when a concurrent committer wrote it *after* the
    claim was taken (``committed_version > claim_version``) — matching Postgres read-committed, which
    re-reads the latest committed row per statement and fails only a write whose row a concurrent
    transaction changed since it was read (not merely since this transaction began). A *blind*
    (rev-less, unlocked) write is not claimed, so it silently loses, as read-committed permits.
    Snapshot/serializable ignore claims and conflict on every write regardless."""
    created: dict[str, set[Any]] = attrs.field(factory=dict)
    """Keys this transaction inserted as NEW rows via a plain ``create`` (INSERT). At commit each is
    re-checked against the live store: an id a concurrent committer already published is a unique
    violation, raised as ``exc.conflict`` (Postgres ``23505``) at **every** level, before any
    serialization check. ``ensure`` / ``upsert`` are ``ON CONFLICT DO NOTHING`` idempotent on the
    real adapters, so their create arm is deliberately not tracked here (no spurious conflict)."""

    # ....................... #

    @classmethod
    def begin(cls, state: Any, *, serializable: bool, read_committed: bool = False) -> MvccTx:
        # Snapshot / serializable freeze an as-of-begin snapshot of every document namespace
        # (shallow: rows are replaced, never mutated in place, so holding the prior row refs is a
        # faithful snapshot). Read-committed reads through to the live store instead, so it needs
        # no frozen snapshot.
        snapshots = (
            {}
            if read_committed
            else {ns: dict(store) for ns, store in state.documents.items()}
        )
        begin_version = state.mvcc_version
        state.mvcc_active.append(begin_version)

        return cls(
            begin_version=begin_version,
            serializable=serializable,
            read_committed=read_committed,
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

    def view(self, ns: str, live_store: Any) -> IsolatedStoreView:
        """Return the buffered view for namespace *ns* (shared per-namespace state).

        Read-committed reads fall through to *live_store* (latest committed); snapshot/serializable
        fall back to the frozen as-of-begin snapshot. Writes always buffer to the overlay.
        """

        snapshot = live_store if self.read_committed else self.snapshots.setdefault(ns, {})

        return IsolatedStoreView(
            snapshot=snapshot,
            overlay=self.overlays.setdefault(ns, {}),
            reads=self.reads.setdefault(ns, set()),
            ns=ns,
            scanned=self.scans,
        )

    # ....................... #

    def mark_rev_guarded(self, ns: str, key: Any, version: int) -> None:
        """Record a *claim* on *key* in *ns* taken at commit-*version* (see :attr:`rev_guarded`).

        The claim version is kept at its earliest (``setdefault``): once a transaction has claimed a
        row, any *later* concurrent commit to it conflicts, so the earliest claim is the conservative
        (sound) anchor — a second claim after a fresh read never widens the safe window backwards.
        """

        self.rev_guarded.setdefault(ns, {}).setdefault(key, version)

    def mark_created(self, ns: str, key: Any) -> None:
        """Record that *key* in *ns* was inserted as a NEW row via ``create`` (see :attr:`created`)."""

        self.created.setdefault(ns, set()).add(key)

    # ....................... #

    def _write_keys(self, ns: str) -> set[Any]:
        return set(self.overlays.get(ns, {}).keys())

    def _check_create_conflicts(self, state: Any) -> None:
        # A create whose id a concurrent committer already published is a unique violation at EVERY
        # level — Postgres raises 23505 regardless of isolation, and it takes precedence over a
        # serialization failure (checked before the commit-log scan so the KIND matches: conflict,
        # not serialization_failure). The transaction's own creates are not yet in the live store
        # (commit publishes after validate), so this fires only on a genuine concurrent duplicate.
        for ns, keys in self.created.items():
            if not keys:
                continue

            live = state.documents.get(ns)

            if not live:
                continue

            for key in keys:
                if key in live:
                    raise exc.conflict(
                        "Unique violation: a concurrent transaction created a row with this id.",
                        details={"id": str(key), "namespace": ns},
                    )

    def validate(self, state: Any) -> None:
        """Raise on a create unique violation or a conflict with a concurrently-committed write.

        First a duplicate ``create`` is rejected as ``exc.conflict`` (unique violation, every level).
        Then, per concurrently-committed write-set: snapshot rejects *write-write* on **every** write
        (first-committer-wins, preventing lost update); read-committed rejects it only for a
        **claimed** row (see :attr:`rev_guarded`) and only when the concurrent write landed *after*
        the claim was taken — the windowed check Postgres read-committed performs, so a fresh
        read-then-update does not spuriously abort. Serializable additionally rejects read-write
        (write skew) and, for any namespace this transaction *scanned*, a concurrent write to that
        namespace — including the insert of a new matching key (phantom / write skew).
        """

        self._check_create_conflicts(state)

        for version, write_sets in state.mvcc_commit_log:
            if version <= self.begin_version:
                continue

            for ns, committed_keys in write_sets.items():
                if self.read_committed:
                    # Windowed: conflict only if the concurrent write landed AFTER this transaction
                    # read / claimed the row (``version > claim``), not merely after it began. A
                    # claim taken at a version >= the concurrent commit (a fresh read-through that
                    # already saw it) is safe — exactly Postgres read-committed's per-statement
                    # re-read semantics.
                    claims = self.rev_guarded.get(ns)

                    if claims:
                        for key in committed_keys:
                            claim = claims.get(key)

                            if claim is not None and version > claim:
                                raise exc.concurrency(
                                    "Write-write conflict: a concurrent transaction modified a "
                                    "row this transaction claimed after it read it (lost update "
                                    "prevented)",
                                    code="serialization_failure",
                                )

                    continue

                if self._write_keys(ns) & committed_keys:
                    raise exc.concurrency(
                        "Write-write conflict: a concurrent transaction modified a row "
                        "this transaction also wrote (lost update prevented)",
                        code="serialization_failure",
                    )

                if not self.serializable:
                    continue

                # Deliberately coarse: a scan's read-set is the whole *namespace*,
                # not the predicate — a conservative stand-in for a predicate /
                # seq-scan SIReadLock that also catches a new matching key. It only
                # OVER-prevents (never admits a non-serializable schedule), and the
                # conformance differential normalizes the extra abort — see the
                # "read-only-abort-vs-safe-snapshot" MECHANISM_DIVERGENCES entry.
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
