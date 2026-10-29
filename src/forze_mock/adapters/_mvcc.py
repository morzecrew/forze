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

import asyncio
from collections.abc import Callable, Iterator, Sequence
from contextvars import ContextVar
from typing import Any, NoReturn

import attrs

from forze.application.execution.tracing import record
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
    waiting_for: int | None = attrs.field(default=None)
    """The owner key this transaction is blocked on, or ``None`` while it is running.

    Published so another transaction can see it before it waits: two transactions each holding
    what the other wants would otherwise sit there until an operation deadline fires, and the
    store this models detects the cycle and aborts one of them instead."""

    write_locks: dict[int, Any] = attrs.field(factory=dict)
    """Per-owner write locks this transaction holds, as ``{key: lock}``.

    Held from the first write to a serialized aggregate until the transaction ends, which is
    what makes a handler's own read-then-write correct: releasing at the write would leave the
    gap between the two open, and that gap is the race the declaration exists to close.

    One entry per key, so a transaction writing the same owner twice waits once. Taken in
    sorted order where a single call writes several owners; across calls the order is the
    caller's, so two transactions can still take two owners in opposite orders. That cycle is
    detected before the second wait begins and one side is refused, which is what the store
    this models does rather than leaving both blocked."""

    guarantee_rechecks: dict[tuple[str, Any], Any] = attrs.field(factory=dict)
    """Keys to re-check against the *committed* store at commit, as ``{(ns, key): check}``.

    Keys rather than rows, and one entry per key rather than one per write: what a transaction
    publishes is the *final* value of each key, so an intermediate value it wrote and then moved
    off is not something the store will hold. Keeping those would reject a transaction over a
    tuple it no longer carries — and the check reads the overlay at commit, so a key deleted
    within the transaction drops out on its own.

    A declared storage guarantee is checked when the row is written, and inside a transaction
    that check sees the transaction's own view — overlay plus snapshot — which by construction
    excludes a concurrent transaction's uncommitted rows. Two transactions inserting different
    ids that carry the same guaranteed tuple therefore both pass, and both commit.

    A real store does not behave that way: a unique index is not snapshot-scoped, so the second
    inserter blocks on the first and then fails. Re-running the write's own check against the
    live store at commit is what restores that, and it belongs here rather than in
    :attr:`created` because the conflict is over a *tuple of values*, which this layer cannot
    see — the check is supplied by the adapter that knows the spec."""

    created: dict[str, set[Any]] = attrs.field(factory=dict)
    """Keys this transaction inserted as NEW rows via a plain ``create`` (INSERT). At commit each is
    re-checked against the live store: an id a concurrent committer already published is a unique
    violation, raised as ``exc.conflict`` (Postgres ``23505``) at **every** level, before any
    serialization check. ``ensure`` / ``upsert`` are ``ON CONFLICT DO NOTHING`` idempotent on the
    real adapters, so their create arm is deliberately not tracked here (no spurious conflict)."""

    block_rows: bool = False
    """Whether a row lock this transaction meets parks it (``MockDepsModule(row_locks="block")``).

    Off, a locking read is a claim checked at commit (see :attr:`rev_guarded`); on, it waits for the
    holder to end, as Postgres does — see :func:`acquire_row_locks`."""

    held_rows: list[tuple[str, Any]] = attrs.field(factory=list)
    """The ``(namespace, pk)`` row locks this transaction holds in blocking mode, until it ends."""

    row_waiting_for: tuple[str, Any] | None = None
    """The row this transaction is parked on, or ``None`` — the wait-for graph's edge out of it."""

    ended: asyncio.Event = attrs.field(factory=asyncio.Event)
    """Set once this transaction commits or rolls back: what a row-lock waiter parks on."""

    # ....................... #

    @classmethod
    def begin(
        cls,
        state: Any,
        *,
        serializable: bool,
        read_committed: bool = False,
        block_rows: bool = False,
    ) -> MvccTx:
        # Snapshot / serializable freeze an as-of-begin snapshot of every document namespace
        # (shallow: rows are replaced, never mutated in place, so holding the prior row refs is a
        # faithful snapshot). Read-committed reads through to the live store instead, so it needs
        # no frozen snapshot.
        snapshots = (
            {} if read_committed else {ns: dict(store) for ns, store in state.documents.items()}
        )
        begin_version = state.mvcc_version
        state.mvcc_active.append(begin_version)

        return cls(
            begin_version=begin_version,
            serializable=serializable,
            read_committed=read_committed,
            snapshots=snapshots,
            block_rows=block_rows,
        )

    # ....................... #

    def finish(self, state: Any) -> None:
        """Deregister this transaction and prune the commit log below the oldest in-flight one.

        Called once on transaction end (commit or abort), which is also where the per-owner
        write locks and the blocking row locks are released — commit or rollback, no path that
        forgets. An entry only
        matters to a
        transaction whose begin-version precedes it, so once no in-flight transaction began
        before an entry, the entry can never be consulted again and is dropped — keeping the
        log bounded and ``validate`` from degrading to O(commits) per call across a run.
        """

        # Before anything else: a writer waiting on one of these has nothing to do with this
        # transaction's bookkeeping, and an exception raised below would strand it for good.
        release_write_locks(self, state)
        release_row_locks(self, state)

        state.mvcc_active.remove(self.begin_version)
        horizon = min(state.mvcc_active) if state.mvcc_active else state.mvcc_version
        state.mvcc_commit_log[:] = [entry for entry in state.mvcc_commit_log if entry[0] > horizon]

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

    def mark_guarantee_recheck(self, ns: str, key: Any, check: Any) -> None:
        """Record that *key* in *ns* must be re-checked at commit (see
        :attr:`guarantee_rechecks`)."""

        self.guarantee_rechecks[(ns, key)] = check

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
                    conflict = exc.conflict(
                        "Unique violation: a concurrent transaction created a row with this id.",
                        details={"id": str(key), "namespace": ns},
                    )
                    # As the backends' mapped violation: its details are not the client's.
                    conflict.expose_details = False
                    raise conflict

    def _check_guarantee_conflicts(self, state: Any) -> None:
        # Re-run each buffered write's own guarantee check against the *committed* store, which
        # now holds whatever concurrent transactions published since this one began. The
        # in-transaction check could not see them (that is what isolation means), so without
        # this a declared uniqueness holds within a transaction and not across two — the
        # in-memory store would permit a pair of rows every real backend's unique index
        # refuses, and a simulation would attest a property a deployment does not have.
        #
        # Before the serialization checks, and raising ``conflict`` rather than
        # ``serialization_failure``, for the same reason a duplicate id does: a unique
        # violation is what the backend raises, at every isolation level.
        # One merged view per namespace, not per key: the merge copies the whole namespace, and
        # the loop runs once per re-checked key — a batch write into a large namespace would pay
        # for that copy on every row. Nothing here mutates either side, so the view is stable.
        published: dict[str, dict[Any, Any]] = {}

        for (ns, key), check in self.guarantee_rechecks.items():
            # No skip for a namespace with nothing committed yet: a guarantee that holds at
            # commit was not checked when it was written, so the transaction's own rows may be
            # all the store holds and still break it.

            # The value this transaction will actually publish, read now rather than recorded
            # when the write happened: a key rewritten since then is checked once, on its final
            # value, and a key deleted since then (``_TOMBSTONE``, or dropped outright) is not
            # checked at all, because nothing lands for it.
            row = (self.overlays.get(ns) or {}).get(key, _TOMBSTONE)

            if row is _TOMBSTONE:
                continue

            if ns not in published:
                published[ns] = self._published(ns, state)

            check(published[ns], key, row)

    def _published(self, ns: str, state: Any) -> dict[Any, Any]:
        """The namespace as it will look once this transaction commits.

        The committed rows with this transaction's own overlay laid over them, which is the state
        the check has to judge — not the committed rows alone. A transaction that moves one row
        out of a guarantee's scope and another into it is the ordinary shape of a correction, and
        against the committed store alone the row being *replaced* is still there, so the
        replacement reads as a duplicate of something that is on its way out.

        Concurrency is unaffected: a row a concurrent transaction published since this one began
        is in the committed store, so it is in here too, and a genuine duplicate is still caught.
        """

        merged = dict(state.documents.get(ns) or {})

        for key, value in (self.overlays.get(ns) or {}).items():
            if value is _TOMBSTONE:
                merged.pop(key, None)

            else:
                merged[key] = value

        return merged

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
        self._check_guarantee_conflicts(state)

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
            ns: frozenset(overlay.keys()) for ns, overlay in self.overlays.items() if overlay
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


@attrs.define(slots=True)
class StatementLocks:
    """The per-owner write locks and row locks one write outside a transaction holds.

    Such a write is a transaction of one statement, and it contends the way one does: it holds
    every owner and row it touches at once, and another transaction deciding whether to wait on
    it has to be able to see it — what it holds and what it is waiting for — or a cycle through
    it is a hang nobody detects. Shaped like :class:`MvccTx`'s own lock fields so either can be
    read.
    """

    write_locks: dict[int, Any] = attrs.field(factory=dict)
    waiting_for: int | None = attrs.field(default=None)
    held_rows: list[tuple[str, Any]] = attrs.field(factory=list)
    row_waiting_for: tuple[str, Any] | None = None
    ended: asyncio.Event = attrs.field(factory=asyncio.Event)


# ....................... #


def release_write_locks(holder: MvccTx | StatementLocks, state: Any) -> None:
    """Give back every per-owner write lock *holder* took, and forget that it held them."""

    for key, lock in holder.write_locks.items():
        state.write_serialization.release(key)
        lock.release()

    holder.write_locks.clear()


# ....................... #


def _on_simulation_loop() -> bool:
    """Whether the running loop raises on quiescence instead of hanging.

    Read by name because ``forze_mock`` may not import ``forze_dst``, whose
    ``SimulationEventLoop`` declares it. A blocking wait is only safe there: a holder that never
    ends becomes a reported deadlock rather than a hung test.
    """

    return getattr(asyncio.get_running_loop(), "raises_on_quiescence", False) is True


# ....................... #


def release_row_locks(holder: MvccTx | StatementLocks, state: Any) -> None:
    """Give back every row lock *holder* holds, then wake whoever is parked on it."""

    if holder.held_rows:
        # The loop running this is the one the rows were taken on: ``finish`` runs in the same
        # coroutine as ``begin``, a statement releases in the call that took them, and rows are
        # only taken on the simulation loop, which is single-threaded and refuses threads — no
        # other loop can share the holder.
        holders = state.row_locks.holders()

        for key in holder.held_rows:
            holders.pop(key, None)

        holder.held_rows.clear()

    # After the release, never before: a waiter woken to a row still marked held would park on
    # an event already set, and spin.
    holder.ended.set()


# ....................... #


def wait_cycle_rows(state: Any, waiter: Any, holder: Any) -> list[tuple[str, Any]] | None:
    """The rows on the wait-for path from *holder* back to *waiter*, or ``None`` with no cycle.

    Each step goes from a transaction to whoever holds what it is parked on: a row
    (``row_waiting_for``) or a ``SerializedBy`` owner (``waiting_for``). Following both is what
    catches a cycle mixing the two, whichever kind of wait closes it. An empty list means the path
    arrived back through owner waits alone.
    """

    path: list[tuple[str, Any]] = []
    seen: set[int] = set()

    while holder is not None and id(holder) not in seen:
        if holder is waiter:
            return path

        seen.add(id(holder))
        row = getattr(holder, "row_waiting_for", None)

        if row is not None:
            path.append(row)
            holder = state.row_locks.holders().get(row)

        else:
            owner = holder.waiting_for
            holder = None if owner is None else state.write_serialization.holder_of(owner)

    return None


# ....................... #


def raise_row_deadlock(rows: Sequence[tuple[str, Any]]) -> NoReturn:
    """Refuse the transaction closing a cycle through row locks, naming the rows.

    Postgres aborts one transaction of such a cycle; this refuses the one about to close it,
    which under one seed and schedule is always the same transaction.

    :raises CoreException: ``concurrency`` with ``code="deadlock_detected"`` and the cycle's rows
        in its details; the same rows go on the runtime trace for the simulation's oracle.
    """

    named = [f"{namespace}/{pk}" for namespace, pk in rows]
    record(domain="lock", op="deadlock", key=named[0], outcome="refused", payload={"rows": named})
    deadlock = exc.concurrency(
        "Deadlock detected: this transaction and another each hold a row the other is waiting "
        "for. This one is refused rather than both waiting; taking the rows in one order "
        "avoids it.",
        code="deadlock_detected",
        details={"rows": named},
    )
    # As the backends' mapped deadlock: its details are not the client's.
    deadlock.expose_details = False
    raise deadlock


# ....................... #


async def acquire_write_locks(
    state: Any,
    holder: MvccTx | StatementLocks,
    keys: Callable[[], Sequence[int]],
) -> None:
    """Take every per-owner write lock *keys* names for *holder*, waiting out each live holder.

    *keys* is asked again after every wait, so a key that appears meanwhile is taken too. The
    caller decides when *holder* gives them back (:func:`release_write_locks`).

    :raises CoreException: ``concurrency`` when a wait would close a cycle
        (:func:`refuse_write_lock_cycle`).
    """

    table = state.write_serialization

    while wanted := [key for key in keys() if key not in holder.write_locks]:
        for key in wanted:
            lock = table.for_key(key)

            if lock.locked():
                refuse_write_lock_cycle(state, holder, key)

            holder.waiting_for = key

            try:
                await lock.acquire()

            finally:
                holder.waiting_for = None

            holder.write_locks[key] = lock
            table.hold(key, holder)


# ....................... #


def refuse_write_lock_cycle(state: Any, holder: MvccTx | StatementLocks, key: int) -> None:
    """Refuse a wait that would close a cycle, instead of joining one.

    Walks from whoever holds *key* to whatever that writer is itself waiting for — an owner,
    or under ``row_locks="block"`` a row — and on to its holder. A chain arriving back at
    *holder* is a deadlock: neither side can release first, and without this both would sit
    there until an operation deadline fired — a hang where the store being modelled detects
    the cycle and aborts one of them.

    Refused on the side about to *join* the cycle, which is the side that can still give up
    without having blocked anyone.

    :raises CoreException: ``concurrency`` naming the owner — the kind Postgres gives the
        same cycle when it detects the deadlock, so a caller's retry policy treats both alike.
        A cycle through a row lock is refused as the row side refuses it, with
        ``code="deadlock_detected"`` and the rows (see :func:`raise_row_deadlock`).
    """

    holders = state.write_serialization
    seen: set[int] = set()
    cursor: int | None = key

    while cursor is not None and cursor not in seen:
        seen.add(cursor)
        other = holders.holder_of(cursor)

        if other is None:
            return

        if other.waiting_for in holder.write_locks:
            deadlock = exc.concurrency(
                "Two writers want each other's owners, so neither can finish. One is "
                "refused rather than both waiting: retrying it after the other finishes "
                "takes the owners in one order and succeeds.",
                details={"key": str(key)},
            )
            # As the backends' mapped deadlock: its details are not the client's.
            deadlock.expose_details = False
            raise deadlock

        if (row := getattr(other, "row_waiting_for", None)) is not None:
            # Parked on a row: the rest of the chain may mix both kinds of wait.
            start = state.row_locks.holders().get(row)

            if (cycle := wait_cycle_rows(state, holder, start)) is not None:
                raise_row_deadlock([row, *cycle])

            return

        cursor = other.waiting_for


# ....................... #


def _refuse_stale_lock(state: Any, tx: MvccTx, key: tuple[str, Any]) -> None:
    """Refuse a snapshot transaction's lock on a row written since its snapshot was taken.

    What Postgres does at ``REPEATABLE READ`` and ``SERIALIZABLE``: the version the snapshot
    sees is no longer the latest, and a lock on it would act on a stale row. Rows are replaced
    on every write, never mutated in place, so a changed or deleted row is one whose live entry
    is no longer the snapshot's. Read committed keeps no snapshot, so it is never refused here.

    :raises CoreException: ``concurrency`` with ``code="serialization_failure"``.
    """

    namespace, pk = key
    seen = tx.snapshots.get(namespace, {}).get(pk)

    if seen is not None and (state.documents.get(namespace) or {}).get(pk) is not seen:
        raise exc.concurrency(
            "Could not serialize access due to concurrent update: a row this transaction "
            "locks was changed after its snapshot was taken",
            code="serialization_failure",
        )


# ....................... #


async def acquire_row_locks(
    state: Any,
    keys: Callable[[], Sequence[tuple[str, Any]]],
    *,
    block_statement: bool,
    holder: MvccTx | StatementLocks | None = None,
) -> bool:
    """Hold every row lock *keys* names, waiting out each live holder, in blocking mode.

    A no-op in the default claim mode, and off a simulation loop, where a wait could hang: the
    claim then stands in, as it always has. A transaction takes each free row and keeps it until
    it ends. Outside one, a write passes its *holder* and keeps its rows until it gives them back
    with its owner locks, right before it writes; a read passes none and holds its row for this
    call only. Outside a transaction it blocks only when *block_statement* says its own module
    does — a claim-mode module sharing the state with a blocking one never waits. *keys* is asked
    again after every wait, so a row deleted or moved meanwhile drops out and the caller proceeds
    against what the holder committed — read committed's re-read. A snapshot transaction is
    refused instead when the row changed since its snapshot (see :func:`_refuse_stale_lock`).

    Waiters park on the holder's :attr:`MvccTx.ended` and wake in the order the loop runs them,
    which the simulation's scheduler fixes from the seed; no wall clock is read.

    :returns: Whether it waited, so a predicate read re-evaluates against the committed row.
    :raises CoreException: ``concurrency`` with ``code="deadlock_detected"`` when a wait would
        close a cycle (see :func:`wait_cycle_rows` and :func:`raise_row_deadlock`), or with
        ``code="serialization_failure"`` for a snapshot transaction's stale row.
    """

    tx = current_mvcc_tx()
    blocks = tx.block_rows if tx is not None else block_statement

    if not blocks or not _on_simulation_loop():
        return False

    holders = state.row_locks.holders()
    taker = tx if tx is not None else holder if holder is not None else StatementLocks()
    waited = False

    try:
        while pending := [key for key in keys() if holders.get(key) is not taker]:
            for key in pending:
                other = holders.get(key)

                if other is None:
                    if tx is not None:
                        _refuse_stale_lock(state, tx, key)

                    holders[key] = taker
                    taker.held_rows.append(key)
                    continue

                if (cycle := wait_cycle_rows(state, taker, other)) is not None:
                    raise_row_deadlock([key, *cycle])

                taker.row_waiting_for = key

                try:
                    await other.ended.wait()

                finally:
                    taker.row_waiting_for = None

                waited = True
                break

    finally:
        if taker is not tx and taker is not holder:
            # A read outside a transaction: its statement ends with this call.
            release_row_locks(taker, state)

    return waited


# ....................... #

_mvcc_tx: ContextVar[MvccTx | None] = ContextVar("forze_mock_mvcc_tx", default=None)
"""The active snapshot/serializable transaction's overlay, or ``None``."""


def current_mvcc_tx() -> MvccTx | None:
    """Return the active MVCC (snapshot/serializable) transaction, if any."""

    return _mvcc_tx.get()
