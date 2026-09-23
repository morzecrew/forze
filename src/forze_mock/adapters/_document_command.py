"""In-memory document command (write) operations for :class:`MockDocumentAdapter`."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    cast,
    overload,
)
from uuid import UUID

from forze.application.contracts.document import KeyedCreate, KeyedUpdate, UpsertItem
from forze.application.contracts.domain import drain_domain_events
from forze.application.contracts.guarantees import (
    NonOverlapping,
    SerializedBy,
    UniqueTogether,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, Period, advisory_lock_key, utcnow
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze_mock.adapters._mvcc import current_mvcc_tx
from forze_mock.adapters.tx import ensure_mock_tx_writable
from forze_mock.query._types import C, D, R, U

if TYPE_CHECKING:
    from forze.application.contracts.base import CountlessPage
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.domain import DomainEventDispatcherPort
    from forze.application.contracts.querying import (
        PaginationExpression,
        QuerySortExpression,
    )
    from forze.base.serialization import ModelCodec
    from forze_mock.state import MockState

# ----------------------- #


class MockDocumentCommandMixin(Generic[R, D, C, U]):
    """Write operations for :class:`~forze_mock.adapters.document.MockDocumentAdapter`.

    The fields, tenancy helpers, and read-side codec helpers these methods rely on are
    supplied by the composed adapter; declared here under ``TYPE_CHECKING`` so the type
    checker sees the shared surface without a runtime dependency.
    """

    if TYPE_CHECKING:
        spec: DocumentSpec[R, D, C, U]
        state: MockState
        domain_model: type[D] | None
        tenant_aware: bool
        dispatcher_provider: Callable[[], DomainEventDispatcherPort | None]

        def require_tenant_if_aware(self) -> UUID | None: ...
        def _store(self) -> dict[UUID, JsonDict]: ...
        def _to_read(self, doc: JsonDict) -> R: ...
        def _to_domain(self, doc: JsonDict) -> D: ...
        def _ensure_exists(self, pk: UUID) -> JsonDict: ...
        def _check_rev(self, current_rev: int, expected_rev: int | None) -> None: ...
        def _mark_rev_guarded(self, pk: UUID) -> None: ...
        def _mark_guarantee_recheck(self, pk: UUID) -> None: ...
        def _mark_created(self, pk: UUID) -> None: ...
        def _create_codec(self) -> ModelCodec[D, Any]: ...
        def _domain_codec(self) -> ModelCodec[D, Any]: ...
        def _patch_codec(self) -> ModelCodec[Any, Any]: ...
        def _matcher(self, filters: QueryFilterExpression | None) -> Callable[[JsonDict], bool]: ...
        def _require_domain_model(self) -> type[D]: ...
        def project_many(
            self,
            fields: Sequence[str],
            filters: QueryFilterExpression | None = None,
            pagination: PaginationExpression | None = None,
            sorts: QuerySortExpression | None = None,
        ) -> Awaitable[CountlessPage[JsonDict]]: ...

    # ....................... #

    def _write_row(self, store: dict[UUID, JsonDict], pk: UUID, row: JsonDict) -> None:
        """Put *row* at *pk*, after refusing it if it would break a declared guarantee.

        Every write goes through here, and that is the point. The store is a plain dict and
        four methods used to assign into it directly; a guarantee checked at one of them is a
        guarantee three paths ignore, which is the same as not having it. Enforcement attaches
        to the assignment rather than to the callers, so a write path added later cannot forget
        it without also failing to store anything.

        The caller holds :attr:`state.lock` — the check and the write have to be one step, or a
        second writer slips between them and both rows land.
        """

        self._check_guarantees(store, pk, row)
        store[pk] = row

        if self.spec.guarantees:
            self._mark_guarantee_recheck(pk)

    # ....................... #

    def _check_guarantees(
        self,
        store: dict[UUID, JsonDict],
        pk: UUID,
        row: JsonDict,
    ) -> None:
        """Raise if *row* at *pk* would break a declared guarantee, given *store*.

        Split from :meth:`_write_row` for the one caller that cannot check and write in the
        same step: a set-based update validates the state the whole batch would produce, so it
        needs the check against a staged store rather than against the live one.
        """

        for guarantee in self.spec.guarantees:
            match guarantee:
                case UniqueTogether():
                    self._refuse_duplicate(store, pk, row, guarantee)

                case NonOverlapping():
                    self._refuse_overlap(store, pk, row, guarantee)

    # ....................... #

    def _refuse_duplicate(
        self,
        store: dict[UUID, JsonDict],
        pk: UUID,
        row: JsonDict,
        guarantee: UniqueTogether,
    ) -> None:
        """Refuse *row* when another row already holds its field tuple.

        ``conflict`` rather than ``validation``, because that is what a real store raises when a
        unique index rejects an insert, and a caller retrying or reporting a conflict should not
        have to know which store it was talking to.
        """

        values = tuple(row.get(field) for field in guarantee.fields)

        if guarantee.skip_null and any(value is None for value in values):
            return

        matches = self._matcher(guarantee.where)

        if not matches(row):
            return

        # ponytail: a scan per write, which is O(rows) — fine for an in-memory store and only
        # paid by a spec that declares a guarantee. An index keyed by the field tuple is the
        # upgrade if a simulation ever writes enough rows to feel it.
        for other_pk, other in store.items():
            if other_pk == pk or not matches(other):
                continue

            if tuple(other.get(field) for field in guarantee.fields) == values:
                raise exc.conflict(
                    f"Document {self.spec.name!r} guarantees at most one row per "
                    f"({', '.join(guarantee.fields)}); {other_pk} already holds "
                    f"{values!r}"
                    + (" among the rows the guarantee selects." if guarantee.where else "."),
                    details={
                        "guarantee": guarantee.kind,
                        "fields": list(guarantee.fields),
                        "conflicting_id": str(other_pk),
                    },
                )

    # ....................... #

    def _period_of(self, row: JsonDict, guarantee: NonOverlapping) -> Period[Any] | None:
        """*row*'s period, or ``None`` when it does not hold one.

        A row missing its start is not a row the guarantee constrains — the same reading the
        backend's range expression gives it, where a null lower bound is not a period at all.
        The end is read as written: ``None`` is an open period, which
        :class:`~forze.base.primitives.Period` already means by it.
        """

        start, end = guarantee.period
        lower = row.get(start)

        if lower is None:
            return None

        return Period(lower, row.get(end), guarantee.bounds)

    # ....................... #

    def _refuse_overlap(
        self,
        store: dict[UUID, JsonDict],
        pk: UUID,
        row: JsonDict,
        guarantee: NonOverlapping,
    ) -> None:
        """Refuse *row* when another row under the same key holds a period overlapping its own.

        ``conflict`` for the reason :meth:`_refuse_duplicate` gives: it is what a store raises
        when an exclusion constraint rejects a write, and a caller should not have to know which
        store answered.

        The comparison is :class:`~forze.base.primitives.Period`'s, never a hand-written pair of
        inequalities — the guarantee names that predicate, so the store enforcing it and the
        caller checking the same thing cannot drift apart over what a boundary or an open end
        means.
        """

        period = self._period_of(row, guarantee)

        if period is None:
            return

        matches = self._matcher(guarantee.where)

        if not matches(row):
            return

        key = tuple(row.get(field) for field in guarantee.key)

        # A null in the key never conflicts, because the mechanism behind this guarantee
        # compares key parts with `=` and `NULL = NULL` is unknown, not true. A store that
        # treated two nulls as the same owner would refuse a pair every real backend accepts.
        if any(value is None for value in key):
            return

        # ponytail: a scan per write, as `_refuse_duplicate` does and for the same reasons — an
        # index keyed by the key tuple is the upgrade if a simulation ever writes enough rows.
        for other_pk, other in store.items():
            if other_pk == pk:
                continue

            if tuple(other.get(field) for field in guarantee.key) != key or not matches(other):
                continue

            other_period = self._period_of(other, guarantee)

            if other_period is None or not period.overlaps(other_period):
                continue

            raise exc.conflict(
                f"Document {self.spec.name!r} guarantees no two rows per "
                f"({', '.join(guarantee.key)}) hold overlapping periods; {other_pk} already "
                f"holds {other_period} for {key!r}, which overlaps {period}"
                + (" among the rows the guarantee selects." if guarantee.where else "."),
                details={
                    "guarantee": guarantee.kind,
                    "key": list(guarantee.key),
                    "conflicting_id": str(other_pk),
                },
            )

    # ....................... #

    def _serialized_by(self) -> tuple[SerializedBy, ...]:
        """Every write-serialization declaration this spec makes.

        All of them, not the first: a spec may serialize on more than one axis, and honouring
        one while ignoring the rest leaves the others reading as rules nothing keeps.
        """

        return tuple(g for g in self.spec.guarantees if isinstance(g, SerializedBy))

    # ....................... #

    def _owner_keys(
        self,
        guarantees: Sequence[SerializedBy],
        rows: Sequence[Any],
    ) -> list[int]:
        """The lock keys *rows* contend on, sorted and without repeats.

        Spec and tenant are part of every key, so two aggregates that happen to key on the same
        value do not wait on each other and two tenants never do. Sorted, because a call writing
        several owners that took them in encounter order would deadlock against another call
        writing the same owners in the other one.

        A row is either an inbound payload or a stored mapping, so the owner is read off
        whichever it is.
        """

        tenant = self.require_tenant_if_aware() if self.tenant_aware else None
        keys: set[int] = set()

        for guarantee in guarantees:
            for row in rows:
                if row is None:
                    continue

                values = [
                    row.get(field) if isinstance(row, Mapping) else getattr(row, field, None)
                    for field in guarantee.key
                ]
                # The axis is in the key, so two declarations over different fields do not
                # collide and a row is not serialized against itself twice.
                keys.add(advisory_lock_key(str(self.spec.name), tenant, *guarantee.key, *values))

        return sorted(keys)

    # ....................... #

    async def _serialize_writes(self, *rows: Any) -> None:
        """Hold this aggregate's per-owner write lock for every owner *rows* touch.

        Taken here rather than around the store assignment, and held until the transaction ends
        rather than until the write returns: what a caller needs serialized is its own
        read-then-write, and a lock released at the write leaves exactly the gap between them
        open.

        Outside a transaction the lock is taken and released around the single write, which is
        what a transaction-scoped lock does for a statement that is its own transaction.
        """

        guarantees = self._serialized_by()

        if not guarantees:
            return

        mvcc = current_mvcc_tx()

        for key in self._owner_keys(guarantees, rows):
            if mvcc is not None and key in mvcc.write_locks:
                continue

            lock = self.state.write_serialization.for_key(key)

            if mvcc is not None and lock.locked():
                self._refuse_lock_cycle(mvcc, key)

            if mvcc is None:
                async with lock:
                    return

            mvcc.waiting_for = key

            try:
                await lock.acquire()

            finally:
                mvcc.waiting_for = None

            mvcc.write_locks[key] = lock
            self.state.write_serialization.hold(key, mvcc)

    # ....................... #

    def _refuse_lock_cycle(self, mvcc: Any, key: int) -> None:
        """Refuse a wait that would close a cycle, instead of joining one.

        Walks from whoever holds *key* to whatever that transaction is itself waiting for, and
        on to its holder. A chain arriving back at a key this transaction already holds is a
        deadlock: neither side can release first, and without this both would sit there until
        an operation deadline fired — a hang where the store being modelled detects the cycle
        and aborts one transaction.

        Refused on the side about to *join* the cycle, which is the side that can still give up
        without having blocked anyone.

        :raises CoreException: ``conflict`` naming the owner.
        """

        holders = self.state.write_serialization
        seen: set[int] = set()
        cursor: int | None = key

        while cursor is not None and cursor not in seen:
            seen.add(cursor)
            holder = holders.holder_of(cursor)

            if holder is None or holder is mvcc:
                return

            if any(held in mvcc.write_locks for held in (holder.waiting_for,) if held):
                raise exc.conflict(
                    "Two transactions want each other's owners, so neither can finish. One is "
                    "refused rather than both waiting: retrying it after the other commits "
                    "takes the owners in one order and succeeds.",
                    details={"key": str(key)},
                )

            cursor = holder.waiting_for

    # ....................... #

    def _destination(self, stored: Any, patch: Mapping[str, Any]) -> JsonDict:
        """The row *patch* produces when applied to *stored*.

        The patch alone is not the destination: a partial one names some of a composite key and
        leaves the rest, which would read as null and derive a key no row can share — a lock on
        an owner that does not exist, held while the real destination went unheld.
        """

        return {**(dict(stored) if isinstance(stored, Mapping) else {}), **dict(patch)}

    # ....................... #

    async def _serialize_stored(self, *pks: UUID) -> None:
        """The same, for a write addressed by primary key.

        The owner is on the stored row rather than in the call, so it is read first. Reading
        before the lock is taken is not a race the lock could have closed: a row whose owner
        changes under us was written by a transaction holding the lock for *both* owners.
        """

        if not self._serialized_by():
            return

        store = self._store()

        await self._serialize_writes(*(store.get(pk) for pk in pks))

    # ....................... #

    def _ensure_writable(self) -> None:
        """Reject writes inside a strict read-only mock transaction.

        A no-op under the default (no-op) transaction manager; under
        :class:`~forze_mock.adapters.tx.MockStrictTxManagerAdapter` this mirrors
        Postgres rejecting writes in ``BEGIN ... READ ONLY``.
        """

        ensure_mock_tx_writable(store=f"documents:{self.spec.name}")

    # ....................... #

    def _apply_tenant(self, serialized: JsonDict) -> JsonDict:
        """Stamp the ambient tenant onto a row before storing (tenant-aware collections).

        Mirrors the integration adapters (e.g. Postgres ``_add_tenant_id``), which scope
        a tenant-aware collection by an injected ``tenant_id`` column on **every**
        create-like write — not just ``create``. The domain model carries no tenant
        field, so without this an ``ensure``/``upsert``/``update``/``touch``
        re-serialization would drop the scope and the row would vanish under its tenant.
        """

        if not self.tenant_aware:
            return serialized

        tid = self.require_tenant_if_aware()

        if tid is None:
            return serialized

        return {**serialized, "tenant_id": str(tid)}

    # ....................... #

    def _build_domain(self, payload: C, id: UUID | None = None) -> D:
        """Build the domain model from a create payload, injecting an explicit id if given.

        ``created_at``/``last_update_at`` carried on the payload (import) flow through the
        codec transform; otherwise the domain self-stamps them. The id is server-generated
        (domain default) unless supplied.
        """

        self._require_domain_model()
        domain = self._create_codec().transform(payload)

        if id is not None:
            domain = domain.model_copy(update={ID_FIELD: id}, deep=True)

        return domain

    # ....................... #

    @overload
    async def create(
        self, payload: C, *, id: UUID | None = None, return_new: Literal[True] = True
    ) -> R: ...

    @overload
    async def create(
        self, payload: C, *, id: UUID | None = None, return_new: Literal[False]
    ) -> None: ...

    async def create(
        self,
        payload: C,
        *,
        id: UUID | None = None,
        return_new: bool = True,
    ) -> R | None:
        return await self._insert(payload, id=id, return_new=return_new, conflict_on_duplicate=True)

    async def _insert(
        self,
        payload: C,
        *,
        id: UUID | None,
        return_new: bool,
        conflict_on_duplicate: bool,
    ) -> R | None:
        # ``conflict_on_duplicate`` is the plain-INSERT contract (a duplicate id is a unique
        # violation). ``upsert`` sets it False for its create arm, which is ``ON CONFLICT DO
        # NOTHING`` idempotent on the real adapters — so a concurrent duplicate must not raise there.
        self._ensure_writable()
        domain = self._build_domain(payload, id)
        serialized = self._apply_tenant(self._domain_codec().encode_persistence_mapping(domain))
        await self._serialize_writes(serialized)

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                # Mirror the integration adapters: Postgres maps a duplicate
                # primary key (UniqueViolation) to ``exc.conflict``.
                raise exc.conflict(
                    "Unique violation.",
                    details={"id": str(domain.id)},
                )
            self._write_row(store, domain.id, serialized)

            # Publish-time unique-violation guard: a concurrent transaction may commit the same id
            # between this statement and this transaction's commit; marking the create lets the MVCC
            # commit raise ``exc.conflict`` then rather than silently merging (matching Postgres,
            # which raises 23505 at every isolation level).
            if conflict_on_duplicate:
                self._mark_created(domain.id)

        await drain_domain_events(
            [domain],
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not payloads:
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.create(p, return_new=True) for p in payloads]
        for p in payloads:
            await self.create(p, return_new=False)
        return None

    # ....................... #

    @overload
    async def ensure(
        self,
        id: UUID,
        payload: C,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def ensure(
        self,
        id: UUID,
        payload: C,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure(self, id: UUID, payload: C, *, return_new: bool = True) -> R | None:
        self._ensure_writable()
        domain = self._build_domain(payload, id)
        await self._serialize_writes(
            self._apply_tenant(self._domain_codec().encode_persistence_mapping(domain))
        )

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                raw = dict(store[domain.id])
            else:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(domain)
                )
                self._write_row(store, domain.id, serialized)
                raw = serialized
        if not return_new:
            return None
        return self._to_read(raw)

    # ....................... #

    @overload
    async def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not items:
            if not return_new:
                return None
            return []

        if len({it.id for it in items}) != len(items):
            raise exc.precondition("ensure_many requires distinct id values in the batch")

        if return_new:
            return [await self.ensure(it.id, it.payload, return_new=True) for it in items]
        for it in items:
            await self.ensure(it.id, it.payload, return_new=False)
        return None

    # ....................... #

    @overload
    async def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: bool = True,
    ) -> R | None:
        # Read-decide-write atomically: holding the (reentrant) state lock across
        # the delegated call keeps the existence check and the resulting
        # create/update in one critical section, so two concurrent upserts on the
        # same id cannot both observe "absent" and race into duplicate creates.
        # The delegated store mutation happens synchronously before any await
        # suspension point, so async tasks cannot interleave either.
        #
        # Which is also why the owner locks are taken *here*: the delegated call would
        # otherwise await for one while holding the state lock, and every other writer in the
        # process would wait behind a lock nobody can release. Both arms' owners are taken,
        # since which arm runs is decided inside the section.
        await self._serialize_stored(id)
        await self._serialize_writes(
            self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(self._build_domain(create, id))
            )
        )

        with self.state.lock:
            if id in self._store():
                rev = self._to_domain(dict(self._store()[id])).rev
                if return_new:
                    return await self.update(id, rev, update, return_new=True)
                await self.update(id, rev, update, return_new=False)
                return None
            # ``ON CONFLICT DO NOTHING`` idempotency: a concurrent upsert of the same id must not
            # raise a unique violation (the real adapters converge silently), so the create arm opts
            # out of the publish-time duplicate guard.
            return await self._insert(
                create,
                id=id,
                return_new=return_new,
                conflict_on_duplicate=False,
            )

    # ....................... #

    @overload
    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not items:
            if not return_new:
                return None
            return []

        if len({it.id for it in items}) != len(items):
            raise exc.precondition("upsert_many requires distinct id values in the batch")

        if return_new:
            return [await self.upsert(it.id, it.create, it.update, return_new=True) for it in items]

        for it in items:
            await self.upsert(it.id, it.create, it.update, return_new=False)

        return None

    # ....................... #

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> R: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> tuple[R, JsonDict]: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> JsonDict: ...

    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> R | JsonDict | tuple[R, JsonDict] | None:
        self._ensure_writable()
        # ``encode_mapping`` is the codec's non-encrypting path, so the patch is
        # plaintext: it merges cleanly into the decrypted domain and the single
        # ``encode_persistence_mapping(updated)`` below encrypts exactly once (an
        # encrypting codec's ``encode`` is not idempotent, so encoding the patch
        # here would double-encrypt). ``computed_fields`` is excluded to match
        # persistence-dump semantics; for a plain codec this is identical to the
        # previous behavior.
        patch = self._patch_codec().encode_mapping(
            cast(Any, dto),
            exclude={"computed_fields": True, "unset": True},
        )
        # Both sides: a patch that moves a row to another owner has to hold the owner it is
        # leaving as well as the one it is joining, or a reader of either sees half the move.
        # The destination is the patch applied to the row it lands on, never the patch alone —
        # a partial patch names part of a composite key and the rest would read as null.
        if self._serialized_by():
            stored = self._store().get(pk)
            await self._serialize_writes(stored, self._destination(stored, patch))

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            updated, diff = current.update(patch, materialized=self.spec.materialized)
            if diff:
                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)

            serialized = self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(updated)
            )
            self._write_row(self._store(), pk, serialized)

            # A rev-guarded write (caller supplied a rev) is the one read-committed must fail on a
            # concurrent same-row commit; a blind write (rev is None) is left to lose silently.
            if rev is not None:  # pyright: ignore[reportUnnecessaryComparison]
                self._mark_rev_guarded(pk)

            write_diff = {**dict(diff), REV_FIELD: updated.rev} if diff else {}

        await drain_domain_events(
            [updated],
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        if not return_new:
            return write_diff if return_diff else None

        read_result = self._to_read(serialized)

        return (read_result, write_diff) if return_diff else read_result

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Sequence[tuple[R, JsonDict]]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Sequence[JsonDict]: ...

    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None:
        if not updates:
            return [] if (return_new or return_diff) else None

        pks = [u.id for u in updates]
        if len(set(pks)) != len(pks):
            raise exc.precondition("update_many requires distinct id values in the batch")

        if return_new:
            if return_diff:
                return [
                    await self.update(u.id, u.rev, u.dto, return_new=True, return_diff=True)
                    for u in updates
                ]

            return [
                await self.update(u.id, u.rev, u.dto, return_new=True, return_diff=False)
                for u in updates
            ]

        if return_diff:
            return [
                await self.update(u.id, u.rev, u.dto, return_new=False, return_diff=True)
                for u in updates
            ]

        for u in updates:
            await self.update(u.id, u.rev, u.dto, return_new=False)

        return None

    # ....................... #

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: Literal[False],
    ) -> int: ...

    async def update_matching(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: bool = True,
    ) -> Sequence[R] | int:
        self._ensure_writable()

        if not self.spec.supports_update():
            raise exc.internal("Update command type is not supported for this model")

        # Mirror the real backends: a set-based bulk update cannot recompute a
        # derived value per row, so reject it here too (the mock could recompute,
        # but dev/prod parity matters more than the extra capability).
        if self.spec.materialized:
            raise exc.precondition(
                "update_matching is unsupported for aggregates with materialized "
                f"fields {sorted(self.spec.materialized)}: a set-based update cannot "
                "recompute a derived value. Update records individually.",
                code="core.document.materialized_bulk_update_unsupported",
            )

        # ``encode_mapping`` is the codec's non-encrypting path, so the patch is
        # plaintext: it merges cleanly into the decrypted domain and the single
        # ``encode_persistence_mapping(updated)`` below encrypts exactly once (an
        # encrypting codec's ``encode`` is not idempotent, so encoding the patch
        # here would double-encrypt). ``computed_fields`` is excluded to match
        # persistence-dump semantics; for a plain codec this is identical to the
        # previous behavior.
        patch = self._patch_codec().encode_mapping(
            cast(Any, dto),
            exclude={"computed_fields": True, "unset": True},
        )

        if not patch:
            return [] if return_new else 0

        results: list[R] = []
        mutated: list[D | None] = []
        staged: dict[UUID, JsonDict] = {}
        n = 0

        match = self._matcher(filters)

        # Every owner the filter selects, before the section rather than inside it: a set-based
        # update is one statement on a real store and takes every lock it needs up front.
        #
        # Re-scanned until the set stops growing, because taking a lock is an await: a writer
        # holding another owner's lock can commit a row into this filter while this call is
        # waiting, and that row's owner was never in the first scan. Each pass can only find
        # owners committed by a transaction that has since ended, so the set converges.
        if self._serialized_by():
            seen: set[int] = set()

            while True:
                rows = [raw for raw in list(self._store().values()) if match(raw)]
                # Each row and where the patch would move it — the same reason the single-row
                # update takes both, over every row the filter selects.
                sides = [side for raw in rows for side in (raw, self._destination(raw, patch))]
                wanted = set(self._owner_keys(self._serialized_by(), sides))

                if wanted <= seen:
                    break

                seen |= wanted
                await self._serialize_writes(*sides)

        with self.state.lock:
            store = self._store()
            merged = dict(store.items())

            for pk, raw in list(store.items()):
                if not match(raw):
                    continue

                current = self._to_domain(dict(raw))
                updated, diff = current.update(patch, materialized=self.spec.materialized)

                if not diff:
                    continue

                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(updated)
                )
                staged[pk] = serialized
                mutated.append(updated)
                n += 1

                if return_new:
                    results.append(self._to_read(serialized))

            # Guarantees are checked against the whole staged result, then published in one
            # pass. A set-based update is one statement on every real backend: a row that
            # breaks a guarantee aborts the statement, it does not commit the rows before it.
            # Checking row by row as the loop went would refuse against a half-applied store
            # and leave that half behind — so the batch is validated as the state it would
            # produce, and reaches the store only if all of it passes.
            merged.update(staged)

            for pk, row in staged.items():
                self._check_guarantees(merged, pk, row)

            for pk, row in staged.items():
                store[pk] = row

                if self.spec.guarantees:
                    self._mark_guarantee_recheck(pk)

        await drain_domain_events(
            mutated,
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        return results if return_new else n

    # ....................... #

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: Literal[True] = True,
        chunk_size: int | None = ...,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: Literal[False],
        chunk_size: int | None = ...,
    ) -> int: ...

    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,
        dto: U,
        *,
        return_new: bool = True,
        chunk_size: int | None = None,
    ) -> Sequence[R] | int:
        if not self.spec.supports_update():
            raise exc.internal("Update command type is not supported for this model")

        eff = 200 if chunk_size is None else chunk_size

        if eff < 1:
            raise exc.internal("chunk_size must be positive")

        n_total = 0
        out: list[R] = []
        last_id: UUID | None = None

        while True:
            chunk_filter: QueryFilterExpression = (
                filters
                if last_id is None
                else {
                    "$and": [
                        filters,
                        {"$values": {ID_FIELD: {"$gt": last_id}}},
                    ]
                }
            )

            page = await self.project_many(
                [ID_FIELD, REV_FIELD],
                filters=chunk_filter,
                pagination={"limit": eff},
                sorts={ID_FIELD: "asc"},
            )

            rows = page.hits

            if not rows:
                break

            updates = [
                KeyedUpdate(id=UUID(str(r[ID_FIELD])), rev=int(r[REV_FIELD]), dto=dto) for r in rows
            ]

            if return_new:
                out.extend(
                    await self.update_many(updates, return_new=True),
                )

            else:
                await self.update_many(updates, return_new=False)

            n_total += len(rows)
            last_id = UUID(str(rows[-1][ID_FIELD]))

            if len(rows) < eff:
                break

        return out if return_new else n_total

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        self._ensure_writable()
        await self._serialize_stored(pk)

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(updated)
            )
            self._write_row(self._store(), pk, serialized)

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not pks:
            return [] if return_new else None

        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")

        if return_new:
            return [await self.touch(pk, return_new=True) for pk in pks]

        for pk in pks:
            await self.touch(pk, return_new=False)

        return None

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        self._ensure_writable()
        await self._serialize_stored(pk)

        with self.state.lock:
            _ = self._ensure_exists(pk)
            del self._store()[pk]

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")

        for pk in pks:
            await self.kill(pk)

    # ....................... #

    def _supports_soft_delete(self) -> bool:
        if self.domain_model is None:
            return False

        return "is_deleted" in getattr(self.domain_model, "model_fields", {})

    # ....................... #

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        self._ensure_writable()

        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        await self._serialize_stored(pk)

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            if cast(Any, current).is_deleted:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(current)
                )
                self._write_row(self._store(), pk, serialized)

            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": True,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(updated)
                )
                self._write_row(self._store(), pk, serialized)

            self._mark_rev_guarded(pk)  # delete is rev-guarded

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        if not deletes:
            return [] if return_new else None

        if return_new:
            return [await self.delete(pk, r, return_new=True) for pk, r in deletes]

        for pk, r in deletes:
            await self.delete(pk, r, return_new=False)

        return None

    # ....................... #

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        self._ensure_writable()

        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        await self._serialize_stored(pk)

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            if not cast(Any, current).is_deleted:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(current)
                )
                self._write_row(self._store(), pk, serialized)

            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": False,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(updated)
                )
                self._write_row(self._store(), pk, serialized)

            self._mark_rev_guarded(pk)  # restore is rev-guarded

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        if not restores:
            return [] if return_new else None

        if return_new:
            return [await self.restore(pk, r, return_new=True) for pk, r in restores]

        for pk, r in restores:
            await self.restore(pk, r, return_new=False)

        return None
