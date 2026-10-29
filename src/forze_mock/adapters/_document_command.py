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

from forze.application.contracts.document import (
    KeyedCreate,
    KeyedUpdate,
    UpsertItem,
    require_whole_update_matching,
)
from forze.application.contracts.document.specs import (
    require_hold_in_transaction,
    serialization_keys,
)
from forze.application.contracts.domain import drain_domain_events
from forze.application.contracts.guarantees import (
    NonOverlapping,
    SerializedBy,
    UniqueTogether,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.querying.internal.matching import instant_key
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, Period, advisory_lock_key, utcnow
from forze.domain.constants import ID_FIELD, LAST_UPDATE_AT_FIELD, REV_FIELD
from forze_mock.adapters._mvcc import (
    MvccTx,
    StatementLocks,
    acquire_write_locks,
    current_mvcc_tx,
    release_row_locks,
    release_write_locks,
)
from forze_mock.adapters.tx import (
    ensure_mock_tx_writable,
    in_mock_transaction,
    strict_tx_locks,
)
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
        def _lock_rows(
            self,
            pks: Callable[[], Sequence[UUID]],
            holder: MvccTx | StatementLocks | None = None,
        ) -> Awaitable[bool]: ...
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

        self._check_guarantees(store, pk, row, deferred=current_mvcc_tx() is None)
        store[pk] = row

        if self.spec.guarantees:
            self._mark_guarantee_recheck(pk)

    # ....................... #

    def _check_guarantees(
        self,
        store: dict[UUID, JsonDict],
        pk: UUID,
        row: JsonDict,
        *,
        deferred: bool = True,
    ) -> None:
        """Raise if *row* at *pk* would break a declared guarantee, given *store*.

        Split from :meth:`_write_row` for the one caller that cannot check and write in the
        same step: a set-based update validates the state the whole batch would produce, so it
        needs the check against a staged store rather than against the live one.

        *deferred* includes the guarantees that hold only at commit. A write inside a
        transaction leaves them out — the commit recheck judges them on what the transaction
        publishes — and a write outside one keeps them, because there is no later moment: the
        write *is* the commit, as a statement in autocommit is to a deferred constraint. The
        commit recheck calls this with the default, so it judges every guarantee.
        """

        for guarantee in self.spec.guarantees:
            match guarantee:
                case UniqueTogether() if deferred or guarantee.holds == "always":
                    self._refuse_duplicate(store, pk, row, guarantee)

                case NonOverlapping() if deferred or guarantee.holds == "always":
                    self._refuse_overlap(store, pk, row, guarantee)

                case _:
                    pass

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
        # Compared as a unique index compares them: an aware datetime by the instant it is.
        key = tuple(instant_key(value) for value in values)

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

            if tuple(instant_key(other.get(field)) for field in guarantee.fields) == key:
                # The backends' summary, so a client sees the same text; the row stays in details.
                conflict = exc.conflict(
                    "Unique violation.",
                    details={
                        "guarantee": guarantee.kind,
                        "fields": list(guarantee.fields),
                        "conflicting_id": str(other_pk),
                        "values": repr(values),
                        "document": self.spec.name,
                    },
                )
                # As the backends' mapped violation: its details are not the client's.
                conflict.expose_details = False
                raise conflict

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

            conflict = exc.conflict(
                "Constraint violation (exclusion).",
                details={
                    "guarantee": guarantee.kind,
                    "key": list(guarantee.key),
                    "conflicting_id": str(other_pk),
                    "values": repr(key),
                    "period": str(period),
                    "conflicting_period": str(other_period),
                    "document": self.spec.name,
                },
            )
            # As the backends' mapped violation: its details are not the client's.
            conflict.expose_details = False
            raise conflict

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

    async def _serialize_writes(
        self,
        rows: Callable[[], Sequence[Any]],
        locking: Callable[[], Sequence[UUID]] = tuple,
    ) -> None:
        """Hold the row locks, then this aggregate's per-owner write locks, a write needs.

        *locking* names the stored rows the write changes, locked first (see :meth:`_lock_rows`).
        Row before owner keeps the owners read next final, since a held row cannot move. Not a
        deadlock-detection need: both cycle walks follow row and owner waits alike.

        *rows* names what the write touches — the rows it inserts, the rows it changes and
        where the change moves them — and is asked again after every wait. Taking a lock is an
        await: while this call waits, a row can move to another owner or be committed into a
        filter, and the owner it has then was never in the first answer. Whatever new owner the
        answer names is taken too, and since a row whose owner is held cannot move, it stops
        growing.

        Held until the transaction ends rather than until the write returns: what a caller needs
        serialized is its own read-then-write, and a lock released at the write leaves exactly the
        gap between them open.

        Outside a transaction the write is a statement of its own: every row and owner is held at
        once, so a locker arriving while it waits for an owner waits for it too, and all are given
        back as this returns. Giving them back before the write is sound only because every caller
        writes before it next awaits, so no other writer can run between the two — which is why a
        caller that decides between writes decides in that same step.
        """

        guarantees = self._serialized_by()

        await self._take_write_locks(
            lambda: self._owner_keys(guarantees, rows()) if guarantees else (), locking
        )

    async def _take_write_locks(
        self,
        keys: Callable[[], Sequence[int]],
        locking: Callable[[], Sequence[UUID]] = tuple,
    ) -> None:
        """Take the row locks *locking* names, then the per-owner locks *keys* names.

        *keys* is asked again after every wait. Held by the open transaction until it ends;
        outside one, given back as this returns.
        """

        mvcc = current_mvcc_tx()
        held_by_tx: MvccTx | StatementLocks | None = mvcc if mvcc is not None else strict_tx_locks()
        holder = held_by_tx if held_by_tx is not None else StatementLocks()

        try:
            await self._lock_rows(locking, holder)
            await acquire_write_locks(self.state, holder, keys)

        finally:
            if held_by_tx is None:
                release_write_locks(holder, self.state)
                release_row_locks(holder, self.state)

    # ....................... #

    def _destination(self, stored: Any, patch: Mapping[str, Any]) -> JsonDict | None:
        """The row *patch* produces when applied to *stored*, or ``None`` if nothing is stored.

        The patch alone is not the destination: a partial one names some of a composite key and
        leaves the rest, which would read as null and derive a key no row can share — a lock on
        an owner that does not exist, held while the real destination went unheld.
        """

        if not isinstance(stored, Mapping):
            return None

        return {**dict(stored), **dict(patch)}

    # ....................... #

    def _sides(self, pk: UUID, patch: Mapping[str, Any]) -> list[Any]:
        """The row *pk* as stored now, and where *patch* would move it.

        Both: a patch that moves a row to another owner has to hold the owner it is leaving as
        well as the one it is joining, or a reader of either sees half the move.
        """

        stored = self._store().get(pk)

        return [stored, self._destination(stored, patch)]

    # ....................... #

    @staticmethod
    def _require_revision_to_check(*, merge: bool, rev: int | None) -> None:
        """Refuse a strict check (``merge=False``) given no revision, as the backends do."""

        if not merge and rev is None:
            raise exc.configuration(
                "A strict revision check (merge=False) needs the revision it checks; got none, "
                "so the write would apply unchecked.",
            )

    def _check_revisions_first(self, pairs: Sequence[tuple[UUID, int | None]]) -> None:
        """Refuse a bulk write before any row is written when one row is not at its revision.

        A row moved between this check and its own write is still refused there, but only a
        transaction undoes the rows written before it.
        """

        with self.state.lock:
            for pk, rev in pairs:
                self._check_rev(int(self._ensure_exists(pk)[REV_FIELD]), rev)

    # ....................... #

    def _encode_patch(self, dto: U) -> JsonDict:
        """The update *dto* as a plaintext patch.

        ``encode_mapping`` is the codec's non-encrypting path, so the patch merges cleanly into
        the decrypted domain and the single ``encode_persistence_mapping(updated)`` afterwards
        encrypts exactly once — an encrypting codec's ``encode`` is not idempotent, so encoding
        the patch here would double-encrypt. ``computed_fields`` is excluded to match
        persistence-dump semantics; for a plain codec this is identical to encoding the model.
        """

        return self._patch_codec().encode_mapping(
            cast(Any, dto),
            exclude={"computed_fields": True, "unset": True},
        )

    # ....................... #

    async def _serialize_stored(self, *pks: UUID) -> None:
        """The same, for a write addressed by primary key: the owner is on the stored row."""

        await self._serialize_writes(lambda: [self._store().get(pk) for pk in pks], lambda: pks)

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
        await self._serialize_writes(lambda: [serialized])

        with self.state.lock:
            self._store_created(domain, serialized, conflict_on_duplicate=conflict_on_duplicate)

        await drain_domain_events(
            [domain],
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        return self._to_read(serialized) if return_new else None

    def _store_created(
        self,
        domain: D,
        serialized: JsonDict,
        *,
        conflict_on_duplicate: bool,
    ) -> None:
        """Store a new row; the caller holds :attr:`state.lock` and the row's owners."""

        store = self._store()
        if domain.id in store:
            # Mirror the integration adapters: Postgres maps a duplicate
            # primary key (UniqueViolation) to ``exc.conflict``, details withheld.
            conflict = exc.conflict(
                "Unique violation.",
                details={"id": str(domain.id)},
            )
            conflict.expose_details = False
            raise conflict
        self._write_row(store, domain.id, serialized)

        # Publish-time unique-violation guard: a concurrent transaction may commit the same id
        # between this statement and this transaction's commit; marking the create lets the MVCC
        # commit raise ``exc.conflict`` then rather than silently merging (matching Postgres,
        # which raises 23505 at every isolation level).
        if conflict_on_duplicate:
            self._mark_created(domain.id)

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
        row = self._apply_tenant(self._domain_codec().encode_persistence_mapping(domain))
        await self._serialize_writes(lambda: [row])

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
        self._ensure_writable()
        patch = self._encode_patch(update)

        # Every owner either arm could touch — the row's, the one the update would move it to,
        # and the one a create would write — since which arm runs is decided below.
        created = (
            self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(self._build_domain(create, id))
            )
            if self._serialized_by()
            else None
        )
        await self._serialize_writes(lambda: [*self._sides(id, patch), created], lambda: [id])

        # Read, decide and write in one step with no await inside it: two concurrent upserts on
        # the same id cannot both observe "absent" and race into duplicate creates, and nothing
        # lands between the decision and the write it chose.
        with self.state.lock:
            if id in self._store():
                rev = self._to_domain(dict(self._store()[id])).rev
                written, serialized, _ = self._store_updated(id, rev, patch)

            else:
                written = self._build_domain(create, id)
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(written)
                )
                # ``ON CONFLICT DO NOTHING`` idempotency: a concurrent upsert of the same id must
                # not raise a unique violation (the real adapters converge silently), so the
                # create arm opts out of the publish-time duplicate guard.
                self._store_created(written, serialized, conflict_on_duplicate=False)

        await drain_domain_events(
            [written],
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[True] = True,
        set_based: Literal[False] = False,
    ) -> Sequence[R]: ...

    @overload
    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[False],
        set_based: bool = False,
    ) -> None: ...

    async def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: bool = True,
        set_based: bool = False,
    ) -> Sequence[R] | None:
        if set_based:
            # Refused as a real store refuses it; an allowed one writes what the usual path
            # writes, which the mock then takes.
            if return_new:
                raise exc.precondition(
                    "A set-based upsert_many reads nothing back; pass return_new=False.",
                    code="set_based_upsert_unsupported",
                )

            self.spec.require_set_based_upsert({type(it.update) for it in items} or None)

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
        merge: bool = True,
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
        merge: bool = True,
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
        merge: bool = True,
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
        merge: bool = True,
    ) -> JsonDict: ...

    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
        merge: bool = True,
    ) -> R | JsonDict | tuple[R, JsonDict] | None:
        # The mock keeps no history, so every update is strict; `merge` has nothing to merge.
        self._require_revision_to_check(merge=merge, rev=rev)
        self._ensure_writable()
        patch = self._encode_patch(dto)
        await self._serialize_writes(lambda: self._sides(pk, patch), lambda: [pk])

        with self.state.lock:
            updated, serialized, write_diff = self._store_updated(pk, rev, patch)

        await drain_domain_events(
            [updated],
            dispatcher_provider=lambda: self.dispatcher_provider(),
            document_name=self.spec.name,
        )

        if not return_new:
            return write_diff if return_diff else None

        read_result = self._to_read(serialized)

        return (read_result, write_diff) if return_diff else read_result

    def _store_updated(
        self,
        pk: UUID,
        rev: int,
        patch: JsonDict,
    ) -> tuple[D, JsonDict, JsonDict]:
        """Apply *patch* to row *pk*; the caller holds :attr:`state.lock` and the row's owners.

        :returns: The updated domain model, the row as stored, and the diff with its new rev.
        """

        current = self._to_domain(dict(self._ensure_exists(pk)))
        self._check_rev(current.rev, rev)

        updated, diff = current.update(patch, materialized=self.spec.materialized)

        # As the real stores: a sealed field the patch names is written, sealed afresh, though
        # its value is unchanged, since a key rotation re-encrypts by just such an update.
        encryption = self.spec.encryption
        sealed = encryption.encrypted | encryption.searchable if encryption is not None else ()
        named = {
            key
            for key in patch
            if key in sealed and key not in diff and getattr(updated, key) is not None
        }

        if named:
            diff = {**diff, **updated.model_dump(mode="python", include=named)}

            if LAST_UPDATE_AT_FIELD not in diff:
                diff[LAST_UPDATE_AT_FIELD] = utcnow()
                updated = updated.model_copy(
                    update={LAST_UPDATE_AT_FIELD: diff[LAST_UPDATE_AT_FIELD]}
                )

        if diff:
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)

        serialized = self._apply_tenant(self._domain_codec().encode_persistence_mapping(updated))
        self._write_row(self._store(), pk, serialized)

        # A rev-guarded write (caller supplied a rev) is the one read-committed must fail on a
        # concurrent same-row commit; a blind write (rev is None) is left to lose silently.
        if rev is not None:  # pyright: ignore[reportUnnecessaryComparison]
            self._mark_rev_guarded(pk)

        return updated, serialized, {**dict(diff), REV_FIELD: updated.rev} if diff else {}

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
        merge: bool = True,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
        merge: bool = True,
    ) -> Sequence[tuple[R, JsonDict]]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
        merge: bool = True,
    ) -> None: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
        merge: bool = True,
    ) -> Sequence[JsonDict]: ...

    async def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
        merge: bool = True,
    ) -> Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None:
        for u in updates:
            self._require_revision_to_check(merge=merge, rev=u.rev)

        if not updates:
            return [] if (return_new or return_diff) else None

        pks = [u.id for u in updates]
        if len(set(pks)) != len(pks):
            raise exc.precondition("update_many requires distinct id values in the batch")

        self._check_revisions_first([(u.id, u.rev) for u in updates])

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

        # Mirror the real backends, which write the patch as it is: the mock merges it.
        require_whole_update_matching(dto, document=str(self.spec.name))

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

        patch = self._encode_patch(dto)

        if not patch:
            return [] if return_new else 0

        results: list[R] = []
        mutated: list[D | None] = []
        staged: dict[UUID, JsonDict] = {}
        n = 0

        selects = self._matcher(filters)

        # Each row it selects, every owner the filter selects, and every owner the patch would
        # move a selected row to, before the section rather than inside it: a set-based update
        # is one statement on a real store and takes every lock it needs up front.
        await self._serialize_writes(
            lambda: [
                side
                for raw in list(self._store().values())
                if selects(raw)
                for side in (raw, self._destination(raw, patch))
            ],
            lambda: [pk for pk, raw in self._store().items() if selects(raw)],
        )

        with self.state.lock:
            store = self._store()
            merged = dict(store.items())

            for pk, raw in list(store.items()):
                if not selects(raw):
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
                self._check_guarantees(merged, pk, row, deferred=current_mvcc_tx() is None)

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
    async def touch(
        self, pk: UUID, *, rev: int | None = None, return_new: Literal[True] = True
    ) -> R: ...

    @overload
    async def touch(
        self, pk: UUID, *, rev: int | None = None, return_new: Literal[False]
    ) -> None: ...

    async def touch(self, pk: UUID, *, rev: int | None = None, return_new: bool = True) -> R | None:
        self._ensure_writable()
        await self._serialize_stored(pk)

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(updated)
            )
            self._write_row(self._store(), pk, serialized)

            # As for update: a rev-guarded touch fails on a concurrent commit to the row.
            if rev is not None:
                self._mark_rev_guarded(pk)

        return self._to_read(serialized) if return_new else None

    # ....................... #

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        return_new: Literal[False],
    ) -> None: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if revs is not None and len(revs) != len(pks):
            raise exc.precondition("touch_many requires one revision per id")

        if not pks:
            return [] if return_new else None

        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")

        pairs = list(zip(pks, revs if revs is not None else [None] * len(pks), strict=True))
        self._check_revisions_first(pairs)

        if return_new:
            return [await self.touch(pk, rev=rev, return_new=True) for pk, rev in pairs]

        for pk, rev in pairs:
            await self.touch(pk, rev=rev, return_new=False)

        return None

    # ....................... #

    async def hold(self, *owners: Mapping[str, Any]) -> None:
        require_hold_in_transaction(in_mock_transaction())
        keys = serialization_keys(
            self._serialized_by(),
            owners,
            domain=self._require_domain_model(),
            scope=str(self.spec.name),
            tenant=self.require_tenant_if_aware() if self.tenant_aware else None,
        )
        await self._take_write_locks(lambda: keys)

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        self.spec.require_hard_delete()
        self._ensure_writable()
        await self._serialize_stored(pk)

        with self.state.lock:
            _ = self._ensure_exists(pk)
            del self._store()[pk]

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        self.spec.require_hard_delete()

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
