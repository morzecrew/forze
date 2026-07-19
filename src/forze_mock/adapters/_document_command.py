"""In-memory document command (write) operations for :class:`MockDocumentAdapter`."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
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
from forze.application.contracts.querying import QueryFilterExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import AggregateRoot
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
    from forze.domain.models import DomainEvent
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

    async def _dispatch_domain_events(self, domains: Sequence[D | None]) -> None:
        """Drain and dispatch domain events from any aggregate-root domains, in-tx.

        Mirrors the integration adapter: a no-op for non-aggregate documents; raises if
        an aggregate emitted events but no dispatcher is registered.
        """

        events: list[DomainEvent] = []

        for domain in domains:
            if isinstance(domain, AggregateRoot) and domain.has_pending_events:
                events.extend(domain.collect_events())

        if not events:
            return

        dispatcher = self.dispatcher_provider()

        if dispatcher is None:
            raise exc.configuration(
                f"Aggregate emitted domain events for document {self.spec.name!r} but "
                "no DomainEventsDepsModule is registered to dispatch them."
            )

        await dispatcher.dispatch(events)

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

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                # Mirror the integration adapters: Postgres maps a duplicate
                # primary key (UniqueViolation) to ``exc.conflict``.
                raise exc.conflict(
                    "Unique violation.",
                    details={"id": str(domain.id)},
                )
            store[domain.id] = serialized

            # Publish-time unique-violation guard: a concurrent transaction may commit the same id
            # between this statement and this transaction's commit; marking the create lets the MVCC
            # commit raise ``exc.conflict`` then rather than silently merging (matching Postgres,
            # which raises 23505 at every isolation level).
            if conflict_on_duplicate:
                self._mark_created(domain.id)

        await self._dispatch_domain_events([domain])

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

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                raw = dict(store[domain.id])
            else:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(domain)
                )
                store[domain.id] = serialized
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
    ) -> R | JsonDict | None | tuple[R, JsonDict]:
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
            self._store()[pk] = serialized

            # A rev-guarded write (caller supplied a rev) is the one read-committed must fail on a
            # concurrent same-row commit; a blind write (rev is None) is left to lose silently.
            if rev is not None:  # pyright: ignore[reportUnnecessaryComparison]
                self._mark_rev_guarded(pk)

            write_diff = {**dict(diff), REV_FIELD: updated.rev} if diff else {}

        await self._dispatch_domain_events([updated])

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
        n = 0

        match = self._matcher(filters)

        with self.state.lock:
            store = self._store()

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
                store[pk] = serialized
                mutated.append(updated)
                n += 1

                if return_new:
                    results.append(self._to_read(serialized))

        await self._dispatch_domain_events(mutated)

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

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = self._apply_tenant(
                self._domain_codec().encode_persistence_mapping(updated)
            )
            self._store()[pk] = serialized

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

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            if cast(Any, current).is_deleted:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(current)
                )
                self._store()[pk] = serialized

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
                self._store()[pk] = serialized

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

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            if not cast(Any, current).is_deleted:
                serialized = self._apply_tenant(
                    self._domain_codec().encode_persistence_mapping(current)
                )
                self._store()[pk] = serialized

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
                self._store()[pk] = serialized

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
