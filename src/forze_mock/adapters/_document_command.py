"""In-memory document command (write) operations for :class:`MockDocumentAdapter`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Literal, Sequence, cast, overload
from uuid import UUID

from forze.application.contracts.document import (
    require_create_id,
    require_create_id_for_many,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import AggregateRoot
from forze_mock.query._types import C, D, R, U
from forze_mock.query.matching import _match_filters  # type: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

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
        def _create_codec(self) -> ModelCodec[D, Any]: ...
        def _domain_codec(self) -> ModelCodec[D, Any]: ...
        def _patch_codec(self) -> ModelCodec[Any, Any]: ...
        def _require_domain_model(self) -> type[D]: ...
        def project_many(
            self,
            fields: Sequence[str],
            filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
            pagination: PaginationExpression | None = None,
            sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
        ) -> Awaitable[CountlessPage[JsonDict]]: ...

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

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        self._require_domain_model()
        domain = self._create_codec().transform(dto)
        serialized = self._domain_codec().encode_persistence_mapping(domain)

        if self.tenant_aware:
            tid = self.require_tenant_if_aware()
            if tid is not None:
                serialized = dict(serialized)
                serialized["tenant_id"] = str(tid)
        with self.state.lock:
            store = self._store()
            store[domain.id] = serialized

        await self._dispatch_domain_events([domain])

        if not return_new:
            return None
        return self._to_read(serialized)

    # ....................... #

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not dtos:
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.create(dto, return_new=True) for dto in dtos]
        for dto in dtos:
            await self.create(dto, return_new=False)
        return None

    # ....................... #

    @overload
    async def ensure(
        self,
        dto: C,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def ensure(
        self,
        dto: C,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure(self, dto: C, *, return_new: bool = True) -> R | None:
        require_create_id(dto)

        self._require_domain_model()
        domain = self._create_codec().transform(dto)

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                raw = dict(store[domain.id])
            else:
                serialized = self._domain_codec().encode_persistence_mapping(domain)
                store[domain.id] = serialized
                raw = serialized
        if not return_new:
            return None
        return self._to_read(raw)

    # ....................... #

    @overload
    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not dtos:
            if not return_new:
                return None
            return []

        require_create_id_for_many(dtos)

        if return_new:
            return [await self.ensure(dto, return_new=True) for dto in dtos]
        for dto in dtos:
            await self.ensure(dto, return_new=False)
        return None

    # ....................... #

    @overload
    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: bool = True,
    ) -> R | None:
        require_create_id(create_dto)

        self._require_domain_model()
        domain = self._create_codec().transform(create_dto)
        with self.state.lock:
            if domain.id in self._store():
                rev = self._to_domain(dict(self._store()[domain.id])).rev
            else:
                rev = None
        if rev is not None:
            return await self.update(  # type: ignore[call-overload]
                domain.id,
                rev,
                update_dto,
                return_new=return_new,
            )
        return await self.create(create_dto, return_new=return_new)  # type: ignore[call-overload]

    # ....................... #

    @overload
    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not pairs:
            if not return_new:
                return None
            return []

        require_create_id_for_many(pairs)

        if return_new:
            return [await self.upsert(c, u, return_new=True) for c, u in pairs]

        for c, u in pairs:
            await self.upsert(c, u, return_new=False)

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
        patch = self._patch_codec().encode_persistence_mapping(
            cast(Any, dto),
            exclude={"unset": True},
        )

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            updated, diff = current.update(patch)
            if diff:
                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)

            serialized = self._domain_codec().encode_persistence_mapping(updated)
            self._store()[pk] = serialized

            if diff:
                write_diff: JsonDict = {**dict(diff), REV_FIELD: updated.rev}
            else:
                write_diff = {}

        await self._dispatch_domain_events([updated])

        if not return_new:
            if return_diff:
                return write_diff

            return None

        read_result = self._to_read(serialized)

        if return_diff:
            return read_result, write_diff

        return read_result

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Sequence[tuple[R, JsonDict]]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Sequence[JsonDict]: ...

    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None:
        if not updates:
            if not return_new:
                return None

            return []

        pks = [u[0] for u in updates]
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")

        if return_new:
            if return_diff:
                return [
                    await self.update(pk, r, dto, return_new=True, return_diff=True)
                    for pk, r, dto in updates
                ]

            return [
                await self.update(pk, r, dto, return_new=True, return_diff=False)
                for pk, r, dto in updates
            ]

        if return_diff:
            return [
                await self.update(pk, r, dto, return_new=False, return_diff=True)
                for pk, r, dto in updates
            ]

        for pk, r, dto in updates:
            await self.update(pk, r, dto, return_new=False)

        return None

    # ....................... #

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
    ) -> int: ...

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
    ) -> Sequence[R] | int:
        if not self.spec.supports_update():
            raise exc.internal("Update command type is not supported for this model")

        patch = self._patch_codec().encode_persistence_mapping(
            cast(Any, dto),
            exclude={"unset": True},
        )

        if not patch:
            return [] if return_new else 0

        results: list[R] = []
        mutated: list[D | None] = []
        n = 0

        with self.state.lock:
            store = self._store()
            for pk, raw in list(store.items()):
                if not _match_filters(raw, filters):
                    continue

                current = self._to_domain(dict(raw))
                updated, diff = current.update(patch)

                if not diff:
                    continue

                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                store[pk] = serialized
                mutated.append(updated)
                n += 1

                if return_new:
                    results.append(self._to_read(serialized))

        await self._dispatch_domain_events(mutated)

        if return_new:
            return results

        return n

    # ....................... #

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
        chunk_size: int | None = ...,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
        chunk_size: int | None = ...,
    ) -> int: ...

    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
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
            chunk_filter: QueryFilterExpression = (  # type: ignore[valid-type]
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

            updates = [(UUID(str(r[ID_FIELD])), int(r[REV_FIELD]), dto) for r in rows]

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

        if return_new:
            return out

        return n_total

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = self._domain_codec().encode_persistence_mapping(updated)
            self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
            if not return_new:
                return None

            return []
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")
        if return_new:
            return [await self.touch(pk, return_new=True) for pk in pks]
        for pk in pks:
            await self.touch(pk, return_new=False)
        return None

    # ....................... #

    async def kill(self, pk: UUID) -> None:
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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if cast(Any, current).is_deleted:
                serialized = self._domain_codec().encode_persistence_mapping(current)
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
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
            if not return_new:
                return None

            return []
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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if not cast(Any, current).is_deleted:
                serialized = self._domain_codec().encode_persistence_mapping(current)
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
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.restore(pk, r, return_new=True) for pk, r in restores]
        for pk, r in restores:
            await self.restore(pk, r, return_new=False)
        return None
