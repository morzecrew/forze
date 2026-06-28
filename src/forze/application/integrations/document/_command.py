"""Document command port methods."""

import asyncio
from typing import Generic, Literal, Sequence, overload
from uuid import UUID

from forze.application.contracts.document import KeyedCreate, KeyedUpdate, UpsertItem
from forze.application.contracts.querying import QueryFilterExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import AggregateRoot, DomainEvent

from ..._logger import logger
from ._base import DocumentAdapterMixinBase, DocumentQueryDelegateMixin
from ._limits import check_page_limit
from ._types import C, D, R, U

# ----------------------- #


def _require_distinct_ids(ids: Sequence[UUID]) -> None:
    """Reject duplicate ids within a bulk ensure/upsert batch."""

    if len(set(ids)) != len(ids):
        raise exc.precondition(
            "ensure_many and upsert_many require distinct id values in the batch"
        )


# ....................... #


class DocumentCommandMixin(
    DocumentQueryDelegateMixin[R],
    DocumentAdapterMixinBase[R, D, C, U],
    Generic[R, D, C, U],
):
    """Command operations mixin for :class:`~.adapter.DocumentAdapter`.

    Composed with :class:`~._query.DocumentQueryMixin` on
    :class:`~.adapter.DocumentAdapter` (query mixin first in the MRO) so
    :meth:`project_many` is available to :meth:`update_matching_strict`.
    """

    async def _dispatch_domain_events(self, domains: Sequence[D | None]) -> None:
        """Drain and dispatch domain events from any aggregate-root domains, in-tx.

        A no-op for non-aggregate documents (the common case). Raises if an aggregate
        emitted events but no dispatcher is registered, so events are never dropped.
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
    async def create(
        self, payload: C, *, id: UUID | None = None, return_new: Literal[True] = True
    ) -> R: ...

    @overload
    async def create(
        self, payload: C, *, id: UUID | None = None, return_new: Literal[False]
    ) -> None: ...

    async def create(
        self, payload: C, *, id: UUID | None = None, return_new: bool = True
    ) -> R | None:
        """Create a new document and populate the cache.

        :param payload: Creation payload (domain fields only).
        :param id: Optional caller-chosen primary key; server-generated when omitted.
        :returns: The created document as the read model.
        """

        w = self._require_write()

        domain = await w.create(payload, id=id)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        """Bulk-create documents and populate the cache.

        :param payloads: Creation payloads (server-assigned ids).
        """

        w = self._require_write()

        if not payloads:
            if not return_new:
                return None

            return []

        domains = await w.create_many(payloads, batch_size=self.eff_batch_size)
        pks_new = [x.id for x in domains]
        await self.document_cache.invalidate_keys_now(*pks_new)
        return await self._finalize_bulk_write(domains, return_new=return_new)

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

    async def ensure(
        self, id: UUID, payload: C, *, return_new: bool = True
    ) -> R | None:
        w = self._require_write()

        domain = await w.ensure(id, payload)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        w = self._require_write()

        if not items:
            if not return_new:
                return None

            return []

        ids = [it.id for it in items]
        _require_distinct_ids(ids)
        payloads = [it.payload for it in items]

        domains = await w.ensure_many(ids, payloads, batch_size=self.eff_batch_size)
        pks = [x.id for x in domains]
        await self.document_cache.invalidate_keys_now(*pks)
        return await self._finalize_bulk_write(domains, return_new=return_new)

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
        w = self._require_write()

        domain = await w.upsert(id, create, update)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        w = self._require_write()

        if not items:
            if not return_new:
                return None
            return []

        ids = [it.id for it in items]
        _require_distinct_ids(ids)
        creates = [it.create for it in items]
        updates = [it.update for it in items]

        domains = await w.upsert_many(
            ids, creates, updates, batch_size=self.eff_batch_size
        )
        pks = [x.id for x in domains]
        await self.document_cache.invalidate_keys_now(*pks)
        return await self._finalize_bulk_write(domains, return_new=return_new)

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
        """Update a document and refresh the cache.

        :param pk: Document primary key.
        :param dto: Update payload.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        (domain, diff), _ = await asyncio.gather(
            w.update(pk, dto, rev=rev),
            self.document_cache.invalidate_keys_now(pk),
        )

        await self._dispatch_domain_events([domain])

        if not return_new:
            if return_diff:
                return diff

            return None

        res = await self._to_read(domain, pk=pk)
        await self.document_cache.after_commit_or_now(
            lambda: self.document_cache.set_one(res)
        )

        if return_diff:
            return res, diff

        return res

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
        """Bulk-update documents and refresh the cache.

        :param updates: One :class:`KeyedUpdate` per document (id, expected rev, patch);
            ids must be unique.
        """

        w = self._require_write()

        if not updates:
            logger.debug(
                "Empty list of updates, skipping update for '%s'",
                self.spec.name,
            )

            if not return_new:
                return [] if return_diff else None

            return []

        pks = [u.id for u in updates]

        if len(set(pks)) != len(pks):
            raise exc.precondition(
                "update_many requires distinct id values in the batch"
            )

        revs = [u.rev for u in updates]
        dtos = [u.dto for u in updates]

        (domains, diffs), _ = await asyncio.gather(
            w.update_many(pks, dtos, revs=revs, batch_size=self.eff_batch_size),
            self.document_cache.invalidate_keys_now(*pks),
        )

        await self._dispatch_domain_events(domains)

        if not return_new:
            if return_diff:
                return diffs

            return None

        res = await self._to_read_many(domains, pks=pks)
        await self.document_cache.after_commit_or_now(
            lambda: self.document_cache.set_many(res)
        )

        if return_diff:
            return list(zip(res, diffs, strict=True))

        return res

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
        w = self._require_write()

        logger.debug("update_matching (fast) on '%s'", self.spec.name)

        count, domains = await w.update_matching(
            filters,
            dto,
            batch_size=self.eff_batch_size,
        )
        pks = [d.id for d in domains]

        if pks:
            await self.document_cache.invalidate_keys_now(*pks)

        if not return_new:
            return count

        res = await self._finalize_bulk_write(domains, return_new=True)

        if res is None:
            raise exc.internal("Failed to finalize bulk write")

        return res

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
        self._require_write()

        eff_chunk = self.eff_batch_size if chunk_size is None else chunk_size

        if eff_chunk < 1:
            raise exc.precondition("chunk_size must be positive")

        logger.debug(
            "update_matching_strict on '%s' (chunk=%s)",
            self.spec.name,
            eff_chunk,
        )

        n_total = 0
        out: list[R] = []
        last_id: UUID | None = None
        page_num = 0

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=self.max_chunked_command_pages,
                label="update_matching_strict",
            )

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

            page = (
                await self.project_many(
                    [ID_FIELD, REV_FIELD],
                    filters=chunk_filter,
                    pagination={"limit": eff_chunk},
                    sorts={ID_FIELD: "asc"},
                )
            ).hits

            if not page:
                break

            page_ids = [UUID(str(r[ID_FIELD])) for r in page]
            page_revs = [int(r[REV_FIELD]) for r in page]

            updates = [
                KeyedUpdate(id=pk, rev=rev, dto=dto)
                for pk, rev in zip(page_ids, page_revs, strict=True)
            ]

            if return_new:
                got = await self.update_many(
                    updates,
                    return_new=True,
                )

                out.extend(got)

            else:
                await self.update_many(updates, return_new=False)

            n_total += len(page)
            last_id = page_ids[-1]

            if len(page) < eff_chunk:
                break

            page_num += 1

        if return_new:
            return out

        return n_total

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        """Touch a document (bump revision) and refresh the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

        domain, _ = await asyncio.gather(
            w.touch(pk),
            self.document_cache.invalidate_keys_now(pk),
        )

        return await self._finalize_single_write(domain, return_new=return_new, pk=pk)

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
        """Touch multiple documents and refresh the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()

        if not pks:
            if not return_new:
                return None

            return []

        domains, _ = await asyncio.gather(
            w.touch_many(pks, batch_size=self.eff_batch_size),
            self.document_cache.invalidate_keys_now(*pks),
        )

        return await self._finalize_bulk_write(domains, return_new=return_new)

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document and evict it from the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

        await asyncio.gather(
            w.kill(pk),
            self.document_cache.invalidate_keys_now(pk),
        )

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents and evict them from the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()

        if not pks:
            return

        await asyncio.gather(
            w.kill_many(pks, batch_size=self.eff_batch_size),
            self.document_cache.invalidate_keys_now(*pks),
        )
