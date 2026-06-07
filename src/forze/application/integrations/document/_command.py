"""Document command port methods."""

import asyncio
from typing import Generic, Literal, Sequence, overload
from uuid import UUID

from forze.application.contracts.document import (
    require_create_id,
    require_create_id_for_many,
)
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
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        """Create a new document and populate the cache.

        :param dto: Creation payload.
        :returns: The created document as the read model.
        """

        w = self._require_write()

        domain = await w.create(dto)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        """Bulk-create documents and populate the cache.

        :param dtos: Creation payloads.
        """

        w = self._require_write()

        if not dtos:
            if not return_new:
                return None

            return []

        domains = await w.create_many(dtos, batch_size=self.eff_batch_size)
        pks_new = [x.id for x in domains]
        await self.document_cache.invalidate_keys_now(*pks_new)
        return await self._finalize_bulk_write(domains, return_new=return_new)

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
        w = self._require_write()
        require_create_id(dto)

        domain = await w.ensure(dto)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        w = self._require_write()

        if not dtos:
            if not return_new:
                return None

            return []

        require_create_id_for_many(dtos)

        domains = await w.ensure_many(dtos, batch_size=self.eff_batch_size)
        pks = [x.id for x in domains]
        await self.document_cache.invalidate_keys_now(*pks)
        return await self._finalize_bulk_write(domains, return_new=return_new)

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
        w = self._require_write()
        require_create_id(create_dto)

        domain = await w.upsert(create_dto, update_dto)
        await self.document_cache.invalidate_keys_now(domain.id)
        return await self._finalize_single_write(domain, return_new=return_new)

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
        w = self._require_write()

        if not pairs:
            if not return_new:
                return None
            return []

        require_create_id_for_many(pairs)

        domains = await w.upsert_many(pairs, batch_size=self.eff_batch_size)
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
        """Bulk-update documents and refresh the cache.

        :param pks: Document primary keys.
        :param dtos: Update payloads matching *pks* by position.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        if not updates:
            logger.debug(
                "Empty list of updates, skipping update for '%s'",
                self.spec.name,
            )

            if not return_new:
                return None

            return []

        pks = [x[0] for x in updates]
        revs = [x[1] for x in updates]
        dtos = [x[2] for x in updates]

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

            updates = list(zip(page_ids, page_revs, [dto] * len(page)))

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
