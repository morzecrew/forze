"""Mongo-backed document adapter implementing read and write port contracts."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

import contextlib
from typing import Optional, Sequence, TypeVar, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentReadPort, DocumentWritePort
from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.gateways import MongoReadGateway, MongoWriteGateway
from .txmanager import MongoTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentAdapter(
    DocumentReadPort[R], DocumentWritePort[R, D, C, U], TxScopedPort
):
    """Mongo adapter bridging domain document ports to gateway operations.

    Implements :class:`~forze.application.contracts.document.DocumentReadPort`
    and :class:`~forze.application.contracts.document.DocumentWritePort`.
    Read operations support an optional :class:`CachePort` for transparent
    caching with versioned invalidation. Write operations delegate to a
    :class:`MongoWriteGateway` and refresh the cache after mutation.
    """

    read_gw: MongoReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: Optional[MongoWriteGateway[D, C, U]] = None
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache: Optional[CachePort] = None
    """Optional cache layer for read-through caching."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=MongoTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.write_gw is not None
            and self.write_gw.client is not self.read_gw.client
        ):
            raise CoreError("Write and read gateways must use the same client")

    # ....................... #

    def _require_write(self) -> MongoWriteGateway[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    def _map_to_cache(self, doc: R) -> JsonDict:
        return pydantic_dump(
            doc,
            exclude={
                "none": True,
                "defaults": True,
                "computed_fields": True,
            },
            mode="json",
        )

    # ....................... #

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict:
        """Fetch a document projected to *return_fields*."""
        ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R:
        """Fetch a document as the read model."""
        ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> R | JsonDict:
        """Fetch a single document by primary key, using the cache when available.

        Cache is bypassed when *return_fields* is set or when no cache is
        configured.  On a cache miss the result is fetched from Mongo and
        written back to the cache with versioned invalidation.

        :param pk: Document primary key.
        :param for_update: Require a transaction context.
        :param return_fields: Optional field subset to project.
        """

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        try:
            cached = await self.cache.get(str(pk))
        except Exception:
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        if cached is not None:
            return pydantic_validate(self.read_gw.model, cached)

        res = await self.read_gw.get(pk)

        with contextlib.suppress(Exception):
            await self.cache.set_versioned(
                str(pk),
                str(res.rev),
                self._map_to_cache(res),
            )

        return res

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Sequence[JsonDict]:
        """Fetch multiple documents projected to *return_fields*."""
        ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Sequence[R]:
        """Fetch multiple documents as the read model."""
        ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        """Fetch multiple documents by primary key with cache-aware batching.

        Cached entries are returned from the cache; misses are fetched from
        Mongo and back-filled into the cache.

        :param pks: Primary keys to fetch.
        :param return_fields: Optional field subset to project.
        """

        if not pks:
            return []

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in pks])
        except Exception:
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        miss_res: list[R] = []

        if misses:
            miss_res = await self.read_gw.get_many([UUID(x) for x in misses])
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in miss_res}
                )

        by_pk: dict[str, R] = {
            k: pydantic_validate(self.read_gw.model, v) for k, v in hits.items()
        }
        by_pk.update({str(x.id): x for x in miss_res})

        return [by_pk[str(pk)] for pk in pks]

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]:
        """Find one document matching filters projected to *return_fields*."""
        ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]:
        """Find one document matching filters as the read model."""
        ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[R | JsonDict]:
        """Find a single document matching the given filters.

        :param filters: Query filter expression.
        :param for_update: Require a transaction context.
        :param return_fields: Optional field subset to project.
        :returns: The matching document or ``None``.
        """

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_fields=return_fields,
        )

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]:
        """Find documents projected to *return_fields* with total count."""
        ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]:
        """Find documents as the read model with total count."""
        ...

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        """Find documents with pagination and return the total matching count.

        Issues a count query first; if zero, returns early with an empty list.

        :param filters: Optional filter expression.
        :param limit: Maximum number of results.
        :param offset: Number of results to skip.
        :param sorts: Sort expression.
        :param return_fields: Optional field subset to project.
        :returns: A tuple of ``(results, total_count)``.
        """

        cnt = await self.read_gw.count(filters)

        if not cnt:
            return [], 0

        res = await self.read_gw.find_many(  # type: ignore[misc]
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            return_fields=return_fields,  # type: ignore[arg-type]
        )

        return res, cnt

    # ....................... #

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression.
        """

        return await self.read_gw.count(filters)

    # ....................... #

    async def create(self, dto: C) -> R:
        """Create a new document and populate the cache.

        :param dto: Creation payload.
        :returns: The created document as the read model.
        """

        w = self._require_write()
        domain = await w.create(dto)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(domain.id)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_versioned(
                    str(res.id),
                    str(res.rev),
                    self._map_to_cache(res),
                )

        return res

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        """Bulk-create documents and populate the cache.

        :param dtos: Creation payloads.
        """

        w = self._require_write()
        domains = await w.create_many(dtos)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many([x.id for x in domains])

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
                )

        return res

    # ....................... #

    async def _clear_cache(self, *pks: UUID) -> None:
        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.delete_many([str(pk) for pk in pks], hard=True)

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        """Update a document and refresh the cache.

        :param pk: Document primary key.
        :param dto: Update payload.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        await w.update(pk, dto, rev=rev)
        await self._clear_cache(pk)

        res = await self.read_gw.get(pk)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_versioned(
                    str(res.id),
                    str(res.rev),
                    self._map_to_cache(res),
                )

        return res

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Bulk-update documents and refresh the cache.

        :param pks: Document primary keys.
        :param dtos: Update payloads matching *pks* by position.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        await w.update_many(pks, dtos, revs=revs)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
                )

        return res

    # ....................... #

    async def touch(self, pk: UUID) -> R:
        """Touch a document (bump revision) and refresh the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

        await w.touch(pk)
        await self._clear_cache(pk)

        res = await self.read_gw.get(pk)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_versioned(
                    str(res.id),
                    str(res.rev),
                    self._map_to_cache(res),
                )

        return res

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        """Touch multiple documents and refresh the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()
        await w.touch_many(pks)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
                )

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document and evict it from the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()
        await w.kill(pk)
        await self._clear_cache(pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents and evict them from the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()
        await w.kill_many(pks)
        await self._clear_cache(*pks)

    # ....................... #

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        """Soft-delete a document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        await w.delete(pk, rev=rev)
        await self._clear_cache(pk)

        res = await self.read_gw.get(pk)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_versioned(
                    str(res.id),
                    str(res.rev),
                    self._map_to_cache(res),
                )

        return res

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Soft-delete multiple documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        await w.delete_many(pks, revs=revs)
        await self._clear_cache(*pks)

        res = await self.read_gw.get_many(pks)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
                )

        return res

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        """Restore a soft-deleted document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        await w.restore(pk, rev=rev)
        await self._clear_cache(pk)

        res = await self.read_gw.get(pk)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_versioned(
                    str(res.id),
                    str(res.rev),
                    self._map_to_cache(res),
                )

        return res

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Restore multiple soft-deleted documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()
        await w.restore_many(pks, revs=revs)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)

        if self.cache is not None:
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
                )

        return res
