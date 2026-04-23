"""Mongo-backed document adapter implementing read and write port contracts."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

import asyncio
from functools import cached_property
from typing import Literal, Sequence, TypeVar, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    assert_unique_ensure_ids,
    require_create_id_for_ensure,
)
from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_cache_dump,
    pydantic_cache_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.gateways import MongoReadGateway, MongoWriteGateway
from ._logger import logger
from .txmanager import MongoTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #
#! Consider adding a method to bound or bind contextvars with 'name' as namespace or so
#! the above is related to logging


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentAdapter(
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
    TxScopedPort,
):
    """Mongo adapter bridging domain document ports to gateway operations."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: MongoReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: MongoWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache: CachePort | None = attrs.field(default=None)
    """Optional cache layer for read-through caching."""

    batch_size: int = 200
    """Batch size for writing."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=MongoTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.write_gw is not None:
            if self.write_gw.client is not self.read_gw.client:
                raise CoreError("Write and read gateways must use the same client")

            if self.write_gw.tenant_aware != self.read_gw.tenant_aware:
                raise CoreError(
                    "Write and read gateways must have the same tenant awareness."
                )

    # ....................... #

    @cached_property
    def eff_batch_size(self) -> int:
        if self.batch_size < 10:
            logger.warning("Batch size is too small, using default value of 200")

            return 200

        if self.batch_size > 1000:
            logger.warning("Batch size is too large, using default value of 200")

            return 200

        return self.batch_size

    # ....................... #

    def _require_write(self) -> MongoWriteGateway[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    async def _set_cache(self, doc: R) -> None:
        if self.cache is not None:

            try:
                dump = pydantic_cache_dump(doc)
                await self.cache.set_versioned(str(doc.id), str(doc.rev), dump)

                logger.trace("Cache set successfully")

            except Exception:
                logger.exception("Cache set failed, continuing")

    # ....................... #

    async def _set_cache_many(self, docs: Sequence[R]) -> None:
        if self.cache is not None:
            try:
                dumps = pydantic_cache_dump_many(docs)
                res_cache_map = {
                    (str(x.id), str(x.rev)): y for x, y in zip(docs, dumps, strict=True)
                }
                await self.cache.set_many_versioned(res_cache_map)

            except Exception:
                logger.debug(
                    "Cache set failed for %s '%s' document(s), continuing",
                    len(docs),
                    self.spec.name,
                    exc_info=True,
                )

    # ....................... #

    async def _clear_cache(self, *pks: UUID) -> None:
        if self.cache is not None:
            try:
                await self.cache.delete_many([str(pk) for pk in pks], hard=True)

            except Exception:
                logger.debug(
                    "Cache clear failed for %s '%s' document(s), continuing",
                    len(pks),
                    self.spec.name,
                    exc_info=True,
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
        return_fields: Sequence[str] | None = None,
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
            return pydantic_validate(self.read_gw.model_type, cached)

        res = await self.read_gw.get(pk)
        await self._set_cache(res)

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
        return_fields: Sequence[str] | None = None,
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
            await self._set_cache_many(miss_res)

        hits_validated = pydantic_validate_many(
            self.read_gw.model_type, list(hits.values())
        )
        by_pk = {x.id: x for x in hits_validated}
        by_pk.update({x.id: x for x in miss_res})

        return [by_pk[pk] for pk in pks]

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None:
        """Find one document matching filters projected to *return_fields*."""
        ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R | None:
        """Find one document matching filters as the read model."""
        ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict | None:
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
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]:
        """Find documents projected to *return_fields* (no count query)."""
        ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[R]:
        """Find documents as the read model (no count query)."""
        ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
        return_count: Literal[True],
    ) -> Page[JsonDict]:
        """Find documents projected to *return_fields* with total count."""
        ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[R]:
        """Find documents as the read model with total count."""
        ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> Page[R] | CountlessPage[R] | Page[JsonDict] | CountlessPage[JsonDict]:
        """Find documents with optional pagination, sort, and total count."""

        pagination = pagination or {}
        cnt = 0
        if return_count:
            cnt = await self.read_gw.count(filters)
            if not cnt:
                return page_from_limit_offset(
                    [],
                    pagination,
                    total=0,
                )

        limit = pagination.get("limit")
        offset = pagination.get("offset")

        res = await self.read_gw.find_many(  # type: ignore[misc]
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            return_fields=return_fields,  # type: ignore[arg-type]
        )

        if return_count:
            return page_from_limit_offset(  # type: ignore[return-value]
                list(res),
                pagination,
                total=cnt,
            )
        return page_from_limit_offset(list(res), pagination, total=None)  # type: ignore[return-value]

    # ....................... #

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
    ) -> CursorPage[R]: ...

    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
    ) -> CursorPage[R] | CursorPage[JsonDict]:
        del filters, cursor, sorts, return_fields
        raise NotImplementedError(
            "MongoDocumentAdapter.find_many_with_cursor is not implemented yet; "
            "requires DocumentSpec or gateway support for a stable keyset order and "
            "encoded cursor (see forze.application.contracts.document.ports module doc).",
        )

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression.
        """

        return await self.read_gw.count(filters)

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

        if not return_new:
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get(domain.id),
            self._clear_cache(domain.id),
        )
        await self._set_cache(res)

        return res

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
            return []

        domains = await w.create_many(dtos, batch_size=self.eff_batch_size)

        if not return_new:
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        pks_new = [x.id for x in domains]

        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks_new),
            self._clear_cache(*pks_new),
        )
        await self._set_cache_many(res)

        return res

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
        _ = require_create_id_for_ensure(dto)
        domain = await w.ensure(dto)

        if not return_new:
            return None
        res, _ = await asyncio.gather(
            self.read_gw.get(domain.id),
            self._clear_cache(domain.id),
        )
        await self._set_cache(res)
        return res

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
        assert_unique_ensure_ids(dtos)
        domains = await w.ensure_many(dtos, batch_size=self.eff_batch_size)
        if not return_new:
            return None
        pks = [x.id for x in domains]
        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks),
            self._clear_cache(*pks),
        )
        await self._set_cache_many(res)
        return res

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

        _, diff = await w.update(pk, dto, rev=rev)

        if not return_new:
            await self._clear_cache(pk)

            if return_diff:
                return diff

            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get(pk),
            self._clear_cache(pk),
        )
        await self._set_cache(res)

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

        _, diffs = await w.update_many(
            pks,
            dtos,
            revs=revs,
            batch_size=self.eff_batch_size,
        )

        if not return_new:
            await self._clear_cache(*pks)

            if return_diff:
                return diffs

            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks),
            self._clear_cache(*pks),
        )
        await self._set_cache_many(res)

        if return_diff:
            return list(zip(res, diffs, strict=True))

        return res

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

        await w.touch(pk)

        if not return_new:
            await self._clear_cache(pk)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get(pk),
            self._clear_cache(pk),
        )
        await self._set_cache(res)

        return res

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

        await w.touch_many(pks, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*pks)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # touch_many + _clear_cache are independent of the subsequent read.
        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks),
            self._clear_cache(*pks),
        )
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document and evict it from the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

        await asyncio.gather(
            w.kill(pk),
            self._clear_cache(pk),
        )

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents and evict them from the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()

        await asyncio.gather(
            w.kill_many(pks),
            self._clear_cache(*pks),
        )

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
        """Soft-delete a document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        await w.delete(pk, rev=rev)

        if not return_new:
            await self._clear_cache(pk)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get(pk),
            self._clear_cache(pk),
        )
        await self._set_cache(res)

        return res

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
        """Soft-delete multiple documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        pks = [x[0] for x in deletes]
        revs = [x[1] for x in deletes]

        await w.delete_many(pks, revs=revs, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*pks)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # delete_many + _clear_cache are independent of the subsequent read.
        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks),
            self._clear_cache(*pks),
        )
        await self._set_cache_many(res)

        return res

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
        """Restore a soft-deleted document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

        await w.restore(pk, rev=rev)

        if not return_new:
            await self._clear_cache(pk)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # _clear_cache and the DB read are independent; run them concurrently.
        res, _ = await asyncio.gather(
            self.read_gw.get(pk),
            self._clear_cache(pk),
        )
        await self._set_cache(res)

        return res

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
        """Restore multiple soft-deleted documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        pks = [x[0] for x in restores]
        revs = [x[1] for x in restores]

        await w.restore_many(pks, revs=revs, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*pks)
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        # restore_many + _clear_cache are independent of the subsequent read.
        res, _ = await asyncio.gather(
            self.read_gw.get_many(pks),
            self._clear_cache(*pks),
        )
        await self._set_cache_many(res)

        return res
