"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Optional, Sequence, TypeVar, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentReadPort, DocumentWritePort
from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ._logger import logger
from .txmanager import PostgresTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter(
    DocumentReadPort[R],
    DocumentWritePort[R, D, C, U],
    TxScopedPort,
):
    """Postgres-backed implementation of :class:`DocumentReadPort` and :class:`DocumentWritePort`.

    Delegates to :class:`PostgresReadGateway` and :class:`PostgresWriteGateway` for
    database access. Supports optional :class:`CachePort` integration for
    read-through caching with versioned invalidation.
    """

    read_gw: PostgresReadGateway[R]
    write_gw: Optional[PostgresWriteGateway[D, C, U]] = None
    cache: Optional[CachePort] = None

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.write_gw is not None
            and self.write_gw.client is not self.read_gw.client
        ):
            raise CoreError("Write and read gateways must use the same client")

    # ....................... #

    @cached_property
    def _rgw_qname(self) -> str:
        return self.read_gw.model.__qualname__

    @cached_property
    def _wgw_qname(self) -> str:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw.model.__qualname__

    # ....................... #

    def _require_write(self) -> PostgresWriteGateway[D, C, U]:
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

    def _map_to_cache_many(self, docs: Sequence[R]) -> list[JsonDict]:
        return pydantic_dump_many(
            docs,
            exclude={
                "none": True,
                "defaults": True,
                "computed_fields": True,
            },
            mode="json",
        )

    # ....................... #

    async def _set_cache(self, doc: R) -> None:
        if self.cache is not None:
            try:
                await self.cache.set_versioned(
                    str(doc.id), str(doc.rev), self._map_to_cache(doc)
                )
                logger.trace("Cache set successfully")

            except Exception:
                logger.exception("Cache set failed, continuing")

    # ....................... #

    async def _set_cache_many(self, docs: Sequence[R]) -> None:
        if self.cache is not None:
            try:
                res_cache = self._map_to_cache_many(docs)
                res_cache_map = {
                    (str(x.id), str(x.rev)): y for x, y in zip(docs, res_cache)
                }
                await self.cache.set_many_versioned(res_cache_map)
                logger.trace(
                    "Cache set successfully for %s %s document(s)",
                    len(docs),
                    self._rgw_qname,
                )

            except Exception as e:
                logger.debug(
                    "Cache set failed for %s %s document(s), continuing",
                    len(docs),
                    self._rgw_qname,
                )

                logger.trace("Cache exception: %s", e)

    # ....................... #

    async def _clear_cache(self, *pks: UUID) -> None:
        if self.cache is not None:
            try:
                await self.cache.delete_many([str(pk) for pk in pks], hard=True)
                logger.trace(
                    "Cache cleared successfully for %s %s document(s)",
                    len(pks),
                    self._rgw_qname,
                )

            except Exception:
                logger.debug(
                    "Cache clear failed for %s %s document(s), continuing",
                    len(pks),
                    self._rgw_qname,
                )

    # ....................... #

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R: ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> R | JsonDict:
        logger.debug(
            "Fetching 1 %s document (pk=%s)",
            self._rgw_qname,
            pk,
        )

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        try:
            cached = await self.cache.get(str(pk))

        except Exception as e:
            logger.debug(
                "Cache get failed for 1 %s document, falling back to read gateway",
                self._rgw_qname,
            )

            logger.trace("Cache exception: %s", e)

            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        if cached is not None:
            logger.trace(
                "Retrieved 1 cached %s document",
                self._rgw_qname,
            )
            return pydantic_validate(self.read_gw.model, cached)

        logger.debug(
            "Fetching 1 %s document from database (cache miss)",
            self._rgw_qname,
        )
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
    ) -> Sequence[JsonDict]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Sequence[R]: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        if not pks:
            return []

        logger.debug(
            "Fetching %s %s document(s) (first_pk=%s)",
            len(pks),
            self._rgw_qname,
            pks[0],
        )

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in pks])

            if hits:
                logger.trace(
                    "Retrieved %s cached %s document(s)",
                    len(hits),
                    self._rgw_qname,
                )

        except Exception as e:
            logger.debug(
                "Cache get failed for %s %s document(s), falling back to read gateway",
                len(pks),
                self._rgw_qname,
            )

            logger.trace("Cache exception: %s", e)

            return await self.read_gw.get_many(pks, return_fields=return_fields)

        miss_res: list[R] = []

        if misses:
            logger.debug(
                "Fetching %s %s document(s) from database (cache miss)",
                len(misses),
                self._rgw_qname,
            )

            miss_res = await self.read_gw.get_many([UUID(x) for x in misses])
            await self._set_cache_many(miss_res)

        hits_validated = pydantic_validate_many(self.read_gw.model, list(hits.values()))
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
    ) -> Optional[JsonDict]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[R | JsonDict]:
        logger.debug(
            "Finding 1 %s document (filter by %s, for_update=%s)",
            self._rgw_qname,
            list(filters.keys()),  # type: ignore[attr-defined]
            for_update,
        )

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
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        logger.debug(
            "Finding %s documents (filter by %s, limit=%s, offset=%s, sorts=%s)",
            self._rgw_qname,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
            limit if limit is not None else "N/A",
            offset if offset is not None else "N/A",
            sorts if sorts is not None else "N/A",
        )

        cnt = await self.read_gw.count(filters)

        if not cnt:
            logger.debug(
                "No %s documents matching filters",
                self._rgw_qname,
            )
            return [], 0

        logger.debug(
            "Found %s %s documents matching filters",
            cnt,
            self._rgw_qname,
        )

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
        logger.debug(
            "Counting %s documents (filter by %s)",
            self._rgw_qname,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
        )

        return await self.read_gw.count(filters)

    # ....................... #

    async def create(self, dto: C) -> R:
        w = self._require_write()

        logger.debug("Creating 1 %s document", self._wgw_qname)

        domain = await w.create(dto)

        # Repeat read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(domain.id)
        await self._set_cache(res)

        return res

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        w = self._require_write()

        if not dtos:
            logger.debug(
                "Empty list of payloads, skipping creation for %s",
                self._wgw_qname,
            )
            return []

        logger.debug(
            "Creating %s %s documents",
            len(dtos),
            self._wgw_qname,
        )

        domains = await w.create_many(dtos)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many([x.id for x in domains])
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        logger.debug(
            "Updating 1 %s document (pk=%s)",
            self._rgw_qname,
            pk,
        )

        await w.update(pk, dto, rev=rev)
        await self._clear_cache(pk)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
        await self._set_cache(res)

        return res

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        if not pks or not dtos:
            logger.debug(
                "Empty list of primary keys or payloads, skipping update for %s",
                self._wgw_qname,
            )
            return []

        logger.debug(
            "Updating %s %s documents (first_pk=%s)",
            len(pks),
            self._wgw_qname,
            pks[0],
        )

        await w.update_many(pks, dtos, revs=revs)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def touch(self, pk: UUID) -> R:
        w = self._require_write()

        logger.debug(
            "Touching 1 %s document (pk=%s)",
            self._wgw_qname,
            pk,
        )

        await w.touch(pk)
        await self._clear_cache(pk)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
        await self._set_cache(res)

        return res

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping touch for %s",
                self._wgw_qname,
            )
            return []

        logger.debug(
            "Touching %s %s documents (first_pk=%s)",
            len(pks),
            self._wgw_qname,
            pks[0],
        )

        await w.touch_many(pks)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        w = self._require_write()

        logger.debug(
            "Hard-deleting 1 %s document (pk=%s)",
            self._rgw_qname,
            pk,
        )

        await w.kill(pk)
        await self._clear_cache(pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping hard-delete for %s",
                self._wgw_qname,
            )
            return None

        logger.debug(
            "Hard-deleting %s %s documents (first_pk=%s)",
            len(pks),
            self._rgw_qname,
            pks[0],
        )

        await w.kill_many(pks)
        await self._clear_cache(*pks)

    # ....................... #

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        logger.debug(
            "Soft-deleting 1 %s document (pk=%s)",
            self._rgw_qname,
            pk,
        )

        await w.delete(pk, rev=rev)
        await self._clear_cache(pk)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
        await self._set_cache(res)

        return res

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping soft-delete for %s",
                self._wgw_qname,
            )
            return []

        logger.debug(
            "Soft-deleting %s %s documents (first_pk=%s)",
            len(pks),
            self._wgw_qname,
            pks[0],
        )

        await w.delete_many(pks, revs=revs)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        logger.debug(
            "Restoring 1 %s document (pk=%s)",
            self._wgw_qname,
            pk,
        )

        await w.restore(pk, rev=rev)
        await self._clear_cache(pk)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
        await self._set_cache(res)

        return res

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping restore for %s",
                self._wgw_qname,
            )
            return []

        logger.debug(
            "Restoring %s %s documents (first_pk=%s)",
            len(pks),
            self._wgw_qname,
            pks[0],
        )

        await w.restore_many(pks, revs=revs)
        await self._clear_cache(*pks)

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res
