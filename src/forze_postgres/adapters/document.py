from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Optional, Sequence, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.document import (
    DocumentCachePort,
    DocumentPort,
    DocumentSearchOptions,
)
from forze.application.contracts.query import FilterExpression, SortExpression
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.gateways import (
    PostgresReadGateway,
    PostgresSearchGateway,
    PostgresWriteGateway,
)
from .txmanager import PostgresTxScopeKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](DocumentPort[R, D, C, U], TxScopedPort):
    read_gw: PostgresReadGateway[R]
    write_gw: Optional[PostgresWriteGateway[D, C, U]] = None
    search_gw: Optional[PostgresSearchGateway[R]] = None
    cache: Optional[DocumentCachePort] = None

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.write_gw is not None
            and self.write_gw.client is not self.read_gw.client
        ):
            raise CoreError("Write and read gateways must use the same client")

        if (
            self.search_gw is not None
            and self.read_gw.client is not self.search_gw.client
        ):
            raise CoreError("Search and read gateways must use the same client")

    # ....................... #

    def _require_write(self) -> PostgresWriteGateway[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    def _require_search(self) -> PostgresSearchGateway[R]:
        if self.search_gw is None:
            raise CoreError("Search gateway is not configured")

        return self.search_gw

    # ....................... #

    def _map_to_cache(self, doc: R) -> JsonDict:
        return pydantic_dump(
            doc,
            exclude={
                "none": True,
                "defaults": True,
                "computed_fields": True,
            },
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
        if return_fields is not None or self.cache is None:
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        cached = await self.cache.get(pk)

        if cached is not None:
            return pydantic_validate(self.read_gw.model, cached)

        res = await self.read_gw.get(pk)

        await self.cache.set(pk, res.rev, self._map_to_cache(res))

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
        if return_fields is not None or self.cache is None:
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        hits, misses = await self.cache.get_many(pks)
        miss_res: list[R] = []

        if misses:
            miss_res = await self.read_gw.get_many(misses)
            miss_mapping = {(x.id, x.rev): self._map_to_cache(x) for x in miss_res}

            await self.cache.set_many(miss_mapping)

        by_pk: dict[UUID, R] = {
            k: pydantic_validate(self.read_gw.model, v) for k, v in hits.items()
        }
        by_pk.update({x.id: x for x in miss_res})

        return [by_pk[pk] for pk in pks]

    # ....................... #

    @overload
    async def find(
        self,
        filters: FilterExpression,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]: ...

    @overload
    async def find(
        self,
        filters: FilterExpression,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]: ...

    async def find(
        self,
        filters: FilterExpression,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[R | JsonDict]:
        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_fields=return_fields,
        )

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: Optional[FilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[SortExpression] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        cnt = await self.read_gw.count(filters)

        if not cnt:
            return [], 0

        res = await self.read_gw.find_many(
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            return_fields=return_fields,
        )

        return res, cnt

    # ....................... #

    async def count(self, filters: Optional[FilterExpression] = None) -> int:
        return await self.read_gw.count(filters)

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        *,
        options: Optional[DocumentSearchOptions] = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        *,
        options: Optional[DocumentSearchOptions] = ...,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[SortExpression] = None,
        *,
        options: Optional[DocumentSearchOptions] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        s = self._require_search()

        cnt = await s.search_count(query, filters, options=options)

        if not cnt:
            return [], 0

        res = await s.search(
            query=query,
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            options=options,
            return_fields=return_fields,
        )

        return res, cnt

    # ....................... #

    async def create(self, dto: C) -> R:
        w = self._require_write()
        domain = await w.create(dto)

        return await self.get(domain.id)

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        w = self._require_write()
        domains = await w.create_many(dtos)

        return await self.get_many([x.id for x in domains])

    # ....................... #

    async def _clear_cache(self, *pks: UUID) -> None:
        if self.cache is not None:
            await self.cache.delete_many(pks, hard=True)

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        w = self._require_write()
        domain = await w.update(pk, dto, rev=rev)

        await self._clear_cache(pk)

        return await self.get(domain.id)

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        await w.update_many(pks, dtos, revs=revs)
        await self._clear_cache(*pks)

        return await self.get_many(pks)

    # ....................... #

    async def touch(self, pk: UUID) -> R:
        w = self._require_write()

        await w.touch(pk)
        await self._clear_cache(pk)

        return await self.get(pk)

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        w = self._require_write()

        await w.touch_many(pks)
        await self._clear_cache(*pks)

        return await self.get_many(pks)

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        w = self._require_write()

        await w.kill(pk)
        await self._clear_cache(pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        w = self._require_write()

        await w.kill_many(pks)
        await self._clear_cache(*pks)

    # ....................... #

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        await w.delete(pk, rev=rev)
        await self._clear_cache(pk)

        return await self.get(pk)

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        await w.delete_many(pks, revs=revs)
        await self._clear_cache(*pks)

        return await self.get_many(pks)

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        await w.restore(pk, rev=rev)
        await self._clear_cache(pk)

        return await self.get(pk)

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        w = self._require_write()

        await w.restore_many(pks, revs=revs)
        await self._clear_cache(*pks)

        return await self.get_many(pks)
