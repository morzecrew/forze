from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import contextlib
from typing import Optional, Sequence, TypeVar, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentPort
from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.gateways import (
    PostgresReadGateway,
    PostgresWriteGateway,
)
from .txmanager import PostgresTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter(DocumentPort[R, D, C, U], TxScopedPort):
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
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]: ...

    async def find(
        self,
        filters: QueryFilterExpression,
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
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
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

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:
        return await self.read_gw.count(filters)

    # ....................... #

    async def create(self, dto: C) -> R:
        w = self._require_write()
        domain = await w.create(dto)

        res = pydantic_validate(self.read_gw.model, domain.model_dump(mode="json"))

        if self.cache is not None:
            await self.cache.set_versioned(
                str(res.id),
                str(res.rev),
                self._map_to_cache(res),
            )

        return res

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        w = self._require_write()
        domains = await w.create_many(dtos)

        res = [
            pydantic_validate(self.read_gw.model, x.model_dump(mode="json"))
            for x in domains
        ]

        if self.cache is not None:
            await self.cache.set_many_versioned(
                {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
            )

        return res

    # ....................... #

    async def _clear_cache(self, *pks: UUID) -> None:
        if self.cache is not None:
            await self.cache.delete_many([str(pk) for pk in pks], hard=True)

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        w = self._require_write()
        domain = await w.update(pk, dto, rev=rev)

        await self._clear_cache(pk)

        res = pydantic_validate(self.read_gw.model, domain.model_dump(mode="json"))

        if self.cache is not None:
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
        w = self._require_write()

        domains = await w.update_many(pks, dtos, revs=revs)
        await self._clear_cache(*pks)

        res = [
            pydantic_validate(self.read_gw.model, x.model_dump(mode="json"))
            for x in domains
        ]

        if self.cache is not None:
            await self.cache.set_many_versioned(
                {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
            )

        return res

    # ....................... #

    async def touch(self, pk: UUID) -> R:
        w = self._require_write()

        domain = await w.touch(pk)
        await self._clear_cache(pk)

        res = pydantic_validate(self.read_gw.model, domain.model_dump(mode="json"))

        if self.cache is not None:
            await self.cache.set_versioned(
                str(res.id),
                str(res.rev),
                self._map_to_cache(res),
            )

        return res

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        w = self._require_write()

        domains = await w.touch_many(pks)
        await self._clear_cache(*pks)

        res = [
            pydantic_validate(self.read_gw.model, x.model_dump(mode="json"))
            for x in domains
        ]

        if self.cache is not None:
            await self.cache.set_many_versioned(
                {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
            )

        return res

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

        domain = await w.delete(pk, rev=rev)
        await self._clear_cache(pk)

        res = pydantic_validate(self.read_gw.model, domain.model_dump(mode="json"))

        if self.cache is not None:
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
        w = self._require_write()

        domains = await w.delete_many(pks, revs=revs)
        await self._clear_cache(*pks)

        res = [
            pydantic_validate(self.read_gw.model, x.model_dump(mode="json"))
            for x in domains
        ]

        if self.cache is not None:
            await self.cache.set_many_versioned(
                {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
            )

        return res

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        w = self._require_write()

        domain = await w.restore(pk, rev=rev)
        await self._clear_cache(pk)

        res = pydantic_validate(self.read_gw.model, domain.model_dump(mode="json"))

        if self.cache is not None:
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
        w = self._require_write()

        domains = await w.restore_many(pks, revs=revs)
        await self._clear_cache(*pks)

        res = [
            pydantic_validate(self.read_gw.model, x.model_dump(mode="json"))
            for x in domains
        ]

        if self.cache is not None:
            await self.cache.set_many_versioned(
                {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in res}
            )

        return res
