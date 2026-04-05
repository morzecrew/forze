"""Postgres adapter implementing the document read/write port contracts."""

from functools import cached_property

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Literal, Sequence, TypeVar, final, overload
from uuid import UUID

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
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

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ._logger import logger
from .txmanager import PostgresTxScopeKey

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
class PostgresDocumentAdapter(
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
    TxScopedPort,
):
    """Postgres-backed implementation of :class:`DocumentQueryPort` and :class:`DocumentCommandPort`."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: PostgresReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: PostgresWriteGateway[D, C, U] | None = None
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache: CachePort | None = None
    """Optional cache layer for read-through caching."""

    batch_size: int = 200
    """Batch size for writing."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

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

    def _require_write(self) -> PostgresWriteGateway[D, C, U]:
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
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict:
        logger.debug("Fetching 1 '%s' document (pk=%s)", self.spec.name, pk)

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        try:
            cached = await self.cache.get(str(pk))

        except Exception:
            logger.debug(
                "Cache get failed for 1 '%s' document, falling back to read gateway",
                self.spec.name,
                exc_info=True,
            )

            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        if cached is not None:
            logger.trace("Retrieved 1 cached '%s' document", self.spec.name)
            return pydantic_validate(self.read_gw.model_type, cached)

        logger.debug(
            "Fetching 1 '%s' document from database (cache miss)", self.spec.name
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
        return_fields: Sequence[str] | None = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        if not pks:
            return []

        logger.debug(
            "Fetching %s '%s' document(s) (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        if return_fields is not None or self.cache is None:
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in pks])

            if hits:
                logger.trace(
                    "Retrieved %s cached '%s' document(s)",
                    len(hits),
                    self.spec.name,
                )

        except Exception as e:
            logger.debug(
                "Cache get failed for %s '%s' document(s), falling back to read gateway",
                len(pks),
                self.spec.name,
                exc_info=True,
            )

            logger.trace("Cache exception: %s", e)

            return await self.read_gw.get_many(pks, return_fields=return_fields)

        miss_res: list[R] = []

        if misses:
            logger.debug(
                "Fetching %s '%s' document(s) from database (cache miss)",
                len(misses),
                self.spec.name,
            )

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
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R | None: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict | None:
        logger.debug(
            "Finding 1 '%s' document (filter by %s, for_update=%s)",
            self.spec.name,
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
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        logger.debug(
            "Finding '%s' documents (filter by %s, limit=%s, offset=%s, sorts=%s)",
            self.spec.name,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
            limit if limit is not None else "N/A",
            offset if offset is not None else "N/A",
            sorts if sorts is not None else "N/A",
        )

        cnt = await self.read_gw.count(filters)

        if not cnt:
            logger.debug(
                "No '%s' documents matching filters",
                self.spec.name,
            )
            return [], 0

        logger.debug(
            "Found %s '%s' documents matching filters",
            cnt,
            self.spec.name,
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

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        logger.debug(
            "Counting '%s' documents (filter by %s)",
            self.spec.name,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
        )

        return await self.read_gw.count(filters)

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug("Creating 1 '%s' document", self.spec.name)

        domain = await w.create(dto)

        if not return_new:
            return None

        # Repeat read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(domain.id)
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
        w = self._require_write()

        if not dtos:
            logger.debug(
                "Empty list of payloads, skipping creation for '%s'",
                self.spec.name,
            )
            return []

        logger.debug(
            "Creating %s '%s' documents",
            len(dtos),
            self.spec.name,
        )

        domains = await w.create_many(dtos, batch_size=self.eff_batch_size)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many([x.id for x in domains])
        await self._set_cache_many(res)

        return res

    # ....................... #

    @overload
    async def update(
        self, pk: UUID, rev: int, dto: U, *, return_new: Literal[True] = True
    ) -> R: ...

    @overload
    async def update(
        self, pk: UUID, rev: int, dto: U, *, return_new: Literal[False]
    ) -> None: ...

    async def update(
        self, pk: UUID, rev: int, dto: U, *, return_new: bool = True
    ) -> R | None:
        w = self._require_write()

        logger.debug(
            "Updating 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await w.update(pk, dto, rev=rev)
        await self._clear_cache(pk)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
        await self._set_cache(res)

        return res

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not updates:
            logger.debug(
                "Empty list of updates, skipping update for '%s'",
                self.spec.name,
            )
            return []

        pks = [x[0] for x in updates]
        revs = [x[1] for x in updates]
        dtos = [x[2] for x in updates]

        logger.debug(
            "Updating %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await w.update_many(pks, dtos, revs=revs, batch_size=self.eff_batch_size)
        await self._clear_cache(*pks)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug(
            "Touching 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await w.touch(pk)
        await self._clear_cache(pk)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
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
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping touch for '%s'",
                self.spec.name,
            )
            return []

        logger.debug(
            "Touching %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await w.touch_many(pks, batch_size=self.eff_batch_size)
        await self._clear_cache(*pks)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        w = self._require_write()

        logger.debug(
            "Hard-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await w.kill(pk)
        await self._clear_cache(pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping hard-delete for '%s'",
                self.spec.name,
            )
            return None

        logger.debug(
            "Hard-deleting %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await w.kill_many(pks, batch_size=self.eff_batch_size)
        await self._clear_cache(*pks)

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
        w = self._require_write()

        logger.debug(
            "Soft-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await w.delete(pk, rev=rev)
        await self._clear_cache(pk)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
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
        w = self._require_write()

        if not deletes:
            logger.debug(
                "Empty list of deletes, skipping soft-delete for '%s'",
                self.spec.name,
            )
            return []

        pks = [x[0] for x in deletes]
        revs = [x[1] for x in deletes]

        logger.debug(
            "Soft-deleting %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await w.delete_many(pks, revs=revs, batch_size=self.eff_batch_size)
        await self._clear_cache(*pks)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
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
        w = self._require_write()

        logger.debug(
            "Restoring 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await w.restore(pk, rev=rev)
        await self._clear_cache(pk)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get(pk)
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
        w = self._require_write()

        if not restores:
            logger.debug(
                "Empty list of restores, skipping restore for '%s'",
                self.spec.name,
            )
            return []

        pks = [x[0] for x in restores]
        revs = [x[1] for x in restores]

        logger.debug(
            "Restoring %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await w.restore_many(pks, revs=revs, batch_size=self.eff_batch_size)
        await self._clear_cache(*pks)

        if not return_new:
            return None

        # Repeate read is required to meet criteria for diverse read and write sources
        res = await self.read_gw.get_many(pks)
        await self._set_cache_many(res)

        return res
