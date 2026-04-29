"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from functools import cached_property
from typing import (
    Any,
    Literal,
    Protocol,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
    runtime_checkable,
)
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    assert_unique_ensure_ids,
    assert_unique_upsert_pairs,
    require_create_id_for_ensure,
    require_create_id_for_upsert,
)
from forze.application.contracts.query import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    encode_keyset_v1,
    normalize_sorts_with_id,
    row_value_for_sort_key,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError, InvalidOperationError
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_cache_dump,
    pydantic_cache_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ._logger import logger
from .txmanager import PostgresTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)
T = TypeVar("T", bound=BaseModel)

# ....................... #
#! Consider adding a method to bound or bind contextvars with 'name' as namespace or so
#! the above is related to logging


@runtime_checkable
class ReadModelWithIdAndRev(Protocol):
    id: UUID
    rev: int


# ....................... #


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

    write_gw: PostgresWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache: CachePort | None = attrs.field(default=None)
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

        if self.batch_size > 20000:
            logger.warning("Batch size is too large, using default value of 200")

            return 200

        return self.batch_size

    # ....................... #

    def _require_write(self) -> PostgresWriteGateway[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    def _check_id_rev_capability(self) -> bool:
        read_model = self.read_gw.model_type
        fields = set(read_model.model_fields.keys())
        required_fields = {ID_FIELD, REV_FIELD}

        return required_fields.issubset(fields)

    # ....................... #

    async def _set_cache(self, doc: R) -> None:
        if self.cache is None:
            return

        if not self._check_id_rev_capability():
            logger.warning(
                "Cannot cache document of type '%s' as it does not have an id and rev",
                type(self.read_gw.model_type).__name__,
            )
            return

        try:
            casted_doc = cast(ReadModelWithIdAndRev, doc)
            dump = pydantic_cache_dump(doc)
            await self.cache.set_versioned(
                str(casted_doc.id), str(casted_doc.rev), dump
            )

            logger.trace("Cache set successfully")

        except Exception:
            logger.exception("Cache set failed, continuing")

    # ....................... #

    async def _set_cache_many(self, docs: Sequence[R]) -> None:
        if self.cache is None or not docs:
            return

        if not self._check_id_rev_capability():
            logger.warning(
                "Cannot cache documents of type '%s' as they do not have an id and rev",
                type(self.read_gw.model_type).__name__,
            )
            return

        docs_casted = [cast(ReadModelWithIdAndRev, x) for x in docs]

        try:
            dumps = pydantic_cache_dump_many(docs)
            res_cache_map = {
                (str(x.id), str(x.rev)): y
                for x, y in zip(docs_casted, dumps, strict=True)
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
        if self.cache is None:
            return

        if not self._check_id_rev_capability():
            logger.warning(
                "Cannot clear cache for documents of type '%s' as they do not have an id and rev",
                type(self.read_gw.model_type).__name__,
            )
            return

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
        if not self._check_id_rev_capability():
            raise InvalidOperationError(
                f"Cannot get document of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

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

        if not self._check_id_rev_capability():
            raise InvalidOperationError(
                f"Cannot get many documents of type '{type(self.read_gw.model_type).__name__}' as they do not have defined id field"
            )

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
        hits_validated_cast = [cast(ReadModelWithIdAndRev, x) for x in hits_validated]
        miss_res_cast = [cast(ReadModelWithIdAndRev, x) for x in miss_res]

        by_pk = {x.id: x for x in hits_validated_cast}
        by_pk.update({x.id: x for x in miss_res_cast})

        results = [cast(R, by_pk[pk]) for pk in pks]

        return results

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
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[R]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True],
    ) -> Page[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[R]: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> (
        Page[R]
        | CountlessPage[R]
        | Page[T]
        | CountlessPage[T]
        | Page[JsonDict]
        | CountlessPage[JsonDict]
    ):
        if aggregates is not None and return_fields is not None:
            raise CoreError("Aggregates cannot be combined with return_fields")
        if aggregates is None and return_type is not None:
            raise CoreError("return_type requires aggregates")

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        logger.debug(
            "Finding '%s' documents (filter by %s, limit=%s, offset=%s, sorts=%s, return_count=%s)",
            self.spec.name,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
            limit if limit is not None else "N/A",
            offset if offset is not None else "N/A",
            sorts if sorts is not None else "N/A",
            return_count,
        )

        cnt = 0

        if return_count:
            cnt = (
                await self.read_gw.count_aggregates(filters, aggregates=aggregates)
                if aggregates is not None
                else await self.read_gw.count(filters)
            )
            if not cnt:
                logger.debug(
                    "No '%s' documents matching filters",
                    self.spec.name,
                )
                return page_from_limit_offset(
                    [],
                    pagination,
                    total=0,
                )
            logger.debug(
                "Found %s '%s' documents matching filters",
                cnt,
                self.spec.name,
            )

        res: list[Any]

        if aggregates is not None:
            res = await self.read_gw.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=aggregates,
                return_model=return_type,
            )
        else:
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
        normalized = normalize_sorts_with_id(sorts)
        sort_keys = [k for k, _ in normalized]
        directions = [d for _, d in normalized]
        if return_fields is not None and not all(f in return_fields for f in sort_keys):
            raise CoreError(
                "When using return_fields with cursor list, the projection must include "
                "all sort and tie-breaker fields (including id).",
            )

        c = dict(cursor or {})
        use_after = c.get("after") is not None
        use_before = c.get("before") is not None

        raw = await self.read_gw.find_many_with_cursor(  # type: ignore[call-overload, misc]
            filters,
            cursor=cursor,
            sorts=sorts,
            return_model=None,
            return_fields=return_fields,  # type: ignore[typeddict, arg-type, misc]
        )
        c2 = dict(cursor or {})
        lim: int = int(  # type: ignore[call-overload]
            (
                c2.get("limit")  # type: ignore[arg-type]
                if c2.get("limit") is not None
                else 10
            ),
        )
        has_more = len(raw) > lim
        page_raw = list(raw)[:lim]

        def _dump(o: R | JsonDict) -> JsonDict:
            if isinstance(o, dict):
                return o

            return o.model_dump(mode="json")  # type: ignore[union-attr, err]

        if has_more and page_raw:
            last = _dump(page_raw[-1])  # type: ignore[assignment, arg-type]
            next_tok = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=[row_value_for_sort_key(last, k) for k in sort_keys],
            )

        else:
            next_tok = None

        if page_raw and (use_after or (use_before and has_more)):
            first = _dump(page_raw[0])  # type: ignore[assignment, arg-type]
            prev_tok = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=[row_value_for_sort_key(first, k) for k in sort_keys],
            )

        else:
            prev_tok = None

        if return_fields is not None:
            return CursorPage(
                hits=cast(list[JsonDict], page_raw),
                next_cursor=next_tok,
                prev_cursor=prev_tok,
                has_more=has_more,
            )

        out = list(page_raw)  # type: ignore[typeddict, var-annotated]

        return CursorPage(
            hits=cast(list[R], out),  # type: ignore[redundant-cast]
            next_cursor=next_tok,
            prev_cursor=prev_tok,
            has_more=has_more,
        )

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
            await self._clear_cache(domain.id)
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
        w = self._require_write()

        if not dtos:
            logger.debug(
                "Empty list of payloads, skipping creation for '%s'",
                self.spec.name,
            )

            if not return_new:
                return None

            return []

        logger.debug("Creating %s '%s' documents", len(dtos), self.spec.name)

        domains = await w.create_many(dtos, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*[x.id for x in domains])
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

        logger.debug("Ensure 1 '%s' document", self.spec.name)

        domain = await w.ensure(dto)

        if not return_new:
            await self._clear_cache(domain.id)
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

        logger.debug("Ensure %s '%s' documents", len(dtos), self.spec.name)

        domains = await w.ensure_many(dtos, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*[x.id for x in domains])
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
        _ = require_create_id_for_upsert(create_dto)

        logger.debug("Upsert 1 '%s' document", self.spec.name)

        domain = await w.upsert(create_dto, update_dto)

        if not return_new:
            await self._clear_cache(domain.id)
            return None

        res, _ = await asyncio.gather(
            self.read_gw.get(domain.id),
            self._clear_cache(domain.id),
        )
        await self._set_cache(res)

        return res

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

        assert_unique_upsert_pairs(pairs)

        logger.debug("Upsert %s '%s' document pairs", len(pairs), self.spec.name)

        domains = await w.upsert_many(pairs, batch_size=self.eff_batch_size)

        if not return_new:
            await self._clear_cache(*[x.id for x in domains])
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
        w = self._require_write()

        logger.debug(
            "Updating 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

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

        logger.debug(
            "Updating %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

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
        w = self._require_write()

        logger.debug(
            "Touching 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

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
        w = self._require_write()

        logger.debug(
            "Hard-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        # _clear_cache and the DB exec are independent; run them concurrently.
        await asyncio.gather(
            w.kill(pk),
            self._clear_cache(pk),
        )

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

        # _clear_cache and the DB exec are independent; run them concurrently.
        await asyncio.gather(
            w.kill_many(pks, batch_size=self.eff_batch_size),
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
        w = self._require_write()

        logger.debug(
            "Soft-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

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
        w = self._require_write()

        logger.debug(
            "Restoring 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

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
