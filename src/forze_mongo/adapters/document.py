from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

import contextlib
from typing import Any, Optional, Sequence, TypeVar, cast, final, overload
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentPort
from forze.application.contracts.query import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    NotFoundError,
    ValidationError,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_field_names,
    pydantic_validate,
)
from forze.domain.constants import ID_FIELD, REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..kernel.platform import MongoClient
from ..kernel.query import MongoQueryRenderer
from .txmanager import MongoTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentAdapter(DocumentPort[R, D, C, U], TxScopedPort):
    client: MongoClient
    read_model: type[R]
    domain_model: type[D]
    create_dto: type[C]
    update_dto: type[U]
    read_source: str
    write_source: Optional[str] = None
    db_name: Optional[str] = None
    cache: Optional[CachePort] = None
    renderer: MongoQueryRenderer = attrs.field(factory=MongoQueryRenderer)

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=MongoTxScopeKey, init=False)

    # ....................... #

    def _read_coll(self) -> AsyncCollection[JsonDict]:
        return self.client.collection(self.read_source, db_name=self.db_name)

    # ....................... #

    def _write_coll(self) -> AsyncCollection[JsonDict]:
        return self.client.collection(
            self.write_source or self.read_source,
            db_name=self.db_name,
        )

    # ....................... #

    def _storage_pk(self, pk: UUID) -> str:
        return str(pk)

    # ....................... #

    def _storage_doc(self, data: JsonDict) -> JsonDict:
        out = dict(data)
        out[ID_FIELD] = str(out[ID_FIELD])
        out["_id"] = out[ID_FIELD]
        return out

    # ....................... #

    def _from_storage_doc(self, raw: JsonDict) -> JsonDict:
        out = dict(raw)
        storage_id = out.pop("_id", None)

        if ID_FIELD not in out and storage_id is not None:
            out[ID_FIELD] = storage_id

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return out

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

    def _coerce_query_value(self, value: Any) -> Any:
        if isinstance(value, UUID):
            return str(value)

        if isinstance(value, list):
            return [
                self._coerce_query_value(x)
                for x in value  # pyright: ignore[reportUnknownVariableType]
            ]
        if isinstance(value, dict):
            return {
                k: self._coerce_query_value(v)
                for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]
            }

        return value

    # ....................... #

    def _render_filters(self, filters: Optional[QueryFilterExpression]) -> JsonDict:
        if not filters:
            return {}

        parsed = QueryFilterExpressionParser.parse(filters)
        rendered = self.renderer.render(parsed)
        coerced = self._coerce_query_value(rendered)

        return coerced

    # ....................... #

    def _sorts(self, sorts: Optional[QuerySortExpression]) -> list[tuple[str, int]]:
        if not sorts:
            sorts = {ID_FIELD: "desc"}

        out: list[tuple[str, int]] = []

        for field, direction in sorts.items():
            target = "_id" if field == ID_FIELD else field
            out.append((target, 1 if direction == "asc" else -1))

        return out

    # ....................... #

    def _projection(self, return_fields: Optional[Sequence[str]]) -> Optional[JsonDict]:
        if return_fields is None:
            return None

        return {**{field: 1 for field in return_fields}, "_id": 0}

    # ....................... #

    def _return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        return {k: raw.get(k, None) for k in return_fields}

    # ....................... #

    def _supports_soft_delete(self) -> bool:
        return SOFT_DELETE_FIELD in pydantic_field_names(self.domain_model)

    # ....................... #

    async def _get_domain(self, pk: UUID) -> D:
        raw = await self.client.find_one(
            self._write_coll(), {"_id": self._storage_pk(pk)}
        )

        if raw is None:
            raise NotFoundError(f"Record not found: {pk}")

        return pydantic_validate(self.domain_model, self._from_storage_doc(raw))

    # ....................... #

    async def _get_uncached(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> R | JsonDict:
        if for_update:
            self.client.require_transaction()

        raw = await self.client.find_one(
            self._read_coll(),
            {"_id": self._storage_pk(pk)},
            projection=self._projection(return_fields),
        )

        if raw is None:
            raise NotFoundError(f"Record not found: {pk}")

        data = self._from_storage_doc(raw)

        if return_fields is not None:
            return self._return_subset(data, return_fields)

        return pydantic_validate(self.read_model, data)

    # ....................... #

    async def _get_many_uncached(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        if not pks:
            return []

        ids = [self._storage_pk(pk) for pk in pks]
        rows = await self.client.find_many(
            self._read_coll(),
            {"_id": {"$in": ids}},
            projection=self._projection(return_fields),
        )

        by_pk: dict[str, JsonDict] = {}
        for row in rows:
            normalized = self._from_storage_doc(row)
            by_pk[str(normalized[ID_FIELD])] = normalized

        missing = [pk for pk in pks if self._storage_pk(pk) not in by_pk]
        if missing:
            raise NotFoundError(f"Some records not found: {missing}")

        ordered = [by_pk[self._storage_pk(pk)] for pk in pks]

        if return_fields is not None:
            return [self._return_subset(row, return_fields) for row in ordered]

        return [pydantic_validate(self.read_model, row) for row in ordered]

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
            return await self._get_uncached(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        try:
            cached = await self.cache.get(str(pk))
        except Exception:
            return await self._get_uncached(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        if cached is not None:
            return pydantic_validate(self.read_model, cached)

        raw = await self.client.find_one(
            self._read_coll(), {"_id": self._storage_pk(pk)}
        )

        if raw is None:
            raise NotFoundError(f"Record not found: {pk}")

        res = pydantic_validate(self.read_model, self._from_storage_doc(raw))

        with contextlib.suppress(Exception):
            await self.cache.set_versioned(
                str(pk), str(res.rev), self._map_to_cache(res)
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
        if not pks:
            return []

        if return_fields is not None or self.cache is None:
            return await self._get_many_uncached(pks, return_fields=return_fields)

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in pks])
        except Exception:
            return await self._get_many_uncached(pks, return_fields=return_fields)

        miss_res: list[R] = []

        if misses:
            uncached = await self._get_many_uncached([UUID(x) for x in misses])
            miss_res = cast(list[R], list(uncached))
            with contextlib.suppress(Exception):
                await self.cache.set_many_versioned(
                    {(str(x.id), str(x.rev)): self._map_to_cache(x) for x in miss_res}
                )

        by_pk: dict[str, R] = {
            k: pydantic_validate(self.read_model, v) for k, v in hits.items()
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
        if for_update:
            self.client.require_transaction()

        query = self._render_filters(filters)
        raw = await self.client.find_one(
            self._read_coll(),
            query,
            projection=self._projection(return_fields),
        )

        if raw is None:
            return None

        data = self._from_storage_doc(raw)

        if return_fields is not None:
            return self._return_subset(data, return_fields)

        return pydantic_validate(self.read_model, data)

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
        if not filters and limit is None:
            raise ValidationError("Filters or limit must be provided")

        cnt = await self.count(filters)

        if not cnt:
            return [], 0

        query = self._render_filters(filters)
        rows = await self.client.find_many(
            self._read_coll(),
            query,
            projection=self._projection(return_fields),
            sort=self._sorts(sorts),
            limit=limit,
            skip=offset,
        )

        normalized = [self._from_storage_doc(row) for row in rows]

        if return_fields is not None:
            return [self._return_subset(row, return_fields) for row in normalized], cnt

        return [pydantic_validate(self.read_model, row) for row in normalized], cnt

    # ....................... #

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:
        query = self._render_filters(filters)
        return await self.client.count(self._read_coll(), query)

    # ....................... #

    async def create(self, dto: C) -> R:
        model = pydantic_validate(
            self.domain_model,
            pydantic_dump(dto, exclude={"unset": True}),
        )
        data = pydantic_dump(model)
        payload = self._storage_doc(data)

        await self.client.insert_one(self._write_coll(), payload)

        res = pydantic_validate(self.read_model, data)

        if self.cache is not None:
            await self.cache.set_versioned(
                str(res.id),
                str(res.rev),
                self._map_to_cache(res),
            )

        return res

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        if not dtos:
            return []

        models = [
            pydantic_validate(
                self.domain_model,
                pydantic_dump(dto, exclude={"unset": True}),
            )
            for dto in dtos
        ]
        docs = [pydantic_dump(model) for model in models]
        payloads = [self._storage_doc(doc) for doc in docs]

        await self.client.insert_many(self._write_coll(), payloads)

        res = [pydantic_validate(self.read_model, data) for data in docs]

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

    async def _patch(
        self,
        pk: UUID,
        update: Optional[JsonDict] = None,
        *,
        rev: Optional[int] = None,
    ) -> R:
        current = await self._get_domain(pk)

        if rev is not None and rev != current.rev:
            raise ConflictError("Revision mismatch", code="revision_mismatch")

        if update is not None:
            _, diff = current.update(update)
        else:
            _, diff = current.touch()

        if not diff:
            return pydantic_validate(
                self.read_model,
                pydantic_dump(current),
            )

        matched = await self.client.update_one(
            self._write_coll(),
            {"_id": self._storage_pk(current.id), REV_FIELD: current.rev},
            {
                "$set": self._coerce_query_value(diff),
                "$inc": {REV_FIELD: 1},
            },
        )

        if matched != 1:
            raise ConcurrencyError("Failed to update record")

        updated = current.model_copy(
            update={**diff, REV_FIELD: current.rev + 1}, deep=True
        )

        return pydantic_validate(self.read_model, pydantic_dump(updated))

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        res = await self._patch(
            pk,
            pydantic_dump(dto, exclude={"unset": True}),
            rev=rev,
        )

        await self._clear_cache(pk)

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
        if not pks:
            return []

        if len(pks) != len(dtos):
            raise CoreError("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[R] = []
        for i, (pk, dto) in enumerate(zip(pks, dtos, strict=True)):
            out.append(
                await self.update(pk, dto, rev=None if revs is None else revs[i])
            )

        return out

    # ....................... #

    async def touch(self, pk: UUID) -> R:
        res = await self._patch(pk)

        await self._clear_cache(pk)

        if self.cache is not None:
            await self.cache.set_versioned(
                str(res.id),
                str(res.rev),
                self._map_to_cache(res),
            )

        return res

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        out: list[R] = []
        for pk in pks:
            out.append(await self.touch(pk))

        return out

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        await self.client.delete_one(self._write_coll(), {"_id": self._storage_pk(pk)})
        await self._clear_cache(pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        await self.client.delete_many(
            self._write_coll(),
            {"_id": {"$in": [self._storage_pk(pk) for pk in pks]}},
        )
        await self._clear_cache(*pks)

    # ....................... #

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res = await self._patch(pk, {SOFT_DELETE_FIELD: True}, rev=rev)

        await self._clear_cache(pk)

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
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[R] = []
        for i, pk in enumerate(pks):
            out.append(await self.delete(pk, rev=None if revs is None else revs[i]))

        return out

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res = await self._patch(pk, {SOFT_DELETE_FIELD: False}, rev=rev)

        await self._clear_cache(pk)

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
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[R] = []
        for i, pk in enumerate(pks):
            out.append(await self.restore(pk, rev=None if revs is None else revs[i]))

        return out
