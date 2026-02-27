from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Any, Never, Optional, Sequence, TypeVar, final, overload

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSearchOptions,
    FilterExpression,
    SortExpression,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate

from .base import PostgresGateway
from .spec import PostgresSearchIndexSpec

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchGateway[M: BaseModel](PostgresGateway[M]):
    indexes: Sequence[PostgresSearchIndexSpec] = attrs.field(
        validator=attrs.validators.min_len(1)
    )
    default_index: Optional[str] = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        idx_names = [idx.name for idx in self.indexes]

        if len(idx_names) != len(set(idx_names)):
            raise CoreError("Index names must be unique")

        if self.default_index is not None and self.default_index not in idx_names:
            raise CoreError(
                f"Default index `{self.default_index}` not found in indexes"
            )

    # ....................... #

    @cached_property
    def _idx_map(self) -> dict[str, PostgresSearchIndexSpec]:
        return {idx.name: idx for idx in self.indexes}

    # ....................... #

    @cached_property
    def _default_idx(self) -> str:
        return self.default_index or self.indexes[0].name

    # ....................... #

    def _search_clause(
        self,
        query: str,
        options: Optional[DocumentSearchOptions] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        options = options or {}
        index = options.get("use_index", self._default_idx)
        use_fuzzy = options.get("use_fuzzy", False)
        overwrite_weights = options.get("overwrite_weights", None)
        overwrite_fuzzy_max = options.get("overwrite_fuzzy_max", None)

        if index not in self._idx_map:
            raise CoreError(f"Index `{index}` not found")

        return self._idx_map[index].build_where(
            query,
            use_fuzzy=use_fuzzy,
            overwrite_weights=overwrite_weights,
            overwrite_fuzzy_max=overwrite_fuzzy_max,
        )

    # ....................... #

    async def search_where_clause(
        self,
        query: str,
        filters: Optional[FilterExpression] = None,
        options: Optional[DocumentSearchOptions] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        sw, sp = self._search_clause(query, options=options)
        fw, fp = await self.where_clause(filters)

        where_parts = sql.SQL(" AND ").join([sw, fw])
        params = [*sp, *fp]

        return where_parts, params

    # ....................... #

    def search_sort_clause(
        self,
        sorts: Optional[SortExpression] = None,
    ) -> sql.Composable:
        parts: list[sql.Composable] = [sql.SQL("pgroonga_score(tableoid, ctid) DESC")]

        if sorts:
            for field, order in sorts.items():
                parts.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(field), sql.SQL(order.upper())
                    )
                )

        return sql.SQL(", ").join(parts)

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        options: Optional[DocumentSearchOptions] = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        options: Optional[DocumentSearchOptions] = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        options: Optional[DocumentSearchOptions] = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[SortExpression] = ...,
        options: Optional[DocumentSearchOptions] = ...,
        *,
        return_model: type[T] = ...,
        return_fields: Sequence[str],
    ) -> Never: ...

    async def search(
        self,
        query: str,
        filters: Optional[FilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[SortExpression] = None,
        options: Optional[DocumentSearchOptions] = None,
        *,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        where, params = await self.search_where_clause(query, filters, options=options)
        sort = self.search_sort_clause(sorts)

        search_stmt = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {sort}"
        ).format(
            cols=self.return_clause(return_model, return_fields),
            table=self.spec.ident(),
            where=where,
            sort=sort,
        )

        if limit is not None:
            search_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            search_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        #! Temp trick to improve search experience
        # await self.client.execute("SET pgroonga.force_match_escalation = on")

        rows = await self.client.fetch_all(search_stmt, params, row_factory="dict")

        if return_model is not None:
            return [pydantic_validate(return_model, row) for row in rows]

        if return_fields is not None:
            return [{k: row.get(k, None) for k in return_fields} for row in rows]

        return [pydantic_validate(self.model, row) for row in rows]

    # ....................... #

    async def search_count(
        self,
        query: str,
        filters: Optional[FilterExpression] = None,
        options: Optional[DocumentSearchOptions] = None,
    ) -> int:
        where, params = await self.search_where_clause(query, filters, options=options)

        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.spec.ident(),
            where=where,
        )

        count = await self.client.fetch_value(count_stmt, params, default=0)

        return int(count)
