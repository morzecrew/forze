"""PGroonga-based search adapter."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Sequence, TypeVar, final, overload

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchSpec,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._utils import calculate_effective_field_weights

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_TABLE_ALIAS: Final[str] = "t"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresPGroongaSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """Postgres-backend implementation of :class:`SearchQueryPort` for PGroonga search engine."""

    spec: SearchSpec[M]
    """Search specification."""

    index_qname: PostgresQualifiedName
    """Qualified name of the PGroonga index."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def _effective_weights(
        self,
        options: SearchOptions | None = None,
    ) -> dict[str, int]:
        """Calculate effective field weights based on options."""

        weights = calculate_effective_field_weights(self.spec, options)

        # renormalize weights
        norm_weights = {f: int(w * 100) for f, w in weights.items()}

        return norm_weights

    # ....................... #

    async def _pgroonga_where(
        self,
        query: str,
        *,
        options: SearchOptions | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        """Build PGroonga ``&@~`` query based on options."""

        options = options or {}
        query = query.strip()
        index = self.index_qname.string()

        if not query:
            return sql.SQL("TRUE"), []

        params: list[Any] = [query, index]

        q_ph = sql.Placeholder()
        idx_ph = sql.Placeholder()
        r_ph = sql.Placeholder()
        w_ph = sql.Placeholder()

        eff_weights = self._effective_weights(options)
        fields = list(eff_weights.keys())
        weights = list(eff_weights.values())

        use_fuzzy = options.get("fuzzy", False)

        if self.spec.fuzzy is not None:
            ratio = self.spec.fuzzy.get("max_distance_ratio", 0.34)

        else:
            ratio = 0.34

        index_info = await self.introspector.get_index_info(
            index=self.index_qname.name,
            schema=self.index_qname.schema,
        )

        # check expression for array or single field
        if index_info.expr is None or ("ARRAY" not in index_info.expr.upper()):
            text_expr = sql.SQL("coalesce({}::text, '')").format(
                sql.Identifier(fields[0])
            )

            if use_fuzzy:
                params.append(float(ratio))
                cond = sql.SQL(
                    (
                        "pgroonga_condition({}::text, "
                        "index_name => {}::text, "
                        "fuzzy_max_distance_ratio => {}::float4)"
                    )
                ).format(q_ph, idx_ph, r_ph)

            else:
                cond = sql.SQL(
                    "pgroonga_condition({}::text, index_name => {}::text)"
                ).format(q_ph, idx_ph)

            return sql.SQL("{} &@~ {}").format(text_expr, cond), params

        # if array in index expression - we need to build array expression
        array_expr = sql.SQL("(ARRAY[{}])").format(
            sql.SQL(", ").join(
                sql.SQL("coalesce({}::text, '')").format(sql.Identifier(f))
                for f in fields
            )
        )
        params.append(weights)

        if use_fuzzy:
            params.append(float(ratio))
            cond = sql.SQL(
                (
                    "pgroonga_condition({}::text, "
                    "index_name => {}::text, "
                    "weights => {}::int[], "
                    "fuzzy_max_distance_ratio => {}::float4)"
                )
            ).format(q_ph, idx_ph, w_ph, r_ph)

        else:
            cond = sql.SQL(
                (
                    "pgroonga_condition({}::text, "
                    "index_name => {}::text, "
                    "weights => {}::int[])"
                )
            ).format(q_ph, idx_ph, w_ph)

        return sql.SQL("{} &@~ {}").format(array_expr, cond), params

    # ....................... #

    def _pgroonga_order_by(
        self,
        sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
    ) -> sql.Composable:
        """Build PGroonga order clause based on options."""

        order_by = self.order_by_clause(sorts)

        if order_by is None:
            order_by = sql.SQL("pgroonga_score({}) DESC").format(
                sql.Identifier(_TABLE_ALIAS)
            )

        return order_by

    # ....................... #

    async def _where_clause(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        options: SearchOptions | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        """Build WHERE clause based on options."""

        sw, sp = await self._pgroonga_where(query, options=options)
        fw, fp = await self.where_clause(filters)

        where_parts = sql.SQL(" AND ").join([sw, fw])
        params = [*sp, *fp]

        return where_parts, params

    # ....................... #
    # API

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        """Search documents."""

        where, params = await self._where_clause(query, filters, options=options)
        order_by = self._pgroonga_order_by(sorts)

        # total
        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where,
        )
        total = int(await self.client.fetch_value(count_stmt, params, default=0))

        # Exit early if no results
        if total == 0:
            return [], total

        # rows
        stmt = sql.SQL(
            """
            SELECT {cols}
            FROM {table} {table_alias}
            WHERE {where}
            ORDER BY {order_by}
            """
        ).format(
            cols=self.return_clause(return_type, return_fields),
            table=self.qname.ident(),
            table_alias=sql.Identifier(_TABLE_ALIAS),
            where=where,
            order_by=order_by,
        )

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params = [*params, int(limit)]

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params = [*params, int(offset)]

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_type is not None:
            return pydantic_validate_many(return_type, rows), total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return pydantic_validate_many(self.model_type, rows), total
