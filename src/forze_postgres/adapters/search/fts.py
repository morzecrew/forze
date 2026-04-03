"""FTS-based search adapter."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from statistics import mean
from typing import Any, Literal, Sequence, TypeVar, final, overload

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchOptions, SearchReadPort, SearchSpec
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._utils import calculate_effective_field_weights

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

FtsGroupLetter = Literal["A", "B", "C", "D"]
"""One of the four Postgres FTS weight labels."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFTSSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchReadPort[M],
    TxScopedPort,
):
    """Postgres-backend implementation of :class:`SearchReadPort` for FTS search engine."""

    spec: SearchSpec[M]
    """Search specification."""

    source_qname: PostgresQualifiedName
    """Source table qualified name (where search index resides)."""

    fts_groups: dict[FtsGroupLetter, Sequence[str]]
    """Mapping of FTS weight letters to field names."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    async def _resolve_tsvector_expr(self) -> sql.Composable:
        index_info = await self.introspector.get_index_info(
            index=self.qname.name,
            schema=self.qname.schema,
        )

        if not index_info.expr:
            raise CoreError(
                "Unable to infer tsvector expression from index definition."
            )

        # NOTE: expr is raw SQL fragment from Postgres catalog; we still treat it as config.
        return sql.SQL(index_info.expr)  # pyright: ignore[reportArgumentType]

    # ....................... #

    def _tsquery_expr(
        self,
        query: str,
        *,
        options: SearchOptions | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        query = query.strip()
        options = options or {}
        params: list[Any] = [query]

        fn = "websearch_to_tsquery"

        #! search mode is not supported in refactor yet ...
        # match spec.mode:
        #     case "phrase":
        #         fn = "phraseto_tsquery"

        #     case "exact":
        #         fn = "plainto_tsquery"

        #     case _:  # fulltext, prefix
        #         fn = "websearch_to_tsquery"

        return sql.SQL(f"{fn}({{}}::text)").format(sql.Placeholder()), params

    # ....................... #

    def _effective_weights(
        self,
        alpha: float = 0.7,
        options: SearchOptions | None = None,
    ) -> dict[FtsGroupLetter, float]:
        """Calculate effective field weights based on options.

        Note: this works properly when options are consistent with FTS groups.
        If some conflicting weights are provided, the result of search weighting
        might be unexpected.
        """

        weights = calculate_effective_field_weights(self.spec, options)

        # M `ap weights to FTS group letters
        group_weights: dict[FtsGroupLetter, list[float]] = {
            k: [weights[x] for x in v] for k, v in self.fts_groups.items()
        }

        agg_weights: dict[FtsGroupLetter, float] = {}

        for g, w in group_weights.items():
            non_zero = [x for x in w if x > 0]
            non_zero_term = mean(non_zero) if non_zero else 0.0
            agg_weights[g] = alpha * max(w) + (1 - alpha) * non_zero_term

        return agg_weights

    # ....................... #

    async def _where_and_order_by_fts(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        options: SearchOptions | None = None,
    ) -> tuple[tuple[sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]]:
        """Build WHERE and ORDER BY clauses for FTS search."""

        query = query.strip()
        tsv = await self._resolve_tsvector_expr()

        # Note: tenant ID is injected automatically
        fw, fp = await self.where_clause(filters)

        # empty query: only filters, no rank
        if not query:
            return (fw, fp), (sql.SQL("0.0"), [])

        tsw, tsp = self._tsquery_expr(query, options=options)
        search_cond = sql.SQL("({tsv}) @@ ({tsw})").format(tsv=tsv, tsw=tsw)
        where_sql = sql.SQL(" AND ").join([search_cond, fw])
        where_params = [*tsp, *fp]

        gw = self._effective_weights(options=options)

        # Canonical order of FTS group letters
        fts_weights = [
            gw.get("D", 0.0),
            gw.get("C", 0.0),
            gw.get("B", 0.0),
            gw.get("A", 0.0),
        ]

        order_by_expr = sql.SQL(
            "ts_rank_cd({weights}::float4[], ({tsv}), ({tsw}))"
        ).format(
            weights=sql.Placeholder(),
            tsv=tsv,
            tsw=tsw,
        )
        order_by_params = [fts_weights, *tsp]

        return (where_sql, where_params), (order_by_expr, order_by_params)

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
        (
            (where_sql, where_params),
            (rank_sql, rank_params),
        ) = await self._where_and_order_by_fts(query, filters, options=options)

        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where_sql,
        )

        total = int(await self.client.fetch_value(count_stmt, where_params, default=0))

        if total == 0:
            return [], total

        # order: rank desc + optional secondary sorts
        order_parts: list[sql.Composable] = [sql.SQL("{} DESC").format(rank_sql)]

        if sorts:
            for field, order in sorts.items():
                order_parts.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(field), sql.SQL(order.upper())
                    )
                )
        order_sql = sql.SQL(", ").join(order_parts)

        # rows
        stmt = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {order}"
        ).format(
            cols=self.return_clause(return_type, return_fields),
            table=self.qname.ident(),
            where=where_sql,
            order=order_sql,
        )

        params = [*where_params, *rank_params]

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_type is not None:
            return pydantic_validate_many(return_type, rows), total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return pydantic_validate_many(self.model_type, rows), total
