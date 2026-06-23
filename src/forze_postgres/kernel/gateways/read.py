"""Read-only gateway for fetching documents from a Postgres relation."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import (
    Any,
    AsyncGenerator,
    Literal,
    Never,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
)
from uuid import UUID

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.document import RowLockMode
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
    validate_cursor_token,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.sql.query import PsycopgQueryRenderer
from forze_postgres.kernel.sql.query.nested import sort_key_expr
from forze.application.contracts.querying.sort_resolution import (
    normalize_sorts_for_keyset,
)
from forze_postgres.kernel.sql import (
    build_order_by_sql,
    build_seek_condition,
)

from forze.application.integrations.persistence import ReadValidationCodecMixin

from .base import PostgresGateway

# ----------------------- #


def _for_update_sql(mode: RowLockMode) -> sql.SQL | None:
    if mode is False:
        return None

    if mode is True:
        return sql.SQL(" FOR UPDATE")

    if mode == "nowait":
        return sql.SQL(" FOR UPDATE NOWAIT")

    if mode == "skip_locked":
        return sql.SQL(" FOR UPDATE SKIP LOCKED")

    raise exc.internal(f"Invalid for_update mode: {mode!r}")


# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresReadGateway[M: BaseModel](
    ReadValidationCodecMixin[M],
    PostgresGateway[M],
):
    """Read-only gateway providing single/batch lookups, filtered queries, and counting."""

    read_validation: Literal["strict", "trusted"] = attrs.field(
        default="strict",
        kw_only=True,
    )

    def _effective_sql_limit(self, limit: int | None) -> int | None:
        """Apply :attr:`~forze_postgres.kernel.gateways.base.PostgresGateway.find_many_implicit_limit` when *limit* is omitted."""

        if limit is not None:
            return limit

        return self.find_many_implicit_limit

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
    ) -> M:
        where_sql = sql.SQL("{pk} = {ph}").format(
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )
        where_params = [pk]

        where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(),
            table=(await self._qname()).ident(),
            where=where_sql,
        )

        fu = _for_update_sql(for_update)

        if fu is not None:
            self.client.require_transaction()
            stmt += fu

        row = await self.client.fetch_one(stmt, where_params, row_factory="dict")

        if row is None:
            raise exc.not_found(f"Record not found: {pk}")

        return await self._adecode_row(row)

    # ....................... #

    async def get_many(self, pks: Sequence[UUID]) -> list[M]:
        if not pks:
            return []

        where_sql = sql.SQL("{pk} = ANY({ph})").format(
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )
        where_params = [list(pks)]

        where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(),
            table=(await self._qname()).ident(),
            where=where_sql,
        )

        rows = await self.client.fetch_all(stmt, where_params, row_factory="dict")

        m = {row[ID_FIELD]: row for row in rows}
        ordered = [m[pk] for pk in pks if pk in m]
        missing = [x for x in pks if x not in m]

        if missing:
            raise exc.not_found(f"Some records not found: {missing}")

        return await self._adecode_rows(ordered)

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> M | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> T | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> M | T | JsonDict | None:
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where} LIMIT 1").format(
            cols=self.return_clause(return_model, return_fields),
            table=(await self._qname()).ident(),
            where=where,
        )

        fu = _for_update_sql(for_update)

        if fu is not None:
            self.client.require_transaction()
            stmt += fu

        row = await self.client.fetch_one(stmt, params, row_factory="dict")

        if row is None:
            return None

        if return_model is not None:
            return await self._adecode_row(row, model=return_model)

        if return_fields is not None:
            [decrypted] = await self._adecrypt_projection_rows((row,))
            return {k: decrypted.get(k, None) for k in return_fields}

        return await self._adecode_row(row)

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: None = ...,
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T],
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
        parsed: QueryExpr | None = ...,
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: Sequence[str],
        parsed: QueryExpr | None = ...,
    ) -> Never: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression | None = None,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        parsed: QueryExpr | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if aggregates is not None:
            return await self.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=aggregates,
                return_model=return_model,
                return_fields=return_fields,
                parsed=parsed,
            )

        # tenant id supplied by where clause
        where, params = await self.where_clause(filters, parsed=parsed)
        sort_clause = await self.order_by_clause(sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=(await self._qname()).ident(),
            where=where,
        )

        if sort_clause is not None:
            stmt += sql.SQL(" ORDER BY {sort}").format(sort=sort_clause)

        eff_limit = self._effective_sql_limit(limit)

        if eff_limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(eff_limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return await self._adecode_rows(rows, model=return_model)

        if return_fields is not None:
            decrypted = await self._adecrypt_projection_rows(rows)
            return [{k: row.get(k, None) for k in return_fields} for row in decrypted]

        return await self._adecode_rows(rows)

    # ....................... #

    async def find_many_chunked(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        fetch_batch_size: int = 2000,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> AsyncGenerator[list[M] | list[T] | list[JsonDict]]:
        """Like :meth:`find_many` but yield validated row batches from the driver.

        Each yielded list has at most ``fetch_batch_size`` rows (except possibly
        the last). Requires the same ``return_model`` / ``return_fields`` rules
        as :meth:`find_many`; aggregates are not supported.
        """

        if return_model is not None and return_fields is not None:
            raise exc.internal("return_model and return_fields cannot be combined")

        where, params = await self.where_clause(filters)
        sort_clause = await self.order_by_clause(sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=(await self._qname()).ident(),
            where=where,
        )

        if sort_clause is not None:
            stmt += sql.SQL(" ORDER BY {sort}").format(sort=sort_clause)

        eff_limit = self._effective_sql_limit(limit)

        if eff_limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(eff_limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        async for raw_chunk in self.client.fetch_all_batched(
            stmt,
            params,
            row_factory="dict",
            batch_size=fetch_batch_size,
        ):
            dict_chunk: list[JsonDict] = cast(list[JsonDict], raw_chunk)
            if not dict_chunk:
                continue

            if return_fields is not None:
                decrypted = await self._adecrypt_projection_rows(dict_chunk)
                yield [
                    {k: row.get(k, None) for k in return_fields} for row in decrypted
                ]

            elif return_model is not None:
                await self._prepare_decode(dict_chunk)
                yield self._decode_rows(dict_chunk, model=return_model)

            else:
                await self._prepare_decode(dict_chunk)
                yield self._decode_rows(dict_chunk)

    # ....................... #

    async def find_many_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        parsed: QueryExpr | None = None,
    ) -> list[T] | list[JsonDict]:
        """Find aggregate rows."""

        if return_fields is not None:
            raise exc.internal("Aggregates cannot be combined with return_fields")

        where, params = await self.where_clause(filters, parsed=parsed)
        types = await self.column_types()
        renderer = PsycopgQueryRenderer(
            types=types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
        )
        parsed_, select_clause, group_clause, aggregate_params = (
            renderer.render_aggregates(
                aggregates,
            )
        )
        params = list(aggregate_params) + list(params)
        sort_clause = renderer.render_aggregate_order_by(parsed_, sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=select_clause,
            table=(await self._qname()).ident(),
            where=where,
        )

        if group_clause is not None:
            stmt += sql.SQL(" GROUP BY {group}").format(group=group_clause)

        if parsed_.having is not None:
            # ``$having`` filters the aggregated rows: wrap the group query and filter on
            # its output aliases (a fresh renderer with no column types — values pass
            # through; Postgres compares against the computed/group columns).
            having_renderer = PsycopgQueryRenderer(table_alias="_agg")
            having_sql, having_params = having_renderer.render(parsed_.having)
            stmt = sql.SQL("SELECT * FROM ({inner}) AS _agg WHERE {having}").format(
                inner=stmt,
                having=having_sql,
            )
            params = list(params) + list(having_params)

        if sort_clause is not None:
            stmt += sql.SQL(" ORDER BY {sort}").format(sort=sort_clause)

        eff_limit = self._effective_sql_limit(limit)

        if eff_limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(eff_limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return await self._adecode_rows(rows, model=return_model)

        return list(rows)

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
        parsed: QueryExpr | None = None,
    ) -> int:
        """Count aggregate result groups."""

        where, params = await self.where_clause(filters, parsed=parsed)
        types = await self.column_types()
        renderer = PsycopgQueryRenderer(
            types=types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
        )
        parsed_, select_clause, group_clause, aggregate_params = (
            renderer.render_aggregates(
                aggregates,
            )
        )
        params = list(aggregate_params) + list(params)

        inner = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=select_clause,
            table=(await self._qname()).ident(),
            where=where,
        )

        if group_clause is not None:
            inner += sql.SQL(" GROUP BY {group}").format(group=group_clause)

        if parsed_.having is not None:
            # ``$having`` filters grouped rows, so apply it before counting (mirrors
            # ``find_many_aggregates``); otherwise the count includes filtered-out groups.
            having_renderer = PsycopgQueryRenderer(table_alias="_agg")
            having_sql, having_params = having_renderer.render(parsed_.having)
            inner = sql.SQL("SELECT * FROM ({inner}) AS _agg WHERE {having}").format(
                inner=inner,
                having=having_sql,
            )
            params = list(params) + list(having_params)

        stmt = sql.SQL("SELECT COUNT(*) FROM ({inner}) AS agg").format(inner=inner)
        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)

    # ....................... #

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        c = dict(cursor or {})

        if c.get("after") and c.get("before"):
            raise exc.internal(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        limit_raw = c.get("limit")

        lim: int = 10 if limit_raw is None else int(limit_raw)  # type: ignore[call-overload]

        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

        use_before = c.get("before") is not None
        use_after = c.get("after") is not None

        normalized = normalize_sorts_for_keyset(
            sorts,
            read_fields=self.read_fields,
            model=self.model_type,
        )
        sort_keys = [k for k, _, _ in normalized]
        directions = [d for _, d, _ in normalized]
        nulls = [n for _, _, n in normalized]

        where_base, params = await self.where_clause(filters)
        types = await self.column_types()
        alias = self.filter_table_alias
        exprs = [
            sort_key_expr(
                field=k,
                column_types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=alias,
            )
            for k, _, _ in normalized
        ]

        seek_params: list[Any] = []
        where_fin = where_base

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tv = validate_cursor_token(
                token,
                sort_keys=sort_keys,
                directions=directions,
                nulls=nulls,
            )

            seek_sql, seek_params = build_seek_condition(
                exprs,
                directions,
                tv,
                "before" if use_before else "after",
                nulls=nulls,
            )
            where_fin = sql.SQL("({} AND ({}))").format(where_base, seek_sql)
            params = list(params) + seek_params  # type: ignore[operator]

        order_fwd = build_order_by_sql(exprs, directions, nulls=nulls, flip=False)
        order_bwd = build_order_by_sql(exprs, directions, nulls=nulls, flip=True)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=(await self._qname()).ident(),
            where=where_fin,
        )
        o_sql = order_fwd if not use_before else order_bwd
        stmt = sql.SQL("{} ORDER BY {}").format(stmt, o_sql)  # type: ignore[assignment]
        plim = int(lim) + 1
        stmt = sql.SQL("{} LIMIT {}").format(stmt, sql.Placeholder())  # type: ignore[assignment]
        params = list(params)  # type: ignore[assignment]
        params.append(plim)

        raw_rows = list(await self.client.fetch_all(stmt, params, row_factory="dict"))

        if use_before:
            raw_rows = list(reversed(raw_rows))

        # At most *lim* + 1 rows; caller slices and derives ``has_more``.
        if return_model is not None:
            return await self._adecode_rows(raw_rows, model=return_model)

        if return_fields is not None:
            decrypted = await self._adecrypt_projection_rows(raw_rows)
            return [{k: r.get(k, None) for k in return_fields} for r in decrypted]

        return await self._adecode_rows(raw_rows)

    # ....................... #

    async def count(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> int:
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters, parsed=parsed)

        stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=(await self._qname()).ident(),
            where=where,
        )

        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)
