"""Write gateway for creating, updating, soft-deleting, and hard-deleting Postgres documents."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections import defaultdict
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, AsyncGenerator, LiteralString, Sequence, cast, final, get_args
from uuid import UUID

import attrs
from psycopg import sql

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import (
    default_resilience_executor,
    occ_retry,
)
from forze.application.integrations.persistence import (
    DocumentWriteCodecMixin,
    HistoryOccMixin,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, Document
from forze_postgres.kernel.catalog.introspect import PostgresColumnTypes, PostgresType
from forze_postgres.kernel.client import gather_db_work
from forze_postgres.kernel.sql.conflict_target import resolve_write_conflict_target

from .base import PostgresGateway
from .history import PostgresHistoryGateway
from .read import PostgresReadGateway
from .types import PostgresBookkeepingStrategy

# ----------------------- #


def _pg_cast_type_sql(pg: PostgresType) -> sql.Composable:
    """Return the ``CAST`` target type for a column (names come from introspection only)."""

    base = cast(LiteralString, pg.base)  # type: ignore[redundant-cast]

    if pg.is_array:
        return sql.SQL(cast(LiteralString, pg.base + "[]"))  # type: ignore[redundant-cast]

    return sql.SQL(base)


def _values_placeholder_for_patch_group(
    *,
    column: str,
    expected_rev_alias: str,
    column_types: PostgresColumnTypes,
) -> sql.Composable:
    """Placeholder for one ``VALUES`` cell, typed so all-``NULL`` columns are not inferred as ``text``."""

    ph = sql.Placeholder()
    if column == ID_FIELD:
        pg_t = column_types.get(ID_FIELD)
    elif column == expected_rev_alias:
        pg_t = column_types.get(REV_FIELD)
    else:
        pg_t = column_types.get(column)

    if pg_t is None:
        return ph

    return sql.SQL("CAST({} AS {})").format(ph, _pg_cast_type_sql(pg_t))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresWriteGateway[D: Document, C: BaseDTO, U: BaseDTO](
    DocumentWriteCodecMixin[D],
    HistoryOccMixin[D],
    PostgresGateway[D],
):
    """Write gateway for document mutations with optimistic concurrency control.

    Requires a companion :class:`PostgresReadGateway` sharing the same client.
    Optionally writes revision history via :class:`PostgresHistoryGateway`.
    All mutating operations are wrapped with the ``occ`` resilience policy via
    :func:`~forze.application.execution.resilience.occ_retry`.
    """

    read_gw: PostgresReadGateway[D]
    """Read gateway for the same document type."""

    resilience: ResilienceExecutorPort = attrs.field(
        factory=default_resilience_executor,
        eq=False,
        repr=False,
    )
    """Resilience executor backing optimistic-concurrency retries."""

    create_cmd_type: type[C]
    """Pydantic model for creation payloads."""

    update_cmd_type: type[U] | None = attrs.field(default=None)
    """Pydantic model for update payloads."""

    create_codec: ModelCodec[D, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for create commands."""

    update_codec: ModelCodec[U, Any] | None = attrs.field(
        kw_only=True, eq=False, repr=False
    )
    """Codec for update commands when :attr:`update_cmd_type` is set; else ``None``."""

    history_gw: PostgresHistoryGateway[D] | None = attrs.field(default=None)  # type: ignore[override]
    """Optional history gateway for revision snapshots."""

    strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    conflict_target: tuple[str, ...] | None = attrs.field(default=None)
    """``ON CONFLICT`` columns for :meth:`ensure` / :meth:`upsert`; ``None`` infers PRIMARY KEY."""

    _conflict_target_cell: OnceCell[tuple[str, ...]] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.client is not self.read_gw.client:
            raise exc.internal(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.tenant_aware != self.read_gw.tenant_aware:
            raise exc.internal(
                "Tenant awareness mismatch. Write gateway and nested read gateway must have the same tenant awareness."
            )

        if self.history_gw is not None:
            if self.client is not self.history_gw.client:
                raise exc.internal(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.tenant_aware != self.history_gw.tenant_aware:
                raise exc.internal(
                    "Tenant awareness mismatch. Write gateway and nested history gateway must have the same tenant awareness."
                )

        if self.strategy not in get_args(PostgresBookkeepingStrategy):
            raise exc.internal(f"Invalid bookkeeping strategy: {self.strategy}")

    # ....................... #

    @asynccontextmanager
    async def _write_tx(self) -> AsyncGenerator[None]:
        """Use an outer transaction for multi-step writes when the caller has not opened one."""

        if self.client.is_in_transaction():
            yield
            return

        async with self.client.transaction():
            yield

    # ....................... #

    def _require_update_cmd(self) -> None:
        if self.update_cmd_type is None:
            raise exc.internal("Update command type is not supported for this model")

    # ....................... #

    def _from_create_dto(self, payload: C, id: UUID | None = None) -> D:
        model = self.create_codec.transform(payload)

        if id is not None:
            model = model.model_copy(update={ID_FIELD: id}, deep=True)

        return model

    # ....................... #

    def _from_create_dto_many(
        self,
        payloads: Sequence[C],
        ids: Sequence[UUID] | None = None,
    ) -> Sequence[D]:
        models = list(self.create_codec.transform_many(payloads))

        if ids is not None:
            models = [
                m.model_copy(update={ID_FIELD: i}, deep=True)
                for m, i in zip(models, ids, strict=True)
            ]

        return models

    # ....................... #

    def _patch_codec(self) -> ModelCodec[Any, Any]:
        if self.update_codec is not None:
            return self.update_codec

        if self.update_cmd_type is not None:
            raise exc.configuration(
                "Update codec is required when update commands are supported"
            )

        return self.read_codec

    # ....................... #

    def _ident_rev(self) -> sql.Composable:
        return sql.Identifier(REV_FIELD)

    # ....................... #

    def _where_pk_rev(self) -> sql.Composable:
        return sql.SQL("{} = {} AND {} = {}").format(
            self.ident_pk(),
            sql.Placeholder(),
            self._ident_rev(),
            sql.Placeholder(),
        )

    # ....................... #

    async def _resolved_conflict_target(self) -> tuple[str, ...]:
        async def _factory() -> tuple[str, ...]:
            return await resolve_write_conflict_target(
                self.introspector,
                schema=(await self._qname()).schema,
                relation=(await self._qname()).name,
                configured=self.conflict_target,
            )

        # Conflict columns are the table's PK/unique columns — identical across
        # tenant schemas (tenant-independent), so always memoized.
        return await self._conflict_target_cell.resolve(_factory)

    async def _ident_conflict_target(self) -> sql.Composable:
        cols = await self._resolved_conflict_target()

        return sql.SQL(", ").join(sql.Identifier(c) for c in cols)

    # ....................... #

    @occ_retry
    async def create(self, payload: C, *, id: UUID | None = None) -> D:
        async with self._write_tx():
            model = self._from_create_dto(payload, id)
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(
                insert_data_raw, create=True
            )

            cols = [sql.Identifier(k) for k in insert_data.keys()]
            vals = [sql.Placeholder() for _ in insert_data.keys()]
            params = list(insert_data.values())

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(
                stmt, params, row_factory="dict", commit=False
            )

            if row is None:
                raise exc.concurrency(
                    "Failed to create a record",
                    code="create_failed",
                )

            res = self._decode_row(row)
            await self._write_history(res)

            return res

    # ....................... #

    @occ_retry
    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not payloads:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None
            payload_batches: list[list[JsonDict]] = []

            async def _insert_batch(batch: Sequence[JsonDict]) -> list[JsonDict]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("insert_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params = [b[k] for b in batch for k in keys]

                stmt = sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {vals} RETURNING {ret}"
                ).format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params,
                    row_factory="dict",
                    commit=False,
                )

                if len(rows) != len(batch):
                    raise exc.concurrency(
                        "Failed to create records (mismatch in number of rows)",
                        code="create_many_mismatch",
                    )

                return rows

            for offset in range(0, len(payloads), batch_size):
                payload_batch = payloads[offset : offset + batch_size]
                models = self._from_create_dto_many(payload_batch)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "create_many: adapted payload keys differ between batches",
                    )

                payload_batches.append(list(insert_data))

            batch_results = await gather_db_work(
                self.client,
                [partial(_insert_batch, b) for b in payload_batches],
            )

            result: list[D] = []
            for rows in batch_results:
                result.extend(self._decode_rows(rows))

            if len(result) != len(payloads):
                raise exc.internal("Failed to create all records")

            await self._write_history(*result)

            return result

    # ....................... #

    @occ_retry
    async def ensure(self, id: UUID, payload: C) -> D:
        """Insert a row at *id* when absent; otherwise return the existing row.

        Conflict is resolved on the primary key column (``id``) without updating
        existing rows.
        """

        async with self._write_tx():
            model = self._from_create_dto(payload, id)
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(
                insert_data_raw, create=True
            )

            cols = [sql.Identifier(k) for k in insert_data.keys()]
            vals = [sql.Placeholder() for _ in insert_data.keys()]
            params = list(insert_data.values())

            conflict = await self._ident_conflict_target()
            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) "
                "ON CONFLICT ({conflict}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                conflict=conflict,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(
                stmt, params, row_factory="dict", commit=False
            )

            if row is not None:
                res = self._decode_row(row)
                await self._write_history(res)
                return res

            existing = await self._fetch_domain_by_pk(model.id)
            return existing

    # ....................... #

    @occ_retry
    async def ensure_many(
        self,
        ids: Sequence[UUID],
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk insert rows when their primary keys are absent; return full rows in order.

        Each id must appear at most once. Conflicts on the primary key column do not
        update existing rows. History is written only for newly inserted documents.
        """

        if not payloads:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None

            def _pk_from_row(r: JsonDict) -> UUID:
                v = r[ID_FIELD]

                if isinstance(v, UUID):
                    return v

                return UUID(str(v))

            async def _ensure_batch(
                batch: Sequence[JsonDict],
                model_batch: Sequence[D],
            ) -> list[D]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("ensure_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params = [b[k] for b in batch for k in keys]

                conflict = await self._ident_conflict_target()
                stmt = sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {vals} "
                    "ON CONFLICT ({conflict}) DO NOTHING "
                    "RETURNING {ret}"
                ).format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    conflict=conflict,
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params,
                    row_factory="dict",
                    commit=False,
                )

                by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}
                need = [m.id for m in model_batch if m.id not in by_returned]

                if need:
                    fetched = await self._fetch_domains_by_pks(need)
                    by_existing = {d.id: d for d in fetched}

                else:
                    by_existing = {}

                ordered: list[D] = []
                inserted: list[D] = []

                for m in model_batch:
                    rj = by_returned.get(m.id)

                    if rj is not None:
                        dom = self._decode_row(rj)
                        inserted.append(dom)
                        ordered.append(dom)

                    else:
                        ex = by_existing.get(m.id)

                        if ex is None:
                            raise exc.not_found(
                                f"Record not found after ensure_many conflict: {m.id!s}",
                            )

                        ordered.append(ex)

                if inserted:
                    await self._write_history(*inserted)

                return ordered

            out: list[D] = []

            for offset in range(0, len(payloads), batch_size):
                id_batch = ids[offset : offset + batch_size]
                payload_batch = payloads[offset : offset + batch_size]
                models = self._from_create_dto_many(payload_batch, id_batch)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "ensure_many: adapted payload keys differ between batches",
                    )

                out.extend(await _ensure_batch(insert_data, models))

            if len(out) != len(payloads):
                raise exc.internal("ensure_many result length does not match input")

            return out

    # ....................... #

    @occ_retry
    async def upsert(self, id: UUID, create: C, update: U) -> D:
        """Insert *create* at *id* when free; otherwise apply ``update`` like :meth:`update`.

        ``database`` and ``application`` strategies both use the same pattern:
        attempt ``INSERT ... ON CONFLICT DO NOTHING``; on conflict, load the row
        and delegate to :meth:`update` with the current revision.
        """

        self._require_update_cmd()

        async with self._write_tx():
            model = self._from_create_dto(create, id)
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(
                insert_data_raw, create=True
            )

            cols = [sql.Identifier(k) for k in insert_data.keys()]
            vals = [sql.Placeholder() for _ in insert_data.keys()]
            params = list(insert_data.values())

            conflict = await self._ident_conflict_target()
            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) "
                "ON CONFLICT ({conflict}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                conflict=conflict,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(
                stmt, params, row_factory="dict", commit=False
            )

            if row is not None:
                res = self._decode_row(row)
                await self._write_history(res)

                return res

            current = await self._fetch_domain_by_pk(model.id, for_update=True)
            res, _ = await self.update(model.id, update, rev=current.rev)

            return res

    # ....................... #

    @occ_retry
    async def upsert_many(
        self,
        ids: Sequence[UUID],
        creates: Sequence[C],
        updates: Sequence[U],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk :meth:`upsert` using batched insert-then-:meth:`update_many` for conflicts."""

        self._require_update_cmd()

        if not creates:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None

            def _pk_from_row(r: JsonDict) -> UUID:
                v = r[ID_FIELD]
                if isinstance(v, UUID):
                    return v
                return UUID(str(v))

            async def _upsert_batch(
                batch: Sequence[JsonDict],
                model_batch: Sequence[D],
                u_for_batch: Sequence[U],
            ) -> list[D]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("upsert_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params_in = [b[k] for b in batch for k in keys]

                conflict = await self._ident_conflict_target()
                stmt = sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {vals} "
                    "ON CONFLICT ({conflict}) DO NOTHING "
                    "RETURNING {ret}"
                ).format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    conflict=conflict,
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params_in,
                    row_factory="dict",
                    commit=False,
                )

                by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}

                inserted: list[D] = []

                for m in model_batch:
                    rj = by_returned.get(m.id)
                    if rj is not None:
                        inserted.append(self._decode_row(rj))

                if inserted:
                    await self._write_history(*inserted)

                need_u: list[tuple[UUID, U]] = []
                u_list = list(u_for_batch)

                for i, m in enumerate(model_batch):
                    if m.id not in by_returned:
                        need_u.append((m.id, u_list[i]))

                by_updated: dict[UUID, D] = {}

                if need_u:
                    pks_u = [a[0] for a in need_u]
                    u_dtos = [a[1] for a in need_u]
                    currents = await self._fetch_domains_by_pks(pks_u, for_update=True)
                    by_cur = {c.id: c for c in currents}
                    revs = [by_cur[pk].rev for pk in pks_u]
                    updated, _ = await self.update_many(
                        pks_u,
                        u_dtos,
                        revs=revs,
                        batch_size=batch_size,
                    )
                    by_updated = {d.id: d for d in updated}

                ordered: list[D] = []

                for i, m in enumerate(model_batch):
                    rj = by_returned.get(m.id)

                    if rj is not None:
                        ordered.append(self._decode_row(rj))

                    else:
                        u_one = by_updated.get(m.id)

                        if u_one is None:
                            raise exc.not_found(
                                f"Record not found after upsert_many conflict: {m.id!s}",
                            )

                        ordered.append(u_one)

                return ordered

            out: list[D] = []

            for offset in range(0, len(creates), batch_size):
                id_batch = ids[offset : offset + batch_size]
                create_batch = creates[offset : offset + batch_size]
                update_batch = updates[offset : offset + batch_size]
                models = self._from_create_dto_many(create_batch, id_batch)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "upsert_many: adapted payload keys differ between batches",
                    )

                u_seq = list(update_batch)
                out.extend(await _upsert_batch(insert_data, models, u_seq))

            if len(out) != len(creates):
                raise exc.internal("upsert_many result length does not match input")

            return out

    # ....................... #

    def __bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.strategy == "application":
            diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @occ_retry
    async def __patch(
        self,
        pk: UUID,
        update: JsonDict | None = None,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        async with self._write_tx():
            current = await self.read_gw.get(pk)

            if update is not None:
                if rev is not None:
                    await self._validate_history((current, rev, update))

                _, diff = current.update(update)

            else:
                # Always historically consistent because we update only the revision and update timestamp
                _, diff = current.touch()

            if not diff:
                return current, diff

            diff = self.__bump_rev(current, diff)

            diff = await self.adapt_payload_for_write(diff, create=False)
            set_parts: list[sql.Composable] = []
            params: list[Any] = []

            for k, v in diff.items():
                set_parts.append(
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                )
                params.append(v)

            where_sql = self._where_pk_rev()
            where_params: list[Any] = [current.id, current.rev]
            where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]
            params.extend(where_params)

            stmt = sql.SQL(
                "UPDATE {table} SET {sets} WHERE {where} RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                sets=sql.SQL(", ").join(set_parts),
                where=where_sql,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(
                stmt, params, row_factory="dict", commit=False
            )

            if row is None:
                raise exc.concurrency("Failed to update record")

            res = self._decode_row(row)
            await self._write_history(res)

            return res, diff

    # ....................... #

    async def update(
        self,
        pk: UUID,
        dto: U,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        self._require_update_cmd()

        update_data = await self._encode_patch_one(dto, record_id=pk)

        return await self.__patch(pk, update_data, rev=rev)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        res, _ = await self.__patch(pk)

        return res

    # ....................... #

    @occ_retry
    async def __patch_group(
        self,
        key: tuple[str, ...],
        batch: list[tuple[UUID, int, JsonDict]],
    ) -> list[D]:
        # First two VALUES columns are the PK and the *expected* revision for the WHERE
        # clause. When the patch bumps ``rev``, the diff also contains a new ``rev`` value
        # for SET; naming the match column ``expected_rev`` avoids duplicate ``rev`` in
        # ``AS v(...)``, which PostgreSQL rejects as ambiguous.
        expected_rev_alias = "expected_rev"
        value_cols = [ID_FIELD, expected_rev_alias] + list(key)
        v_col_idents: list[sql.Composable] = [
            self.ident_pk(),
            sql.Identifier(expected_rev_alias),
            *(sql.Identifier(k) for k in key),
        ]
        values_rows: list[sql.Composable] = []
        params: list[Any] = []

        column_types = await self.column_types()

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(
                _values_placeholder_for_patch_group(
                    column=c,
                    expected_rev_alias=expected_rev_alias,
                    column_types=column_types,
                )
                for c in value_cols
            )
            + sql.SQL(")")
        )

        for _id, _rev, d in batch:
            row_params = [_id, _rev] + [d[k] for k in key]
            params.extend(row_params)
            values_rows.append(row_template)

        where_sql = sql.SQL("t.{tpk} = v.{vpk} AND t.{trev} = v.{vexp}").format(
            tpk=self.ident_pk(),
            vpk=self.ident_pk(),
            trev=self._ident_rev(),
            vexp=sql.Identifier(expected_rev_alias),
        )
        where_params: list[Any] = []
        where_sql, where_params = self._add_tenant_where(  # type: ignore[assignment]
            where_sql,
            where_params,
            table_alias="t",
        )
        params.extend(where_params)

        set_parts = [sql.SQL("{c} = v.{c}").format(c=sql.Identifier(k)) for k in key]

        stmt = sql.SQL(
            """
            UPDATE {table} AS t
            SET {sets}
            FROM (VALUES {vals}) AS v({cols})
            WHERE {where}
            RETURNING {ret}
            """
        ).format(
            table=(await self._qname()).ident(),
            sets=sql.SQL(", ").join(set_parts),
            vals=sql.SQL(", ").join(values_rows),
            cols=sql.SQL(", ").join(v_col_idents),
            where=where_sql,
            ret=self.return_clause(table_alias="t"),
        )

        rows = await self.client.fetch_all(
            stmt,
            params,
            row_factory="dict",
            commit=False,
        )
        updated_ids = {row[ID_FIELD] for row in rows}
        expected_ids = {_id for _id, _, _ in batch}

        missing = expected_ids - updated_ids

        if missing:
            raise exc.concurrency("Failed to update records")

        return self._decode_rows(rows)

    # ....................... #

    async def __patch_many(
        self,
        pks: Sequence[UUID],
        updates: Sequence[JsonDict] | None = None,
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:
        if not pks or (not updates and updates is not None):
            return [], []

        if updates is not None and len(pks) != len(updates):
            raise exc.internal("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        async with self._write_tx():
            currents = await self.read_gw.get_many(pks)

            groups: dict[tuple[str, ...], list[tuple[UUID, int, JsonDict]]] = (
                defaultdict(list)
            )

            if updates is None:

                async def _prepare_touch(c: D) -> tuple[UUID, int, JsonDict]:
                    _, diff = c.touch()
                    diff = self.__bump_rev(c, diff)
                    adapted_diff = await self.adapt_payload_for_write(
                        diff, create=False
                    )

                    return c.id, c.rev, adapted_diff

                results = await gather_db_work(
                    self.client,
                    [partial(_prepare_touch, c) for c in currents],
                )
                for cid, crev, diff in results:
                    # always the same key so we can handle only one group
                    key = tuple(sorted(diff.keys()))
                    groups[key].append((cid, crev, diff))

            else:
                # if revisions are provided, validate historical consistency
                if revs is not None:
                    data = [
                        (c, r, u)
                        for c, r, u in zip(
                            currents,
                            revs,
                            updates,
                            strict=True,
                        )
                    ]
                    await self._validate_history(*data)

                async def _prepare_update(
                    c: D,
                    u: JsonDict,
                ) -> tuple[UUID, int, JsonDict] | None:
                    _, diff = c.update(u)
                    if not diff:
                        return None

                    diff = self.__bump_rev(c, diff)

                    return (
                        c.id,
                        c.rev,
                        await self.adapt_payload_for_write(diff, create=False),
                    )

                results = await gather_db_work(
                    self.client,
                    [
                        partial(_prepare_update, c, u)  # type: ignore[misc]
                        for c, u in zip(currents, updates, strict=True)
                    ],
                )
                for r in results:
                    if r:
                        cid, crev, diff = r
                        # always the same key so we can handle only one group
                        key = tuple(sorted(diff.keys()))
                        groups[key].append((cid, crev, diff))

            if not groups:
                return currents, [{} for _ in currents]

            updated_models: dict[UUID, D] = {}
            update_diffs: dict[UUID, JsonDict] = {}
            work: list[tuple[tuple[str, ...], list[tuple[UUID, int, JsonDict]]]] = []

            for fields_key, rows in groups.items():
                for start in range(0, len(rows), batch_size):
                    work.append((fields_key, rows[start : start + batch_size]))

            batch_results = await gather_db_work(
                self.client,
                [
                    partial(
                        self.__patch_group,
                        fk,
                        bb,
                    )
                    for fk, bb in work
                ],
            )
            for (_, batch), updated in zip(work, batch_results, strict=True):
                updated_models.update({m.id: m for m in updated})

                for m, d in zip(updated, batch, strict=True):
                    update_diffs[m.id] = d[-1]

            res = [updated_models.get(c.id, c) for c in currents]
            res_diffs = [update_diffs.get(c.id, {}) for c in res]

            await self._write_history(*res)

            return res, res_diffs

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:

        self._require_update_cmd()

        updates: list[JsonDict] = []
        for start in range(0, len(dtos), batch_size):
            stop = start + batch_size
            updates.extend(
                await self._encode_patch_many(
                    dtos[start:stop], record_ids=pks[start:stop]
                ),
            )

        res, res_diffs = await self.__patch_many(
            pks,
            updates,
            revs=revs,
            batch_size=batch_size,
        )

        return res, res_diffs

    # ....................... #

    @occ_retry
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        batch_size: int = 200,
    ) -> tuple[int, Sequence[D]]:
        """Bulk-update rows matching *filters* in a single ``UPDATE … RETURNING``.

        Revision is bumped with ``rev = rev + 1`` when :attr:`strategy` is
        ``"application"``; for ``"database"`` the revision is left to triggers.
        """

        self._require_update_cmd()

        update_data = await self._encode_patch_one(dto)

        if not update_data:
            return 0, []

        adapted = dict(await self.adapt_payload_for_write(update_data, create=False))
        adapted.pop(REV_FIELD, None)

        if not adapted:
            return 0, []

        async with self._write_tx():
            set_parts: list[sql.Composable] = []
            params: list[Any] = []

            for k, v in adapted.items():
                set_parts.append(
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                )
                params.append(v)

            if self.strategy == "application":
                set_parts.append(
                    sql.SQL("{} = {} + 1").format(
                        self._ident_rev(),
                        self._ident_rev(),
                    )
                )

            where_sql, where_params = await self.where_clause(filters)
            params.extend(where_params)

            stmt = sql.SQL(
                "UPDATE {table} SET {sets} WHERE {where} RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                sets=sql.SQL(", ").join(set_parts),
                where=where_sql,
                ret=self.return_clause(),
            )

            rows = await self.client.fetch_all(
                stmt,
                params,
                row_factory="dict",
                commit=False,
            )

            doms = self._decode_rows(rows)
            await self._write_history(*doms)

            return len(doms), doms

    # ....................... #

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        res, _ = await self.__patch_many(pks, None, batch_size=batch_size)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        where_sql = sql.SQL("{pk} = {value}").format(
            pk=self.ident_pk(),
            value=sql.Placeholder(),
        )
        params: list[Any] = [pk]
        where_sql, params = self._add_tenant_where(where_sql, params)  # type: ignore[assignment]

        stmt = sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=(await self._qname()).ident(),
            where=where_sql,
        )

        n = await self.client.execute(stmt, params, return_rowcount=True)

        if n == 0:
            raise exc.not_found(f"Record not found: {pk}")

    # ....................... #

    async def kill_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> None:
        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        async with self._write_tx():
            where_sql = sql.SQL("{pk} = ANY({ids})").format(
                pk=self.ident_pk(),
                ids=sql.Placeholder(),
            )
            trailing_params: list[Any] = []
            where_sql, trailing_params = self._add_tenant_where(  # type: ignore[assignment]
                where_sql,
                trailing_params,
            )

            stmt = sql.SQL("DELETE FROM {table} WHERE {where}").format(
                table=(await self._qname()).ident(),
                where=where_sql,
            )

            async def _delete_batch(batch: list[UUID]) -> None:
                params: list[Any] = [list(batch), *trailing_params]

                n = await self.client.execute(stmt, params, return_rowcount=True)

                if n != len(batch):
                    if self.tenant_aware:
                        raise exc.not_found(
                            "Some records not found or not accessible in this tenant scope"
                        )

                    raise exc.not_found("Some records not found")

            batches = [
                list(pks[start : start + batch_size])
                for start in range(0, len(pks), batch_size)
            ]

            await gather_db_work(
                self.client,
                [partial(_delete_batch, b) for b in batches],
            )
