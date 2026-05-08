"""Write gateway for creating, updating, soft-deleting, and hard-deleting Postgres documents."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections import defaultdict
from functools import partial
from typing import Any, Sequence, final, get_args
from uuid import UUID

import attrs
from psycopg import sql
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from forze.application.contracts.query import QueryFilterExpression
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
    pydantic_dump_many,
    pydantic_transform,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import ID_FIELD, REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..db_gather import gather_db_work
from .base import PostgresGateway
from .history import PostgresHistoryGateway
from .read import PostgresReadGateway
from .types import PostgresBookkeepingStrategy

# ----------------------- #


def optimistic_retry(*, attempts: int = 3):  # type: ignore[no-untyped-def]
    """Return a tenacity retry decorator for :exc:`~forze.base.errors.ConcurrencyError`.

    Uses exponential back-off and re-raises the error after *attempts* failures.

    :param attempts: Maximum number of attempts before re-raising.
    """

    return retry(
        retry=retry_if_exception_type(ConcurrencyError),
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=0.01, min=0.01, max=0.2),
        reraise=True,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresWriteGateway[D: Document, C: CreateDocumentCmd, U: BaseDTO](
    PostgresGateway[D]
):
    """Write gateway for document mutations with optimistic concurrency control.

    Requires a companion :class:`PostgresReadGateway` sharing the same client.
    Optionally writes revision history via :class:`PostgresHistoryGateway`.
    All mutating operations are decorated with :func:`optimistic_retry`.
    """

    read_gw: PostgresReadGateway[D]
    """Read gateway for the same document type."""

    create_cmd_type: type[C]
    """Pydantic model for creation payloads."""

    update_cmd_type: type[U] | None = attrs.field(default=None)
    """Pydantic model for update payloads."""

    history_gw: PostgresHistoryGateway[D] | None = attrs.field(default=None)
    """Optional history gateway for revision snapshots."""

    strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.source_qname != self.read_gw.source_qname:
            raise CoreError(
                f"Table specification mismatch. Write gateway and nested read gateway must have the same specification. Write: {self.source_qname}, Read: {self.read_gw.source_qname}"
            )

        if self.client is not self.read_gw.client:
            raise CoreError(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.tenant_aware != self.read_gw.tenant_aware:
            raise CoreError(
                "Tenant awareness mismatch. Write gateway and nested read gateway must have the same tenant awareness."
            )

        if self.history_gw is not None:
            if self.client is not self.history_gw.client:
                raise CoreError(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.source_qname != self.history_gw.target_qname:
                raise CoreError(
                    f"Table specification mismatch. Write gateway and nested history gateway must have the same specification. Write: {self.source_qname}, History: {self.history_gw.target_qname}"
                )

            if self.tenant_aware != self.history_gw.tenant_aware:
                raise CoreError(
                    "Tenant awareness mismatch. Write gateway and nested history gateway must have the same tenant awareness."
                )

        if self.strategy not in get_args(PostgresBookkeepingStrategy):
            raise CoreError(f"Invalid bookkeeping strategy: {self.strategy}")

    # ....................... #

    def _require_update_cmd(self) -> None:
        if self.update_cmd_type is None:
            raise CoreError("Update command type is not supported for this model")

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history_gw is not None:
            await self.history_gw.write_many(data)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        if self.history_gw is None:
            for current, rev, _ in data:
                if rev != current.rev:
                    raise ConflictError("Revision mismatch", code="revision_mismatch")

            return

        to_check = [
            (current, rev, update)
            for current, rev, update in data
            if rev != current.rev
        ]
        bad_records = [rev for current, rev, _ in to_check if rev > current.rev]

        if bad_records:
            raise ValidationError("Invalid revision number")

        if to_check:
            pks_to_check = [c.id for c, _, _ in to_check]
            revs_to_check = [r for _, r, _ in to_check]
            hist_records = await self.history_gw.read_many(pks_to_check, revs_to_check)

            if len(hist_records) != len(to_check):
                raise NotFoundError(
                    "History records not found. Please retry with actual revision number."
                )

            for (c, _, u), h in zip(to_check, hist_records, strict=True):
                if not c.validate_historical_consistency(h, u):
                    raise ConflictError(
                        "Historical consistency violation during update",
                        code="historical_consistency_violation",
                    )

    # ....................... #

    def _ident_rev(self) -> sql.Composable:
        return sql.Identifier(REV_FIELD)

    # ....................... #

    def supports_soft_delete(self) -> bool:
        return issubclass(self.model_type, SoftDeletionMixin)

    # ....................... #

    def _where_pk_rev(self) -> sql.Composable:
        return sql.SQL("{} = {} AND {} = {}").format(
            self.ident_pk(),
            sql.Placeholder(),
            self._ident_rev(),
            sql.Placeholder(),
        )

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create(self, dto: C) -> D:
        model = pydantic_transform(self.model_type, dto)
        insert_data_raw = pydantic_dump(model)
        insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {ret}"
        ).format(
            table=self.source_qname.ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is None:
            raise ConcurrencyError(
                message="Failed to create a record",
                code="create_failed",
            )

        res = pydantic_validate(self.model_type, row)
        await self._write_history(res)

        return res

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not dtos:
            return []

        models = pydantic_transform_many(self.model_type, dtos)
        insert_data_raw = pydantic_dump_many(models)
        insert_data = await self.adapt_many_payload_for_write(
            insert_data_raw,
            create=True,
        )

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
            + sql.SQL(")")
        )

        async def _insert_batch(batch: Sequence[JsonDict]) -> list[JsonDict]:
            # ⚡ Bolt: Duplicate the precomputed row template
            value_parts = [row_template] * len(batch)
            params = [b[k] for b in batch for k in keys]

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES {vals} RETURNING {ret}"
            ).format(
                table=self.source_qname.ident(),
                cols=sql.SQL(", ").join(col_idents),
                vals=sql.SQL(", ").join(value_parts),
                ret=self.return_clause(),
            )

            rows = await self.client.fetch_all(
                stmt,
                params,
                row_factory="dict",
                commit=True,
            )

            if len(rows) != len(batch):
                raise ConcurrencyError(
                    message="Failed to create records (mismatch in number of rows)",
                    code="create_many_mismatch",
                )

            return rows

        batches = [
            insert_data[offset : offset + batch_size]
            for offset in range(0, len(insert_data), batch_size)
        ]
        batch_results = await gather_db_work(
            self.client,
            [partial(_insert_batch, b) for b in batches],
        )

        result_raw: list[JsonDict] = []
        for rows in batch_results:
            result_raw.extend(rows)

        if len(result_raw) != len(dtos):
            raise CoreError("Failed to create all records")

        result = pydantic_validate_many(self.model_type, result_raw)
        await self._write_history(*result)

        return result

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def ensure(self, dto: C) -> D:
        """Insert a row when the primary key is absent; otherwise return the existing row.

        The caller must supply a primary key on the create command; conflict
        is resolved on the primary key column (``id``) without updating
        existing rows.
        """

        model = pydantic_transform(self.model_type, dto)
        insert_data_raw = pydantic_dump(model)
        insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({vals}) "
            "ON CONFLICT ({pk}) DO NOTHING "
            "RETURNING {ret}"
        ).format(
            table=self.source_qname.ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
            pk=self.ident_pk(),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is not None:
            res = pydantic_validate(self.model_type, row)
            await self._write_history(res)
            return res

        existing = await self.read_gw.get(model.id)
        return existing

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk insert rows when their primary keys are absent; return full rows in order.

        The caller must supply a primary key on every create command; each id
        must appear at most once in ``dtos``. Conflicts on the primary key
        column do not update existing rows. History is written only for newly
        inserted documents.
        """

        if not dtos:
            return []

        models = pydantic_transform_many(self.model_type, dtos)
        insert_data_raw = pydantic_dump_many(models)
        insert_data = await self.adapt_many_payload_for_write(
            insert_data_raw,
            create=True,
        )

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
            + sql.SQL(")")
        )

        def _pk_from_row(r: JsonDict) -> UUID:
            v = r[ID_FIELD]
            if isinstance(v, UUID):
                return v
            return UUID(str(v))

        async def _ensure_batch(
            batch: Sequence[JsonDict],
            model_batch: Sequence[D],
        ) -> list[D]:
            value_parts = [row_template] * len(batch)
            params = [b[k] for b in batch for k in keys]

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES {vals} "
                "ON CONFLICT ({pk}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=self.source_qname.ident(),
                cols=sql.SQL(", ").join(col_idents),
                vals=sql.SQL(", ").join(value_parts),
                pk=self.ident_pk(),
                ret=self.return_clause(),
            )

            rows = await self.client.fetch_all(
                stmt,
                params,
                row_factory="dict",
                commit=True,
            )

            by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}
            need = [m.id for m in model_batch if m.id not in by_returned]
            if need:
                fetched = await self.read_gw.get_many(need)
                by_existing = {d.id: d for d in fetched}
            else:
                by_existing = {}

            ordered: list[D] = []
            inserted: list[D] = []
            for m in model_batch:
                rj = by_returned.get(m.id)
                if rj is not None:
                    dom = pydantic_validate(self.model_type, rj)
                    inserted.append(dom)
                    ordered.append(dom)
                else:
                    ex = by_existing.get(m.id)
                    if ex is None:
                        raise NotFoundError(
                            f"Record not found after ensure_many conflict: {m.id!s}",
                        )
                    ordered.append(ex)

            if inserted:
                await self._write_history(*inserted)

            return ordered

        out: list[D] = []
        for offset in range(0, len(insert_data), batch_size):
            data_batch = insert_data[offset : offset + batch_size]
            model_batch = models[offset : offset + batch_size]
            out.extend(await _ensure_batch(data_batch, model_batch))

        if len(out) != len(dtos):
            raise CoreError("ensure_many result length does not match input")

        return out

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def upsert(self, create_dto: C, update_dto: U) -> D:
        """Insert when the primary key is free; otherwise apply ``update_dto`` like :meth:`update`.

        ``database`` and ``application`` strategies both use the same pattern:
        attempt ``INSERT ... ON CONFLICT DO NOTHING``; on conflict, load the row
        and delegate to :meth:`update` with the current revision.
        """

        self._require_update_cmd()

        model = pydantic_transform(self.model_type, create_dto)
        insert_data_raw = pydantic_dump(model)
        insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({vals}) "
            "ON CONFLICT ({pk}) DO NOTHING "
            "RETURNING {ret}"
        ).format(
            table=self.source_qname.ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
            pk=self.ident_pk(),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is not None:
            res = pydantic_validate(self.model_type, row)
            await self._write_history(res)
            return res

        current = await self.read_gw.get(model.id)
        res, _ = await self.update(model.id, update_dto, rev=current.rev)
        return res

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk :meth:`upsert` using batched insert-then-:meth:`update_many` for conflicts."""

        self._require_update_cmd()

        if not pairs:
            return []

        creates = [c for c, _ in pairs]
        models = pydantic_transform_many(self.model_type, creates)
        insert_data_raw = pydantic_dump_many(models)
        insert_data = await self.adapt_many_payload_for_write(
            insert_data_raw,
            create=True,
        )

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
            + sql.SQL(")")
        )

        def _pk_from_row(r: JsonDict) -> UUID:
            v = r[ID_FIELD]
            if isinstance(v, UUID):
                return v
            return UUID(str(v))

        u_seq = [u for _, u in pairs]

        async def _upsert_batch(
            batch: Sequence[JsonDict],
            model_batch: Sequence[D],
            u_for_batch: Sequence[U],
        ) -> list[D]:
            value_parts = [row_template] * len(batch)
            params_in = [b[k] for b in batch for k in keys]

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES {vals} "
                "ON CONFLICT ({pk}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=self.source_qname.ident(),
                cols=sql.SQL(", ").join(col_idents),
                vals=sql.SQL(", ").join(value_parts),
                pk=self.ident_pk(),
                ret=self.return_clause(),
            )

            rows = await self.client.fetch_all(
                stmt,
                params_in,
                row_factory="dict",
                commit=True,
            )

            by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}

            inserted: list[D] = []
            for m in model_batch:
                rj = by_returned.get(m.id)
                if rj is not None:
                    inserted.append(pydantic_validate(self.model_type, rj))

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
                currents = await self.read_gw.get_many(pks_u)
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
                    ordered.append(pydantic_validate(self.model_type, rj))
                else:
                    u_one = by_updated.get(m.id)
                    if u_one is None:
                        raise NotFoundError(
                            f"Record not found after upsert_many conflict: {m.id!s}",
                        )
                    ordered.append(u_one)

            return ordered

        out: list[D] = []
        for offset in range(0, len(insert_data), batch_size):
            data_b = insert_data[offset : offset + batch_size]
            model_b = models[offset : offset + batch_size]
            u_b = u_seq[offset : offset + batch_size]
            out.extend(await _upsert_batch(data_b, model_b, u_b))

        if len(out) != len(pairs):
            raise CoreError("upsert_many result length does not match input")

        return out

    # ....................... #

    def __bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.strategy == "application":
            diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def __patch(
        self,
        pk: UUID,
        update: JsonDict | None = None,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
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
            table=self.source_qname.ident(),
            sets=sql.SQL(", ").join(set_parts),
            where=where_sql,
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is None:
            raise ConcurrencyError("Failed to update record")

        res = pydantic_validate(self.model_type, row)
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

        update_data = pydantic_dump(dto, exclude={"unset": True})

        return await self.__patch(pk, update_data, rev=rev)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        res, _ = await self.__patch(pk)

        return res

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
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

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in value_cols)
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
            table=self.source_qname.ident(),
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
            commit=True,
        )
        updated_ids = {row[ID_FIELD] for row in rows}
        expected_ids = {_id for _id, _, _ in batch}

        missing = expected_ids - updated_ids

        if missing:
            raise ConcurrencyError("Failed to update records")

        return pydantic_validate_many(self.model_type, rows)

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
            raise CoreError("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        currents = await self.read_gw.get_many(pks)

        await self.column_types()

        groups: dict[tuple[str, ...], list[tuple[UUID, int, JsonDict]]] = defaultdict(
            list
        )

        if updates is None:

            async def _prepare_touch(c: D) -> tuple[UUID, int, JsonDict]:
                _, diff = c.touch()
                diff = self.__bump_rev(c, diff)
                adapted_diff = await self.adapt_payload_for_write(diff, create=False)

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
            [partial(self.__patch_group, fk, bb) for fk, bb in work],
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

        updates = pydantic_dump_many(dtos, exclude={"unset": True})

        res, res_diffs = await self.__patch_many(
            pks,
            updates,
            revs=revs,
            batch_size=batch_size,
        )

        return res, res_diffs

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
    ) -> tuple[int, Sequence[D]]:
        """Bulk-update rows matching *filters* in a single ``UPDATE … RETURNING``.

        Revision is bumped with ``rev = rev + 1`` when :attr:`strategy` is
        ``"application"``; for ``"database"`` the revision is left to triggers.
        """

        self._require_update_cmd()

        update_data = pydantic_dump(dto, exclude={"unset": True})

        if not update_data:
            return 0, []

        adapted = dict(await self.adapt_payload_for_write(update_data, create=False))
        adapted.pop(REV_FIELD, None)

        if not adapted:
            return 0, []

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
            table=self.source_qname.ident(),
            sets=sql.SQL(", ").join(set_parts),
            where=where_sql,
            ret=self.return_clause(),
        )

        rows = await self.client.fetch_all(
            stmt,
            params,
            row_factory="dict",
            commit=True,
        )

        doms = pydantic_validate_many(self.model_type, rows)
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

    async def delete(self, pk: UUID, *, rev: int | None = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res, _ = await self.__patch(pk, {SOFT_DELETE_FIELD: True}, rev=rev)

        return res

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res, _ = await self.__patch_many(
            pks,
            [{SOFT_DELETE_FIELD: True} for _ in pks],
            batch_size=batch_size,
            revs=revs,
        )

        return res

    # ....................... #

    async def restore(self, pk: UUID, *, rev: int | None = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res, _ = await self.__patch(pk, {SOFT_DELETE_FIELD: False}, rev=rev)

        return res

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        res, _ = await self.__patch_many(
            pks,
            [{SOFT_DELETE_FIELD: False} for _ in pks],
            batch_size=batch_size,
            revs=revs,
        )

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
            table=self.source_qname.ident(),
            where=where_sql,
        )

        if self.tenant_aware:
            n = await self.client.execute(stmt, params, return_rowcount=True)

            if n == 0:
                raise NotFoundError(f"Record not found: {pk}")

        else:
            await self.client.execute(stmt, params)

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
            raise ValidationError("Primary keys must be unique")

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
            table=self.source_qname.ident(),
            where=where_sql,
        )

        async def _delete_batch(batch: list[UUID]) -> None:
            params: list[Any] = [list(batch), *trailing_params]

            if self.tenant_aware:
                n = await self.client.execute(stmt, params, return_rowcount=True)

                if n != len(batch):
                    raise NotFoundError(
                        "Some records not found or not accessible in this tenant scope"
                    )
            else:
                await self.client.execute(stmt, params)

        batches = [
            list(pks[start : start + batch_size])
            for start in range(0, len(pks), batch_size)
        ]

        await gather_db_work(
            self.client,
            [partial(_delete_batch, b) for b in batches],
        )
