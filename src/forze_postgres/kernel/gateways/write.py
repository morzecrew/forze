"""Write gateway for creating, updating, soft-deleting, and hard-deleting Postgres documents."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from collections import defaultdict
from typing import Any, Literal, Sequence, final, get_args
from uuid import UUID

import attrs
from psycopg import sql
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

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
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import ID_FIELD, REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .base import PostgresGateway
from .history import PostgresHistoryGateway
from .read import PostgresReadGateway

# ----------------------- #

PostgresRevBumpStrategy = Literal["database", "application"]
"""Strategy for incrementing the document revision: ``"database"`` (trigger) or ``"application"``."""

# ....................... #


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

    read: PostgresReadGateway[D]
    create_dto: type[C]
    update_dto: type[U]
    history: PostgresHistoryGateway[D] | None = None
    rev_bump_strategy: PostgresRevBumpStrategy = "database"

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.qname != self.read.qname:
            raise CoreError(
                "Table specification mismatch. Write gateway and nested read gateway must have the same specification."
            )

        if self.client is not self.read.client:
            raise CoreError(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.history is not None:
            if self.client is not self.history.client:
                raise CoreError(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.qname != self.history.target_qname:
                raise CoreError(
                    "Table specification mismatch. Write gateway and nested history gateway must have the same specification."
                )

        if self.rev_bump_strategy not in get_args(PostgresRevBumpStrategy):
            raise CoreError(f"Invalid revision bump strategy: {self.rev_bump_strategy}")

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history is not None:
            await self.history.write_many(data)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        if self.history is None:
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
            hist_records = await self.history.read_many(pks_to_check, revs_to_check)

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
    #! TODO: get rid of this or replace with mixin check (subclass or so)

    def supports_soft_delete(self) -> bool:
        return SOFT_DELETE_FIELD in self.read_fields

    # ....................... #

    def _from_cdto(self, dto: C) -> D:
        data = pydantic_dump(dto, exclude={"unset": True})

        return pydantic_validate(self.model, data)

    # ....................... #

    def _from_cdto_many(self, dtos: Sequence[C]) -> Sequence[D]:
        data = pydantic_dump_many(dtos, exclude={"unset": True})
        return pydantic_validate_many(self.model, data)

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
        model = self._from_cdto(dto)
        insert_data_raw = pydantic_dump(model)  #! mode=python ??????
        insert_data = await self.adapt_payload_for_write(insert_data_raw)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {ret}"
        ).format(
            table=self.qname.ident(),
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

        res = pydantic_validate(self.model, row)
        await self._write_history(res)

        return res

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not dtos:
            return []

        models = self._from_cdto_many(dtos)
        insert_data_raw = pydantic_dump_many(models)
        insert_data = await self.adapt_many_payload_for_write(insert_data_raw)

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
            + sql.SQL(")")
        )

        result_raw: list[JsonDict] = []
        offset = 0

        while offset < len(insert_data):
            batch = insert_data[offset : offset + batch_size]

            # ⚡ Bolt: Duplicate the precomputed row template
            value_parts = [row_template] * len(batch)
            params = [b[k] for b in batch for k in keys]

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES {vals} RETURNING {ret}"
            ).format(
                table=self.qname.ident(),
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

            result_raw.extend(rows)
            offset += batch_size

        if len(result_raw) != len(dtos):
            raise CoreError("Failed to create all records")

        result = pydantic_validate_many(self.model, result_raw)
        await self._write_history(*result)

        return result

    # ....................... #

    def __bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.rev_bump_strategy == "application":
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
    ) -> D:
        current = await self.read.get(pk)

        if update is not None:
            if rev is not None:
                await self._validate_history((current, rev, update))

            _, diff = current.update(update)

        else:
            # Always historically consistent because we update only the revision and update timestamp
            _, diff = current.touch()

        if not diff:
            return current

        diff = self.__bump_rev(current, diff)

        diff = await self.adapt_payload_for_write(diff)
        set_parts: list[sql.Composable] = []
        params: list[Any] = []

        for k, v in diff.items():
            set_parts.append(
                sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
            )
            params.append(v)

        params.extend([current.id, current.rev])

        stmt = sql.SQL(
            "UPDATE {table} SET {sets} WHERE {where} RETURNING {ret}"
        ).format(
            table=self.qname.ident(),
            sets=sql.SQL(", ").join(set_parts),
            where=self._where_pk_rev(),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is None:
            raise ConcurrencyError("Failed to update record")

        res = pydantic_validate(self.model, row)
        await self._write_history(res)

        return res

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: int | None = None) -> D:
        update_data = pydantic_dump(dto, exclude={"unset": True})

        return await self.__patch(pk, update_data, rev=rev)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        return await self.__patch(pk)

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def __patch_group(
        self,
        key: tuple[str, ...],
        batch: list[tuple[UUID, int, JsonDict]],
    ) -> list[D]:
        cols = [ID_FIELD, REV_FIELD] + list(key)
        values_rows: list[sql.Composable] = []
        params: list[Any] = []

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in cols)
            + sql.SQL(")")
        )

        for _id, _rev, d in batch:
            row_params = [_id, _rev] + [d[k] for k in key]
            params.extend(row_params)
            values_rows.append(row_template)

        set_parts = [sql.SQL("{c} = v.{c}").format(c=sql.Identifier(k)) for k in key]

        stmt = sql.SQL(
            """
            UPDATE {table} AS t
            SET {sets}
            FROM (VALUES {vals}) AS v({cols})
            WHERE t.{pk} = v.{pk} AND t.{rev} = v.{rev}
            RETURNING {ret}
            """
        ).format(
            table=self.qname.ident(),
            sets=sql.SQL(", ").join(set_parts),
            vals=sql.SQL(", ").join(values_rows),
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            pk=self.ident_pk(),
            rev=self._ident_rev(),
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

        return pydantic_validate_many(self.model, rows)

    # ....................... #

    async def __patch_many(
        self,
        pks: Sequence[UUID],
        updates: Sequence[JsonDict] | None = None,
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not pks or (not updates and updates is not None):
            return []

        if updates is not None and len(pks) != len(updates):
            raise CoreError("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        currents = await self.read.get_many(pks)

        groups: dict[tuple[str, ...], list[tuple[UUID, int, JsonDict]]] = defaultdict(
            list
        )

        if updates is None:

            async def _prepare_touch(c: D) -> tuple[UUID, int, JsonDict]:
                _, diff = c.touch()
                diff = self.__bump_rev(c, diff)
                adapted_diff = await self.adapt_payload_for_write(diff)

                return c.id, c.rev, adapted_diff

            results = await asyncio.gather(*(_prepare_touch(c) for c in currents))
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
                c: D, u: JsonDict
            ) -> tuple[UUID, int, JsonDict] | None:
                _, diff = c.update(u)
                if not diff:
                    return None

                diff = self.__bump_rev(c, diff)
                return c.id, c.rev, await self.adapt_payload_for_write(diff)

            results = await asyncio.gather(  # type: ignore[assignment]
                *(_prepare_update(c, u) for c, u in zip(currents, updates))
            )
            for r in results:
                if r:
                    cid, crev, diff = r
                    # always the same key so we can handle only one group
                    key = tuple(sorted(diff.keys()))
                    groups[key].append((cid, crev, diff))

        if not groups:
            return currents

        updated_models: dict[UUID, D] = {}

        for fields_key, rows in groups.items():
            for start in range(0, len(rows), batch_size):
                batch = rows[start : start + batch_size]
                updated = await self.__patch_group(fields_key, batch)
                updated_models.update({m.id: m for m in updated})

        res = [updated_models.get(c.id, c) for c in currents]
        await self._write_history(*res)

        return res

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        updates = pydantic_dump_many(dtos, exclude={"unset": True})

        return await self.__patch_many(pks, updates, revs=revs, batch_size=batch_size)

    # ....................... #

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 500,
    ) -> Sequence[D]:
        return await self.__patch_many(pks, None, batch_size=batch_size)

    # ....................... #

    async def delete(self, pk: UUID, *, rev: int | None = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch(pk, {SOFT_DELETE_FIELD: True}, rev=rev)

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch_many(
            pks,
            [{SOFT_DELETE_FIELD: True} for _ in pks],
            batch_size=batch_size,
            revs=revs,
        )

    # ....................... #

    async def restore(self, pk: UUID, *, rev: int | None = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch(pk, {SOFT_DELETE_FIELD: False}, rev=rev)

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch_many(
            pks,
            [{SOFT_DELETE_FIELD: False} for _ in pks],
            batch_size=batch_size,
            revs=revs,
        )

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        stmt = sql.SQL("DELETE FROM {table} WHERE {pk} = {value}").format(
            value=sql.Placeholder(),
            table=self.qname.ident(),
            pk=self.ident_pk(),
        )

        await self.client.execute(stmt, [pk])

    # ....................... #

    async def kill_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 500,
    ) -> None:
        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        for start in range(0, len(pks), batch_size):
            batch = pks[start : start + batch_size]
            stmt = sql.SQL("DELETE FROM {table} WHERE {pk} = ANY({ids})").format(
                table=self.qname.ident(),
                pk=self.ident_pk(),
                ids=sql.Placeholder(),
            )
            await self.client.execute(stmt, [list(batch)])
