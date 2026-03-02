from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections import defaultdict
from typing import Any, Literal, Optional, Sequence, final, get_args
from uuid import UUID

import attrs
from psycopg import sql

from forze.base.errors import ConflictError, CoreError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.constants import REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .base import PostgresGateway
from .history_v2 import PostgresHistoryGateway
from .read import PostgresReadGateway

# ----------------------- #

PostgresRevBumpStrategy = Literal["database", "application"]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresWriteGateway[D: Document, C: CreateDocumentCmd, U: BaseDTO](
    PostgresGateway[D]
):
    read: PostgresReadGateway[D]
    create_dto: type[C]
    update_dto: type[U]
    history: Optional[PostgresHistoryGateway[D]] = None
    rev_bump_strategy: PostgresRevBumpStrategy = "database"

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.spec != self.read.spec:
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

            if self.spec != self.history.target_spec:
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
        if self.history is not None:
            currents = [c for c, _, _ in data]
            revs = [r for _, r, _ in data]
            updates = [u for _, _, u in data]

            to_check = [
                (c, r, u)
                for c, r, u in zip(currents, revs, updates, strict=True)
                if r != c.rev
            ]

            if to_check:
                pks_to_check = [c.id for c, _, _ in to_check]
                revs_to_check = [r for _, r, _ in to_check]

                hist_records = await self.history.read_many(pks_to_check, revs_to_check)

                for (c, _, u), h in zip(to_check, hist_records, strict=True):
                    if not c.validate_historical_consistency(h, u):
                        raise ConflictError(
                            "Historical consistency violation during update"
                        )

    # ....................... #

    def _ident_rev(self) -> sql.Composable:
        return sql.Identifier(REV_FIELD)

    # ....................... #
    #! TODO: get rid of this or replace with mixin check (subclass or so)

    def supports_soft_delete(
        self,
    ) -> bool:
        return SOFT_DELETE_FIELD in self.read_fields

    # ....................... #

    def _from_cdto(self, dto: C) -> D:
        data = pydantic_dump(dto, exclude={"unset": True})

        return pydantic_validate(self.model, data)

    # ....................... #

    def _where_pk_rev(self) -> sql.Composable:
        return sql.SQL("{} = {} AND {} = {}").format(
            self.ident_pk(),
            sql.Placeholder(),
            self._ident_rev(),
            sql.Placeholder(),
        )

    # ....................... #

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
            table=self.spec.ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is None:
            raise CoreError("Failed to create a record")

        res = pydantic_validate(self.model, row)
        await self._write_history(res)

        return res

    # ....................... #

    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not dtos:
            return []

        models = [self._from_cdto(d) for d in dtos]
        insert_data_raw = [pydantic_dump(m) for m in models]
        insert_data = [await self.adapt_payload_for_write(d) for d in insert_data_raw]

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]

        result: list[D] = []
        offset = 0

        while offset < len(insert_data):
            batch = insert_data[offset : offset + batch_size]

            value_parts: list[sql.Composable] = []
            params: list[Any] = []

            for b in batch:
                value_parts.append(
                    sql.SQL("(")
                    + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                    + sql.SQL(")")
                )
                params.extend(b[k] for k in keys)

            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES {vals} RETURNING {ret}"
            ).format(
                table=self.spec.ident(),
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
                raise CoreError("Failed to create records (mismatch in number of rows)")

            result.extend(pydantic_validate(self.model, row) for row in rows)
            offset += batch_size

        if len(result) != len(dtos):
            raise CoreError("Failed to create all records")

        await self._write_history(*result)

        return result

    # ....................... #

    async def __patch(
        self,
        pk: UUID,
        update: Optional[JsonDict] = None,
        *,
        rev: Optional[int] = None,
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

        #! TODO: validate this
        if self.rev_bump_strategy == "application":
            diff["rev"] = current.rev + 1

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
            table=self.spec.ident(),
            sets=sql.SQL(", ").join(set_parts),
            where=self._where_pk_rev(),
            ret=self.return_clause(),
        )

        row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=True)

        if row is None:
            raise CoreError("Не удалось обновить запись")  #! TODO: translate

        res = pydantic_validate(self.model, row)
        await self._write_history(res)

        return res

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> D:
        update_data = pydantic_dump(dto, exclude={"unset": True})

        return await self.__patch(pk, update_data, rev=rev)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        return await self.__patch(pk)

    # ....................... #

    async def __patch_many(
        self,
        pks: Sequence[UUID],
        updates: Optional[Sequence[JsonDict]] = None,
        *,
        revs: Optional[Sequence[int]] = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        if not pks or (not updates and updates is not None):
            return []

        if updates is not None and len(pks) != len(updates):
            raise ValidationError(
                "Pks и updates должны иметь одинаковую длину"
            )  #! TODO: translate

        if len(pks) != len(set(pks)):
            raise ValidationError("Pks должны быть уникальными")  #! TODO: translate

        currents = await self.read.get_many(pks)

        groups: dict[tuple[str, ...], list[tuple[UUID, int, JsonDict]]] = defaultdict(
            list
        )

        if updates is None:
            for c in currents:
                _, diff = c.touch()

                #! TODO: validate this
                if self.rev_bump_strategy == "application":
                    diff["rev"] = c.rev + 1

                diff = await self.adapt_payload_for_write(diff)
                # always the same key so we can handle only one group
                key = tuple(sorted(diff.keys()))
                groups[key].append((c.id, c.rev, diff))

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

            for c, u in zip(currents, updates):
                _, diff = c.update(u)

                if not diff:
                    continue

                #! TODO: validate this (replace with custom method)
                if self.rev_bump_strategy == "application":
                    diff["rev"] = c.rev + 1

                diff = await self.adapt_payload_for_write(diff)
                key = tuple(sorted(diff.keys()))
                groups[key].append((c.id, c.rev, diff))

        if not groups:
            return currents

        for fields_key, rows in groups.items():
            for start in range(0, len(rows), batch_size):
                batch = rows[start : start + batch_size]

                set_parts = [
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                    for k in fields_key
                ]

                stmt = sql.SQL("UPDATE {table} SET {sets} WHERE {where}").format(
                    table=self.spec.ident(),
                    sets=sql.SQL(", ").join(set_parts),
                    where=self._where_pk_rev(),
                )

                params_for_many: list[Sequence[Any]] = []

                for _id, _rev, d in batch:
                    params = [d[k] for k in fields_key]
                    params.extend([_id, _rev])
                    params_for_many.append(params)

                await self.client.execute_many(stmt, params_for_many)

        res = await self.read.get_many(pks)
        await self._write_history(*res)

        return res

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
        batch_size: int = 500,
    ) -> Sequence[D]:
        updates = [pydantic_dump(d, exclude={"unset": True}) for d in dtos]

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

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch(pk, {SOFT_DELETE_FIELD: True}, rev=rev)

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
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

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> D:
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self.__patch(pk, {SOFT_DELETE_FIELD: False}, rev=rev)

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
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
        current = await self.read.get(pk)

        stmt = sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=self.spec.ident(),
            where=self._where_pk_rev(),
        )
        params = [current.id, current.rev]

        await self.client.execute(stmt, params)

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
            raise ValidationError("Pks должны быть уникальными")  #! TODO: translate

        currents = await self.read.get_many(pks)
        pairs = [(c.id, c.rev) for c in currents]
        expected = len(pairs)
        killed_total = 0

        for start in range(0, len(pairs), batch_size):
            batch = pairs[start : start + batch_size]

            values_sql = sql.SQL(", ").join(
                sql.SQL("({}, {})").format(sql.Placeholder(), sql.Placeholder())
                for _ in batch
            )

            stmt = sql.SQL(
                """
                    DELETE FROM {table} AS t
                    USING (VALUES {vals}) AS v(id, rev)
                    WHERE t.{pk} = v.id AND t.{rev} = v.rev
                    """
            ).format(
                table=self.spec.ident(),
                vals=values_sql,
                pk=self.ident_pk(),
                rev=self._ident_rev(),
            )

            params: list[Any] = []

            for _id, _rev in batch:
                params.extend([_id, _rev])

            killed = await self.client.execute(stmt, params, return_rowcount=True)
            killed_total += killed

        if killed_total != expected:
            raise CoreError("Не удалось удалить записи")  #! TODO: translate
