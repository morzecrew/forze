from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Literal, Sequence, final, get_args
from uuid import UUID

import attrs
from psycopg import sql

from forze.base.errors import CoreError, NotFoundError, ValidationError
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

from .base import PostgresGateway, PostgresQualifiedName

# ----------------------- #

PostgresHistoryWriteStrategy = Literal["database", "application"]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHistoryGateway[D: Document](PostgresGateway[D]):
    strategy: PostgresHistoryWriteStrategy = "database"
    target_qname: PostgresQualifiedName

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.strategy not in get_args(PostgresHistoryWriteStrategy):
            raise CoreError(f"Invalid history write strategy: {self.strategy}")

    # ....................... #

    async def read(self, pk: UUID, rev: int) -> D:
        where = sql.SQL("{h} = {h_v} AND {pk} = {pk_v} AND {rev} = {rev_v}").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=self.target_qname.literal(),
            pk=sql.Identifier(ID_FIELD),
            pk_v=sql.Placeholder(),
            rev=sql.Identifier(REV_FIELD),
            rev_v=sql.Placeholder(),
        )

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=self.qname.ident(),
            where=where,
        )

        row = await self.client.fetch_one(stmt, (pk, rev), row_factory="dict")

        if row is None:
            raise NotFoundError(f"History not found: {pk}, {rev}")

        return pydantic_validate(self.model, row[HISTORY_DATA_FIELD])

    # ....................... #

    async def read_many(self, pks: Sequence[UUID], revs: Sequence[int]) -> Sequence[D]:
        if len(pks) != len(revs):
            raise ValidationError("Length of pks and revs must be the same")

        values_sql = sql.SQL(", ").join(
            sql.SQL("({}, {})").format(sql.Placeholder(), sql.Placeholder())
            for _ in revs
        )

        where = sql.SQL("{h} = {h_v} AND ({pk}, {rev}) IN ({vals})").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=self.target_qname.literal(),
            pk=sql.Identifier(ID_FIELD),
            rev=sql.Identifier(REV_FIELD),
            vals=values_sql,
        )

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=self.qname.ident(),
            where=where,
        )

        params: list[Any] = []

        for p, r in zip(pks, revs, strict=True):
            params.extend([p, r])

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")
        return [pydantic_validate(self.model, row[HISTORY_DATA_FIELD]) for row in rows]

    # ....................... #

    def _from_data(self, data: D) -> DocumentHistory[D]:
        return DocumentHistory(
            source=self.target_qname.string(),
            id=data.id,
            rev=data.rev,
            data=data,
        )

    # ....................... #

    async def write(self, data: D) -> None:
        if self.strategy == "database":
            return

        record = self._from_data(data)
        insert_data_raw = pydantic_dump(record)
        insert_data = await self.adapt_payload_for_write(insert_data_raw)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
            table=self.qname.ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
        )

        await self.client.execute(stmt, params)

    # ....................... #

    async def write_many(self, data: Sequence[D], *, batch_size: int = 500) -> None:
        if self.strategy == "database":
            return

        records = [self._from_data(d) for d in data]
        insert_data_raw = [pydantic_dump(r) for r in records]
        insert_data = [await self.adapt_payload_for_write(d) for d in insert_data_raw]

        keys = list(insert_data[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]

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

            stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES {vals}").format(
                table=self.qname.ident(),
                cols=sql.SQL(", ").join(col_idents),
                vals=sql.SQL(", ").join(value_parts),
            )

            await self.client.execute(stmt, params)
            offset += batch_size
