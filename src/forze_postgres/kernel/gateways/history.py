"""Gateway for reading and writing document history records in Postgres."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, final, get_args
from uuid import UUID

import attrs
from psycopg import sql

from forze.base.exceptions import exc
from forze.base.serialization import ModelCodec
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory
from forze_postgres.kernel.relation import RelationSpec, resolve_postgres_qname

from .base import PostgresGateway, PostgresQualifiedName
from .types import PostgresBookkeepingStrategy

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHistoryGateway[D: Document](PostgresGateway[D]):
    """Gateway for document revision history backed by a dedicated Postgres table."""

    strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    target_relation: RelationSpec
    """Write-table relation (schema, name) or resolver for ``HISTORY_SOURCE_FIELD``."""

    history_codec: ModelCodec[Any, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for :class:`~forze.domain.models.DocumentHistory` persistence rows."""

    _target_qname_resolved: PostgresQualifiedName | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.strategy not in get_args(PostgresBookkeepingStrategy):
            raise exc.internal(f"Invalid bookkeeping strategy: {self.strategy}")

    # ....................... #

    # ....................... #

    async def _target_qname(self) -> PostgresQualifiedName:
        if self._target_qname_resolved is not None:
            return self._target_qname_resolved

        resolved = await resolve_postgres_qname(
            self.target_relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_target_qname_resolved", resolved)

        return resolved

    # ....................... #

    async def read(self, pk: UUID, rev: int) -> D:
        target = await self._target_qname()
        where = sql.SQL("{h} = {h_v} AND {pk} = {pk_v} AND {rev} = {rev_v}").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=target.literal(),
            pk=sql.Identifier(ID_FIELD),
            pk_v=sql.Placeholder(),
            rev=sql.Identifier(REV_FIELD),
            rev_v=sql.Placeholder(),
        )
        where_params = [pk, rev]

        # if gateway is tenant aware, add tenant ID filter to the query
        where, where_params = self._add_tenant_where(where, where_params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=(await self._qname()).ident(),
            where=where,
        )

        row = await self.client.fetch_one(stmt, where_params, row_factory="dict")

        if row is None:
            raise exc.not_found(f"History not found: {pk}, {rev}")

        return self._decode_row(row[HISTORY_DATA_FIELD])

    # ....................... #

    async def read_many(self, pks: Sequence[UUID], revs: Sequence[int]) -> Sequence[D]:
        if len(pks) != len(revs):
            raise exc.precondition("Length of pks and revs must be the same")

        target = await self._target_qname()

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = sql.SQL("({}, {})").format(sql.Placeholder(), sql.Placeholder())
        values_sql = sql.SQL(", ").join(row_template for _ in revs)

        where = sql.SQL("{h} = {h_v} AND ({pk}, {rev}) IN ({vals})").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=target.literal(),
            pk=sql.Identifier(ID_FIELD),
            rev=sql.Identifier(REV_FIELD),
            vals=values_sql,
        )
        params: list[Any] = []

        for p, r in zip(pks, revs, strict=True):
            params.extend([p, r])

        where, params = self._add_tenant_where(where, params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=(await self._qname()).ident(),
            where=where,
        )

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        return self._decode_rows([row[HISTORY_DATA_FIELD] for row in rows])

    # ....................... #

    async def _from_data(self, data: D) -> DocumentHistory[D]:
        target = await self._target_qname()

        return DocumentHistory(
            source=target.string(),
            id=data.id,
            rev=data.rev,
            data=data,
        )

    # ....................... #

    async def write(self, data: D) -> None:
        if self.strategy == "database":
            return

        record = await self._from_data(data)
        insert_data_raw = self.history_codec.encode_persistence_mapping(record)
        insert_data = await self.adapt_payload_for_write(insert_data_raw)

        cols = [sql.Identifier(k) for k in insert_data.keys()]
        vals = [sql.Placeholder() for _ in insert_data.keys()]
        params = list(insert_data.values())

        stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
            table=(await self._qname()).ident(),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(vals),
        )

        await self.client.execute(stmt, params)

    # ....................... #

    async def write_many(self, data: Sequence[D], *, batch_size: int = 500) -> None:
        if self.strategy == "database":
            return

        records = [await self._from_data(item) for item in data]
        insert_data_raw = self.history_codec.encode_persistence_mapping_many(records)

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

        offset = 0

        while offset < len(insert_data):
            batch = insert_data[offset : offset + batch_size]
            params: list[Any] = []

            for b in batch:
                params.extend(b[k] for k in keys)

            # ⚡ Bolt: Duplicate the precomputed row template
            value_parts = [row_template] * len(batch)

            stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES {vals}").format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(col_idents),
                vals=sql.SQL(", ").join(value_parts),
            )

            await self.client.execute(stmt, params)
            offset += batch_size
