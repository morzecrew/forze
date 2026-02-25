from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, final
from uuid import UUID

from psycopg import sql

from forze.base.errors import ConflictError, NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document

from .base import PostgresGateway
from .spec import PostgresTableSpec

# ----------------------- #
#! TODO: Review logics


@final
class PostgresHistoryGateway[D: Document](PostgresGateway[D]):
    async def _get(
        self,
        *,
        target_spec: PostgresTableSpec,
        pk: UUID,
        rev: int,
    ) -> D:
        where = sql.SQL("{h} = {h_v} AND {pk} = {pk_v} AND {rev} = {rev_v}").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=target_spec.literal(),
            pk=sql.Identifier(ID_FIELD),
            pk_v=sql.Placeholder(),
            rev=sql.Identifier(REV_FIELD),
            rev_v=sql.Placeholder(),
        )

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=self.spec.ident(),
            where=where,
        )

        row = await self.client.fetch_one(stmt, (pk, rev), row_factory="dict")

        if row is None:
            raise NotFoundError(f"История не найдена: {pk}, {rev}")

        return pydantic_validate(self.model, row[HISTORY_DATA_FIELD])

    # ....................... #

    async def _get_many(
        self,
        *,
        target_spec: PostgresTableSpec,
        pks: Sequence[UUID],
        revs: Sequence[int],
    ) -> Sequence[D]:
        if len(pks) != len(revs):
            raise ValidationError("Длина pks и revs должна быть одинаковой")

        values_sql = sql.SQL(", ").join(
            sql.SQL("({}, {})").format(sql.Placeholder(), sql.Placeholder())
            for _ in revs
        )

        where = sql.SQL("{h} = {h_v} AND ({pk}, {rev}) IN ({vals})").format(
            h=sql.Identifier(HISTORY_SOURCE_FIELD),
            h_v=target_spec.literal(),
            pk=sql.Identifier(ID_FIELD),
            rev=sql.Identifier(REV_FIELD),
            vals=values_sql,
        )

        stmt = sql.SQL("SELECT {data} FROM {table} WHERE {where}").format(
            data=sql.Identifier(HISTORY_DATA_FIELD),
            table=self.spec.ident(),
            where=where,
        )

        params: list[Any] = []

        for p, r in zip(pks, revs, strict=True):
            params.extend([p, r])

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        return [pydantic_validate(self.model, row[HISTORY_DATA_FIELD]) for row in rows]

    # ....................... #

    async def validate(
        self,
        *,
        target_spec: PostgresTableSpec,
        current: D,
        update: JsonDict,
        rev: int,
    ) -> None:
        if rev != current.rev:
            hist = await self._get(target_spec=target_spec, pk=current.id, rev=rev)

            if not current.validate_historical_consistency(hist, update):
                raise ConflictError(
                    "Нарушение согласованности истории во время обновления"
                )

    # ....................... #

    async def validate_many(
        self,
        *,
        target_spec: PostgresTableSpec,
        currents: Sequence[D],
        updates: Sequence[JsonDict],
        revs: Sequence[int],
    ) -> None:
        to_check = [
            (c, r, u)
            for c, r, u in zip(currents, revs, updates, strict=True)
            if r != c.rev
        ]

        if to_check:
            pks_to_check = [c.id for c, _, _ in to_check]
            revs_to_check = [r for _, r, _ in to_check]

            hists = await self._get_many(
                target_spec=target_spec,
                pks=pks_to_check,
                revs=revs_to_check,
            )

            for (c, _, u), h in zip(to_check, hists, strict=True):
                if not c.validate_historical_consistency(h, u):
                    raise ConflictError(
                        "Нарушение согласованности истории во время обновления"
                    )
