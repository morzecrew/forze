"""Postgres durable-schedule store (recurring cron triggers)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence, final

import attrs
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.durable.function import (
    DurableScheduleRecord,
    DurableScheduleStorePort,
)
from forze.base.primitives import utcnow
from forze_postgres.execution.deps.configs.durable import PostgresDurableScheduleConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #

_COLUMNS = "schedule_id, name, cron, tz, input, next_fire_at, enabled, tenant_id"
"""Row projection for schedule reads."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableScheduleStore(DurableScheduleStorePort):
    """Postgres-backed recurring-schedule store.

    :meth:`put` upserts a schedule; :meth:`claim_due` hands out due schedules
    (``FOR UPDATE SKIP LOCKED``); :meth:`advance` compare-and-sets the next fire so two
    schedulers firing the same instant converge to one advance (and one run, via the run's
    ``{schedule_id}:{fire_epoch}`` idempotency key). Single-relation, tagged tenancy.

    The table is provided by the application; expected schema::

        CREATE TABLE <relation> (
            schedule_id  text        NOT NULL,
            name         text        NOT NULL,
            cron         text        NOT NULL,
            tz           text,
            input        jsonb,
            next_fire_at timestamptz NOT NULL,
            enabled      boolean     NOT NULL DEFAULT true,
            tenant_id    uuid,
            created_at   timestamptz NOT NULL,
            updated_at   timestamptz NOT NULL,
            PRIMARY KEY (schedule_id)
        );
    """

    client: PostgresClientPort
    config: PostgresDurableScheduleConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        return await resolve_postgres_qname(self.config.relation, None)

    # ....................... #

    async def put(self, record: DurableScheduleRecord) -> None:
        table = await self._table()
        now = utcnow()

        await self.client.execute(
            sql.SQL(
                """
                INSERT INTO {table}
                    (schedule_id, name, cron, tz, input, next_fire_at, enabled,
                     tenant_id, created_at, updated_at)
                VALUES
                    ({sid}, {name}, {cron}, {tz}, {input}, {next_fire}, {enabled},
                     {tenant_id}, {now}, {now})
                ON CONFLICT (schedule_id) DO UPDATE SET
                    name = EXCLUDED.name, cron = EXCLUDED.cron, tz = EXCLUDED.tz,
                    input = EXCLUDED.input, next_fire_at = EXCLUDED.next_fire_at,
                    enabled = EXCLUDED.enabled, tenant_id = EXCLUDED.tenant_id,
                    updated_at = EXCLUDED.updated_at
                """
            ).format(
                table=table.ident(),
                sid=sql.Placeholder("sid"),
                name=sql.Placeholder("name"),
                cron=sql.Placeholder("cron"),
                tz=sql.Placeholder("tz"),
                input=sql.Placeholder("input"),
                next_fire=sql.Placeholder("next_fire"),
                enabled=sql.Placeholder("enabled"),
                tenant_id=sql.Placeholder("tenant_id"),
                now=sql.Placeholder("now"),
            ),
            {
                "sid": record.schedule_id,
                "name": record.name,
                "cron": record.cron,
                "tz": record.tz,
                "input": None if record.input_json is None else Jsonb(record.input_json),
                "next_fire": record.next_fire_at,
                "enabled": record.enabled,
                "tenant_id": record.tenant_id,
                "now": now,
            },
        )

    # ....................... #

    async def claim_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Sequence[DurableScheduleRecord]:
        table = await self._table()

        rows = await self.client.fetch_all(
            sql.SQL(
                """
                SELECT {columns} FROM {table}
                WHERE enabled AND next_fire_at <= {now}
                ORDER BY next_fire_at
                LIMIT {limit}
                FOR UPDATE SKIP LOCKED
                """
            ).format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                now=sql.Placeholder("now"),
                limit=sql.Placeholder("limit"),
            ),
            {"now": now, "limit": limit},
        )

        return [_record_from_row(row) for row in rows]

    # ....................... #

    async def advance(
        self,
        schedule_id: str,
        *,
        from_fire_at: datetime,
        to_fire_at: datetime,
    ) -> bool:
        table = await self._table()

        rowcount = await self.client.execute(
            sql.SQL(
                """
                UPDATE {table} SET next_fire_at = {to}, updated_at = now()
                WHERE schedule_id = {sid} AND next_fire_at = {from_fire}
                """
            ).format(
                table=table.ident(),
                to=sql.Placeholder("to"),
                sid=sql.Placeholder("sid"),
                from_fire=sql.Placeholder("from_fire"),
            ),
            {"to": to_fire_at, "sid": schedule_id, "from_fire": from_fire_at},
            return_rowcount=True,
        )

        return bool(rowcount)

    # ....................... #

    async def load(self, schedule_id: str) -> DurableScheduleRecord | None:
        table = await self._table()

        row = await self.client.fetch_one(
            sql.SQL(
                "SELECT {columns} FROM {table} WHERE schedule_id = {sid}"
            ).format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                sid=sql.Placeholder("sid"),
            ),
            {"sid": schedule_id},
        )

        return None if row is None else _record_from_row(row)


# ....................... #


def _record_from_row(row: dict[str, Any]) -> DurableScheduleRecord:
    return DurableScheduleRecord(
        schedule_id=row["schedule_id"],
        name=row["name"],
        cron=row["cron"],
        next_fire_at=row["next_fire_at"],
        tz=row["tz"],
        input_json=row["input"],
        enabled=row["enabled"],
        tenant_id=row["tenant_id"],
    )
