"""Postgres-backed counter — single-statement atomic upsert-increment."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, final

import attrs
from psycopg import sql

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze_postgres.execution.deps.configs.counter import PostgresCounterConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

from ._logger import logger

# ----------------------- #

_UNSUFFIXED: Final[str] = ""
"""Stored form of the unsuffixed counter — the primary key column cannot hold ``NULL``."""

_SUFFIX_PREFIX: Final[str] = "s:"
"""Prefix for suffixed rows, so no suffix (including ``""``) can collide with the
unsuffixed sentinel."""

_NO_TENANT: Final[str] = ""
"""Stored ``tenant_id`` when no tenant is bound — the primary key column cannot hold ``NULL``."""


def _encode_suffix(suffix: str | None) -> str:
    return f"{_SUFFIX_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED


def _decode_suffix(stored: str) -> str | None:
    return None if stored == _UNSUFFIXED else stored.removeprefix(_SUFFIX_PREFIX)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _PostgresCounterBase(TenancyMixin):
    """Shared table/tenant resolution for the counter data and admin adapters."""

    client: PostgresClientPort
    config: PostgresCounterConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        tenant_id = self.require_tenant_if_aware()
        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    def _tenant_value(self) -> str:
        # Tagged-tier isolation lives in the primary key: a shared table keyed on
        # ``suffix`` alone would collide two tenants' sequences silently.
        tenant_id = self.require_tenant_if_aware()
        return str(tenant_id) if tenant_id is not None else _NO_TENANT


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresCounterAdapter(_PostgresCounterBase, CounterPort):
    """Postgres implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Every operation is a single ``INSERT ... ON CONFLICT DO UPDATE ... RETURNING``
    statement, so allocation is atomic without an explicit transaction: concurrent callers
    serialize on the counter's row lock and each sees a distinct value. Operations run on
    a **detached** connection — never on the caller's transaction — so an allocation
    survives the caller's rollback; otherwise the same value could be handed out twice
    (Redis parity: a counter value is burned the moment it is returned).

    The table is provided by the application; expected schema::

        CREATE TABLE <relation> (
            tenant_id text   NOT NULL,   -- '' = no tenant bound
            suffix    text   NOT NULL,   -- '' = the unsuffixed counter; else 's:<suffix>'
            value     bigint NOT NULL,
            PRIMARY KEY (tenant_id, suffix)
        );
    """

    async def _bump(self, by: int, suffix: str | None) -> int:
        table = await self._table()

        stmt = sql.SQL(
            "INSERT INTO {table} (tenant_id, suffix, value) "
            "VALUES ({tenant}, {suffix}, {by}) "
            "ON CONFLICT (tenant_id, suffix) "
            "DO UPDATE SET value = {table}.value + EXCLUDED.value "
            "RETURNING value"
        ).format(
            table=table.ident(),
            tenant=sql.Placeholder(),
            suffix=sql.Placeholder(),
            by=sql.Placeholder(),
        )

        async with self.client.detached():
            row = await self.client.fetch_one(
                stmt,
                [self._tenant_value(), _encode_suffix(suffix), by],
                row_factory="tuple",
            )

        if row is None:  # pragma: no cover - RETURNING always yields the upserted row
            raise exc.internal("Counter upsert returned no row")

        return int(row[0])

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Incrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(by, suffix)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        logger.debug(
            "Incrementing counter suffix '%s' by %s, returning batch range",
            suffix,
            size,
        )

        max_cnt = await self._bump(size, suffix)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Decrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(-by, suffix)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        table = await self._table()

        logger.debug("Resetting counter suffix '%s' to %s", suffix, value)

        stmt = sql.SQL(
            "INSERT INTO {table} (tenant_id, suffix, value) "
            "VALUES ({tenant}, {suffix}, {value}) "
            "ON CONFLICT (tenant_id, suffix) DO UPDATE SET value = EXCLUDED.value "
            "RETURNING value"
        ).format(
            table=table.ident(),
            tenant=sql.Placeholder(),
            suffix=sql.Placeholder(),
            value=sql.Placeholder(),
        )

        async with self.client.detached():
            await self.client.fetch_one(
                stmt,
                [self._tenant_value(), _encode_suffix(suffix), value],
                row_factory="tuple",
            )

        return value


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresCounterAdminAdapter(_PostgresCounterBase, CounterAdminPort):
    """Enumerate the counters allocated in one Postgres counters table."""

    async def list_counters(self) -> Sequence[CounterEntry]:
        table = await self._table()

        # Filtered on the stored tenant key, so a shared tagged-tier table only ever
        # reports the bound tenant's counters. Ordered so enumeration is deterministic.
        stmt = sql.SQL(
            "SELECT suffix, value FROM {table} WHERE tenant_id = {tenant} ORDER BY suffix"
        ).format(table=table.ident(), tenant=sql.Placeholder())

        async with self.client.detached():
            rows = await self.client.fetch_all(stmt, [self._tenant_value()], row_factory="tuple")

        return [
            CounterEntry(suffix=_decode_suffix(str(suffix)), value=int(value))
            for suffix, value in rows
        ]
