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


def _route_prefix(route: str) -> str:
    """Length-prefixed route tag: ``<len>:<route>``.

    Folds the counter spec's route into the stored ``suffix`` key so two specs wired to
    one physical table do not silently share rows (their key was ``(tenant_id, suffix)``
    with no route component). Length-prefixed so no route/suffix content can be mistaken
    for the other — the boundary is by position, never by a delimiter a name could carry.
    """

    return f"{len(route)}:{route}"


def _encode_suffix(route: str, suffix: str | None) -> str:
    body = f"{_SUFFIX_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED

    return f"{_route_prefix(route)}{body}"


def _legacy_suffix(suffix: str | None) -> str:
    """The pre-route stored suffix key (``""`` or ``s:<suffix>``).

    A counter allocated before the route fold was keyed here. The allocation path seeds the
    new route-prefixed row from this legacy row's value on first touch, so a live sequence
    continues instead of restarting from zero (which would reissue already-handed-out
    numbers). Backward-compat only; new writes never use it."""

    return f"{_SUFFIX_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED


def _decode_suffix(route: str, stored: str) -> str | None:
    length_str, rest = stored.split(":", 1)
    body = rest[int(length_str) :]  # skip the route by position, not by delimiter

    return None if body == _UNSUFFIXED else body.removeprefix(_SUFFIX_PREFIX)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _PostgresCounterBase(TenancyMixin):
    """Shared table/tenant resolution for the counter data and admin adapters."""

    client: PostgresClientPort
    config: PostgresCounterConfig
    route: str
    """The counter spec's route — folded into the stored key so two specs sharing one
    table do not merge (see :func:`_encode_suffix`)."""

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        # Namespace-tier resolution: ``_tenant_id_for_resolve`` returns the bound tenant
        # for a per-tenant relation even *without* tagged-tier ``tenant_aware`` — using
        # ``require_tenant_if_aware`` here dropped it, collapsing every tenant onto one
        # relation. Tagged-tier isolation still lives in the PK (see ``_tenant_value``).
        tenant_id = self._tenant_id_for_resolve()
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
            suffix    text   NOT NULL,   -- '<len>:<route>' + ('' | 's:<suffix>')
            value     bigint NOT NULL,
            PRIMARY KEY (tenant_id, suffix)
        );

    The ``suffix`` column folds the spec's route into its key (see :func:`_encode_suffix`),
    so several counter specs may share one table without merging.
    """

    async def _bump(self, by: int, suffix: str | None) -> int:
        table = await self._table()

        # On the first touch of the new route-prefixed row, seed its value from the
        # pre-route legacy row (``COALESCE(… , 0)``) so an existing sequence continues
        # instead of restarting at zero — a restart would reissue numbers already handed
        # out. ``by`` is bound twice: the INSERT adds it to the seed, and the DO UPDATE
        # adds only ``by`` (not ``EXCLUDED.value``, which carries the one-time seed) so a
        # subsequent bump never re-adds the legacy value. The legacy row is left in place
        # (route-filtered out of enumeration); it is simply never read again.
        stmt = sql.SQL(
            "INSERT INTO {table} (tenant_id, suffix, value) "
            "VALUES ({tenant}, {suffix}, "
            "  COALESCE("
            "    (SELECT value FROM {table} WHERE tenant_id = {tenant} AND suffix = {legacy}), 0"
            "  ) + {by}) "
            "ON CONFLICT (tenant_id, suffix) "
            "DO UPDATE SET value = {table}.value + {by} "
            "RETURNING value"
        ).format(
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            suffix=sql.Placeholder("suffix"),
            legacy=sql.Placeholder("legacy"),
            by=sql.Placeholder("by"),
        )

        async with self.client.detached():
            row = await self.client.fetch_one(
                stmt,
                {
                    "tenant": self._tenant_value(),
                    "suffix": _encode_suffix(self.route, suffix),
                    "legacy": _legacy_suffix(suffix),
                    "by": by,
                },
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
                [self._tenant_value(), _encode_suffix(self.route, suffix), value],
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

        # Filtered on the stored tenant key *and* the route prefix, so a shared table
        # only ever reports the bound tenant's counters for *this* spec — never another
        # tenant's, nor another spec sharing the relation. ``starts_with`` needs no LIKE
        # escaping; the length-prefixed route tag makes it an exact route boundary.
        stmt = sql.SQL(
            "SELECT suffix, value FROM {table} "
            "WHERE tenant_id = {tenant} AND starts_with(suffix, {route}) "
            "ORDER BY suffix"
        ).format(table=table.ident(), tenant=sql.Placeholder(), route=sql.Placeholder())

        async with self.client.detached():
            rows = await self.client.fetch_all(
                stmt,
                [self._tenant_value(), _route_prefix(self.route)],
                row_factory="tuple",
            )

        return [
            CounterEntry(suffix=_decode_suffix(self.route, str(suffix)), value=int(value))
            for suffix, value in rows
        ]
