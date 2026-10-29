"""Postgres-backed counter — single-statement atomic upsert-increment."""

from __future__ import annotations

import weakref
from collections.abc import Sequence
from datetime import timedelta
from typing import Any, Final, final

import attrs
from psycopg import sql

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterCapabilities,
    CounterEntry,
    CounterPort,
    counter_ttl_unsupported,
    validate_counter_ttl,
    validate_counter_value,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import CacheLane
from forze_postgres.execution.deps.configs.counter import PostgresCounterConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import is_static_relation, resolve_postgres_qname

from ._logger import logger

# ----------------------- #

_UNSUFFIXED: Final[str] = ""
"""Stored form of the unsuffixed counter — the primary key column cannot hold ``NULL``."""

_SUFFIX_PREFIX: Final[str] = "s:"
"""Prefix for suffixed rows, so no suffix (including ``""``) can collide with the
unsuffixed sentinel."""

_NO_TENANT: Final[str] = ""
"""Stored ``tenant_id`` when no tenant is bound — the primary key column cannot hold ``NULL``."""

_EXPIRY_COLUMN: Final[str] = "expires_at"
"""Optional column holding the instant a counter lapses (``NULL`` = never)."""

_PURGE_BATCH: Final[int] = 1000
"""Rows one purge statement deletes, so a large backlog never holds one long lock."""

_RECHECK_AFTER: Final[float] = 60.0
"""Seconds before a table found without ``expires_at`` is read from the catalog again.

The column can be added to a running deployment, so its absence is re-checked — but on a
cooldown rather than on every ``ttl`` call. A caller that falls back on the refusal (the
login lockout does, on every failed login) would otherwise turn each call into a catalog
query and a cache eviction.
"""

_EXPIRY_RECHECK: Final[
    weakref.WeakKeyDictionary[PostgresIntrospector, CacheLane[tuple[str, str], bool]]
] = weakref.WeakKeyDictionary()
"""Per introspector, the ``(cache partition, table)`` pairs re-checked recently and still
without the column.

Keyed by the introspector, because two runtimes in one process can reach different databases
under the same partition and table name. Then by its cache partition, because with
database-per-tenant routing one tenant's answer is not another's. Each lane is capped,
because table-per-tenant names are unbounded.
"""


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

    introspector: PostgresIntrospector = attrs.field(
        default=attrs.Factory(
            lambda self: PostgresIntrospector(client=self.client),
            takes_self=True,
        ),
    )
    """Catalog access used to detect the optional ``expires_at`` column.

    Wiring passes the module's shared introspector. A direct construction gets one of its own,
    cached for the adapter's life, so it still detects the column rather than answering as if
    it were absent. On a client routed per tenant, pass the module's introspector, whose cache
    is partitioned by tenant.
    """

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

    async def _has_expiry(self, table: PostgresQualifiedName, *, required: bool) -> bool:
        """Whether *table* carries the optional ``expires_at`` column.

        The answer picks between two statement shapes, because a statement naming a column
        the table lacks does not execute at all. A table created before counters could
        expire keeps working for every call that does not ask for a ``ttl``; a call that
        does (*required*) is refused with ``configuration``.

        A present column is answered from the introspector's cache. An absent one is re-read
        past the cache only when *required*, and at most once per :data:`_RECHECK_AFTER`
        per table, so the migration can be applied to a running deployment and a ``ttl``
        call picks it up within that cooldown.
        """

        columns = await self.introspector.get_column_types(
            schema=table.schema,
            relation=table.name,
        )

        if _EXPIRY_COLUMN in columns:
            return True

        recheck = self._recheck_key(table.string())
        rechecks = self._rechecks()

        if required and rechecks.lookup(recheck) is None:
            rechecks.store(recheck, True)
            self.introspector.invalidate_relation(schema=table.schema, relation=table.name)
            columns = await self.introspector.get_column_types(
                schema=table.schema,
                relation=table.name,
            )

            if _EXPIRY_COLUMN in columns:
                rechecks.invalidate(recheck)
                return True

        if not required:
            # shortcut: a process that cached the table before the column was added keeps
            # the column-less statements for ttl-less calls until a ttl call or a restart.
            return False

        raise counter_ttl_unsupported(
            f"the counters table {table.string()!r} has no {_EXPIRY_COLUMN!r} column (add it "
            f"with ALTER TABLE {table.string()} ADD COLUMN {_EXPIRY_COLUMN} timestamptz)",
            details={"relation": table.string(), "partition": recheck[0]},
        )

    # ....................... #

    def _rechecks(self) -> CacheLane[tuple[str, str], bool]:
        """The introspector's lane in :data:`_EXPIRY_RECHECK`."""

        lane = _EXPIRY_RECHECK.get(self.introspector)

        if lane is None:
            lane = CacheLane(max_entries=2048, ttl_seconds=_RECHECK_AFTER)
            _EXPIRY_RECHECK[self.introspector] = lane

        return lane

    # ....................... #

    def _recheck_key(self, table: str) -> tuple[str, str]:
        """*table*'s key in its lane: the introspector's cache partition and it."""

        partition = self.introspector.cache_partition_key

        return ((partition() or "") if partition is not None else "", table)


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
            tenant_id  text        NOT NULL,   -- '' = no tenant bound
            suffix     text        NOT NULL,   -- '<len>:<route>' + ('' | 's:<suffix>')
            value      bigint      NOT NULL,
            expires_at timestamptz,            -- optional; NULL = never expires
            PRIMARY KEY (tenant_id, suffix)
        );

    The ``suffix`` column folds the spec's route into its key (see :func:`_encode_suffix`),
    so several counter specs may share one table without merging.

    ``expires_at`` is **optional and additive**: ``ALTER TABLE <relation> ADD COLUMN
    expires_at timestamptz;`` is the whole migration. Without it every call works as before
    except one that passes a ``ttl``, which is refused with ``configuration``. With it a
    lapsed row reads as absent and the next increment overwrites it from zero; the row stays
    until :meth:`PostgresCounterAdminAdapter.purge_expired` or a scheduled ``DELETE``
    removes it. Lifetimes run on the database's clock, read when the row is processed
    (``clock_timestamp()``) rather than when the statement began (``now()``): a write that
    waited on a row lock still sees a lapse that happened meanwhile and gets its whole ``ttl``.
    """

    def capabilities(self) -> CounterCapabilities:
        """Report expiry support, as far as it is known without a catalog read.

        The column check is asynchronous and this is not, so it reports ``expiry=False`` only
        for a static table re-checked within the last :data:`_RECHECK_AFTER` and found
        without ``expires_at``. Otherwise it reports ``True``, and a ``ttl`` call can still be
        refused with ``counter_ttl_unsupported`` — which is what then records the answer.
        """

        relation = self.config.relation

        if not is_static_relation(relation):
            return CounterCapabilities(expiry=True)

        schema, name = relation
        known_absent = self._rechecks().lookup(self._recheck_key(f"{schema}.{name}"))

        return CounterCapabilities(expiry=known_absent is None)

    # ....................... #

    def _keys(self, suffix: str | None) -> dict[str, Any]:
        return {
            "tenant": self._tenant_value(),
            "suffix": _encode_suffix(self.route, suffix),
            "legacy": _legacy_suffix(suffix),
        }

    # ....................... #

    async def _read(self, table: PostgresQualifiedName, suffix: str | None, expiry: bool) -> int:
        """The counter's current value, without creating it.

        Falls back to the pre-route legacy row only when the current row is absent — the
        first-touch rule the allocation path seeds by — and a lapsed row reads as 0.
        """

        stmt = sql.SQL(
            "SELECT CASE WHEN cur.suffix IS NULL THEN COALESCE(old.value, 0) "
            "{lapsed}ELSE cur.value END "
            "FROM (SELECT 1) AS anchor "
            "LEFT JOIN {table} AS cur ON cur.tenant_id = {tenant} AND cur.suffix = {suffix} "
            "LEFT JOIN {table} AS old ON old.tenant_id = {tenant} AND old.suffix = {legacy}"
        ).format(
            lapsed=sql.SQL("WHEN cur.expires_at <= clock_timestamp() THEN 0 " if expiry else ""),
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            suffix=sql.Placeholder("suffix"),
            legacy=sql.Placeholder("legacy"),
        )

        async with self.client.detached():
            value = await self.client.fetch_value(stmt, self._keys(suffix), default=0)

        return int(value)

    # ....................... #

    async def _bump(self, by: int, suffix: str | None, ttl: timedelta | None) -> int:
        validate_counter_ttl(ttl)

        table = await self._table()
        expiry = await self._has_expiry(table, required=ttl is not None)

        if by == 0:
            return await self._read(table, suffix, expiry)

        # On the first touch of the new route-prefixed row, seed its value from the
        # pre-route legacy row (``COALESCE(… , 0)``) so an existing sequence continues
        # instead of restarting at zero — a restart would reissue numbers already handed
        # out. ``by`` is bound twice: the INSERT adds it to the seed, and the DO UPDATE
        # adds only ``by`` (not ``EXCLUDED.value``, which carries the one-time seed) so a
        # subsequent bump never re-adds the legacy value. The legacy row is left in place
        # (route-filtered out of enumeration); it is simply never read again.
        #
        # With ``expires_at``, a lapsed row is overwritten as if it were absent — from
        # ``by``, with the new lifetime — and a live one keeps the lifetime it was created
        # with, so a fixed window is never extended by the traffic inside it. The update reads
        # the clock once, after the row lock is taken, so both columns judge the same instant.
        if expiry:
            upsert = (
                "INSERT INTO {table} (tenant_id, suffix, value, expires_at) "
                "VALUES ({tenant}, {suffix}, "
                "  COALESCE("
                "    (SELECT value FROM {table} WHERE tenant_id = {tenant} AND suffix = {legacy}), 0"
                "  ) + {by}, clock_timestamp() + {ttl}::interval) "
                "ON CONFLICT (tenant_id, suffix) DO UPDATE SET (value, expires_at) = ("
                "  SELECT CASE WHEN {table}.expires_at <= clock.at THEN {by} "
                "    ELSE {table}.value + {by} END, "
                "  CASE WHEN {table}.expires_at <= clock.at THEN clock.at + {ttl}::interval "
                "    ELSE {table}.expires_at END "
                "  FROM (SELECT clock_timestamp() AS at) AS clock"
                ") "
                "RETURNING value"
            )
        else:
            upsert = (
                "INSERT INTO {table} (tenant_id, suffix, value) "
                "VALUES ({tenant}, {suffix}, "
                "  COALESCE("
                "    (SELECT value FROM {table} WHERE tenant_id = {tenant} AND suffix = {legacy}), 0"
                "  ) + {by}) "
                "ON CONFLICT (tenant_id, suffix) "
                "DO UPDATE SET value = {table}.value + {by} "
                "RETURNING value"
            )

        stmt = sql.SQL(upsert).format(
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            suffix=sql.Placeholder("suffix"),
            legacy=sql.Placeholder("legacy"),
            by=sql.Placeholder("by"),
            ttl=sql.Placeholder("ttl"),
        )

        async with self.client.detached():
            row = await self.client.fetch_one(
                stmt,
                {**self._keys(suffix), "by": by, "ttl": ttl},
                row_factory="tuple",
            )

        if row is None:  # pragma: no cover - RETURNING always yields the upserted row
            raise exc.internal("Counter upsert returned no row")

        return int(row[0])

    # ....................... #

    async def incr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        logger.debug("Incrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(by, suffix, ttl)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> list[int]:
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        logger.debug(
            "Incrementing counter suffix '%s' by %s, returning batch range",
            suffix,
            size,
        )

        max_cnt = await self._bump(size, suffix, ttl)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        logger.debug("Decrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(-by, suffix, ttl)

    # ....................... #

    async def reset(
        self,
        value: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        # Known up front, so it is refused here rather than by the backend — or, on
        # Redis, silently accepted and left to break the *next* allocation.
        validate_counter_value(value, operation="reset")
        validate_counter_ttl(ttl)

        table = await self._table()
        expiry = await self._has_expiry(table, required=ttl is not None)

        logger.debug("Resetting counter suffix '%s' to %s", suffix, value)

        # A reset replaces the lifetime along with the value: no ttl clears it.
        if expiry:
            upsert = (
                "INSERT INTO {table} (tenant_id, suffix, value, expires_at) "
                "VALUES ({tenant}, {suffix}, {value}, clock_timestamp() + {ttl}::interval) "
                "ON CONFLICT (tenant_id, suffix) DO UPDATE "
                "SET value = EXCLUDED.value, expires_at = clock_timestamp() + {ttl}::interval "
                "RETURNING value"
            )
        else:
            upsert = (
                "INSERT INTO {table} (tenant_id, suffix, value) "
                "VALUES ({tenant}, {suffix}, {value}) "
                "ON CONFLICT (tenant_id, suffix) DO UPDATE SET value = EXCLUDED.value "
                "RETURNING value"
            )

        stmt = sql.SQL(upsert).format(
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            suffix=sql.Placeholder("suffix"),
            value=sql.Placeholder("value"),
            ttl=sql.Placeholder("ttl"),
        )

        async with self.client.detached():
            await self.client.fetch_one(
                stmt,
                {
                    "tenant": self._tenant_value(),
                    "suffix": _encode_suffix(self.route, suffix),
                    "value": value,
                    "ttl": ttl,
                },
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
        expiry = await self._has_expiry(table, required=False)

        # Filtered on the stored tenant key *and* the route prefix, so a shared table
        # only ever reports the bound tenant's counters for *this* spec — never another
        # tenant's, nor another spec sharing the relation. ``starts_with`` needs no LIKE
        # escaping; the length-prefixed route tag makes it an exact route boundary.
        stmt = sql.SQL(
            "SELECT suffix, value FROM {table} "
            "WHERE tenant_id = {tenant} AND starts_with(suffix, {route}) {live}"
            "ORDER BY suffix"
        ).format(
            table=table.ident(),
            tenant=sql.Placeholder(),
            route=sql.Placeholder(),
            live=sql.SQL(
                "AND (expires_at IS NULL OR expires_at > clock_timestamp()) " if expiry else ""
            ),
        )

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

    # ....................... #

    async def purge_expired(self) -> int:
        """Delete this spec's lapsed counters for the bound tenant and return how many.

        A lapsed row already reads as absent, so this reclaims storage and changes no
        answer. Scoped like :meth:`list_counters` — tenant and route — and walked in
        primary-key order, :data:`_PURGE_BATCH` rows at a time, deleting the lapsed ones
        among them: each row is visited once, every statement is short, and the walk needs no
        index beyond the primary key. The ``expires_at`` test is re-checked against a row an
        increment revived between a batch's read and its delete, so a live counter is never
        removed.

        A table without ``expires_at`` holds nothing that can lapse, and returns 0. To purge
        a whole table across tenants and routes, schedule
        ``DELETE FROM <relation> WHERE expires_at <= now()`` instead (e.g. with pg_cron),
        backed by ``CREATE INDEX ON <relation> (expires_at) WHERE expires_at IS NOT NULL``.
        """

        table = await self._table()

        if not await self._has_expiry(table, required=False):
            return 0

        stmt = sql.SQL(
            "WITH batch AS ("
            "  SELECT tenant_id, suffix FROM {table} "
            "  WHERE tenant_id = {tenant} AND suffix > {after} AND starts_with(suffix, {route}) "
            "  ORDER BY suffix LIMIT {batch}"
            "), gone AS ("
            "  DELETE FROM {table} AS t USING batch "
            "  WHERE t.tenant_id = batch.tenant_id AND t.suffix = batch.suffix "
            "  AND t.expires_at <= clock_timestamp() RETURNING 1"
            ") "
            "SELECT (SELECT count(*) FROM batch), (SELECT max(suffix) FROM batch), "
            "(SELECT count(*) FROM gone)"
        ).format(
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            after=sql.Placeholder("after"),
            route=sql.Placeholder("route"),
            batch=sql.Placeholder("batch"),
        )
        params: dict[str, Any] = {
            "tenant": self._tenant_value(),
            "route": _route_prefix(self.route),
            "batch": _PURGE_BATCH,
            "after": "",  # every stored suffix sorts after it: the route tag is never empty
        }
        purged = 0

        async with self.client.detached():
            while True:
                row = await self.client.fetch_one(stmt, params, row_factory="tuple")

                if row is None:  # pragma: no cover - an aggregate SELECT always yields a row
                    raise exc.internal("Counter purge returned no row")

                scanned, last, deleted = row
                purged += int(deleted)

                if int(scanned) < _PURGE_BATCH:
                    return purged

                params["after"] = last
