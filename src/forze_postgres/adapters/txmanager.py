"""Postgres adapters implementing the transaction manager and transaction-scoped lock ports."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext
from typing import final

import attrs
from psycopg import sql

from forze.application.contracts.transaction import (
    IsolationLevel as CoreIsolationLevel,
)
from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
    TxLockPort,
    require_tx_lock_in_transaction,
)
from forze.base.primitives import driver_deadline_budget

from ..kernel.client import PostgresClientPort, PostgresTransactionOptions
from ..kernel.client.read_only_root import read_only_root, read_only_root_enforced
from ..kernel.client.types import IsolationLevel as PgIsolationLevel
from ..kernel.client.value_objects import DeadlinePushdownPolicy
from ._logger import logger

# ----------------------- #

PostgresTxScopeKey = TransactionScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

_PG_ISOLATION: dict[CoreIsolationLevel, PgIsolationLevel] = {
    CoreIsolationLevel.READ_COMMITTED: "read_committed",
    CoreIsolationLevel.SNAPSHOT: "repeatable_read",  # Postgres snapshot isolation
    CoreIsolationLevel.SERIALIZABLE: "serializable",
}
"""Map the kernel's intent-named isolation to the Postgres level (Postgres can do all three)."""


def _statement_timeout_ms(
    policy: DeadlinePushdownPolicy | None, budget: float | None
) -> int | None:
    """The per-transaction ``statement_timeout`` (ms) for a *budget* under *policy*, or ``None``.

    ``None`` when the push-down is disabled (``policy is None``) or no deadline is bound
    (``budget is None``). Tighten-only against the policy's static cap (the min wins) and
    floored to a positive value — a ``statement_timeout`` of ``0`` means *unlimited*."""

    if policy is None or budget is None:
        return None

    ms = int(budget * 1000)

    if policy.statement_timeout_cap is not None:
        ms = min(ms, int(policy.statement_timeout_cap.total_seconds() * 1000))

    return max(1, ms)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TransactionManagerPort):
    """Postgres-backed :class:`TxManagerPort` that delegates to :meth:`PostgresClient.transaction`."""

    client: PostgresClientPort
    """Client instance."""

    options: PostgresTransactionOptions = attrs.field(factory=PostgresTransactionOptions)
    """Transaction options forwarded to the Postgres client."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return PostgresTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # Postgres honors every level natively (snapshot via REPEATABLE READ).
        return TxCapabilities(isolation=frozenset(CoreIsolationLevel))

    # ....................... #

    def read_only_enforced(self) -> bool:
        """Whether the open root began ``BEGIN … READ ONLY`` and nothing has bypassed it.

        ``READ ONLY`` makes Postgres refuse writes to this database; it does not cover
        ``dblink`` or ``postgres_fdw``. ``False`` for a scope opened inside an outer client
        transaction (a savepoint, which cannot be read-only, under a transaction that may
        write), and once a :meth:`~forze_postgres.kernel.client.client.PostgresClient.detached`
        statement, another client's autocommit statement, or another client's root
        transaction could have committed beside it.
        """

        return read_only_root_enforced()

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: CoreIsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        """Open Postgres transaction for the duration of the context.

        ``read_only`` (set for ``QUERY`` operations) opens the transaction with
        ``BEGIN ... READ ONLY`` so the database rejects writes. A route configured
        read-only at construction stays read-only regardless (restrictive OR).

        ``isolation`` (a kernel :class:`IsolationLevel`, validated against
        :meth:`capabilities` before this runs) maps to the Postgres level on the ``BEGIN``.
        """

        options = self.options

        if read_only and not options.read_only:
            options = attrs.evolve(options, read_only=True)

        if isolation is not None:
            options = attrs.evolve(options, isolation=_PG_ISOLATION[isolation])

        logger.debug("Starting transaction (read_only=%s)", options.read_only)

        # Apply the invocation-deadline backstop only at the root transaction (a nested
        # savepoint rides the root's SET LOCAL).
        at_root = not self.client.is_in_transaction()
        # Only a root BEGIN carries READ ONLY; inside a client transaction this is a savepoint.
        # A nested scope records nothing, so a bypass inside it still clears the root's record.
        record = read_only_root() if at_root and options.read_only else nullcontext()

        with record:
            async with self.client.transaction(options=options):
                if at_root:
                    await self._apply_deadline_backstop()

                try:
                    yield

                #! Hmmm.. should it be like that?
                except Exception:
                    logger.debug("Transaction rolled back")
                    raise

                else:
                    logger.debug("Transaction committed")

    # ....................... #

    async def _apply_deadline_backstop(self) -> None:
        """Bound the transaction by the remaining invocation deadline as a ``statement_timeout``.

        A loose backstop: set to ``remaining + grace`` (tighten-only against any static
        ``statement_timeout``), so the authoritative :func:`asyncio.timeout` at the invocation
        boundary fires first and classifies the expiry, while the server cancels a query the
        deadline would kill anyway — freeing the connection instead of leaving it stuck behind
        an asyncio-cancelled-but-server-running statement. No-op when the push-down is disabled
        or no deadline is bound.
        """

        ms = _statement_timeout_ms(self.client.deadline_pushdown(), driver_deadline_budget())

        if ms is None:
            return

        # Defer to the client: a lazy root scope carries this until materialization, so
        # applying the backstop never forces an early connection checkout.
        await self.client.apply_statement_timeout(ms)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxLockAdapter(TxLockPort):
    """``pg_advisory_xact_lock`` on the connection of the client's open transaction."""

    client: PostgresClientPort
    """Client instance — the one the route's transaction manager opens transactions on."""

    # ....................... #

    async def hold(self, *keys: int) -> None:
        """Take ``pg_advisory_xact_lock`` for each of *keys*, sorted, until the transaction ends.

        The advisory-lock space ``SerializedBy`` writes take theirs in, so a deadlock between
        the two kinds is detected and refused like one between either. A wait is bounded by
        the operation's deadline where the client pushes it down as ``statement_timeout``.

        :raises CoreException: ``precondition`` (``tx_lock_outside_transaction``) outside a
            transaction, where the lock would end with its own statement.
        """

        require_tx_lock_in_transaction(self.client.is_in_transaction())
        stmt = sql.SQL("SELECT pg_advisory_xact_lock({})").format(sql.Placeholder())

        for key in sorted(set(keys)):
            await self.client.execute(stmt, [key])
