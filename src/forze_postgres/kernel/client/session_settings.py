"""Scoping for transaction-local session settings (``SET LOCAL`` / ``set_config(..., true)``).

Transaction-local settings are transaction-scoped, not savepoint-scoped: when a scope
runs inside a caller transaction it executes in a *savepoint*, and on the savepoint's
RELEASE the settings merge into the outer transaction — leaking one scope's settings
into every later statement. Both the param-bound document reads and the analytics
timeout/``search_path`` overrides share this seam, so the undo discipline lives here
once instead of drifting apart per call site.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import timedelta

from psycopg import errors, sql

from forze_postgres.kernel._logger import logger

from .port import PostgresClientPort

# ----------------------- #


def _in_aborted_transaction(error: BaseException) -> bool:
    """Whether *error* (or anything in its chain) is a command rejected by an aborted transaction.

    The client's exception interceptor may wrap the psycopg error, so the raw
    :class:`~psycopg.errors.InFailedSqlTransaction` can sit anywhere in the
    cause/context chain.
    """

    seen: set[int] = set()
    current: BaseException | None = error

    while current is not None and id(current) not in seen:
        if isinstance(current, errors.InFailedSqlTransaction):
            return True

        seen.add(id(current))
        current = current.__cause__ or current.__context__

    return False


# ....................... #


async def _undo_without_masking(undo: Callable[[], Awaitable[None]]) -> None:
    """Run *undo* while another error is already propagating; never raise in its place.

    In an aborted transaction every command fails — including the undo — but the
    rollback discards the transaction-local settings anyway, so that failure is
    harmless noise (debug). Any other failure is unexpected and logged as a warning;
    either way the in-flight error stays the one the caller sees.
    """

    try:
        await undo()

    except Exception as undo_error:
        if _in_aborted_transaction(undo_error):
            logger.debug(
                "Skipped the session-settings reset in an aborted transaction; "
                "the rollback discards the transaction-local settings.",
            )

        else:
            logger.warning(
                "Failed to reset transaction-local session settings while another "
                "error was propagating; surfacing the original error.",
                exc_info=True,
            )


# ....................... #


@asynccontextmanager
async def undo_local_settings_on_exit(
    undo: Callable[[], Awaitable[None]],
) -> AsyncGenerator[None]:
    """Run the body, then *undo* the transaction-local settings it applied — without masking.

    - Body succeeds: *undo* runs and its own failure propagates — the settings could
      otherwise outlive the scope's savepoint and leak into the caller's transaction.
    - Body raises: *undo* is best-effort. The body's error always surfaces; an undo
      failure is only logged (see :func:`_undo_without_masking`). A failed undo cannot
      leak the settings either — the enclosing savepoint/transaction rolls back on the
      body's error, discarding them.
    - Cancellation and other non-``Exception`` exits skip the undo entirely: the
      rollback discards the settings, and no extra I/O runs during unwinding.
    """

    try:
        yield

    except Exception:
        await _undo_without_masking(undo)
        raise

    else:
        await undo()


# ....................... #


def _setting_values(
    *,
    statement_timeout: timedelta | None,
    search_path: str | None,
) -> list[tuple[str, sql.Composable]]:
    """The ``(name, value expression)`` pairs to set, each evaluated server-side."""

    values: list[tuple[str, sql.Composable]] = []

    if statement_timeout is not None:
        # Tighten-only: the bound in force (the pool's, or an invocation deadline's) still wins
        # when lower; ``0`` there means no bound at all. 0 ms here would mean the same.
        ms = max(1, int(statement_timeout.total_seconds() * 1000))
        values.append(
            (
                "statement_timeout",
                sql.SQL(
                    "LEAST({}, NULLIF(EXTRACT(EPOCH FROM current_setting('statement_timeout')"
                    "::interval) * 1000, 0))::bigint::text"
                ).format(sql.Literal(ms)),
            )
        )

    if search_path is not None:
        # The tenant schema first (its tables shadow ``public``), then ``public``, so
        # unqualified extension objects and shared lookups stay reachable.
        values.append(
            (
                "search_path",
                sql.SQL("quote_ident({}) || ', public'").format(sql.Literal(search_path)),
            )
        )

    return values


# ....................... #


def _set_configs(values: list[tuple[str, sql.Composable]]) -> sql.Composed:
    return sql.SQL(", ").join(
        sql.SQL("set_config({}, {}, true)").format(sql.Literal(name), value)
        for name, value in values
    )


# ....................... #


def local_settings_statement(
    *,
    statement_timeout: timedelta | None = None,
    search_path: str | None = None,
) -> sql.Composed | None:
    """One statement setting the transaction-local ``statement_timeout`` and ``search_path``.

    The timeout only ever lowers the one in force. ``None`` when there is nothing to set.
    """

    values = _setting_values(statement_timeout=statement_timeout, search_path=search_path)

    return sql.SQL("SELECT {}").format(_set_configs(values)) if values else None


# ....................... #


@asynccontextmanager
async def scoped_local_settings(
    client: PostgresClientPort,
    *,
    statement_timeout: timedelta | None = None,
    search_path: str | None = None,
    restore: bool,
) -> AsyncGenerator[None]:
    """Set the transaction-local ``statement_timeout`` and ``search_path`` for the body.

    Enter it inside the transaction. With *restore* (the transaction is a savepoint in a
    caller's), the caller's values are captured by the same statement that sets the new
    ones, and put back when the body exits: ``SET LOCAL`` is transaction-scoped, and a
    released savepoint would merge it into the caller's transaction. Without it the
    transaction ends with the body and takes the values along. The restore follows
    :func:`undo_local_settings_on_exit`: it never masks an error raised by the body.
    """

    values = _setting_values(statement_timeout=statement_timeout, search_path=search_path)

    if not values:
        yield
        return

    if not restore:
        await client.execute(sql.SQL("SELECT {}").format(_set_configs(values)))
        yield
        return

    names = [name for name, _ in values]
    # The CTE is computed before the projection that sets, so it holds the caller's values.
    capture_and_set = sql.SQL(
        "WITH prior AS MATERIALIZED (SELECT {}) SELECT {}, {} FROM prior"
    ).format(
        sql.SQL(", ").join(
            sql.SQL("current_setting({}, true) AS {}").format(
                sql.Literal(name), sql.Identifier(f"p{i}")
            )
            for i, name in enumerate(names)
        ),
        sql.SQL(", ").join(sql.Identifier("prior", f"p{i}") for i in range(len(names))),
        _set_configs(values),
    )
    row = await client.fetch_one(capture_and_set, None, row_factory="tuple")
    priors: tuple[str | None, ...] = (
        tuple(row[: len(names)]) if row is not None else (None,) * len(names)
    )

    async def _restore() -> None:
        restore_stmt = sql.SQL("SELECT {}").format(
            sql.SQL(", ").join(
                sql.SQL("set_config({}, {}, true)").format(sql.Placeholder(), sql.Placeholder())
                for _ in names
            )
        )
        params: list[str | None] = []

        for name, prior in zip(names, priors, strict=True):
            params.extend((name, prior))

        await client.execute(restore_stmt, params)

    async with undo_local_settings_on_exit(_restore):
        yield
