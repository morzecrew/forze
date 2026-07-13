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

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager

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


@asynccontextmanager
async def restore_local_settings_on_exit(
    client: PostgresClientPort,
    names: Sequence[str],
    *,
    enabled: bool = True,
) -> AsyncGenerator[None]:
    """Capture the current values of *names* and put them back when the body exits.

    For built-in settings (``statement_timeout``, ``search_path``) the body is about to
    override with ``SET LOCAL``: unlike a custom parameter GUC there is no ``NULL``
    default to reset to — the caller transaction's own values must come back. Enter this
    scope inside the transaction but *before* issuing the ``SET LOCAL`` statements.

    Pass ``enabled=False`` when the scope's transaction is the root (no caller
    transaction): it then ends right after the body, taking the ``SET LOCAL`` values
    with it, so the capture/restore round-trips are skipped.

    The restore itself follows :func:`undo_local_settings_on_exit`: it never masks an
    error raised by the body.
    """

    if not enabled or not names:
        yield
        return

    capture_stmt = sql.SQL("SELECT {}").format(
        sql.SQL(", ").join(
            sql.SQL("current_setting({}, true)").format(sql.Placeholder()) for _ in names
        )
    )
    row = await client.fetch_one(capture_stmt, list(names), row_factory="tuple")
    priors: tuple[str | None, ...] = tuple(row) if row is not None else (None,) * len(names)

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
