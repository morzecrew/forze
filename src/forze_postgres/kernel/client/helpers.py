from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from psycopg import sql

from forze.base.exceptions import exc

from .types import IsolationLevel
from .value_objects import PostgresTransactionOptions

# ----------------------- #


def isolation_level_sql_fragment(isolation: IsolationLevel) -> sql.Composable:
    """Return an SQL fragment for ``SET TRANSACTION ISOLATION LEVEL …`` (keyword, not quoted)."""

    levels: dict[IsolationLevel, sql.SQL] = {
        "read_committed": sql.SQL("READ COMMITTED"),
        "repeatable_read": sql.SQL("REPEATABLE READ"),
        "serializable": sql.SQL("SERIALIZABLE"),
    }
    frag = levels.get(isolation)

    if frag is None:
        raise exc.internal(
            f"Unsupported transaction isolation level {isolation!r}; "
            f"expected one of: {', '.join(sorted(levels))}",
        )

    return frag


def set_transaction_sql(options: PostgresTransactionOptions) -> sql.Composed:
    """Build the ``SET TRANSACTION …`` statement for a just-started root transaction.

    Emitted *inside* the open transaction so the options apply to that
    transaction only and never mutate psycopg connection attributes (which
    would persist across pool check-ins — psycopg_pool only rolls back on
    return). ``READ ONLY`` is appended only when requested; otherwise the
    session/server default access mode is left untouched.
    """

    return sql.SQL("SET TRANSACTION ISOLATION LEVEL {isolation}{read_only}").format(
        isolation=isolation_level_sql_fragment(options.isolation),
        read_only=sql.SQL(" READ ONLY") if options.read_only else sql.SQL(""),
    )
