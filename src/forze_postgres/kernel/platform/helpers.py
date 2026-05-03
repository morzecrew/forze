from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from psycopg import sql

from forze.base.errors import CoreError

from .types import IsolationLevel

# ----------------------- #


def isolation_level_sql_fragment(isolation: IsolationLevel) -> sql.Composable:
    """Return an SQL fragment for ``SET TRANSACTION ISOLATION LEVEL …`` (keyword, not quoted)."""

    levels: dict[IsolationLevel, sql.SQL] = {
        "read committed": sql.SQL("READ COMMITTED"),
        "repeatable read": sql.SQL("REPEATABLE READ"),
        "serializable": sql.SQL("SERIALIZABLE"),
    }
    frag = levels.get(isolation)

    if frag is None:
        raise CoreError(
            f"Unsupported transaction isolation level {isolation!r}; "
            f"expected one of: {', '.join(sorted(levels))}",
        )

    return frag
