from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import TYPE_CHECKING

from psycopg import sql

from forze.base.errors import CoreError

from .types import IsolationLevel

if TYPE_CHECKING:
    from psycopg import IsolationLevel as PsycopgIsolationLevel  # noqa: F401

# ----------------------- #


def isolation_level_psycopg(isolation: IsolationLevel) -> "PsycopgIsolationLevel":
    """Map framework isolation names to :class:`psycopg.IsolationLevel`."""

    from psycopg import IsolationLevel as IL

    levels: dict[IsolationLevel, IL] = {
        "read committed": IL.READ_COMMITTED,
        "repeatable read": IL.REPEATABLE_READ,
        "serializable": IL.SERIALIZABLE,
    }
    out = levels.get(isolation)

    if out is None:
        raise CoreError(
            f"Unsupported transaction isolation level {isolation!r}; "
            f"expected one of: {', '.join(sorted(levels))}",
        )

    return out


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
