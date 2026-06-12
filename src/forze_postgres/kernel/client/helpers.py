from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from psycopg import IsolationLevel as PsycopgIsolationLevel

from forze.base.exceptions import exc

from .types import IsolationLevel

# ----------------------- #

_ISOLATION_LEVELS: dict[IsolationLevel, PsycopgIsolationLevel] = {
    "read_committed": PsycopgIsolationLevel.READ_COMMITTED,
    "repeatable_read": PsycopgIsolationLevel.REPEATABLE_READ,
    "serializable": PsycopgIsolationLevel.SERIALIZABLE,
}


def isolation_level_enum(isolation: IsolationLevel) -> PsycopgIsolationLevel:
    """Map a Forze isolation level literal to :class:`psycopg.IsolationLevel`.

    Used to set :attr:`psycopg.AsyncConnection.isolation_level` *before* a root
    transaction starts, so psycopg composes the level into the ``BEGIN``
    statement itself (``BEGIN ISOLATION LEVEL …``) with zero extra round-trips.
    """

    level = _ISOLATION_LEVELS.get(isolation)

    if level is None:
        raise exc.internal(
            f"Unsupported transaction isolation level {isolation!r}; "
            f"expected one of: {', '.join(sorted(_ISOLATION_LEVELS))}",
        )

    return level
