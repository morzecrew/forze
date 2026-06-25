from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import re
from typing import Any, Mapping

from psycopg import errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #

FK_pattern = re.compile(
    r'Key \((?P<column>[^)]+)\)=\((?P<value>[0-9a-fA-F-]+)\) is not present in table "(?P<table>[^"]+)"'
)

# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _psycopg_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Translate psycopg exceptions into domain :class:`~forze.base.errors.exc.internal` subtypes."""

    _ = site

    match exc:
        # Integrity / constraints

        case errors.ForeignKeyViolation():
            msg = str(exc.diag.message_detail)
            match = FK_pattern.match(msg)

            if match:
                details = {
                    "table": match.group("table"),
                    "value": match.group("value"),
                }
            else:
                details = {"raw": msg}

            return CoreException.not_found(
                "Reference document not found.",
                details=details,
            )

        case errors.UniqueViolation():
            return CoreException.conflict(
                "Unique violation.",
                details=details,
            )

        case errors.ExclusionViolation():
            # e.g. gist exclusion constraints (overlaps, etc.)
            return CoreException.precondition(
                "Constraint violation (exclusion).",
                details=details,
            )

        case errors.CheckViolation():
            return CoreException.precondition(
                "Invalid value (check constraint).",
                details=details,
            )

        case errors.NotNullViolation():
            return CoreException.precondition(
                "Missing required value (not-null constraint).",
                details=details,
            )

        case errors.StringDataRightTruncation() | errors.DataError():
            # too long for varchar/char etc.
            return CoreException.precondition(
                "Invalid value (data too long or invalid format).",
                details=details,
            )

        case errors.NumericValueOutOfRange():
            return CoreException.precondition(
                "Invalid value (number out of range).",
                details=details,
            )

        case errors.InvalidTextRepresentation():
            # e.g. invalid uuid, invalid int, etc.
            return CoreException.precondition(
                "Invalid value (text representation).",
                details=details,
            )

        case errors.DatetimeFieldOverflow() | errors.InvalidDatetimeFormat():
            return CoreException.precondition(
                "Invalid datetime value.",
                details=details,
            )

        # Concurrency / retryable

        case errors.DeadlockDetected():
            # usually safe to retry
            return CoreException.concurrency(
                "Deadlock detected. Please retry.",
                details=details,
            )

        case errors.SerializationFailure():
            # SERIALIZABLE / REPEATABLE READ conflicts
            return CoreException.concurrency(
                "Transaction serialization failure. Please retry.",
                details=details,
            )

        # Connection / availability

        case errors.LockNotAvailable():
            # NOWAIT lock couldn't be acquired
            return CoreException.concurrency(
                "Lock not available. Please retry.",
                details=details,
            )

        case (
            errors.AdminShutdown() | errors.CrashShutdown() | errors.CannotConnectNow()
        ):
            return CoreException.infrastructure(
                "Database is not available (shutdown/starting).",
                details=details,
            )

        case errors.ConnectionException() | errors.ConnectionDoesNotExist():
            return CoreException.infrastructure(
                "Database connection error.",
                details=details,
            )

        case (
            errors.SqlclientUnableToEstablishSqlconnection()
            | errors.SqlserverRejectedEstablishmentOfSqlconnection()
        ):
            return CoreException.infrastructure(
                "Unable to establish database connection.",
                details=details,
            )

        # Programming / schema issues #! Should be InfrastructureError ?

        case errors.UndefinedTable():
            return CoreException.infrastructure(
                "Database schema error (undefined table).",
                details=details,
            )

        case errors.UndefinedColumn():
            return CoreException.infrastructure(
                "Database schema error (undefined column).",
                details=details,
            )

        case errors.UndefinedFunction():
            return CoreException.infrastructure(
                "Database schema error (undefined function).",
                details=details,
            )

        case errors.SyntaxError() | errors.InvalidSqlStatementName():
            return CoreException.infrastructure(
                "Database query syntax error.",
                details=details,
            )

        case errors.InsufficientPrivilege():
            return CoreException.infrastructure(
                "Database permission error.",
                details=details,
            )

        # Timeouts / resource limits

        case errors.QueryCanceled():
            # statement_timeout / user cancel
            return CoreException.infrastructure(
                "Database query canceled (timeout).",
                details=details,
            )

        case errors.TooManyConnections():
            return CoreException.concurrency(
                "Database is overloaded (too many connections). Please retry.",
                details=details,
            )

        case errors.OutOfMemory() | errors.DiskFull():
            return CoreException.infrastructure(
                "Database resource exhaustion.",
                details=details,
            )

        # Fallbacks by broad class

        case errors.IntegrityError():
            # any other constraint-ish problem
            return CoreException.conflict(
                "Integrity constraint violation.",
                details=details,
            )

        case errors.OperationalError() as oe:
            # Typed connection/availability errors (class 08*, 57P0x, 53300, …)
            # are handled by the specific cases above, so an OperationalError
            # reaching this catch-all with no SQLSTATE is a client-side
            # connectivity failure (server closed the connection, reset,
            # timeout, broken pipe) — transient and retryable. Classifying on
            # SQLSTATE rather than message text keeps this correct regardless of
            # the server/client message locale (``lc_messages`` / libpq gettext).
            sqlstate = oe.sqlstate

            if sqlstate is None or sqlstate.startswith("08"):
                return CoreException.concurrency(
                    "Transient database connectivity issue. Please retry.",
                    details=details,
                )

            return CoreException.infrastructure(
                "Database operational error.",
                details=details,
            )

        case errors.ProgrammingError():
            return CoreException.infrastructure(
                "Database programming error.",
                details=details,
            )

        case errors.GroupingError():
            return CoreException.infrastructure(
                "Database grouping error",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Postgres", _psycopg_eh)
