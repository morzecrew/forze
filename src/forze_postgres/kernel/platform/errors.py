from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import re
from functools import partial
from typing import Any

from psycopg import errors

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
    error_handler,
    handled,
)

# ----------------------- #

FK_pattern = re.compile(
    r'Key \((?P<column>[^)]+)\)=\((?P<value>[0-9a-fA-F-]+)\) is not present in table "(?P<table>[^"]+)"'
)

# ....................... #


@error_handler
def _psycopg_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Translate psycopg exceptions into domain :class:`~forze.base.errors.CoreError` subtypes."""

    match e:
        case CoreError():
            return e

        # Integrity / constraints

        case errors.ForeignKeyViolation():
            msg = str(e.diag.message_detail)
            match = FK_pattern.match(msg)

            if match:
                details = {
                    "table": match.group("table"),
                    "value": match.group("value"),
                }
            else:
                details = {"raw": msg}

            return NotFoundError(
                message="Reference document not found.",
                details=details,
            )

        case errors.UniqueViolation():
            return ConflictError("Unique violation.")

        case errors.ExclusionViolation():
            # e.g. gist exclusion constraints (overlaps, etc.)
            return ConflictError("Constraint violation (exclusion).")

        case errors.CheckViolation():
            return ValidationError("Invalid value (check constraint).")

        case errors.NotNullViolation():
            return ValidationError("Missing required value (not-null constraint).")

        case errors.StringDataRightTruncation() | errors.DataError():
            # too long for varchar/char etc.
            return ValidationError("Invalid value (data too long or invalid format).")

        case errors.NumericValueOutOfRange():
            return ValidationError("Invalid value (number out of range).")

        case errors.InvalidTextRepresentation():
            # e.g. invalid uuid, invalid int, etc.
            return ValidationError("Invalid value (text representation).")

        case errors.DatetimeFieldOverflow() | errors.InvalidDatetimeFormat():
            return ValidationError("Invalid datetime value.")

        # Concurrency / retryable

        case errors.DeadlockDetected():
            # usually safe to retry
            return ConcurrencyError(
                message="Deadlock detected. Please retry.",
                code="deadlock",
            )

        case errors.SerializationFailure():
            # SERIALIZABLE / REPEATABLE READ conflicts
            return ConcurrencyError(
                message="Transaction serialization failure. Please retry.",
                code="serialization_failure",
            )

        # Connection / availability

        case errors.LockNotAvailable():
            # NOWAIT lock couldn't be acquired
            return ConcurrencyError(
                message="Lock not available. Please retry.",
                code="lock_not_available",
            )

        case (
            errors.AdminShutdown() | errors.CrashShutdown() | errors.CannotConnectNow()
        ):
            return InfrastructureError("Database is not available (shutdown/starting).")

        case errors.ConnectionException() | errors.ConnectionDoesNotExist():
            return InfrastructureError("Database connection error.")

        case (
            errors.SqlclientUnableToEstablishSqlconnection()
            | errors.SqlserverRejectedEstablishmentOfSqlconnection()
        ):
            return InfrastructureError("Unable to establish database connection.")

        # Programming / schema issues #! Should be InfrastructureError ?

        case errors.UndefinedTable():
            return InfrastructureError("Database schema error (undefined table).")

        case errors.UndefinedColumn():
            return InfrastructureError("Database schema error (undefined column).")

        case errors.UndefinedFunction():
            return InfrastructureError("Database schema error (undefined function).")

        case errors.SyntaxError() | errors.InvalidSqlStatementName():
            return InfrastructureError("Database query syntax error.")

        case errors.InsufficientPrivilege():
            return InfrastructureError("Database permission error.")

        # Timeouts / resource limits

        case errors.QueryCanceled():
            # statement_timeout / user cancel
            return InfrastructureError("Database query canceled (timeout).")

        case errors.TooManyConnections():
            return ConcurrencyError(
                message="Database is overloaded (too many connections). Please retry.",
                code="too_many_connections",
            )

        case errors.OutOfMemory() | errors.DiskFull():
            return InfrastructureError("Database resource exhaustion.")

        # Fallbacks by broad class

        case errors.IntegrityError():
            # any other constraint-ish problem
            return ConflictError("Integrity constraint violation.")

        case errors.OperationalError() as oe:
            msg = str(oe).lower()
            transient_markers = (
                "connection",
                "closed",
                "timeout",
                "eof",
                "reset",
                "broken pipe",
                "server closed",
                "could not receive",
                "could not send",
            )
            if any(s in msg for s in transient_markers):
                return ConcurrencyError(
                    message="Transient database connectivity issue. Please retry.",
                    code="transient_operational",
                )

            return InfrastructureError("Database operational error.")

        case errors.ProgrammingError():
            return InfrastructureError("Database programming error.")

        case errors.GroupingError():
            return InfrastructureError("Database grouping error")

        case _:
            return InfrastructureError(
                f"An error occurred while executing the operation {op}: {e}"
            )


# ....................... #

psycopg_handled = partial(handled, _psycopg_eh)
