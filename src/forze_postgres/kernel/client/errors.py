from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import re
from collections.abc import Mapping
from typing import Any, Final

import psycopg_pool as pool_errors
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

POOL_ACQUIRE_TIMEOUT_CODE: Final[str] = "pool_acquire_timeout"
"""Code for a connection this process could not obtain from its own pool in time.

Named for what was observed rather than for a cause, because ``PoolTimeout`` does not carry
one. It arrives both when every connection is checked out and busy, and when none could be
established at all — a refused connection reaches a caller as exactly this timeout, verified
against a closed port.

Distinct either way from the server refusing connections (``too many connections``, SQLSTATE
53300, which stays a concurrency error): that is the database's own ceiling, this is
``max_size`` or ``acquire_timeout`` on this client.

Which of the two it was is decided by :func:`pool_acquire_error`, where the caller can measure
it; this code is what remains when the answer is "a busy pool"."""

DATABASE_UNREACHABLE_CODE: Final[str] = "database_unreachable"
"""Code for a wait that ended with the pool unable to establish any connection.

The other half of :data:`POOL_ACQUIRE_TIMEOUT_CODE`: same ``PoolTimeout``, opposite cause and
opposite remedy — nobody should be scaling a pool while the server is refusing connections."""

COPY_ERRORS: tuple[type[errors.Error], ...] = (
    errors.DataError,
    errors.BadCopyFileFormat,
    errors.ProtocolViolation,
)
"""The psycopg families a ``COPY`` failure can arrive as.

``ProtocolViolation`` earns its place by measurement rather than by category: a binary-mode
type mismatch derails the stream's framing, so the server reports ``insufficient data left
in message`` (SQLSTATE 08P01) — an ``OperationalError``, nowhere near ``DataError``, and
mapped to a retryable class if left to the generic arm."""

COPY_CONTEXT_pattern = re.compile(
    r"COPY\s+.+?,\s+line\s+(?P<line>\d+)(?:,\s+column\s+(?P<column>[^:]+?))?(?::|$)",
)
"""Postgres reports data errors during ``COPY`` as ``CONTEXT: COPY t, line N, column c: …``.

At 10⁶ rows that line number is the difference between a debugging session and a ``sed -n``,
so it is lifted out of the driver's free-text diagnostic and onto the error's details.

The relation name is skipped rather than captured, and *lazily*: the server writes it
unquoted, so a table called ``odd,name`` yields ``COPY odd,name, line 3`` and a name matched
up to the first comma finds no line at all. Lazy matching also settles which ``, line N`` wins
when the error detail quotes a value containing one — the leading occurrence is the server's,
any later one is inside the text it is complaining about."""

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

        case errors.AdminShutdown() | errors.CrashShutdown() | errors.CannotConnectNow():
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

        # Local resource limits — the pool, not the server

        case pool_errors.PoolTimeout() | pool_errors.TooManyRequests():
            # This process could not get a connection out of its own pool in time. It has to
            # be matched *before* the OperationalError fallback below, which both of these
            # subclass with no SQLSTATE, and which would otherwise call local saturation a
            # connectivity failure and hand the caller a 409.
            #
            # Throttled rather than concurrency: nothing conflicted, so 429 tells a caller to
            # back off where 409 tells it to reconcile state it has no quarrel with. That
            # instruction is right whichever cause produced the timeout — a full pool and an
            # unreachable server both mean "not now, try later" — which is why the kind is
            # chosen for what the caller should do rather than for what went wrong. Throttled
            # also keeps details off the wire, which suits a limit that is ours.
            #
            # Both kinds are retryable at egress, so brokers, consumers and sagas treat this
            # exactly as before. What does change: the built-in ``occ`` policy retries
            # ``concurrency`` only, so a write gateway no longer spends three in-process
            # attempts on a starved pool — which is the right call anyway, since those
            # attempts contend for the very connections that are missing.
            return CoreException.throttled(
                "Timed out acquiring a database connection. Please retry.",
                code=POOL_ACQUIRE_TIMEOUT_CODE,
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


def pool_acquire_error(
    *,
    connection_errors_before: int,
    connection_errors_after: int,
    details: Mapping[str, Any] | None = None,
) -> CoreException:
    """Decide which of a ``PoolTimeout``'s two causes actually happened.

    The exception itself says only "no connection within the timeout". It says that both when
    every connection is checked out and busy, and when none could be established at all — and
    the remedies are opposite, so answering "at capacity" to an unreachable server sends
    whoever reads it to scale a pool that is not the problem.

    The evidence is the pool's own ``connections_errors``, sampled either side of the wait. It
    counts failed connection *attempts*, so a rise means the pool was trying to open
    connections and failing while this caller waited. A saturated pool at ``max_size`` makes no
    attempts and the counter stands still.

    A delta rather than a total, deliberately: the total is cumulative for the pool's whole
    life, so a server that dies after serving traffic keeps ``errors < attempts`` forever and
    would never be recognised — which is the outage that matters most. What the delta cannot
    separate is a single connection recycling badly during a genuinely saturated wait; that
    reports as unreachable, a rare and safe direction to be wrong in.

    :param connection_errors_before: ``connections_errors`` sampled before the wait.
    :param connection_errors_after: the same counter once the wait has failed.
    :returns: the classified failure — never ``None``, since the caller already has a timeout.
    """

    if connection_errors_after > connection_errors_before:
        return CoreException.infrastructure(
            "Database is unreachable (no connection could be established).",
            code=DATABASE_UNREACHABLE_CODE,
            details=details,
        )

    return CoreException.throttled(
        "Timed out acquiring a database connection. Please retry.",
        code=POOL_ACQUIRE_TIMEOUT_CODE,
        details=details,
    )


# ....................... #


def copy_data_error(error: BaseException, *, binary: bool) -> CoreException | None:
    """Map a ``COPY`` data error to the copy taxonomy, or ``None`` to defer.

    Deferring is the important half. A ``COPY`` can fail for reasons that have nothing to
    do with the rows — a statement timeout, a serialization failure, a missing table — and
    those already have mappings that callers handle. Only an error the server attributes to
    a specific input line, or a binary-format rejection, is this function's business;
    everything else is left for :func:`_psycopg_eh` and keeps the mapping it always had.
    """

    if not isinstance(error, COPY_ERRORS):
        return None

    failure: errors.Error = error
    match = COPY_CONTEXT_pattern.search(str(failure.diag.context or ""))

    if match is None:
        # No ``COPY … line N`` context means the server did not attribute this to the
        # stream — a protocol violation on a broken connection, a data error raised
        # somewhere else entirely. Defer, so it keeps the mapping it has always had rather
        # than being relabelled as a bad row.
        return None

    details: dict[str, Any] = {
        "detail": str(failure.diag.message_primary or failure),
        "line": int(match.group("line")),
    }

    if match.group("column"):
        details["column"] = match.group("column").strip()

    # In binary mode the server parses a fixed-width stream, so a declared type that does
    # not match the column derails the framing itself — it arrives as a protocol violation
    # ("insufficient data left in message"), not as a bad value. The fix is the caller's
    # `column_types`, not the row, and the code has to say which. Text mode has no such
    # case: the server casts, and a rejection there really is the row's fault.
    if binary and isinstance(error, errors.ProtocolViolation | errors.BadCopyFileFormat):
        return CoreException.validation(
            "COPY rejected the binary stream: declared column types do not match the table.",
            code="copy_type_mismatch",
            details=details,
        )

    return CoreException.validation(
        "COPY rejected an input row.",
        code="copy_row_invalid",
        details=details,
    )


# ....................... #

exc_interceptor = build_exc_interceptor("Postgres", _psycopg_eh)
