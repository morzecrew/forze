"""Mongo error handler that maps PyMongo exceptions to :class:`~forze.base.errors.exc.internal` subtypes."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Any, Mapping

from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ConfigurationError,
    ConnectionFailure,
    DuplicateKeyError,
    ExecutionTimeout,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    ServerSelectionTimeoutError,
    WriteError,
    WTimeoutError,
)

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _mongo_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a PyMongo exception into an :class:`~forze.base.exceptions.CoreException`."""

    _ = site

    match exc:
        # --- write conflicts (must precede OperationFailure/WriteError) ---

        case DuplicateKeyError():
            return CoreException.conflict(
                "Duplicate key violation.",
                details=details,
            )

        case BulkWriteError():
            det: dict[str, Any] = getattr(exc, "details", None) or {}
            write_errors = det.get("writeErrors", [])

            if any(err.get("code") == 11000 for err in write_errors):
                return CoreException.conflict(
                    "Bulk write duplicate key violation.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"Bulk write error during {site}.",
                details=details,
            )

        case WriteError():
            code = getattr(exc, "code", None)

            if code == 11000:
                return CoreException.conflict(
                    "Duplicate key violation.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"Write error during {site}.",
                details=details,
            )

        case WTimeoutError():
            return CoreException.concurrency(
                "Write concern timeout. Please retry.",
                details=details,
            )

        # --- connection/topology (most-specific subclasses first) ---

        case NotPrimaryError():
            return CoreException.concurrency(
                "Not primary node. Please retry.",
                details=details,
            )

        case ServerSelectionTimeoutError():
            return CoreException.infrastructure(
                "Mongo server selection timed out.",
                details=details,
            )

        case NetworkTimeout() | ExecutionTimeout():
            return CoreException.infrastructure(
                "Mongo operation timed out.",
                details=details,
            )

        case AutoReconnect():
            return CoreException.concurrency(
                "Connection lost, automatic reconnect pending. Please retry.",
                details=details,
            )

        case ConnectionFailure():
            return CoreException.infrastructure(
                "Mongo connection failure.",
                details=details,
            )

        case ConfigurationError():
            return CoreException.infrastructure(
                "Mongo configuration error.",
                details=details,
            )

        # --- operation failures (must come after DuplicateKeyError/WTimeoutError) ---

        case OperationFailure():
            code = getattr(exc, "code", None)

            if code == 11600:
                return CoreException.concurrency(
                    "Interrupted due to replica set state change. Please retry.",
                    details=details,
                )

            if code == 251:
                return CoreException.concurrency(
                    "Transaction aborted due to conflict. Please retry.",
                    details=details,
                )

            if code == 112:
                # WriteConflict: standard retryable in-transaction conflict.
                return CoreException.concurrency(
                    "Write conflict during transaction. Please retry.",
                    details=details,
                )

            if exc.has_error_label("TransientTransactionError"):
                return CoreException.concurrency(
                    "Transient transaction error. Please retry.",
                    details=details,
                )

            # Classify by the server error code (13 = Unauthorized), not by
            # matching English substrings in the message text.
            if code == 13:
                return CoreException.infrastructure(
                    "Mongo authorization error.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"Mongo operation failure during {site}: {exc}",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Mongo", _mongo_eh)
