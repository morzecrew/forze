"""Mongo error handler that maps PyMongo exceptions to :class:`~forze.base.errors.CoreError` subtypes."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from functools import partial
from typing import Any

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

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    InfrastructureError,
    error_handler,
    handled,
)

# ----------------------- #


@error_handler
def _mongo_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Convert a PyMongo exception into a :class:`~forze.base.errors.CoreError` subtype."""

    match e:
        case CoreError():
            return e

        # --- write conflicts (must precede OperationFailure/WriteError) ---

        case DuplicateKeyError():
            return ConflictError("Duplicate key violation.")

        case BulkWriteError():
            details: dict[str, Any] = getattr(e, "details", None) or {}
            write_errors = details.get("writeErrors", [])

            if any(err.get("code") == 11000 for err in write_errors):
                return ConflictError("Bulk write duplicate key violation.")

            return InfrastructureError(f"Bulk write error during {op}.")

        case WriteError():
            code = getattr(e, "code", None)
            if code == 11000:
                return ConflictError("Duplicate key violation.")
            return InfrastructureError(f"Write error during {op}.")

        case WTimeoutError():
            return ConcurrencyError(
                message="Write concern timeout. Please retry.",
                code="write_concern_timeout",
            )

        # --- connection/topology (most-specific subclasses first) ---

        case NotPrimaryError():
            return ConcurrencyError(
                message="Not primary node. Please retry.",
                code="not_primary",
            )

        case ServerSelectionTimeoutError():
            return InfrastructureError("Mongo server selection timed out.")

        case NetworkTimeout() | ExecutionTimeout():
            return InfrastructureError("Mongo operation timed out.")

        case AutoReconnect():
            return ConcurrencyError(
                message="Connection lost, automatic reconnect pending. Please retry.",
                code="auto_reconnect",
            )

        case ConnectionFailure():
            return InfrastructureError("Mongo connection failure.")

        case ConfigurationError():
            return InfrastructureError("Mongo configuration error.")

        # --- operation failures (must come after DuplicateKeyError/WTimeoutError) ---

        case OperationFailure():
            code = getattr(e, "code", None)
            if code == 11600:
                return ConcurrencyError(
                    message="Interrupted due to replica set state change. Please retry.",
                    code="interrupted",
                )
            if code == 251:
                return ConcurrencyError(
                    message="Transaction aborted due to conflict. Please retry.",
                    code="transaction_conflict",
                )
            msg = str(e)
            if "not authorized" in msg.lower() or "unauthorized" in msg.lower():
                return InfrastructureError("Mongo authorization error.")
            return InfrastructureError(f"Mongo operation failure during {op}: {msg}")

        # --- fallback ---

        case _:
            return InfrastructureError(
                message=f"An error occurred while executing Mongo operation {op}: {e}"
            )


# ----------------------- #

mongo_handled = partial(handled, _mongo_eh)
