"""Firestore error handler mapping Google API errors to Forze core errors."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from functools import partial
from typing import Any

from google.api_core import exceptions as gax_exceptions

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


@error_handler
def _firestore_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Convert a Firestore/Google API exception into a :class:`~forze.base.errors.CoreError`."""

    match e:
        case CoreError():
            return e

        case gax_exceptions.NotFound():
            return NotFoundError(str(e))

        case gax_exceptions.AlreadyExists():
            return ConflictError("Document already exists.")

        case gax_exceptions.Aborted() | gax_exceptions.FailedPrecondition():
            return ConcurrencyError(
                message="Firestore transaction conflict. Please retry.",
                code="transaction_conflict",
            )

        case gax_exceptions.InvalidArgument():
            return ValidationError(str(e))

        case gax_exceptions.DeadlineExceeded() | gax_exceptions.ServiceUnavailable():
            return InfrastructureError(f"Firestore operation timed out or unavailable: {op}")

        case gax_exceptions.PermissionDenied() | gax_exceptions.Unauthenticated():
            return InfrastructureError("Firestore authorization error.")

        case _:
            return InfrastructureError(
                message=f"An error occurred while executing Firestore operation {op}: {e}"
            )


# ----------------------- #

firestore_handled = partial(handled, _firestore_eh)
