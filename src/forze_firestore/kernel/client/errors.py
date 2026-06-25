"""Firestore error handler mapping Google API errors to Forze core errors."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Any, Mapping

from google.api_core import exceptions as gax_exceptions

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _firestore_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a Firestore/Google API exception into a :class:`~forze.base.errors.exc.internal`."""

    _ = site

    match exc:
        case gax_exceptions.NotFound():
            return CoreException.not_found(
                str(exc),
                details=details,
            )

        case gax_exceptions.AlreadyExists():
            return CoreException.conflict(
                "Document already exists.",
                details=details,
            )

        case gax_exceptions.Aborted() | gax_exceptions.FailedPrecondition():
            return CoreException.concurrency(
                "Firestore transaction conflict. Please retry.",
                details=details,
            )

        case gax_exceptions.InvalidArgument():
            return CoreException.validation(str(exc))

        case gax_exceptions.DeadlineExceeded() | gax_exceptions.ServiceUnavailable():
            return CoreException.infrastructure(
                f"Firestore operation timed out or unavailable: {site}",
                details=details,
            )

        case gax_exceptions.PermissionDenied() | gax_exceptions.Unauthenticated():
            return CoreException.authentication(
                "Firestore authorization error.",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Firestore", _firestore_eh)
