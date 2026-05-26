"""Firestore error handler mapping Google API errors to Forze core errors."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Any, Mapping

from google.api_core import exceptions as gax_exceptions

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _firestore_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a Firestore/Google API exception into a :class:`~forze.base.errors.exc.internal`."""

    match exc:
        case CoreException():
            return exc

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
            return CoreException.infrastructure(
                f"An error occurred while executing Firestore operation {site}: {exc}",
                details=details,
            )


# ....................... #

_fs_chain = default_chain_exc_mapper.chain(_firestore_eh)
exc_interceptor = ExceptionInterceptor(mapper=_fs_chain)
