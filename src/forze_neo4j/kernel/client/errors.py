"""Map neo4j driver exceptions to :class:`~forze.base.exceptions.CoreException`."""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

from typing import Any, Mapping

from neo4j.exceptions import (
    ClientError,
    ConstraintError,
    DriverError,
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
    fallback_exception_mapper,
)

# ----------------------- #

_fallback = fallback_exception_mapper("Neo4j")

# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _neo4j_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a neo4j driver exception into a :class:`CoreException`."""

    match exc:
        case CoreException():
            return exc

        case ConstraintError():
            return CoreException.conflict(
                "Neo4j constraint violation.",
                details=details,
            )

        case TransientError():
            return CoreException.concurrency(
                "Neo4j transient error. Please retry.",
                details=details,
            )

        case SessionExpired():
            return CoreException.concurrency(
                "Neo4j session expired. Please retry.",
                details=details,
            )

        case ServiceUnavailable():
            return CoreException.infrastructure(
                "Neo4j service unavailable.",
                details=details,
            )

        case ClientError():
            return CoreException.infrastructure(
                f"Neo4j client error during {site}: {exc}",
                details=details,
            )

        case Neo4jError() | DriverError():
            return CoreException.infrastructure(
                f"Neo4j error during {site}: {exc}",
                details=details,
            )

        case _:
            return _fallback(exc, site=site, details=details)


# ....................... #

_neo4j_chain = default_chain_exc_mapper.chain(_neo4j_eh)
exc_interceptor = ExceptionInterceptor(mapper=_neo4j_chain)
"""Context manager / decorator that maps neo4j errors to ``CoreException``."""
