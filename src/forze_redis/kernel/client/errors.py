"""Redis error handler that maps ``redis-py`` exceptions to :class:`~forze.base.errors.exc.internal` subtypes."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Any, Mapping

from redis import exceptions as redis_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
    fallback_exception_mapper,
)

# ----------------------- #

_fallback = fallback_exception_mapper("Redis")

# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _redis_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a ``redis-py`` exception into an :class:`~forze.base.exceptions.CoreException`.

    Connection, timeout, authentication, and data errors are mapped to specific
    messages. Unrecognised exceptions fall back to a generic infrastructure error
    that includes the operation name.
    """

    match exc:
        case CoreException():
            return exc

        # --- infra / availability ---
        # ``AuthenticationError`` and ``BusyLoadingError`` subclass
        # ``ConnectionError``; match them before the broad connection case.
        case redis_errors.AuthenticationError():
            return CoreException.infrastructure(
                "Redis authentication failed.",
                details=details,
            )

        case redis_errors.BusyLoadingError():
            return CoreException.infrastructure(
                "Redis is loading data, try again later.",
                details=details,
            )

        case redis_errors.ConnectionError():
            return CoreException.infrastructure(
                "Redis connection error.",
                details=details,
            )

        case redis_errors.TimeoutError():
            return CoreException.infrastructure(
                "Redis timeout.",
                details=details,
            )

        # ``ReadOnlyError`` subclasses ``ResponseError``; handle before the
        # generic response branch.
        case redis_errors.ReadOnlyError():
            return CoreException.infrastructure(
                "Redis instance is read-only.",
                details=details,
            )

        # --- semantic / client-side errors ---
        case redis_errors.DataError():
            return CoreException.infrastructure(
                "Invalid Redis command arguments.",
                details=details,
            )

        case redis_errors.ResponseError() as re:
            msg = str(re)

            if "WRONGTYPE" in msg:
                return CoreException.infrastructure(
                    "Redis key has wrong type.",
                    details=details,
                )

            if "BUSY" in msg:
                return CoreException.infrastructure(
                    "Redis resource is busy.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"Redis response error: {msg}",
                details=details,
            )

        # --- fallback ---
        case _:
            return _fallback(exc, site=site, details=details)


# ....................... #

_redis_chain = default_chain_exc_mapper.chain(_redis_eh)
exc_interceptor = ExceptionInterceptor(mapper=_redis_chain)
