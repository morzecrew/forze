"""Redis error handler that maps ``redis-py`` exceptions to :class:`~forze.base.errors.exc.internal` subtypes."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from collections.abc import Mapping
from typing import Any

from redis import exceptions as redis_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _redis_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Convert a ``redis-py`` exception into an :class:`~forze.base.exceptions.CoreException`.

    Connection, timeout, authentication, and data errors are mapped to specific
    messages; unrecognised exceptions defer (``None``) to the chain's fallback.
    """

    _ = site

    match exc:
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
            # RESP error codes are the leading token (e.g. ``WRONGTYPE ...``,
            # ``BUSYGROUP ...``). Match on that token, not an unanchored
            # substring that could hit a key/script name echoed in the message.
            head = msg.split(" ", 1)[0]

            if head == "WRONGTYPE":
                return CoreException.infrastructure(
                    "Redis key has wrong type.",
                    details=details,
                )

            if head.startswith("BUSY"):
                return CoreException.infrastructure(
                    "Redis resource is busy.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"Redis response error: {msg}",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Redis", _redis_eh)
