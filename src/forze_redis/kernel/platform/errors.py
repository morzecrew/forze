"""Redis error handler that maps ``redis-py`` exceptions to :class:`~forze.base.errors.CoreError` subtypes."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from functools import partial
from typing import Any

from redis import exceptions as redis_errors

from forze.base.errors import CoreError, InfrastructureError, error_handler, handled

# ----------------------- #


@error_handler
def _redis_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Convert a ``redis-py`` exception into an :class:`~forze.base.errors.InfrastructureError`.

    Connection, timeout, authentication, and data errors are mapped to specific
    messages. Unrecognised exceptions fall back to a generic infrastructure error
    that includes the operation name.
    """

    match e:
        case CoreError():
            return e

        # --- infra / availability ---
        case redis_errors.ConnectionError():
            return InfrastructureError("Redis connection error.")

        case redis_errors.TimeoutError():
            return InfrastructureError("Redis timeout.")

        case redis_errors.AuthenticationError():
            return InfrastructureError("Redis authentication failed.")

        case redis_errors.BusyLoadingError():
            return InfrastructureError("Redis is loading data, try again later.")

        case redis_errors.ReadOnlyError():
            return InfrastructureError("Redis instance is read-only.")

        # --- semantic / client-side errors ---
        case redis_errors.DataError():
            return InfrastructureError("Invalid Redis command arguments.")

        case redis_errors.ResponseError() as re:
            msg = str(re)

            if "WRONGTYPE" in msg:
                return InfrastructureError("Redis key has wrong type.")

            if "BUSY" in msg:
                return InfrastructureError("Redis resource is busy.")

            return InfrastructureError(f"Redis response error: {msg}")

        # --- fallback ---
        case _:
            return InfrastructureError(
                f"An error occurred while executing Redis operation {op}: {e}"
            )


# ----------------------- #

redis_handled = partial(handled, _redis_eh)
