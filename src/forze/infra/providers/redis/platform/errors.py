from functools import partial
from typing import Any

from redis import exceptions as redis_errors

from forze.base.errors import CoreError, error_handler, handled
from forze.infra.errors import InfrastructureError

# ----------------------- #


@error_handler
def _redis_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
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
