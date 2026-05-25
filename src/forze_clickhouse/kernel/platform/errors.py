from forze_clickhouse._compat import require_clickhouse

require_clickhouse()

# ....................... #

from functools import partial
from typing import Any

import aiohttp

from forze.base.errors import CoreError, InfrastructureError, error_handler, handled

# ----------------------- #


def _response_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)

    if status is not None:
        return int(status)

    code = getattr(exc, "code", None)

    if code is not None:
        return int(code)

    return None


@error_handler
def _clickhouse_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Normalize clickhouse-connect / aiohttp errors."""

    match e:
        case CoreError():
            return e

        case aiohttp.ClientResponseError() as cre:
            status = _response_status(cre)

            if status == 404:
                return InfrastructureError("ClickHouse resource not found.")

            if status in {401, 403}:
                return InfrastructureError("ClickHouse access denied.")

            if status == 429:
                return InfrastructureError("ClickHouse request throttled.")

            return InfrastructureError(f"ClickHouse request failed ({status}).")

        case _:
            msg = str(e).lower()

            if "authentication" in msg or "password" in msg:
                return InfrastructureError("ClickHouse access denied.")

            return InfrastructureError(f"ClickHouse error during {op}.")


clickhouse_handled = partial(handled, _clickhouse_eh)
