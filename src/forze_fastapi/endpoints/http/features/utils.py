from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

import orjson
from fastapi import Response
from pydantic import TypeAdapter

# ----------------------- #


def serialize_endpoint_result(
    result: Any,
    response_model: type[Any] | None,
    exclude_none: bool = True,
) -> tuple[bytes, str]:
    if isinstance(result, Response):
        body = getattr(result, "body", None)

        if isinstance(body, (bytes, bytearray)):
            content_type = result.media_type or result.headers.get(
                "content-type",
                "application/octet-stream",
            )
            return bytes(body), content_type

        return b"", result.media_type or result.headers.get(
            "content-type",
            "application/octet-stream",
        )

    if result is None:
        return b"", "application/json"

    if response_model is not None:
        adapter = TypeAdapter(response_model)
        dumped = adapter.dump_python(result, mode="json", exclude_none=exclude_none)
        return orjson.dumps(dumped), "application/json"

    if isinstance(result, (bytes, bytearray)):
        return bytes(result), "application/octet-stream"

    if isinstance(result, str):
        return result.encode("utf-8"), "text/plain; charset=utf-8"

    return orjson.dumps(result), "application/json"


# ....................... #


def response_from_endpoint_result(
    result: Any,
    *,
    response_model: type[Any] | None = None,
    status_code: int | None,
    extra_headers: dict[str, str] | None = None,
    exclude_none: bool = True,
) -> Response:
    if isinstance(result, Response):
        if extra_headers:
            for k, v in extra_headers.items():
                result.headers[k] = v

        return result

    body_bytes, content_type = serialize_endpoint_result(
        result, response_model, exclude_none
    )
    headers = dict(extra_headers or {})

    return Response(
        content=body_bytes,
        status_code=status_code or 200,
        media_type=content_type,
        headers=headers,
    )
