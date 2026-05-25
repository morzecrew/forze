"""Serialize handler results to HTTP response bodies."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

import orjson
from fastapi import Response
from pydantic import TypeAdapter

# ----------------------- #


def serialize_response_body(
    result: Any,
    response_model: type[Any] | None,
    *,
    exclude_none: bool = True,
) -> tuple[bytes, str]:
    """Serialize a handler result to bytes and a content type."""

    if isinstance(result, Response):
        body = getattr(result, "body", None)

        if isinstance(body, (bytes, bytearray)):
            content_type = result.media_type or result.headers.get(
                "content-type",
                "application/octet-stream",
            )
            return bytes(body), str(content_type)

        return b"", str(
            result.media_type or result.headers.get("content-type", "application/octet-stream"),
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


def build_json_response(
    result: Any,
    *,
    response_model: type[Any] | None = None,
    status_code: int | None = None,
    extra_headers: dict[str, str] | None = None,
    exclude_none: bool = True,
) -> Response:
    """Build a Starlette response from a handler result."""

    if isinstance(result, Response):
        if extra_headers:
            for key, value in extra_headers.items():
                result.headers[key] = value
        return result

    body_bytes, content_type = serialize_response_body(
        result,
        response_model,
        exclude_none=exclude_none,
    )
    headers = dict(extra_headers or {})

    return Response(
        content=body_bytes,
        status_code=status_code or 200,
        media_type=content_type,
        headers=headers,
    )
