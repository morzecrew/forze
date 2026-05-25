"""Apply ETag headers and optional 304 responses to handler results."""

from typing import Any

from fastapi import Request, Response

from forze_fastapi.transport.http.etag.constants import ETAG_HEADER_KEY, IF_NONE_MATCH_HEADER_KEY
from forze_fastapi.transport.http.etag.provider import ETagProviderPort
from forze_fastapi.transport.http.etag.utils import ensure_quoted_etag, etag_matches
from forze_fastapi.transport.http.serialize import build_json_response, serialize_response_body

# ----------------------- #


def apply_etag(
    request: Request,
    result: Any,
    *,
    provider: ETagProviderPort,
    response_model: type[Any] | None,
    status_code: int | None,
    auto_304: bool,
    exclude_none: bool = True,
) -> Any:
    """Return *result* or a 304/JSON :class:`~starlette.responses.Response` with an ETag."""

    if isinstance(result, Response):
        return result

    body, _ = serialize_response_body(result, response_model, exclude_none=exclude_none)
    raw_tag = provider(body)

    if raw_tag is None:
        return build_json_response(
            result,
            response_model=response_model,
            status_code=status_code,
            exclude_none=exclude_none,
        )

    etag = ensure_quoted_etag(raw_tag)
    if_none_match = request.headers.get(IF_NONE_MATCH_HEADER_KEY)

    if auto_304 and if_none_match and etag_matches(etag, if_none_match):
        return Response(status_code=304, headers={ETAG_HEADER_KEY: etag})

    return build_json_response(
        result,
        response_model=response_model,
        status_code=status_code,
        extra_headers={ETAG_HEADER_KEY: etag},
        exclude_none=exclude_none,
    )
