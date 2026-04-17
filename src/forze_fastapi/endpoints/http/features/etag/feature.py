from typing import final

import attrs
from fastapi import Response

from ...contracts import (
    HttpEndpointContext,
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
)
from ...contracts.typevars import B, C, F, H, In, P, Q, R
from ..utils import response_from_endpoint_result, serialize_endpoint_result
from .constants import ETAG_HEADER_KEY, IF_NONE_MATCH_HEADER_KEY
from .ports import ETagProviderPort
from .utils import ensure_quoted_etag, etag_matches

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ETagFeature(HttpEndpointFeaturePort[Q, P, H, C, B, In, R, F]):
    """Feature that adds ETag support to an HTTP endpoint."""

    provider: ETagProviderPort
    """Provider used to generate the ETag value."""

    auto_304: bool = attrs.field(default=True)
    """Whether to return a 304 Not Modified response when the ETag matches."""

    exclude_none: bool = attrs.field(default=True)
    """Whether to exclude ``None`` values from the ETag calculation."""

    # ....................... #

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F]:
        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, R, F],
        ) -> R | Response:
            result = await handler(ctx)

            body, _ = serialize_endpoint_result(
                result,
                ctx.spec.response,
                self.exclude_none,
            )

            raw_tag = self.provider(body)

            if raw_tag is None:
                return result

            etag = ensure_quoted_etag(raw_tag)
            if_none_match = ctx.raw_request.headers.get(IF_NONE_MATCH_HEADER_KEY)

            if self.auto_304 and if_none_match and etag_matches(etag, if_none_match):
                return Response(status_code=304, headers={ETAG_HEADER_KEY: etag})

            return response_from_endpoint_result(
                result,
                response_model=ctx.spec.response,
                status_code=ctx.spec.http.get("status_code", 200),
                extra_headers={ETAG_HEADER_KEY: etag},
                exclude_none=self.exclude_none,
            )

        return wrapped
