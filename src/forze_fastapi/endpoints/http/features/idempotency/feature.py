from typing import final

import attrs
from fastapi import HTTPException, Response

from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySpec
from forze.base.serialization import pydantic_model_hash
from forze_fastapi.endpoints._logger import logger

from ...contracts import (
    HttpEndpointContext,
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
)
from ...contracts.typevars import B, C, F, H, In, P, Q, R
from ..utils import serialize_endpoint_result
from .constants import IDEMPOTENCY_KEY_HEADER

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class IdempotencyFeature(HttpEndpointFeaturePort[Q, P, H, C, B, In, R, F]):
    """Feature that adds idempotency semantics to an HTTP endpoint.

    Before executing the endpoint, checks for an existing idempotency
    snapshot and returns it if present. After execution, commits the
    response as a snapshot for future replay.
    """

    spec: IdempotencySpec
    """Specification for the idempotency feature."""

    # ....................... #

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F]:

        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, R, F],
        ) -> R | Response:
            idem_key = ctx.raw_request.headers.get(IDEMPOTENCY_KEY_HEADER)

            if not idem_key:
                raise HTTPException(
                    status_code=400,
                    detail=f"Idempotency key is required (header: '{IDEMPOTENCY_KEY_HEADER}')",
                )

            idem_f = ctx.exec_ctx.dep(IdempotencyDepKey, route=self.spec.name)
            idem = idem_f(ctx.exec_ctx, self.spec)
            payload_hash = pydantic_model_hash(ctx.input)

            snap = await idem.begin(ctx.operation_id, idem_key, payload_hash)

            if snap is not None:
                #! log as replay response
                return Response(
                    content=snap["body"],
                    status_code=int(snap["code"]),
                    media_type=snap["content_type"],
                )

            result = await handler(ctx)

            if isinstance(result, Response):
                return result

            try:
                body_bytes, content_type = serialize_endpoint_result(
                    result, ctx.spec.response
                )

                await idem.commit(
                    ctx.operation_id,
                    idem_key,
                    payload_hash,
                    {
                        "code": int(ctx.spec.http.get("status_code", 200)),
                        "content_type": content_type,
                        "body": body_bytes,
                    },
                )

            except Exception:
                logger.exception("Failed to commit idempotency snapshot")

            return result

        return wrapped
