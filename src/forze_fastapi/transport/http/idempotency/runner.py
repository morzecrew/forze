"""Idempotency guard executed inside route handlers (after request validation)."""

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException, Request, Response

from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
    IdempotencySnapshot,
    IdempotencySpec,
)
from forze.application.execution import ExecutionContext
from forze.base.serialization import pydantic_model_hash
from forze_fastapi.transport.http.idempotency.constants import IDEMPOTENCY_KEY_HEADER
from forze_fastapi.transport.http.serialize import serialize_response_body

logger = logging.getLogger(__name__)

# ----------------------- #


async def run_idempotent(
    request: Request,
    ctx: ExecutionContext,
    *,
    operation_id: str,
    spec: IdempotencySpec,
    payload: Any,
    inner: Callable[[], Awaitable[Any]],
    response_model: type[Any] | None,
    status_code: int | None,
    exclude_none: bool = True,
) -> Any:
    """Run *inner* with idempotency begin/replay/commit semantics."""

    idem_key = request.headers.get(IDEMPOTENCY_KEY_HEADER)

    if not idem_key:
        raise HTTPException(
            status_code=400,
            detail=f"Idempotency key is required (header: '{IDEMPOTENCY_KEY_HEADER}')",
        )

    idem = ctx.deps.resolve_configurable(
        ctx,
        IdempotencyDepKey,
        spec,
        route=spec.name,
    )
    payload_hash = pydantic_model_hash(payload)

    snap = await idem.begin(operation_id, idem_key, payload_hash)

    if snap is not None:
        return Response(
            content=snap.body,
            status_code=snap.code,
            media_type=snap.content_type,
        )

    result = await inner()

    if isinstance(result, Response):
        return result

    try:
        body_bytes, content_type = serialize_response_body(
            result,
            response_model,
            exclude_none=exclude_none,
        )
        await idem.commit(
            operation_id,
            idem_key,
            payload_hash,
            IdempotencySnapshot(
                code=int(status_code or 200),
                content_type=content_type,
                body=body_bytes,
            ),
        )
    except Exception:
        logger.exception("Failed to commit idempotency snapshot")

    return result
