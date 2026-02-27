from datetime import timedelta

import orjson
from pydantic import BaseModel, TypeAdapter

from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.base.serialization import pydantic_model_hash
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, NotRequired, Optional, TypedDict

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

from forze.application.execution import ExecutionContext

# ----------------------- #

ExecutionContextDependencyPort = Callable[[], ExecutionContext]

# ....................... #


class IdempotentRouteConfig(TypedDict):
    operation: str
    ttl: timedelta
    header_key: str
    adapter: TypeAdapter[Any]
    dto_param: NotRequired[Optional[str]]


# ....................... #


class _EmptyStub(BaseModel):
    empty: bool = True


class _RawStub(BaseModel):
    raw: str


# ....................... #


class IdempotentRoute(APIRoute):
    def __init__(
        self,
        *args: Any,
        ctx_dep: ExecutionContextDependencyPort,
        idempotency_config: IdempotentRouteConfig,
        **kwargs: Any,
    ) -> None:
        self._ctx_dep = ctx_dep
        self._idempotency_config = idempotency_config

        super().__init__(*args, **kwargs)

    # ....................... #

    def get_route_handler(self):
        orig_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            idem_key = request.headers.get(self._idempotency_config["header_key"])

            if not idem_key:
                raise HTTPException(
                    status_code=400, detail="Idempotency key is required"
                )

            ctx = self._ctx_dep()
            idem_f = ctx.dep(IdempotencyDepKey)
            idem = idem_f(context=ctx, ttl=self._idempotency_config["ttl"])

            raw_body = await request.body()
            payload_hash: str

            try:
                payload_hash = self._hash_payload(raw_body)

            except Exception:
                return await orig_handler(request)

            snap = await idem.begin(
                self._idempotency_config["operation"], idem_key, payload_hash
            )

            if snap is not None:
                return Response(
                    content=snap["body"],
                    status_code=int(snap.get("code", 200)),
                    media_type=snap.get("content_type", "application/json"),
                )

            resp = await orig_handler(request)

            try:
                body_bytes = await self._response_body_bytes(resp)
                await idem.commit(
                    self._idempotency_config["operation"],
                    idem_key,
                    payload_hash,
                    {
                        "code": int(resp.status_code),
                        "content_type": resp.media_type
                        or resp.headers.get("content-type", "application/octet-stream"),
                        "body": body_bytes,
                    },
                )

            except Exception:
                pass

            return resp

        return handler

    # ....................... #

    def _hash_payload(self, raw_body: bytes) -> str:
        if not raw_body:
            return pydantic_model_hash(_EmptyStub())

        try:
            data = orjson.loads(raw_body)

        except Exception:
            return pydantic_model_hash(_RawStub(raw=raw_body.hex()))

        dto_param = self._idempotency_config.get("dto_param")

        if dto_param and isinstance(data, dict) and dto_param in data:
            data = data[dto_param]  # pyright: ignore[reportUnknownVariableType]

        validated = self._idempotency_config["adapter"].validate_python(data)

        return pydantic_model_hash(validated)

    # ....................... #

    async def _response_body_bytes(self, resp: Response) -> bytes:
        body = getattr(resp, "body", None)

        if isinstance(body, (bytes, bytearray)):
            return bytes(body)

        body_iter = getattr(resp, "body_iterator", None)

        if body_iter is None:
            return b""

        chunks: list[bytes] = []
        async for chunk in body_iter:
            if isinstance(chunk, str):
                chunk = chunk.encode(resp.charset or "utf-8")
            chunks.append(chunk)

        new_body = b"".join(chunks)

        new_resp = Response(
            content=new_body,
            status_code=resp.status_code,
            headers=dict(resp.headers),
            media_type=resp.media_type,
        )
        resp.__dict__.update(new_resp.__dict__)

        return new_body


# ....................... #


def make_idempotent_route_class(
    *,
    ctx_dep: ExecutionContextDependencyPort,
    operation: str,
    ttl: timedelta,
    header_key: str,
    adapter: TypeAdapter[Any],
    dto_param: Optional[str] = None,
) -> type[IdempotentRoute]:
    cfg = IdempotentRouteConfig(
        operation=operation,
        ttl=ttl,
        header_key=header_key,
        adapter=adapter,
        dto_param=dto_param,
    )

    class _Route(IdempotentRoute):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, ctx_dep=ctx_dep, idempotency_config=cfg, **kwargs)

    return _Route
