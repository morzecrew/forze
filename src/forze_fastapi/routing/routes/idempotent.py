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
"""Callable that returns an :class:`ExecutionContext` (used as a FastAPI dependency)."""

# ....................... #


class IdempotentRouteConfig(TypedDict):
    """Configuration for an idempotent route."""

    operation: str
    """Operation identifier used as the idempotency scope."""

    ttl: timedelta
    """Time-to-live for the idempotency snapshot."""

    header_key: str
    """HTTP header name carrying the idempotency key."""

    adapter: TypeAdapter[Any]
    """Adapter used to validate and hash the request payload."""

    dto_param: NotRequired[Optional[str]]
    """Name of the DTO parameter in the endpoint signature."""


# ....................... #


class _EmptyStub(BaseModel):
    """Stub model for hashing requests with empty bodies."""

    empty: bool = True


class _RawStub(BaseModel):
    """Stub model for hashing non-JSON request bodies."""

    raw: str


# ....................... #


class IdempotentRoute(APIRoute):
    """Custom :class:`APIRoute` that adds idempotency semantics to POST routes.

    Before executing the endpoint, checks for an existing idempotency
    snapshot and returns it if present. After execution, commits the
    response as a snapshot for future replay.
    """

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

    def get_route_handler(self):  # type: ignore[no-untyped-def]
        """Return a handler that wraps the original with idempotency logic."""

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

            except Exception:  # nosec: B110
                pass

            return resp

        return handler

    # ....................... #

    def _hash_payload(self, raw_body: bytes) -> str:
        """Hash the request body into a stable payload fingerprint."""

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
        """Extract raw bytes from a response, consuming streaming iterators if needed."""

        body = getattr(resp, "body", None)

        if isinstance(body, (bytes, bytearray)):
            return bytes(body)

        body_iter = getattr(resp, "body_iterator", None)

        if body_iter is None:
            return b""

        chunks = [chunk async for chunk in body_iter]

        if not chunks:
            new_body = b""

        else:
            try:
                if isinstance(chunks[0], str):
                    new_body = "".join(chunks).encode(resp.charset or "utf-8")
                else:
                    new_body = b"".join(chunks)

            except TypeError:
                charset = resp.charset or "utf-8"
                new_body = b"".join(
                    c.encode(charset) if isinstance(c, str) else c for c in chunks
                )

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
    """Create a route class pre-configured with idempotency settings."""

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
