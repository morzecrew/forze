"""JWKS route — publishes access-token signing public keys for verifiers."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

# ----------------------- #

JwksProvider = Callable[[], Awaitable[Mapping[str, Any]]]
"""Returns the current JWKS document, e.g. ``lambda: jwks_document(signer, *verifiers)``."""


def attach_jwks_route(
    router: APIRouter,
    jwks_provider: JwksProvider,
    *,
    path: str = "/.well-known/jwks.json",
    cache_max_age: int = 300,
) -> APIRouter:
    """Attach a public JWKS endpoint built from an async *jwks_provider*.

    The provider is supplied by the app so this route stays decoupled from the
    identity plane — typically ``lambda: jwks_document(access_svc.signer,
    *access_svc.additional_verifiers)``. During key rotation, include both the new
    and previous asymmetric signers so verifiers can validate tokens from either.
    Symmetric (HS256) signers contribute no keys. Excluded from the OpenAPI schema.
    """

    @router.get(path, include_in_schema=False)
    async def jwks() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        document = await jwks_provider()

        return JSONResponse(
            dict(document),
            headers={"Cache-Control": f"public, max-age={cache_max_age}"},
        )

    return router
