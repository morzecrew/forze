"""Readiness route bound to the runtime's drain state."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from forze.application.execution import ExecutionRuntime

# ----------------------- #


def attach_readiness_route(
    router: APIRouter,
    runtime: ExecutionRuntime,
    *,
    path: str = "/readyz",
) -> APIRouter:
    """Attach a readiness probe reflecting the runtime's scope state.

    ``200`` while a scope is active and not draining; ``503`` otherwise —
    ``draining`` once shutdown flipped the drain gate (point your load
    balancer's readiness check here so routing stops before the drain window),
    ``unavailable`` before the scope exists. Excluded from the OpenAPI schema.
    """

    @router.get(path, include_in_schema=False)
    async def readyz() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        if runtime.ready:
            return JSONResponse({"status": "ready"})

        status = "draining" if runtime.draining else "unavailable"

        return JSONResponse({"status": status}, status_code=503)

    return router
