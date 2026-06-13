"""Run an :class:`ExecutionRuntime` for the lifetime of a FastMCP server."""

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncGenerator, Callable

from fastmcp import FastMCP

from forze.application.execution import ExecutionRuntime

# ----------------------- #


def runtime_lifespan(
    runtime: ExecutionRuntime,
) -> Callable[[FastMCP], AbstractAsyncContextManager[None]]:
    """Return a lifespan callable that holds ``runtime.scope()`` open for the server.

    The MCP counterpart to ``forze_fastapi.runtime_lifespan``. Pass the result to
    ``FastMCP(lifespan=...)`` (or :func:`~forze_mcp.server.build_mcp_server`'s
    ``lifespan=`` parameter) and wire ``ctx_factory=runtime.get_context`` so **every
    tool call reuses the one scope context** — with its warm per-scope
    operation/port caches — instead of constructing a fresh
    :class:`~forze.application.execution.context.ExecutionContext` per call (an
    unsupported mode that rebuilds the operation plan every call and accumulates
    per-instance ``ContextVar`` state). On startup the runtime creates its context and
    runs lifecycle startup; on shutdown lifecycle shutdown runs and the context is
    reset, even when the server's lifetime ends with an error::

        runtime = build_runtime(...)
        server = build_mcp_server(
            registry,
            runtime.get_context,
            name="my-server",
            lifespan=runtime_lifespan(runtime),
        )

    :param runtime: Runtime to run for the server's lifetime.
    :returns: Lifespan callable for ``FastMCP(lifespan=...)``.
    """

    @asynccontextmanager
    async def lifespan(_server: FastMCP) -> AsyncGenerator[None]:
        async with runtime.scope():
            yield

    return lifespan
