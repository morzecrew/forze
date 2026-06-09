"""Batteries-included convenience for standing up an MCP server from a registry.

This is the optional "build it all" entrypoint — for full control (auth, transport, custom
tools) construct your own :class:`FastMCP` and call
:func:`~forze_mcp.registration.register_tools` instead.
"""

from fastmcp import FastMCP

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry

from .identity import MCPIdentityResolver
from .registration import register_tools

# ----------------------- #


def build_mcp_server(
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    *,
    name: str,
    identity: MCPIdentityResolver | None = None,
    include_writes: bool = False,
) -> FastMCP:
    """Build a FastMCP server with the registry's exposed operations registered as tools.

    A thin convenience over :func:`~forze_mcp.registration.register_tools`. Run it with
    FastMCP's own transports (e.g. ``server.run()`` / ``server.run_stdio_async()`` /
    ``server.streamable_http_app()``).
    """

    server: FastMCP = FastMCP(name)

    register_tools(
        server,
        registry,
        ctx_factory,
        identity=identity,
        include_writes=include_writes,
    )

    return server
