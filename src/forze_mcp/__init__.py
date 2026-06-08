"""Model Context Protocol (MCP) integration for Forze.

A toolkit for exposing operations from a frozen operation registry as MCP tools. Bring your
own :class:`~mcp.server.fastmcp.FastMCP` server (with your auth, transport, and any
hand-written tools) and call :func:`register_operations` to add Forze operations as tools —
each call runs through the same governed pipeline as any other entrypoint. The adapter
holds no business logic and enforces no authorization; governance stays in the engine,
upstream of this boundary.

``build_mcp_server`` is an optional batteries-included convenience that constructs a server
for you. The read-only MVP exposes only ``QUERY`` operations and binds a configurable static
identity; write exposure and token-derived delegated identity are follow-up phases.
"""

from ._compat import require_mcp

require_mcp()

# ....................... #

from .identity import MCPIdentityResolver, StaticIdentityResolver  # noqa: E402
from .projection import exposed_operations  # noqa: E402
from .registration import register_operations  # noqa: E402
from .server import build_mcp_server  # noqa: E402

# ----------------------- #

__all__ = [
    "MCPIdentityResolver",
    "StaticIdentityResolver",
    "build_mcp_server",
    "exposed_operations",
    "register_operations",
]
