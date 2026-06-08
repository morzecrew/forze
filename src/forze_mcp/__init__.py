"""Model Context Protocol (MCP) integration for Forze.

A toolkit for exposing operations from a frozen operation registry as MCP tools. Bring your
own :class:`~mcp.server.fastmcp.FastMCP` server (with your auth, transport, and any
hand-written tools) and call :func:`register_tools` to add Forze operations as tools —
each call runs through the same governed pipeline as any other entrypoint. The adapter
holds no business logic and enforces no authorization; governance stays in the engine,
upstream of this boundary. :func:`register_dsl_query_prompts` additionally attaches
framework-level MCP prompts that teach an LLM the Forze querying DSL (filters, sorts,
pagination, aggregates), and :func:`register_schema_resources` publishes per-aggregate field
schemas (read-model fields + which are filterable/sortable) as MCP resources — together they
let the model drive the ``list``/``search`` tools correctly.

``build_mcp_server`` is an optional batteries-included convenience that constructs a server
for you. By default only ``QUERY`` operations are exposed and a configurable static identity
is bound; pass ``include_writes=True`` to expose command operations (tagged with destructive
hints) and supply a :class:`DelegatedIdentityResolver` to run calls on behalf of a user with
the agent attached as actor (least-privilege intersection enforced by the engine).
"""

from ._compat import require_mcp

require_mcp()

# ....................... #

from .identity import (  # noqa: E402
    DelegatedIdentityResolver,
    MCPIdentityResolver,
    StaticIdentityResolver,
)
from .middlewares import LoggingMiddleware  # noqa: E402
from .projection import exposed_operations  # noqa: E402
from .prompts import register_dsl_query_prompts  # noqa: E402
from .registration import register_tools  # noqa: E402
from .schemas import register_schema_resources  # noqa: E402
from .server import build_mcp_server  # noqa: E402

# ----------------------- #

__all__ = [
    "DelegatedIdentityResolver",
    "LoggingMiddleware",
    "MCPIdentityResolver",
    "StaticIdentityResolver",
    "build_mcp_server",
    "exposed_operations",
    "register_dsl_query_prompts",
    "register_schema_resources",
    "register_tools",
]
