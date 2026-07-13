"""Model Context Protocol (MCP) integration for Forze.

A toolkit for exposing operations from a frozen operation registry as MCP tools. Bring your
own :class:`~mcp.server.fastmcp.FastMCP` server (with your auth, transport, and any
hand-written tools) and call :func:`register_tools` to add Forze operations as tools —
each call runs through the same governed pipeline as any other entrypoint. The adapter
holds no business logic and enforces no authorization; governance stays in the engine,
upstream of this boundary. :func:`register_dsl_query_prompts` additionally attaches
framework-level MCP prompts that teach an LLM the Forze querying DSL (filters, sorts,
pagination, aggregates), :func:`register_schema_resources` publishes per-aggregate field
schemas (read-model fields + which are filterable/sortable) as MCP resources, and
:func:`register_resource_templates` exposes get-by-id operations as MCP resource templates
(``notes://{id}``) — together they let the model discover, query, and fetch domain data.

``build_mcp_server`` is an optional batteries-included convenience that constructs a server
for you. By default only ``QUERY`` operations are exposed and a configurable static identity
is bound; pass ``include_writes=True`` to expose command operations (tagged with destructive
hints) and supply a :class:`DelegatedIdentityResolver` to run calls on behalf of a user with
the agent attached as actor (least-privilege intersection enforced by the engine).

To protect the server with API-key auth backed by the same forze_identity brain as the HTTP
edge, pass :class:`ForzeApiKeyVerifier` as ``auth`` (FastMCP validates the bearer and rejects
unauthenticated calls) and :class:`AccessTokenIdentityResolver` as the identity resolver (it
binds the verified principal, attaching a fixed agent service principal as the delegation
actor). No OAuth flow — the bearer is a forze API key the caller already holds.
"""

from ._compat import require_mcp

require_mcp()

# ....................... #

from .auth import (
    AccessTokenIdentityResolver,
    ForzeApiKeyVerifier,
)
from .identity import (
    DelegatedIdentityResolver,
    MCPIdentityResolver,
    StaticIdentityResolver,
)
from .lifespan import runtime_lifespan
from .middlewares import LoggingMiddleware
from .projection import exposed_operations
from .prompts import register_dsl_query_prompts
from .registration import register_tools
from .resource_templates import (
    ResourceTemplateSpec,
    register_resource_templates,
)
from .schemas import register_schema_resources
from .server import build_mcp_server

# ----------------------- #

__all__ = [
    "AccessTokenIdentityResolver",
    "DelegatedIdentityResolver",
    "ForzeApiKeyVerifier",
    "LoggingMiddleware",
    "MCPIdentityResolver",
    "ResourceTemplateSpec",
    "StaticIdentityResolver",
    "build_mcp_server",
    "exposed_operations",
    "runtime_lifespan",
    "register_dsl_query_prompts",
    "register_resource_templates",
    "register_schema_resources",
    "register_tools",
]
