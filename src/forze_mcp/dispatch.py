"""Run an MCP tool call as a governed operation invocation.

The flow mirrors the FastAPI middleware boundary: validate the input DTO, establish a fresh
execution context, bind invocation metadata + identity, then run the operation through the
normal pipeline (so authz / tenancy / read-only enforcement / audit all apply). The adapter
enforces nothing itself — it only binds the boundary context and dispatches, through the
ingress shared by every generated surface (:mod:`forze.application.execution.ingress`).
Result serialization is left to the host MCP server (FastMCP); a boundary ``CoreException``
is translated to a client-safe ``ToolError`` / ``ResourceError`` at the registering surface
(see :mod:`forze_mcp._errors`), and any other exception is masked by the server's
``mask_error_details``.
"""

from collections.abc import Mapping
from typing import Any

from forze.application.contracts.querying import QueryFieldGuard
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.ingress import (
    check_query_policy,
    invoke_at_ingress,
    mint_invocation_metadata,
    validate_operation_input,
)
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
)
from forze.base.primitives import StrKey

from .identity import MCPIdentityResolver

# ----------------------- #


def build_args(
    descriptor: OperationDescriptor | None,
    arguments: Mapping[str, Any],
) -> Any:
    """Validate raw tool arguments into the operation's input DTO (``None`` if no DTO).

    A rejection is translated here rather than left to propagate: Pydantic's own error text
    embeds the offending ``input_value``, echoing whatever the caller sent — including the
    secret they mistyped into the wrong field — straight back to the agent. Masking that is
    the *host* server's setting (``mask_error_details``), which this package only controls
    on the server it builds itself; a caller who brings their own FastMCP (the documented
    path for custom auth/transport) gets FastMCP's default, which does not mask.

    So the boundary masks at its own edge, like the realtime WS route: a fixed summary plus
    field-level errors with the raw values stripped. The result is a ``CoreException``, so
    the registering surface's translation renders it through the egress-masked envelope.
    An unknown argument is left to the tool's synthesized signature, which rejects it. That
    signature names each field by its own name, so validation reads fields by name: an
    aliased field would otherwise drop the argument and run on its default.
    """

    return validate_operation_input(
        descriptor,
        arguments,
        code="mcp_invalid_arguments",
        forbid_unknown=False,
        summary="Invalid tool arguments",
        by_name=True,
    )


# ....................... #


async def invoke_operation(
    *,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    identity: MCPIdentityResolver,
    op: StrKey,
    descriptor: OperationDescriptor | None,
    arguments: Mapping[str, Any],
    query_guard: QueryFieldGuard | None = None,
) -> Any:
    """Establish a boundary context and run *op* through the operation pipeline.

    *query_guard* is the tool's narrower query policy, when it was registered with one: a
    call reaching outside it is refused before any context is bound.
    """

    args = build_args(descriptor, arguments)
    check_query_policy(query_guard, args)

    ctx = ctx_factory()
    authn, tenant = await identity.resolve()

    return await invoke_at_ingress(
        registry,
        ctx,
        op,
        args,
        authn=authn,
        tenant=tenant,
        metadata=mint_invocation_metadata(),
    )
