"""Run an MCP tool call as a governed operation invocation.

The flow mirrors the FastAPI middleware boundary: validate the input DTO, establish a fresh
execution context, bind invocation metadata + identity, then run the operation through the
normal pipeline (so authz / tenancy / read-only enforcement / audit all apply). The adapter
enforces nothing itself — it only binds the boundary context and dispatches. Result serialization
is left to the host MCP server (FastMCP); a boundary ``CoreException`` is translated to a
client-safe ``ToolError`` / ``ResourceError`` at the registering surface (see
:mod:`forze_mcp._errors`), and any other exception is masked by the server's
``mask_error_details``.
"""

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from forze.application.execution.context import (
    ExecutionContextFactory,
    InvocationMetadata,
)
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
    run_operation,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, uuid7
from forze.base.scrubbing import sanitize_pydantic_errors

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
    """

    if descriptor is None or descriptor.input_type is None:
        return None

    try:
        return descriptor.input_type.model_validate(dict(arguments))

    except ValidationError as error:
        raise exc.validation(
            "Invalid tool arguments",
            code="mcp_invalid_arguments",
            details={"errors": sanitize_pydantic_errors(error.errors())},
        ) from error


# ....................... #


async def invoke_operation(
    *,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    identity: MCPIdentityResolver,
    op: StrKey,
    descriptor: OperationDescriptor | None,
    arguments: Mapping[str, Any],
) -> Any:
    """Establish a boundary context and run *op* through the operation pipeline."""

    args = build_args(descriptor, arguments)

    ctx = ctx_factory()
    authn, tenant = await identity.resolve()
    metadata = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
        causation_id=None,
    )

    with ctx.inv_ctx.bind(metadata=metadata, authn=authn, tenant=tenant):
        return await run_operation(registry, op, args, ctx)
