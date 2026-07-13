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

from typing import Any, Mapping

from forze.application.execution.context import (
    ExecutionContextFactory,
    InvocationMetadata,
)
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
    run_operation,
)
from forze.base.primitives import StrKey, uuid7

from .identity import MCPIdentityResolver

# ----------------------- #


def build_args(
    descriptor: OperationDescriptor | None,
    arguments: Mapping[str, Any],
) -> Any:
    """Validate raw tool arguments into the operation's input DTO (``None`` if no DTO)."""

    if descriptor is None or descriptor.input_type is None:
        return None

    return descriptor.input_type.model_validate(dict(arguments))


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
