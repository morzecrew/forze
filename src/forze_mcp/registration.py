"""Register Forze operations as tools on a user-owned FastMCP server.

This is the toolkit entrypoint: bring your own :class:`FastMCP` (with your auth, transport,
and any hand-written tools) and call :func:`register_operations` to add the operations from
a frozen registry as additional tools. Each tool's arguments are the operation's input-DTO
fields at top level (a flat signature is synthesized so MCP clients see a natural tool
contract); the result is whatever the operation returns, serialized by FastMCP.
"""

import inspect
from typing import Any, Awaitable, Callable

from fastmcp import FastMCP
from fastmcp.tools import Tool
from mcp.types import ToolAnnotations

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
)
from forze.base.primitives import StrKey

from .dispatch import invoke_operation
from .identity import MCPIdentityResolver, StaticIdentityResolver
from .projection import exposed_operations

# ----------------------- #


def _flat_tool_handler(
    *,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    identity: MCPIdentityResolver,
    op: StrKey,
    descriptor: OperationDescriptor | None,
) -> Callable[..., Awaitable[Any]]:
    """Build a tool callable whose signature is the operation's input-DTO fields.

    FastMCP derives the tool's ``inputSchema`` from the callable's signature, so a flat
    signature (one parameter per DTO field) yields top-level arguments rather than a single
    nested object. The body re-validates against the real DTO (applying its own validators)
    before dispatching.
    """

    input_type = descriptor.input_type if descriptor is not None else None
    output_type = descriptor.output_type if descriptor is not None else None

    async def _handler(**kwargs: Any) -> Any:
        return await invoke_operation(
            registry=registry,
            ctx_factory=ctx_factory,
            identity=identity,
            op=op,
            descriptor=descriptor,
            arguments=kwargs,
        )

    params: list[inspect.Parameter] = []
    annotations: dict[str, Any] = {}

    if input_type is not None:
        for field_name, field in input_type.model_fields.items():
            default = (
                inspect.Parameter.empty
                if field.is_required()
                else field.get_default(call_default_factory=True)
            )
            params.append(
                inspect.Parameter(
                    field_name,
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=field.annotation,
                    default=default,
                )
            )
            annotations[field_name] = field.annotation

    return_annotation: Any = (
        output_type if output_type is not None else inspect.Signature.empty
    )

    _handler.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        params, return_annotation=return_annotation
    )

    if output_type is not None:
        annotations["return"] = output_type

    _handler.__annotations__ = annotations

    return _handler


# ....................... #


def register_operations(
    server: FastMCP,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    *,
    identity: MCPIdentityResolver | None = None,
    include_writes: bool = False,
) -> list[str]:
    """Add the registry's exposed operations to *server* as MCP tools.

    :param server: A FastMCP server the caller owns (and configures with auth/transport).
    :param registry: The frozen operation registry to project.
    :param ctx_factory: Factory yielding a fresh execution context per tool call.
    :param identity: Resolver for the principal/tenant bound per call (defaults to a
        no-identity :class:`StaticIdentityResolver`).
    :param include_writes: When ``False`` (default, read-only) only ``QUERY`` operations are
        exposed; when ``True`` command operations are exposed too.
    :returns: The list of registered tool names.
    """

    catalog = registry.catalog()
    exposed = exposed_operations(catalog, include_writes=include_writes)
    resolver = identity or StaticIdentityResolver()

    for tool_name, op in exposed.items():
        entry = catalog[op]
        descriptor = entry.descriptor

        server.add_tool(
            Tool.from_function(
                _flat_tool_handler(
                    registry=registry,
                    ctx_factory=ctx_factory,
                    identity=resolver,
                    op=op,
                    descriptor=descriptor,
                ),
                name=tool_name,
                description=descriptor.description if descriptor is not None else None,
                annotations=ToolAnnotations(
                    readOnlyHint=entry.is_read_only,
                    destructiveHint=not entry.is_read_only,
                ),
            )
        )

    return list(exposed)
