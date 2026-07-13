"""Register Forze operations as tools on a user-owned FastMCP server.

This is the toolkit entrypoint: bring your own :class:`FastMCP` (with your auth, transport,
and any hand-written tools) and call :func:`register_tools` to add the operations from
a frozen registry as additional tools. Each tool's arguments are the operation's input-DTO
fields at top level (a flat signature is synthesized so MCP clients see a natural tool
contract); the result is whatever the operation returns, serialized by FastMCP.
"""

import inspect
import warnings
from typing import Any, Awaitable, Callable, Final

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic.json_schema import PydanticJsonSchemaWarning

from forze.application.contracts.querying import QUANTIFIER_OPS, QueryDiscovery
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
    OperationDescriptor,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

# ----------------------- #

_UNSET: Final[Any] = object()
"""Sentinel signalling a tool argument the client omitted (see :func:`_flat_tool_handler`)."""

from ._errors import client_safe_error
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
    before dispatching, and translates a boundary :class:`CoreException` into a client-safe
    :class:`ToolError` (the same egress-masked envelope the HTTP edge renders) so internal
    error details never leak to an agent while a caller-caused message still gets through.
    """

    input_type = descriptor.input_type if descriptor is not None else None
    output_type = descriptor.output_type if descriptor is not None else None

    async def _handler(**kwargs: Any) -> Any:
        # Drop arguments the client omitted (FastMCP fills them with the signature default): a
        # field with a ``default_factory`` must run that factory *per call* inside the DTO, not
        # reuse a value frozen when the tool was registered (e.g. a stale ``uuid`` / timestamp).
        arguments = {name: value for name, value in kwargs.items() if value is not _UNSET}

        try:
            return await invoke_operation(
                registry=registry,
                ctx_factory=ctx_factory,
                identity=identity,
                op=op,
                descriptor=descriptor,
                arguments=arguments,
            )
        except CoreException as e:
            # Shared egress-masked translation (see :mod:`forze_mcp._errors`): a caller-caused
            # kind keeps its message + code, an internal/server error is masked to a generic
            # detail and logged server-side before the ToolError reaches the agent.
            raise client_safe_error(e, ToolError) from e

    params: list[inspect.Parameter] = []
    annotations: dict[str, Any] = {}

    if input_type is not None:
        for field_name, field in input_type.model_fields.items():
            if field.is_required():
                default: Any = inspect.Parameter.empty
            elif field.default_factory is not None:
                # Don't freeze the factory value into the signature (FastMCP would forward that
                # stale value on every omitted call) — mark it optional with a sentinel that the
                # handler strips so the DTO re-runs the factory.
                default = _UNSET
            else:
                default = field.get_default(call_default_factory=False)

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


def _tool_description(entry: OperationCatalogEntry) -> str | None:
    """Tool description: descriptor text plus catalog-derived suffix sentences.

    Write operations whose plan carries an idempotency wrap advertise key-based
    retry replay (the key is bound by the invoking boundary; without one the wrap
    is a no-op). Operations whose plan requires a bound principal advertise that
    too. Declared permissions are appended as well — declared-hook
    introspection only, **not** a complete security statement: an operation may
    enforce authorization inside its handler invisibly. A plan-declared deadline
    documents the call's time budget so agents can set client timeouts and avoid
    retrying a call that failed by running out of budget.
    """

    parts: list[str] = []

    if entry.descriptor is not None and entry.descriptor.description:
        parts.append(entry.descriptor.description)

    # NB: idempotency is deliberately NOT advertised here. The MCP boundary binds no idempotency
    # key (there is no per-call key channel, unlike the HTTP ``Idempotency-Key`` header), so the
    # operation's idempotency wrap is a no-op — telling an agent a retry is safe when a duplicate
    # call would re-execute the write would actively invite duplicate writes.

    if entry.requires_authn:
        parts.append(
            "Requires authentication: a verified principal must be bound for this "
            "call (declared by the operation's plan; it may enforce more internally)."
        )

    if entry.required_permissions:
        keys = ", ".join(entry.required_permissions)
        parts.append(
            f"Requires permissions: {keys} (declared by attached authorization "
            "hooks; the operation may enforce additional checks internally)."
        )

    if entry.deadline is not None:
        budget = f"{entry.deadline.total_seconds():g}"
        parts.append(
            f"Calls are bounded by a {budget}s time budget; exceeding it fails "
            "with a non-retryable timeout (deadline_exceeded)."
        )

    if entry.descriptor is not None and entry.descriptor.query_discovery is not None:
        parts.append(_query_discovery_sentence(entry.descriptor.query_discovery))

    return " ".join(parts) if parts else None


# ....................... #


def _query_discovery_sentence(discovery: QueryDiscovery) -> str:
    """One-line filter contract for an LLM: filterable fields + operators, sort, aggregate.

    Spells out which operator each field accepts (so the agent doesn't guess ``$like`` on
    a number) and which array fields take element quantifiers — the type-derived upper
    bound, independent of the serving backend.
    """

    parts: list[str] = []

    field_bits: list[str] = []

    for field in discovery.filterable:
        ops = ", ".join(field.operators)

        if field.quantifiable:
            ops += f"; element quantifiers {', '.join(QUANTIFIER_OPS)}"

        field_bits.append(f"{field.field} ({field.type}: {ops})")

    if field_bits:
        parts.append("Filterable fields — " + "; ".join(field_bits) + ".")

    if discovery.sortable:
        parts.append("Sortable by: " + ", ".join(discovery.sortable) + ".")

    if discovery.aggregatable:
        parts.append("Aggregatable by: " + ", ".join(discovery.aggregatable) + ".")

    return " ".join(parts)


# ....................... #


def register_tools(
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
    :param ctx_factory: Yields the execution context for a tool call. Use the scope's
        shared context (e.g. ``runtime.get_context`` under
        :func:`~forze_mcp.lifespan.runtime_lifespan`) so resolved operations/ports stay
        warm across calls; constructing a fresh context per call is unsupported.
    :param identity: Resolver for the principal/tenant bound per call (defaults to a
        no-identity :class:`StaticIdentityResolver`).
    :param include_writes: When ``False`` (default, read-only) only ``QUERY`` operations are
        exposed; when ``True`` command operations are exposed too.
    :returns: The list of registered tool names.
    :raises CoreException: When an exposed operation projects a sensitive read model
        (its spec is marked ``sensitive=True``).
    """

    catalog = registry.catalog()
    exposed = exposed_operations(catalog, include_writes=include_writes)
    resolver = identity or StaticIdentityResolver()

    # Refuse sensitive operations up front (before any tool is added) so a
    # credential-bearing read model can never leak through a generated tool.
    for op in exposed.values():
        descriptor = catalog[op].descriptor

        if descriptor is not None and descriptor.sensitive:
            raise exc.configuration(
                f"Refusing to register MCP tools: operation '{op}' projects a "
                "sensitive read model (its spec is marked sensitive=True; "
                "credential/secret material must not be exposed on generated "
                "external surfaces)"
            )

    for tool_name, op in exposed.items():
        entry = catalog[op]
        descriptor = entry.descriptor

        with warnings.catch_warnings():
            # A ``default_factory`` field carries no fixed default (it is stripped to the ``_UNSET``
            # sentinel), so pydantic warns it can't serialize that default into the JSON schema —
            # which is correct and intended (the field stays optional, just without a frozen default).
            warnings.simplefilter("ignore", PydanticJsonSchemaWarning)
            tool = FunctionTool.from_function(
                _flat_tool_handler(
                    registry=registry,
                    ctx_factory=ctx_factory,
                    identity=resolver,
                    op=op,
                    descriptor=descriptor,
                ),
                name=tool_name,
                description=_tool_description(entry),
                annotations=ToolAnnotations(
                    readOnlyHint=entry.is_read_only,
                    destructiveHint=not entry.is_read_only,
                ),
            )

        server.add_tool(tool)

    return list(exposed)
