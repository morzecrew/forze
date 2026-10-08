"""Register Forze operations as tools on a user-owned FastMCP server.

This is the toolkit entrypoint: bring your own :class:`FastMCP` (with your auth, transport,
and any hand-written tools) and call :func:`register_tools` to add the operations from
a frozen registry as additional tools. Each tool's arguments are the operation's input-DTO
fields at top level (a flat signature is synthesized so MCP clients see a natural tool
contract); the result is whatever the operation returns, serialized by FastMCP.
"""

import functools
import inspect
import json
import warnings
import weakref
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Final

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.resources import Resource
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import TypeAdapter
from pydantic.json_schema import PydanticJsonSchemaWarning

from forze.application.contracts.querying import QueryFilterExpression, describe_query_discovery
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

FILTER_GRAMMAR_URI: Final = "forze://filter-grammar"
"""Where :func:`register_tools` publishes the filter grammar when it shares it."""

_GRAMMAR_PUBLISHED: Final[weakref.WeakSet[FastMCP]] = weakref.WeakSet()
"""The servers :func:`register_tools` has published the filter grammar resource on."""

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

    FastMCP derives the tool's ``input_schema`` from the callable's signature, so a flat
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

    return_annotation: Any = output_type if output_type is not None else inspect.Signature.empty

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
        # A policy that withholds every field still attaches a discovery, and its sentence
        # is empty — appending that would leave a trailing space on the description, or
        # turn an operation with nothing else to say into an empty string rather than no
        # description at all.
        sentence = describe_query_discovery(entry.descriptor.query_discovery)

        if sentence:
            parts.append(sentence)

    return " ".join(parts) if parts else None


# ....................... #


@functools.cache
def _filter_grammar() -> str:
    """The filter expression's JSON schema, as compact JSON: the grammar every
    filter-accepting tool takes."""

    return json.dumps(TypeAdapter(QueryFilterExpression).json_schema(), separators=(",", ":"))


def _refs(node: Any) -> set[str]:
    """The ``$defs`` entries *node* references by ``$ref``, by name."""

    if isinstance(node, list):
        return set().union(*(_refs(item) for item in node))  # pyright: ignore[reportUnknownVariableType]

    if not isinstance(node, dict):
        return set()

    found: set[str] = set().union(*(_refs(value) for value in node.values()))  # pyright: ignore[reportUnknownVariableType]
    ref = node.get("$ref")  # pyright: ignore[reportUnknownVariableType]

    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        found.add(ref.removeprefix("#/$defs/"))

    return found


def _without_filter_grammar(schema: dict[str, Any]) -> dict[str, Any]:
    """*schema* with each filter expression a plain object pointing at the shared grammar,
    and the definitions nothing references any more dropped.

    A filter expression is the ``anyOf`` of the grammar's root definitions, wherever it sits:
    a tool's ``filters``, or ``$having`` inside an aggregate. A schema that names the
    grammar's definitions differently (pydantic qualifies a name another model also uses) is
    left as it is, whole.
    """

    roots = {option["$ref"] for option in json.loads(_filter_grammar())["anyOf"]}
    pointer = f"Its grammar is in the server instructions and at {FILTER_GRAMMAR_URI}."

    def strip(node: Any) -> Any:
        if isinstance(node, list):
            return [strip(item) for item in node]  # pyright: ignore[reportUnknownVariableType]

        if not isinstance(node, dict):
            return node

        out: dict[str, Any] = {key: strip(value) for key, value in node.items()}  # pyright: ignore[reportUnknownVariableType]
        options = out.get("anyOf")

        if isinstance(options, list):
            refs = {option.get("$ref") for option in options if isinstance(option, dict)}  # pyright: ignore[reportUnknownVariableType]

            if roots <= refs:
                rest = [option for option in options if option.get("$ref") not in roots]  # pyright: ignore[reportUnknownVariableType]
                del out["anyOf"]
                plain = {"type": "object", "description": f"A filter expression. {pointer}"}

                if rest:
                    return {**out, "anyOf": [plain, *rest]}

                own = out.get("description")

                return {**out, **plain, **({"description": f"{own} {pointer}"} if own else {})}

        return out

    stripped = strip(schema)
    defs: dict[str, Any] = stripped.pop("$defs", {})
    kept: dict[str, Any] = {}
    pending = _refs(stripped)

    while pending:
        name = pending.pop()

        if name in defs and name not in kept:
            kept[name] = defs[name]
            pending |= _refs(defs[name])

    return {**stripped, "$defs": kept} if kept else stripped


def _share_filter_grammar(server: FastMCP) -> None:
    """State the filter grammar once on *server*: after its instructions, and as a resource."""

    grammar = _filter_grammar()

    # Published once per server, tracked here, as FastMCP has no synchronous look-up. A
    # resource the caller put at the same URI meets the server's own ``on_duplicate``, so it
    # is never removed behind their back; a refusal comes before the instructions change.
    if server not in _GRAMMAR_PUBLISHED:
        server.add_resource(
            Resource.from_function(
                lambda: grammar,
                uri=FILTER_GRAMMAR_URI,
                name="Filter grammar",
                description=(
                    "The JSON schema of a filter expression, as list and search tools take it."
                ),
                mime_type="application/json",
            )
        )
        _GRAMMAR_PUBLISHED.add(server)

    if server.instructions and grammar in server.instructions:
        return

    note = (
        "Filter expressions (the `filters` argument of list and search tools, and `$having` "
        f"in aggregates) follow this JSON schema, also at {FILTER_GRAMMAR_URI}:\n{grammar}"
    )
    server.instructions = f"{server.instructions}\n\n{note}" if server.instructions else note


# ....................... #


def register_tools(
    server: FastMCP,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    *,
    identity: MCPIdentityResolver | None = None,
    include_writes: bool = False,
    operations: Iterable[StrKey] | None = None,
    output_schemas: bool = True,
    shared_filter_grammar: bool = False,
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
    :param operations: Expose only these operations, by key (default: every exposed one).
        An unknown key, or a command operation without *include_writes*, is refused.
    :param output_schemas: Give each tool its output schema (the default). ``False`` leaves
        them out, for a smaller tool list. A call still returns its result as text, but a
        list or scalar result then carries no structured content, and an object comes back
        as a plain mapping rather than a typed model.
    :param shared_filter_grammar: State the filter grammar once instead of in every
        filter-accepting tool: each ``filters`` (and aggregate ``$having``) becomes a plain
        object in the tool's schema, and the grammar is appended to the server's
        instructions and published at :data:`FILTER_GRAMMAR_URI`. Set the instructions
        before registering: assigning them afterwards replaces the grammar (the tools still
        point at the resource). A call is validated against the full grammar either way.
    :returns: The list of registered tool names.
    :raises CoreException: When an exposed operation projects a sensitive read model
        (its spec is marked ``sensitive=True``), or *operations* names an operation it
        cannot expose.
    """

    catalog = registry.catalog()
    exposed = exposed_operations(catalog, include_writes=include_writes, operations=operations)
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

    tools: list[FunctionTool] = []
    shared = False

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
                    read_only_hint=entry.is_read_only,
                    destructive_hint=not entry.is_read_only,
                ),
                **({} if output_schemas else {"output_schema": None}),
            )

        if shared_filter_grammar:
            parameters = _without_filter_grammar(tool.parameters)

            if parameters != tool.parameters:
                tool = tool.model_copy(update={"parameters": parameters})
                shared = True

        tools.append(tool)

    # Before any tool, so a tool never reaches the server pointing at a grammar that isn't.
    if shared:
        _share_filter_grammar(server)

    for tool in tools:
        server.add_tool(tool)

    return list(exposed)
