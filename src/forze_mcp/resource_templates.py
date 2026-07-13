"""Expose point-read operations as MCP resource templates.

A resource *template* (``notes://{id}``) advertises a family of read-only resources without
enumerating them — the MCP-native shape for "fetch one object by id" when the id space is
open. :func:`register_resource_templates` maps a get-by-id operation onto a URI template;
reading a concrete URI (``notes://<uuid>``) dispatches that operation through the **same
governed pipeline** as a tool call (:func:`~forze_mcp.dispatch.invoke_operation` — identity
binding, read-only enforcement, audit), so a resource read is just a governed point read.

``list`` / ``search`` stay tools: their filter/sort/pagination arguments don't map onto a URI.
``forze_mcp`` is generic over the registry, so the get-by-id *operation key* is supplied
explicitly (it does not assume any aggregate-kit naming convention).
"""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

import attrs
from fastmcp import FastMCP
from fastmcp.exceptions import ResourceError
from fastmcp.resources import ResourceTemplate
from pydantic import BaseModel

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

from ._errors import client_safe_error
from .dispatch import invoke_operation
from .identity import MCPIdentityResolver, StaticIdentityResolver

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResourceTemplateSpec:
    """Maps a get-by-id operation onto a ``{scheme}://{id_param}`` resource template."""

    op: StrKey
    """Operation key of the read-only point read (e.g. an aggregate's ``get``)."""

    scheme: str
    """URI scheme for the template, e.g. ``"notes"`` → ``notes://{id}``."""

    id_param: str = "id"
    """URI placeholder name; must be a field of the operation's input DTO."""


# ....................... #


def _template_handler(
    *,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    identity: MCPIdentityResolver,
    op: StrKey,
    descriptor: Any,
    id_param: str,
) -> Callable[..., Awaitable[Any]]:
    """Build a single-parameter resource reader that dispatches the point-read operation.

    FastMCP binds the URI placeholder to the handler parameter by name, so the synthesized
    signature carries exactly one parameter named ``id_param``. The operation result (a
    Pydantic read model) is returned as a JSON-serializable mapping. A boundary
    :class:`CoreException` is translated to a client-safe :class:`ResourceError` (the same
    egress-masked envelope tool calls raise as ``ToolError``) so internal error details
    never leak to an agent while a caller-caused message still gets through.
    """

    async def _handler(**kwargs: Any) -> Any:
        try:
            result = await invoke_operation(
                registry=registry,
                ctx_factory=ctx_factory,
                identity=identity,
                op=op,
                descriptor=descriptor,
                arguments=kwargs,
            )
        except CoreException as e:
            # Shared egress-masked translation (see :mod:`forze_mcp._errors`): a caller-caused
            # kind keeps its message + code, an internal/server error is masked to a generic
            # detail and logged server-side before the ResourceError reaches the agent.
            raise client_safe_error(e, ResourceError) from e

        if isinstance(result, BaseModel):
            return result.model_dump(mode="json")

        return result

    _handler.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter(
                id_param,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=str,
            )
        ],
        return_annotation=dict,
    )
    _handler.__annotations__ = {id_param: str, "return": dict}

    return _handler


# ....................... #


def register_resource_templates(
    server: FastMCP,
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
    templates: list[ResourceTemplateSpec],
    *,
    identity: MCPIdentityResolver | None = None,
) -> list[str]:
    """Add point-read operations to *server* as MCP resource templates.

    :param server: A FastMCP server the caller owns.
    :param registry: The frozen operation registry to dispatch through.
    :param ctx_factory: Yields the execution context for a read. Use the scope's shared
        context (e.g. ``runtime.get_context`` under
        :func:`~forze_mcp.lifespan.runtime_lifespan`); constructing a fresh context per
        read is unsupported.
    :param templates: One :class:`ResourceTemplateSpec` per get-by-id operation to expose.
    :param identity: Resolver for the principal/tenant bound per read (defaults to a
        no-identity :class:`StaticIdentityResolver`).
    :returns: The list of registered URI templates.
    :raises CoreException: When an operation is missing, not read-only, projects a
        sensitive read model, has no descriptor with an input type, or ``id_param``
        is not a field of that input DTO.
    """

    catalog = registry.catalog()
    resolver = identity or StaticIdentityResolver()
    uris: list[str] = []

    for template in templates:
        entry = catalog.get(template.op)

        if entry is None:
            raise exc.configuration(
                f"Resource template operation {template.op!r} is not in the registry.",
            )

        if not entry.is_read_only:
            raise exc.configuration(
                f"Resource template operation {template.op!r} must be read-only "
                "(resources must not mutate state).",
            )

        descriptor = entry.descriptor

        if descriptor is not None and descriptor.sensitive:
            raise exc.configuration(
                f"Refusing to register resource template for operation "
                f"{template.op!r}: it projects a sensitive read model (its spec "
                "is marked sensitive=True; credential/secret material must not "
                "be exposed on generated external surfaces).",
            )

        if descriptor is None or descriptor.input_type is None:
            raise exc.configuration(
                f"Resource template operation {template.op!r} needs a descriptor with an "
                "input type to bind the URI id.",
            )

        if template.id_param not in descriptor.input_type.model_fields:
            raise exc.configuration(
                f"id_param {template.id_param!r} is not a field of the input DTO for "
                f"operation {template.op!r}.",
            )

        uri = f"{template.scheme}://{{{template.id_param}}}"

        server.add_template(
            ResourceTemplate.from_function(
                _template_handler(
                    registry=registry,
                    ctx_factory=ctx_factory,
                    identity=resolver,
                    op=template.op,
                    descriptor=descriptor,
                    id_param=template.id_param,
                ),
                uri_template=uri,
                name=f"{template.scheme} by {template.id_param}",
                description=(
                    descriptor.description
                    or f"Read one {template.scheme!r} resource by {template.id_param}."
                ),
                mime_type="application/json",
            )
        )
        uris.append(uri)

    return uris
