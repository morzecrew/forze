"""Shared core for projecting registry operations onto a FastAPI router.

Per-aggregate attachers (document, search, storage) declare *which* operations
map to *which* HTTP surface via :class:`RouteBinding` tables; this module owns
the mechanics — descriptor-derived schemas, endpoint synthesis, verbatim
``operation_id``, and dispatch through ``run_operation``. A binding carries its
endpoint builder, so attachers with transport-specific shapes (e.g. multipart
upload, binary download) supply their own builders next to the common ones here.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from enum import Enum
from typing import (
    AbstractSet,
    Any,
    Awaitable,
    Callable,
    Literal,
    Mapping,
    final,
)
from uuid import UUID

import attrs
from fastapi import APIRouter
from pydantic import BaseModel, ValidationError

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
    run_operation,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.middlewares.invocation import IDEMPOTENCY_KEY_HEADER

# ----------------------- #

RouteStyle = Literal["rest", "rpc"]
"""Path/verb mapping for generated routes.

Both styles use REST verbs (``GET``/``POST``/``PATCH``/``DELETE``); they differ
only in how a resource is addressed. ``"rest"`` maps operations onto
resource-style paths with the id in the path (``GET /{id}``). ``"rpc"`` exposes
one operation-named path per operation — mirroring the catalog one-to-one — with
the id (and rev) carried as query parameters (``GET /notes.get?id=``); only
genuine bodies (create, filter/list payloads, multipart upload) stay ``POST``.
Each attacher documents its concrete mapping. Attachers whose operations have a
single natural surface (search, where every request is a filter body) take no
style argument.
"""

OperationRunner = Callable[[Any], Awaitable[Any]]
"""Async callable dispatching validated operation args through the pipeline."""

EndpointBuilder = Callable[
    [OperationRunner, type[BaseModel] | None, str],
    Callable[..., Awaitable[Any]],
]
"""Builds a route endpoint from ``(runner, descriptor input type, op key)``."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RouteBinding:
    """HTTP surface for a single operation."""

    method: str
    """HTTP method."""

    path: str
    """Route path relative to the router (empty string targets the prefix root)."""

    build: EndpointBuilder
    """Endpoint builder for this route's input mapping."""

    status_code: int = 200
    """Success status code."""


# ....................... #


def require_input_type(
    input_type: type[BaseModel] | None,
    op: str,
) -> type[BaseModel]:
    """Fail loud when a builder needs the descriptor's input DTO and it is absent."""

    if input_type is None:
        raise exc.configuration(
            f"Operation '{op}' has no descriptor with an input type — "
            "route schemas cannot be derived"
        )

    return input_type


# ....................... #


def _require_satisfiable(
    dto_type: type[BaseModel],
    op: str,
    supplied: AbstractSet[str],
) -> None:
    """Fail at attach time when the route cannot satisfy the DTO's required fields.

    Endpoints that assemble the input DTO from path/query parameters can only
    supply the fields in *supplied*; a DTO with other required fields would fail
    ``model_validate`` on every request. Catching the mismatch here turns a
    request-time 500 into a configuration error at attach time.
    """

    required = {
        name for name, field in dto_type.model_fields.items() if field.is_required()
    }

    if missing := required - set(supplied):
        raise exc.configuration(
            f"Input type '{dto_type.__name__}' of operation '{op}' has required "
            f"fields {sorted(missing)} the route cannot supply "
            f"(only {sorted(supplied)} are available)"
        )


# ....................... #


def validate_payload(
    dto_type: type[BaseModel],
    data: Mapping[str, Any],
    op: str,
) -> BaseModel:
    """Validate *data* against *dto_type*, surfacing failures as 422.

    Endpoints that assemble the input DTO manually (path/query/multipart shapes)
    bypass FastAPI's request validation, so a raw pydantic ``ValidationError``
    would escape as an unhandled 500. Re-raising it as a validation
    :class:`CoreException` keeps the response a standard 422 error payload,
    matching the body endpoints.
    """

    try:
        return dto_type.model_validate(dict(data))
    except ValidationError as error:
        raise exc.validation(
            f"Invalid input for operation '{op}'",
            details={
                "errors": error.errors(
                    include_url=False,
                    include_input=False,
                    include_context=False,
                )
            },
        ) from error


# ....................... #


def _operation_runner(
    registry: FrozenOperationRegistry,
    op: str,
    ctx_dep: ExecutionContextFactory,
) -> OperationRunner:
    """Build the dispatch core shared by every endpoint shape."""

    async def run(args: Any) -> Any:
        return await run_operation(registry, op, args, ctx_dep())

    return run


# ....................... #


def _route_description(
    entry: OperationCatalogEntry,
) -> str | None:
    """Route description: the descriptor's text plus catalog-derived lines.

    The permissions line reflects *declared-hook introspection* only (the catalog's
    ``required_permissions``), not a complete security statement — an operation may
    enforce further checks inside its handler invisibly. A plan-declared deadline
    documents the operation's time budget: exceeding it fails with **504**.
    """

    base = entry.descriptor.description if entry.descriptor is not None else None
    lines: list[str] = [base] if base else []

    if entry.required_permissions:
        keys = ", ".join(f"`{key}`" for key in entry.required_permissions)
        lines.append(
            f"Requires permissions: {keys} (declared by attached authorization "
            "hooks; the operation may enforce additional checks internally)."
        )

    if entry.deadline is not None:
        budget = f"{entry.deadline.total_seconds():g}"
        lines.append(
            f"Time budget: {budget}s — requests exceeding it fail with "
            "504 (`deadline_exceeded`)."
        )

    return "\n\n".join(lines) if lines else None


# ....................... #


def _route_openapi_extra(
    entry: OperationCatalogEntry,
) -> dict[str, Any] | None:
    """Catalog-derived OpenAPI additions for one route, or ``None`` when unflagged.

    Idempotency-capable operations (``supports_idempotency_key``) document the
    ``Idempotency-Key`` request header as an **optional** parameter — the wrap
    replays only for callers that send a key; there is no enforcement. A
    "required-mode" knob (reject keyless requests) is a follow-up.

    Declared permissions surface as the ``x-required-permissions`` vendor
    extension; mapping them onto OpenAPI ``securitySchemes`` is a follow-up.
    A plan-declared deadline surfaces as ``x-deadline-seconds`` (the merged
    per-invocation budget; expiry returns **504**). FastAPI deep-merges
    ``openapi_extra`` into the operation object, appending to ``parameters``,
    so unflagged routes (``None``) are emitted unchanged.
    """

    extra: dict[str, Any] = {}

    if entry.supports_idempotency_key:
        extra["parameters"] = [
            {
                "name": IDEMPOTENCY_KEY_HEADER,
                "in": "header",
                "required": False,
                "schema": {"type": "string", "title": IDEMPOTENCY_KEY_HEADER},
                "description": (
                    "Optional idempotency key. Retrying with the same key replays "
                    "the stored result instead of re-executing the operation."
                ),
            }
        ]

    if entry.required_permissions:
        extra["x-required-permissions"] = list(entry.required_permissions)

    if entry.deadline is not None:
        extra["x-deadline-seconds"] = entry.deadline.total_seconds()

    return extra or None


# ....................... #


def body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint taking the whole input DTO as the request body."""

    dto_type = require_input_type(input_type, op)

    async def endpoint(payload: Any) -> Any:
        return await runner(payload)

    endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter(
                "payload",
                inspect.Parameter.KEYWORD_ONLY,
                annotation=dto_type,
            )
        ]
    )
    endpoint.__annotations__ = {"payload": dto_type}

    return endpoint


# ....................... #


def id_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling the input DTO from an ``{id}`` path parameter."""

    dto_type = require_input_type(input_type, op)
    _require_satisfiable(dto_type, op, {"id"})

    async def endpoint(id: UUID) -> Any:
        return await runner(validate_payload(dto_type, {"id": id}, op))

    return endpoint


# ....................... #


def id_rev_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling the input DTO from ``{id}`` path and ``rev`` query."""

    dto_type = require_input_type(input_type, op)
    _require_satisfiable(dto_type, op, {"id", "rev"})

    async def endpoint(id: UUID, rev: int) -> Any:
        return await runner(validate_payload(dto_type, {"id": id, "rev": rev}, op))

    return endpoint


# ....................... #


def id_rev_body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling an update DTO from ``{id}`` path, ``rev`` query, and body.

    The body carries only the inner patch DTO; the wrapper (``DocumentUpdateDTO``)
    is reassembled before dispatch.
    """

    dto_type = require_input_type(input_type, op)
    fields = dto_type.model_fields

    if not {"id", "rev", "dto"} <= set(fields):
        raise exc.configuration(
            f"Input type '{dto_type.__name__}' is not an update wrapper "
            "(expected 'id', 'rev' and 'dto' fields)"
        )

    inner = fields["dto"].annotation

    async def endpoint(id: UUID, rev: int, payload: Any) -> Any:
        return await runner(
            validate_payload(dto_type, {"id": id, "rev": rev, "dto": payload}, op)
        )

    endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter("id", inspect.Parameter.KEYWORD_ONLY, annotation=UUID),
            inspect.Parameter("rev", inspect.Parameter.KEYWORD_ONLY, annotation=int),
            inspect.Parameter(
                "payload", inspect.Parameter.KEYWORD_ONLY, annotation=inner
            ),
        ]
    )
    endpoint.__annotations__ = {"id": UUID, "rev": int, "payload": inner}

    return endpoint


# ....................... #


def attach_operation_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    bindings: Mapping[str, RouteBinding],
    include: AbstractSet[Any] | None,
) -> APIRouter:
    """Attach the registered operations under *ns* to *router* per *bindings*.

    One route per binding whose operation the registry holds — unregistered
    operations are skipped unless explicitly listed in *include*, which makes the
    omission a configuration error. Each route's ``operation_id`` is the operation
    key verbatim; schemas come from the operation descriptors.
    """

    known = set(bindings)
    wanted = known if include is None else {str(o) for o in include}

    if unknown := wanted - known:
        raise exc.configuration(
            f"Unknown operations: {sorted(unknown)} (expected {sorted(known)})"
        )

    catalog = {str(key): entry for key, entry in registry.catalog().items()}
    attached = 0

    for suffix, binding in bindings.items():
        if suffix not in wanted:
            continue

        op = ns.key(suffix)
        entry = catalog.get(op)

        if entry is None:
            if include is not None:
                raise exc.configuration(f"Operation '{op}' is not registered")
            continue

        descriptor = entry.descriptor

        if descriptor is not None and descriptor.sensitive:
            raise exc.configuration(
                f"Refusing to attach routes under namespace '{ns.prefix}': "
                f"operation '{op}' projects a sensitive read model (its spec is "
                "marked sensitive=True; credential/secret material must not be "
                "exposed on generated external surfaces)"
            )

        input_type = descriptor.input_type if descriptor is not None else None
        output_type = descriptor.output_type if descriptor is not None else None

        endpoint = binding.build(
            _operation_runner(registry, op, ctx_dep),
            input_type,
            op,
        )

        # Descriptor tags project onto OpenAPI route tags (additive to any
        # router-level tags). MCP attachers have no tag concept — this mapping
        # is HTTP-surface-specific by design.
        tags: list[str | Enum] = (
            list(descriptor.tags) if descriptor is not None else []
        )

        router.add_api_route(
            binding.path,
            endpoint,
            methods=[binding.method],
            response_model=output_type,
            status_code=binding.status_code,
            operation_id=op,
            name=op,
            summary=descriptor.title if descriptor is not None else None,
            description=_route_description(entry),
            tags=tags or None,
            openapi_extra=_route_openapi_extra(entry),
        )
        attached += 1

    if not attached:
        raise exc.configuration(
            f"No matching operations registered under namespace '{ns.prefix}'"
        )

    return router
