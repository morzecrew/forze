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
import re
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

from forze.application.contracts.querying import QUANTIFIER_OPS, QueryDiscovery
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


def resolve_namespace(
    ns: StrKeyNamespace | None,
    resource: str | None,
) -> StrKeyNamespace:
    """Resolve the operation namespace from an explicit namespace or a resource prefix.

    Exactly one of *ns* or *resource* must be provided. The resolved namespace must
    match the prefix under which the operations were registered in the catalog.

    Args:
        ns (StrKeyNamespace | None): An explicit namespace, returned as-is when given.
        resource (str | None): A prefix string the namespace is built from (using the
            default separator) when *ns* is omitted.

    Returns:
        StrKeyNamespace: The resolved namespace.

    Raises:
        CoreException: If neither or both of *ns* and *resource* are provided
            (a configuration error).
    """

    if ns is not None and resource is None:
        return ns

    if resource is not None and ns is None:
        return StrKeyNamespace(prefix=resource)

    raise exc.configuration(
        "Provide exactly one of 'ns' (an explicit namespace) or 'resource' "
        "(a prefix string to build the namespace from)."
    )


# ....................... #


_PATH_PARAM = re.compile(r"\{([^}:]+)(?::[^}]*)?\}")
"""Matches a FastAPI path placeholder, capturing the parameter name.

Handles the bare ``{name}`` form and the converter form ``{name:path}`` (used by
the storage routes for slash-bearing keys), capturing ``name`` in both.
"""


def _path_params(path: str) -> set[str]:
    """The set of path-parameter names a route template binds."""

    return set(_PATH_PARAM.findall(path))


# ....................... #


def require_input_type(
    input_type: type[BaseModel] | None,
    op: str,
) -> type[BaseModel]:
    """Return the operation's input DTO type, failing when it is absent.

    Args:
        input_type (type[BaseModel] | None): The descriptor-derived input type, or
            ``None`` when the operation has no descriptor input type.
        op (str): Operation key, surfaced in the error message.

    Returns:
        type[BaseModel]: The input DTO type used to derive the route schema.

    Raises:
        CoreException: If *input_type* is ``None`` — route schemas cannot be derived
            (a configuration error).
    """

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
    extension; an operation that declares it needs a bound principal surfaces as
    ``x-requires-authn: true`` — :func:`forze_fastapi.security.apply_openapi_security`
    reads that flag to attach OpenAPI ``security`` to the protected operations.
    A plan-declared deadline surfaces as ``x-deadline-seconds`` (the merged
    per-invocation budget; expiry returns **504**). A filter-accepting operation's
    query surface (filterable fields and their operators, sortable/aggregatable fields)
    surfaces as the ``x-forze-query`` extension. FastAPI deep-merges ``openapi_extra``
    into the operation object, appending to ``parameters``, so unflagged routes
    (``None``) are emitted unchanged.
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

    if entry.requires_authn:
        extra["x-requires-authn"] = True

    if entry.deadline is not None:
        extra["x-deadline-seconds"] = entry.deadline.total_seconds()

    if entry.descriptor is not None and entry.descriptor.query_discovery is not None:
        extra["x-forze-query"] = _query_discovery_extension(
            entry.descriptor.query_discovery,
        )

    return extra or None


# ....................... #


def _query_discovery_extension(discovery: QueryDiscovery) -> dict[str, Any]:
    """The ``x-forze-query`` vendor extension: the read model's filter surface.

    Tells a client which fields are filterable (and which operators each accepts, plus
    element quantifiers for array fields), sortable, and aggregatable — the type-derived
    upper bound, independent of the serving backend.
    """

    filterable: list[dict[str, Any]] = []

    for field in discovery.filterable:
        entry: dict[str, Any] = {
            "field": field.field,
            "type": field.type,
            "operators": list(field.operators),
        }

        if field.quantifiable:
            entry["quantifiers"] = list(QUANTIFIER_OPS)

        filterable.append(entry)

    return {
        "filterable": filterable,
        "sortable": list(discovery.sortable),
        "aggregatable": list(discovery.aggregatable),
    }


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
    path_overrides: Mapping[Any, str] | None = None,
    exclude_none: bool = True,
) -> APIRouter:
    """Attach the registered operations under *ns* to *router* per *bindings*.

    One route per binding whose operation the registry holds — unregistered
    operations are skipped unless explicitly listed in *include*, which makes the
    omission a configuration error. Each route's ``operation_id`` is the operation
    key verbatim; schemas come from the operation descriptors.

    *path_overrides* maps an operation (the same kernel-op/str key accepted by
    *include*) to a replacement route path. Only the path changes — method,
    status, builder, and the verbatim ``operation_id`` are untouched, so the
    catalog identity is preserved. An override must bind **exactly** the path
    parameters the default path binds — no more, no less (the endpoint builders
    synthesize fixed parameter names and FastAPI maps a name to the path only when
    it appears as a ``{placeholder}``). Dropping one is a configuration error
    (a silent demotion to a query parameter); adding one the endpoint never
    synthesizes is too (the placeholder would never be filled).

    Args:
        router (APIRouter): Router the generated routes are added to.
        registry (FrozenOperationRegistry): Frozen registry providing the operation
            catalog (handlers, descriptors).
        ns (StrKeyNamespace): Namespace prefixing each binding suffix into its full
            operation key.
        ctx_dep (ExecutionContextFactory): Dependency yielding the per-request
            execution context the endpoints dispatch through.
        bindings (Mapping[str, RouteBinding]): Per-operation HTTP surface (method,
            path, status, endpoint builder), keyed by namespace-relative suffix.
        include (AbstractSet[Any] | None): When given, the exact operations to attach;
            a listed operation missing from the registry is a configuration error.
            ``None`` attaches every registered binding.
        path_overrides (Mapping[Any, str] | None): Per-operation replacement paths,
            keyed like *include*; each must bind exactly the default path's parameters.
        exclude_none (bool): When ``True`` (default) generated JSON responses omit fields
            whose value is ``None`` (``response_model_exclude_none``) — a smaller wire
            payload, and the OpenAPI schema is unchanged (the fields stay optional). Set
            ``False`` to always emit explicit ``null``\\ s. Only affects routes with a
            response model; raw-``Response`` routes (download/head bytes) are untouched.

    Returns:
        APIRouter: The same *router*, with the routes attached.

    Raises:
        CoreException: On an unknown *include*/override operation, a sensitive read
            model, or a path override that drops or adds a path parameter (all
            configuration errors).
    """

    known = set(bindings)
    wanted = known if include is None else {str(o) for o in include}

    if unknown := wanted - known:
        raise exc.configuration(
            f"Unknown operations: {sorted(unknown)} (expected {sorted(known)})"
        )

    overrides = {str(key): path for key, path in (path_overrides or {}).items()}

    if unknown := set(overrides) - wanted:
        raise exc.configuration(
            f"Unknown path override operations: {sorted(unknown)} "
            f"(expected {sorted(map(str, wanted))})"
        )

    catalog = {str(key): entry for key, entry in registry.catalog().items()}
    attached = 0

    for suffix, binding in bindings.items():
        if suffix not in wanted:
            continue

        op = ns.key(suffix)
        path = overrides.get(str(suffix), binding.path)

        default_params = _path_params(binding.path)
        override_params = _path_params(path)

        if missing := default_params - override_params:
            raise exc.configuration(
                f"Path override '{path}' for operation '{op}' drops path "
                f"parameter(s) {sorted(missing)} the default path '{binding.path}' "
                "binds (the endpoint requires them in the path, not the query)"
            )

        if extra := override_params - default_params:
            raise exc.configuration(
                f"Path override '{path}' for operation '{op}' adds path "
                f"parameter(s) {sorted(extra)} the default path '{binding.path}' "
                "does not bind (the endpoint synthesizes fixed parameter names, so "
                "an unknown placeholder would never be filled)"
            )

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
            path,
            endpoint,
            methods=[binding.method],
            response_model=output_type,
            response_model_exclude_none=exclude_none,
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
