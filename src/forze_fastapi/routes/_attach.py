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
from pydantic import BaseModel

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    run_operation,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace

# ----------------------- #

RouteStyle = Literal["rest", "rpc"]
"""Path/verb mapping for generated routes.

``"rest"`` maps operations onto resource-style paths and verbs; ``"rpc"``
exposes one operation-named path per operation, mirroring the catalog
one-to-one. Each attacher documents its concrete mapping. Attachers whose
operations have a single natural surface (search, where every request is a
filter body) take no style argument.
"""

OperationRunner = Callable[[Any], Awaitable[Any]]
"""Async callable dispatching validated operation args through the pipeline."""

EndpointBuilder = Callable[
    [OperationRunner, type[BaseModel] | None, str],
    Callable[..., Awaitable[Any]],
]
"""Builds a route endpoint from ``(runner, descriptor input type, op key)``."""


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

    async def endpoint(id: UUID) -> Any:
        return await runner(dto_type.model_validate({"id": id}))

    return endpoint


# ....................... #


def id_rev_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling the input DTO from ``{id}`` path and ``rev`` query."""

    dto_type = require_input_type(input_type, op)

    async def endpoint(id: UUID, rev: int) -> Any:
        return await runner(dto_type.model_validate({"id": id, "rev": rev}))

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
            dto_type.model_validate({"id": id, "rev": rev, "dto": payload})
        )

    endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter(
                "id", inspect.Parameter.KEYWORD_ONLY, annotation=UUID
            ),
            inspect.Parameter(
                "rev", inspect.Parameter.KEYWORD_ONLY, annotation=int
            ),
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
        input_type = descriptor.input_type if descriptor is not None else None
        output_type = descriptor.output_type if descriptor is not None else None

        endpoint = binding.build(
            _operation_runner(registry, op, ctx_dep),
            input_type,
            op,
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
            description=descriptor.description if descriptor is not None else None,
        )
        attached += 1

    if not attached:
        raise exc.configuration(
            f"No matching operations registered under namespace '{ns.prefix}'"
        )

    return router
