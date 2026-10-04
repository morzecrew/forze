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
from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from types import NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Final,
    ForwardRef,
    Literal,
    TypeAliasType,
    Union,
    final,
    get_args,
    get_origin,
)
from uuid import UUID

import attrs
from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.utils import is_body_allowed_for_status_code
from pydantic import (
    AnyUrl,
    AwareDatetime,
    BaseModel,
    EmailStr,
    FutureDate,
    FutureDatetime,
    IPvAnyAddress,
    IPvAnyInterface,
    IPvAnyNetwork,
    NaiveDatetime,
    NameEmail,
    PastDate,
    PastDatetime,
    PlainSerializer,
    ValidationError,
)
from pydantic.fields import FieldInfo
from starlette.datastructures import QueryParams

from forze.application.contracts.querying import QUANTIFIER_OPS, QueryDiscovery
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
    run_operation,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.middlewares.bypass import GOVERNED_OPERATION_ATTR
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


def _encode_untyped(value: Any) -> Any:
    # A model is left to pydantic, which writes it as it writes a typed route's result;
    # anything else keeps the encoding FastAPI gives a route without a response model.
    return value if isinstance(value, BaseModel) else jsonable_encoder(value)


_UntypedResponse = Annotated[Any, PlainSerializer(_encode_untyped, return_type=Any)]
"""Response model of an operation whose descriptor names no output type.

Without a response model FastAPI encodes a result through ``jsonable_encoder``, a Python walk
of the whole value; with this one a model result is written by pydantic directly, as a typed
route's is.
"""

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

    required = {name for name, field in dto_type.model_fields.items() if field.is_required()}

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
            f"Time budget: {budget}s — requests exceeding it fail with 504 (`deadline_exceeded`)."
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


PATH_PARAMS_ATTR: Final = "path_params"
"""Attribute naming the path placeholders an endpoint builder can fill.

A builder carrying it (a ``frozenset[str]``) has each binding's path checked when the route is
attached: a ``{placeholder}`` the builder does not fill is refused, since FastAPI would publish
it with no parameter behind it. A builder without the attribute is not checked.
"""


def _fills_path(*names: str) -> Callable[[EndpointBuilder], EndpointBuilder]:
    def declare(build: EndpointBuilder) -> EndpointBuilder:
        setattr(build, PATH_PARAMS_ATTR, frozenset(names))
        return build

    return declare


# ....................... #

_QUERY_SCALARS: Final = (
    str,
    int,
    float,
    Decimal,
    UUID,
    date,
    datetime,
    time,
    timedelta,
    Enum,
    IPv4Address,
    IPv6Address,
    IPv4Network,
    IPv6Network,
    IPv4Interface,
    IPv6Interface,
    AnyUrl,
    EmailStr,
    NameEmail,
    IPvAnyAddress,
    IPvAnyNetwork,
    IPvAnyInterface,
    AwareDatetime,
    NaiveDatetime,
    PastDate,
    FutureDate,
    PastDatetime,
    FutureDatetime,
)
"""Types one query-string value can carry (``bool`` is an ``int``): each parses from text.

``bytes`` is not one: a query value is decoded as text, so ``%FF`` arrives as a replacement
character rather than the byte."""

_QUERY_SEQUENCES: Final = (list, tuple, set, frozenset)
"""Containers a repeated query parameter (``?tag=a&tag=b``) can carry."""


def _query_carries(annotation: Any, *, in_sequence: bool = False) -> bool:
    """Whether a query string can carry a field of type *annotation*.

    With *in_sequence* only a single value can, as for a path parameter or a list's items.
    """

    # FastAPI reads a type alias or a NewType as one value, whatever it names: an alias of a
    # list would take one value and fail every request, so only a scalar passes through one.
    if isinstance(annotation, TypeAliasType):
        return _query_carries(annotation.__value__, in_sequence=True)

    if (supertype := getattr(annotation, "__supertype__", None)) is not None:  # a NewType
        return _query_carries(supertype, in_sequence=True)

    origin = get_origin(annotation)

    if origin is Annotated:
        return _query_carries(get_args(annotation)[0], in_sequence=in_sequence)

    if origin is Literal:
        return True

    if origin in (Union, UnionType):
        return all(
            _query_carries(arg, in_sequence=in_sequence)
            for arg in get_args(annotation)
            if arg is not NoneType
        )

    if origin in _QUERY_SEQUENCES and not in_sequence:
        return all(
            _query_carries(arg, in_sequence=True)
            for arg in get_args(annotation)
            if arg is not Ellipsis
        )

    # Pydantic's datetime variants are classes at runtime, though typed as Annotated aliases.
    return isinstance(annotation, type) and issubclass(annotation, _QUERY_SCALARS)  # pyright: ignore[reportArgumentType]


def _complete(dto_type: type[BaseModel], op: str) -> type[BaseModel]:
    """*dto_type* with its forward references resolved, or a refusal saying which remain."""

    if not dto_type.__pydantic_complete__ and not dto_type.model_rebuild(raise_errors=False):
        unresolved = sorted(
            name
            for name, field in dto_type.model_fields.items()
            if isinstance(field.annotation, ForwardRef | str)
        )
        raise exc.configuration(
            f"Input type '{dto_type.__name__}' of operation '{op}' has an unresolved forward "
            f"reference in {unresolved}; define the type it names and call "
            f"{dto_type.__name__}.model_rebuild() before attaching the route"
        )

    return dto_type


def _param(name: str, annotation: Any, default: Any = inspect.Parameter.empty) -> inspect.Parameter:
    return inspect.Parameter(
        name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation, default=default
    )


def _single_value(
    dto_type: type[BaseModel], name: str, op: str, fallback: Any
) -> inspect.Parameter:
    """Path/query parameter *name* as the DTO declares its field: type, and default if any."""

    field = dto_type.model_fields.get(name)

    if field is None:
        return _param(name, fallback)

    if not _query_carries(field.annotation, in_sequence=True):
        raise exc.configuration(
            f"Field '{name}' of input type '{dto_type.__name__}' (operation '{op}') cannot be "
            "carried by a single path or query value — only a scalar can"
        )

    if field.is_required():
        return _param(name, field.annotation)

    return _param(name, field.annotation, field.get_default(call_default_factory=True))


def _signed(endpoint: Callable[..., Awaitable[Any]], *params: inspect.Parameter) -> None:
    """Give *endpoint* the keyword-only parameters FastAPI reads its inputs from."""

    endpoint.__signature__ = inspect.Signature(list(params))  # type: ignore[attr-defined]
    endpoint.__annotations__ = {param.name: param.annotation for param in params}


# ....................... #


@_fills_path()
def body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint taking the whole input DTO as the request body."""

    dto_type = require_input_type(input_type, op)

    async def endpoint(payload: Any) -> Any:
        return await runner(payload)

    _signed(endpoint, _param("payload", dto_type))

    return endpoint


# ....................... #


@_fills_path("id")
def id_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling the input DTO from ``id`` — a path parameter where the route's path
    has an ``{id}`` placeholder, a query parameter otherwise. It takes the DTO's own ``id``
    type."""

    dto_type = require_input_type(input_type, op)
    _require_satisfiable(dto_type, op, {"id"})

    async def endpoint(id: Any) -> Any:
        return await runner(validate_payload(dto_type, {"id": id}, op))

    _signed(endpoint, _single_value(dto_type, "id", op, UUID))

    return endpoint


# ....................... #


@_fills_path("id", "rev")
def id_rev_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling the input DTO from ``id`` and ``rev``, each a path parameter where
    the path has its placeholder and a query parameter otherwise."""

    dto_type = require_input_type(input_type, op)
    _require_satisfiable(dto_type, op, {"id", "rev"})

    async def endpoint(id: Any, rev: Any) -> Any:
        return await runner(validate_payload(dto_type, {"id": id, "rev": rev}, op))

    _signed(
        endpoint,
        _single_value(dto_type, "id", op, UUID),
        _single_value(dto_type, "rev", op, int),
    )

    return endpoint


# ....................... #


@_fills_path("id", "rev")
def id_rev_body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint assembling an update DTO from ``id`` and ``rev`` (path or query) and a body.

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

    async def endpoint(id: Any, rev: Any, payload: Any) -> Any:
        return await runner(validate_payload(dto_type, {"id": id, "rev": rev, "dto": payload}, op))

    _signed(
        endpoint,
        _single_value(dto_type, "id", op, UUID),
        _single_value(dto_type, "rev", op, int),
        _param("payload", fields["dto"].annotation),
    )

    return endpoint


# ....................... #


def _query_key(name: str, field: FieldInfo) -> str:
    """The query key FastAPI reads a field from: its validation alias, alias, or name."""

    if isinstance(field.validation_alias, str) and field.validation_alias:
        return field.validation_alias

    return field.alias or name


def _is_sequence(annotation: Any) -> bool:
    """Whether FastAPI reads a field as a repeated query parameter."""

    origin = get_origin(annotation)

    if origin is Annotated:
        return _is_sequence(get_args(annotation)[0])

    if origin in (Union, UnionType):
        return any(_is_sequence(arg) for arg in get_args(annotation))

    return origin in _QUERY_SEQUENCES


def _query_data(dto_type: type[BaseModel], params: QueryParams) -> dict[str, Any]:
    """The payload a request body sending the same keys would carry.

    A field is read from the key FastAPI reads it from — every value of a repeated one, the
    last of a single one — and a key no field reads is passed through, for the DTO's own
    ``extra`` policy and ``populate_by_name`` to decide on, as for a body. A field not sent is
    left out, so it is not reported as set.
    """

    data: dict[str, Any] = {}
    read: set[str] = set()

    for name, field in dto_type.model_fields.items():
        key = _query_key(name, field)
        read.add(key)

        if values := params.getlist(key):
            data[key] = values if _is_sequence(field.annotation) else values[-1]

    for key in params:
        if key not in read:
            values = params.getlist(key)
            data[key] = values[0] if len(values) == 1 else values

    return data


@_fills_path()
def query_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint taking the whole input DTO as query parameters (``GET /levels?sku=``).

    Each field is one query parameter; a list field repeats it (``?tag=a&tag=b``). A field a
    query string cannot carry (a nested model, a mapping) is refused when the route is
    attached rather than silently dropped on every request. The operation receives the DTO a
    request body sending the same keys would build: only the parameters sent count as set
    (``model_fields_set``), so a patch encoded from it leaves defaulted fields alone, and
    validators, private attributes and extras behave as for a body. FastAPI validates the
    parameters first, which documents them and answers a malformed request with a 422; that
    instance is discarded, so the DTO's validators run twice per request and must not have
    side effects.
    """

    dto_type = _complete(require_input_type(input_type, op), op)

    for name, field in dto_type.model_fields.items():
        if not _query_carries(field.annotation):
            raise exc.configuration(
                f"Field '{name}' of input type '{dto_type.__name__}' (operation '{op}') "
                "cannot be carried by a query string — only scalars and lists of scalars "
                "can; take this input as a request body instead"
            )

    # Built at runtime from the descriptor, so no static checker can read it as a type.
    query_model = Annotated[dto_type, Query()]  # type: ignore[valid-type]

    async def endpoint(payload: Any, request: Request) -> Any:
        # The query model is FastAPI's: it publishes the parameters and answers 422 on a bad
        # query. It fills every default before validating, though, so every field reads as
        # set; the operation gets the model built from what the request actually sent.
        _ = payload
        data = _query_data(dto_type, request.query_params)

        return await runner(validate_payload(dto_type, data, op))

    _signed(endpoint, _param("payload", query_model), _param("request", Request))

    return endpoint


# ....................... #


def attach_operation_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    bindings: Mapping[str, RouteBinding],
    include: AbstractSet[Any] | None = None,
    path_overrides: Mapping[Any, str] | None = None,
    exclude_none: bool = True,
    skip_unregistered: bool = False,
) -> APIRouter:
    """Attach the registered operations under *ns* to *router* per *bindings*.

    One route per binding. A binding whose operation the registry does not hold is a
    configuration error — a typo in an app's own table would otherwise answer 404 —
    unless *skip_unregistered* is set, which the shipped attachers use to mirror a
    registry that omits operations on purpose (e.g. writes on a read-only spec); an
    operation listed in *include* is required either way. Each route's ``operation_id``
    is the operation key verbatim; schemas come from the operation descriptors.

    A builder carrying ``path_params`` (see :data:`PATH_PARAMS_ATTR`; every shipped
    builder does) has the binding's path checked against it: a ``{placeholder}`` the
    builder does not fill is refused. A builder of your own without it is not checked.

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
        skip_unregistered (bool): Skip a binding whose operation is not registered instead
            of refusing it (default ``False``).
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
        raise exc.configuration(f"Unknown operations: {sorted(unknown)} (expected {sorted(known)})")

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
        fillable: AbstractSet[str] | None = getattr(binding.build, PATH_PARAMS_ATTR, None)

        if fillable is not None and (unfilled := default_params - fillable):
            raise exc.configuration(
                f"Path '{binding.path}' for operation '{op}' has placeholder(s) "
                f"{sorted(unfilled)} its endpoint builder does not fill (it fills "
                f"{sorted(fillable) or 'none'}); FastAPI would publish them with no "
                "parameter behind them"
            )

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
            if include is not None or not skip_unregistered:
                hint = (
                    f"; '{suffix}' is registered outside the namespace, and only keys "
                    f"under '{ns.prefix}' are routed"
                    if suffix in catalog
                    else ""
                )
                raise exc.configuration(f"Operation '{op}' is not registered{hint}")
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
        tags: list[str | Enum] = list(descriptor.tags) if descriptor is not None else []

        # The governed marker check_bypass_paths verifies at startup: a path listed in
        # a middleware's bypass_paths must not be one of these, since the operation
        # behind it reads and writes tenant data the bypassed middleware would bind.
        setattr(endpoint, GOVERNED_OPERATION_ATTR, True)

        # A status that carries no body (204) takes no response model at all.
        response_model = output_type
        if response_model is None and is_body_allowed_for_status_code(binding.status_code):
            response_model = _UntypedResponse

        router.add_api_route(
            path,
            endpoint,
            methods=[binding.method],
            response_model=response_model,
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
        raise exc.configuration(f"No matching operations registered under namespace '{ns.prefix}'")

    return router
