from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from datetime import timedelta
from enum import Enum
from typing import (
    Any,
    Callable,
    Sequence,
    TypedDict,
    Union,
    final,
    get_type_hints,
)

from fastapi import APIRouter as APIRouter
from fastapi import Header, HTTPException
from fastapi.datastructures import Default, DefaultPlaceholder
from fastapi.params import Depends
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from fastapi.types import DecoratedCallable, IncEx
from fastapi.utils import generate_unique_id
from pydantic import BaseModel, TypeAdapter
from starlette.routing import BaseRoute
from starlette.types import ASGIApp, Lifespan

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.constants import IDEMPOTENCY_KEY_HEADER

from .routes import (
    ETagFeature,
    ETagProvider,
    IdempotencyFeature,
    RouteFeature,
    compose_route_class,
)

# ----------------------- #

ExecutionContextDependencyPort = Callable[[], ExecutionContext]
"""Callable that returns an :class:`ExecutionContext` (used as a FastAPI dependency)."""

# ....................... #


class RouteIdempotencyConfig(TypedDict, total=False):
    """Configuration for idempotency of a route / route handler."""

    ttl: timedelta
    """Time to live for the idempotency snapshot. Default is 30 seconds."""

    dto_param: str
    """Name of the DTO parameter to be used for the idempotency payload. Default is ``None``."""


# ....................... #


@final
class RouterIdempotencyConfig(RouteIdempotencyConfig, TypedDict, total=False):
    """Configuration for idempotency of a router.

    Router-level configuration is used as a default for all idempotent routes
    unless overridden via per-route :class:`RouteIdempotencyConfig`.
    """

    header_key: str
    """Name of the header key to be used for the idempotency key."""


# ....................... #


class RouteETagConfig(TypedDict, total=False):
    """Per-route ETag configuration.

    Overrides router-level defaults when supplied to individual route
    registrations.
    """

    enabled: bool
    """Enable or disable ETag generation for this route."""

    provider: ETagProvider
    """Strategy used to generate the tag value."""

    auto_304: bool
    """Automatically return *304 Not Modified* when ``If-None-Match`` matches."""


# ....................... #


@final
class RouterETagConfig(RouteETagConfig, TypedDict, total=False):
    """Router-level default ETag configuration.

    Applied to every ETag-enabled route unless overridden via per-route
    :class:`RouteETagConfig`.
    """


# ....................... #


def make_idem_header_dependency(header_key: str):  # type: ignore[no-untyped-def]
    """Create a FastAPI dependency that validates the idempotency header is present."""

    async def dep(idempotency_key: str = Header(..., alias=header_key)) -> None:
        if not idempotency_key:
            raise HTTPException(
                400, f"Idempotency key is required in header: {header_key}"
            )

    return dep


# ....................... #


@final
class ForzeAPIRouter(APIRouter):
    """FastAPI router with composable route feature integration.

    The router extends :class:`fastapi.APIRouter` with support for:

    * injecting an :class:`ExecutionContext` via dependency
    * per-router and per-route idempotency configuration
    * per-router and per-route ETag configuration
    * composable :class:`~.routes.RouteFeature` stacking on a single route
    """

    def __init__(
        self,
        *,
        prefix: str = "",
        tags: list[str | Enum] | None = None,
        dependencies: Sequence[Depends] | None = None,
        default_response_class: type[Response] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        routes: list[BaseRoute] | None = None,
        redirect_slashes: bool = True,
        default: ASGIApp | None = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type[APIRoute] = APIRoute,
        lifespan: Lifespan[Any] | None = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
        strict_content_type: bool = Default(True),
        # extra parameters
        context_dependency: ExecutionContextDependencyPort,
        idempotency_config: RouterIdempotencyConfig | None = None,
        etag_config: RouterETagConfig | None = None,
    ) -> None:
        super().__init__(
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            lifespan=lifespan,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function,
            strict_content_type=strict_content_type,
        )

        self.__idempotency_config = idempotency_config or {}
        self.__etag_config: RouterETagConfig = etag_config or {}
        self.__context_dependency = context_dependency

    # ....................... #

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        response_model: Any = Default(None),
        status_code: int | None = None,
        tags: list[Union[str, Enum]] | None = None,
        dependencies: Sequence[Depends] | None = None,
        summary: str | None = None,
        description: str | None = None,
        response_description: str = "Successful Response",
        responses: dict[Union[int, str], dict[str, Any]] | None = None,
        deprecated: bool | None = None,
        methods: Union[set[str], list[str]] | None = None,
        operation_id: str | None = None,
        response_model_include: IncEx | None = None,
        response_model_exclude: IncEx | None = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,  # overridden below by default `True`
        include_in_schema: bool = True,
        response_class: Union[type[Response], DefaultPlaceholder] = Default(
            JSONResponse
        ),
        name: str | None = None,
        route_class_override: type[APIRoute] | None = None,
        callbacks: list[BaseRoute] | None = None,
        openapi_extra: dict[str, Any] | None = None,
        generate_unique_id_function: Union[
            Callable[[APIRoute], str], DefaultPlaceholder
        ] = Default(generate_unique_id),
        strict_content_type: bool | DefaultPlaceholder = Default(True),
        # extra parameters
        idempotent: bool = False,
        idempotency_config: RouteIdempotencyConfig | None = None,
        etag: bool = False,
        etag_config: RouteETagConfig | None = None,
        route_features: Sequence[RouteFeature] | None = None,
    ) -> None:
        """Register a route with optional composable feature wrapping.

        Features activated via ``idempotent``/``etag`` flags and any
        explicit *route_features* are composed into a single
        :class:`~fastapi.routing.APIRoute` subclass.  The composition
        order is: explicit *route_features* first, then built-in
        features (idempotency before ETag).
        """

        idempotency_config = idempotency_config or self.__idempotency_config
        deps = list(dependencies or [])
        features: list[RouteFeature] = list(route_features or [])

        if idempotent and methods and "POST" in methods:
            if operation_id is None:
                raise CoreError("Operation ID is required for idempotent routes")

            hints = get_type_hints(endpoint)
            resp_model = response_model or hints.get("return")

            if resp_model is None or not isinstance(resp_model, type):
                raise CoreError(
                    "Response model or return annotation is required for idempotent routes"
                )

            status_code = status_code or 200
            dto_param = idempotency_config.get("dto_param") or self.__guess_dto_param(
                endpoint
            )
            header_key = self.__idempotency_config.get(
                "header_key", IDEMPOTENCY_KEY_HEADER
            )
            request_adapter = self.__get_request_model_adapter(endpoint, dto_param)

            from .routes.idempotent import IdempotentRouteConfig

            cfg = IdempotentRouteConfig(
                operation=operation_id,
                ttl=idempotency_config.get("ttl", timedelta(seconds=30)),
                header_key=header_key,
                adapter=request_adapter,
                dto_param=dto_param,
            )

            features.append(
                IdempotencyFeature(
                    ctx_dep=self.__context_dependency,
                    config=cfg,
                    extra_dependencies=(
                        Depends(make_idem_header_dependency(header_key)),
                    ),
                )
            )

        if etag:
            merged: RouteETagConfig = {**self.__etag_config, **(etag_config or {})}
            provider: ETagProvider | None = merged.get("provider")

            if provider is None:
                raise CoreError("ETag provider is required when ETag is enabled")

            auto_304: bool = merged.get("auto_304", True)

            from .routes.etag import ETagRouteConfig

            features.append(
                ETagFeature(
                    config=ETagRouteConfig(provider=provider, auto_304=auto_304),
                )
            )

        for feature in features:
            deps.extend(feature.extra_dependencies)

        if features:
            base = route_class_override or APIRoute
            route_class_override = compose_route_class(*features, base=base)

        return super().add_api_route(
            path,
            endpoint,
            response_model=response_model,
            status_code=status_code,
            tags=tags,
            dependencies=deps,
            summary=summary,
            description=description,
            response_description=response_description,
            responses=responses,
            deprecated=deprecated,
            methods=methods,
            operation_id=operation_id,
            response_model_include=response_model_include,
            response_model_exclude=response_model_exclude,
            response_model_by_alias=response_model_by_alias,
            response_model_exclude_unset=response_model_exclude_unset,
            response_model_exclude_defaults=response_model_exclude_defaults,
            response_model_exclude_none=True,  # override default value
            include_in_schema=include_in_schema,
            response_class=response_class,
            name=name,
            route_class_override=route_class_override,
            callbacks=callbacks,
            openapi_extra=openapi_extra,
            generate_unique_id_function=generate_unique_id_function,
            strict_content_type=strict_content_type,
        )

    # ....................... #

    def post(
        self,
        path: str,
        *,
        response_model: Any = Default(None),
        status_code: int | None = None,
        tags: list[Union[str, Enum]] | None = None,
        dependencies: Sequence[Depends] | None = None,
        summary: str | None = None,
        description: str | None = None,
        response_description: str = "Successful Response",
        responses: dict[Union[int, str], dict[str, Any]] | None = None,
        deprecated: bool | None = None,
        operation_id: str | None = None,
        response_model_include: IncEx | None = None,
        response_model_exclude: IncEx | None = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        include_in_schema: bool = True,
        response_class: type[Response] = Default(JSONResponse),
        name: str | None = None,
        callbacks: list[BaseRoute] | None = None,
        openapi_extra: dict[str, Any] | None = None,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
        # extra parameters
        idempotent: bool = False,
        idempotency_config: RouteIdempotencyConfig | None = None,
        route_features: Sequence[RouteFeature] | None = None,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """Add a *path operation* using an HTTP POST operation."""

        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            self.add_api_route(
                path,
                func,
                response_model=response_model,
                status_code=status_code,
                tags=tags,
                dependencies=dependencies,
                summary=summary,
                description=description,
                response_description=response_description,
                responses=responses,
                deprecated=deprecated,
                methods=["POST"],
                operation_id=operation_id,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
                include_in_schema=include_in_schema,
                response_class=response_class,
                name=name,
                callbacks=callbacks,
                openapi_extra=openapi_extra,
                generate_unique_id_function=generate_unique_id_function,
                idempotent=idempotent,
                idempotency_config=idempotency_config,
                route_features=route_features,
            )
            return func

        return decorator

    # ....................... #

    def get(
        self,
        path: str,
        *,
        response_model: Any = Default(None),
        status_code: int | None = None,
        tags: list[Union[str, Enum]] | None = None,
        dependencies: Sequence[Depends] | None = None,
        summary: str | None = None,
        description: str | None = None,
        response_description: str = "Successful Response",
        responses: dict[Union[int, str], dict[str, Any]] | None = None,
        deprecated: bool | None = None,
        operation_id: str | None = None,
        response_model_include: IncEx | None = None,
        response_model_exclude: IncEx | None = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        include_in_schema: bool = True,
        response_class: type[Response] = Default(JSONResponse),
        name: str | None = None,
        callbacks: list[BaseRoute] | None = None,
        openapi_extra: dict[str, Any] | None = None,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
        # extra parameters
        etag: bool = False,
        etag_config: RouteETagConfig | None = None,
        route_features: Sequence[RouteFeature] | None = None,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """Add a *path operation* using an HTTP GET operation."""

        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            self.add_api_route(
                path,
                func,
                response_model=response_model,
                status_code=status_code,
                tags=tags,
                dependencies=dependencies,
                summary=summary,
                description=description,
                response_description=response_description,
                responses=responses,
                deprecated=deprecated,
                methods=["GET"],
                operation_id=operation_id,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
                include_in_schema=include_in_schema,
                response_class=response_class,
                name=name,
                callbacks=callbacks,
                openapi_extra=openapi_extra,
                generate_unique_id_function=generate_unique_id_function,
                etag=etag,
                etag_config=etag_config,
                route_features=route_features,
            )
            return func

        return decorator

    # ....................... #

    def __guess_dto_param(self, endpoint: Callable[..., Any]) -> str:
        """Infer the name of the first Pydantic-model parameter in *endpoint*."""

        sig = inspect.signature(endpoint)

        for name, p in sig.parameters.items():
            ann = p.annotation

            if isinstance(ann, type) and issubclass(ann, BaseModel):
                return name

        raise RuntimeError(  #!? Replace with CoreError or so
            "Cannot infer DTO param for idempotent route; pass it explicitly"
        )

    # ....................... #

    def __get_request_model_adapter(
        self, endpoint: Callable[..., Any], dto_param: str
    ) -> TypeAdapter[Any]:
        """Build a :class:`TypeAdapter` for the Pydantic DTO parameter."""

        sig = inspect.signature(endpoint)
        p = sig.parameters.get(dto_param)

        if p is None:
            raise CoreError("DTO param not found in endpoint signature")

        ann = p.annotation

        if not isinstance(ann, type) or not issubclass(ann, BaseModel):
            raise CoreError("DTO param must be a Pydantic model")

        return TypeAdapter[Any](ann)
