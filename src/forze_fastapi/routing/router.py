from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from datetime import timedelta
from enum import Enum
from typing import (
    Any,
    Callable,
    Optional,
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

from .routes import make_idempotent_route_class

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
    """FastAPI router with idempotency integration.

    The router extends :class:`fastapi.APIRouter` with support for:

    * injecting an :class:`AppRuntimePort` via dependency
    * per-router and per-route idempotency configuration
    * automatic wrapping of idempotent POST routes.
    """

    def __init__(
        self,
        *,
        prefix: str = "",
        tags: Optional[list[str | Enum]] = None,
        dependencies: Optional[Sequence[Depends]] = None,
        default_response_class: type[Response] = Default(JSONResponse),
        responses: Optional[dict[int | str, dict[str, Any]]] = None,
        callbacks: Optional[list[BaseRoute]] = None,
        routes: Optional[list[BaseRoute]] = None,
        redirect_slashes: bool = True,
        default: Optional[ASGIApp] = None,
        dependency_overrides_provider: Optional[Any] = None,
        route_class: type[APIRoute] = APIRoute,
        lifespan: Optional[Lifespan[Any]] = None,
        deprecated: Optional[bool] = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
        strict_content_type: bool = Default(True),
        # extra parameters
        context_dependency: ExecutionContextDependencyPort,
        idempotency_config: Optional[RouterIdempotencyConfig] = None,
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
        self.__context_dependency = context_dependency

    # ....................... #

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        response_model: Any = Default(None),
        status_code: Optional[int] = None,
        tags: Optional[list[Union[str, Enum]]] = None,
        dependencies: Optional[Sequence[Depends]] = None,
        summary: Optional[str] = None,
        description: Optional[str] = None,
        response_description: str = "Successful Response",
        responses: Optional[dict[Union[int, str], dict[str, Any]]] = None,
        deprecated: Optional[bool] = None,
        methods: Optional[Union[set[str], list[str]]] = None,
        operation_id: Optional[str] = None,
        response_model_include: Optional[IncEx] = None,
        response_model_exclude: Optional[IncEx] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,  # overridden below by default `True`
        include_in_schema: bool = True,
        response_class: Union[type[Response], DefaultPlaceholder] = Default(
            JSONResponse
        ),
        name: Optional[str] = None,
        route_class_override: Optional[type[APIRoute]] = None,
        callbacks: Optional[list[BaseRoute]] = None,
        openapi_extra: Optional[dict[str, Any]] = None,
        generate_unique_id_function: Union[
            Callable[[APIRoute], str], DefaultPlaceholder
        ] = Default(generate_unique_id),
        strict_content_type: bool | DefaultPlaceholder = Default(True),
        # extra parameters
        idempotent: bool = False,
        idempotency_config: Optional[RouteIdempotencyConfig] = None,
    ) -> None:
        """Register a route with optional idempotency wrapping for POST methods."""

        idempotency_config = idempotency_config or self.__idempotency_config
        deps = list(dependencies or [])

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

            route_class_override = make_idempotent_route_class(
                ctx_dep=self.__context_dependency,
                operation=operation_id,
                ttl=idempotency_config.get("ttl", timedelta(seconds=30)),
                header_key=header_key,
                adapter=request_adapter,
                dto_param=dto_param,
            )
            deps.append(Depends(make_idem_header_dependency(header_key)))

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
        status_code: Optional[int] = None,
        tags: Optional[list[Union[str, Enum]]] = None,
        dependencies: Optional[Sequence[Depends]] = None,
        summary: Optional[str] = None,
        description: Optional[str] = None,
        response_description: str = "Successful Response",
        responses: Optional[dict[Union[int, str], dict[str, Any]]] = None,
        deprecated: Optional[bool] = None,
        operation_id: Optional[str] = None,
        response_model_include: Optional[IncEx] = None,
        response_model_exclude: Optional[IncEx] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        include_in_schema: bool = True,
        response_class: type[Response] = Default(JSONResponse),
        name: Optional[str] = None,
        callbacks: Optional[list[BaseRoute]] = None,
        openapi_extra: Optional[dict[str, Any]] = None,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
        # extra parameters
        idempotent: bool = False,
        idempotency_config: Optional[RouteIdempotencyConfig] = None,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """
        Add a *path operation* using an HTTP POST operation.
        """

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
