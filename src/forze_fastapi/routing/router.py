from forze.base.serialization import pydantic_dump, pydantic_model_hash
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from datetime import timedelta
from enum import Enum
from functools import partial, wraps
from typing import (
    Annotated,
    Any,
    Callable,
    Optional,
    Sequence,
    TypedDict,
    Union,
    final,
    get_type_hints,
)

import orjson
from annotated_doc import Doc
from fastapi import APIRouter as APIRouter
from fastapi import Depends as Dependency
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
from typing_extensions import deprecated

from forze.application.kernel.dependencies import IdempotencyDependencyPort
from forze.application.kernel.ports import AppRuntimePort, IdempotencyPort
from forze.base.errors import CoreError
from forze_fastapi.constants import IDEMPOTENCY_KEY_HEADER

# ----------------------- #

AppRuntimeDependencyPort = Callable[[], AppRuntimePort]

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
    """Configuration for idempotency of a router."""

    header_key: str
    """Name of the header key to be used for the idempotency key. Default is ``"X-Idempotency-Key"``."""


# ....................... #


def _idem_header_dependency(header_key: str):
    def dependency(idempotency_key: str = Header(..., alias=header_key)) -> str:
        if not idempotency_key:
            raise HTTPException(status_code=400, detail="Idempotency key is required")

        return idempotency_key

    return dependency


def _idempotency_dependency(
    app_runtime: AppRuntimeDependencyPort,
    idempotency: IdempotencyDependencyPort,
):
    def dependency(ttl: timedelta):
        return idempotency(runtime=app_runtime(), ttl=ttl)

    return dependency


# ....................... #


@final
class ForzeAPIRouter(APIRouter):
    def __init__(
        self,
        *,
        prefix: Annotated[str, Doc("An optional path prefix for the router.")] = "",
        tags: Annotated[
            list[str | Enum] | None,
            Doc(
                """
                A list of tags to be applied to all the *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """
            ),
        ] = None,
        dependencies: Annotated[
            Sequence[Depends] | None,
            Doc(
                """
                A list of dependencies (using `Depends()`) to be applied to all the
                *path operations* in this router.

                Read more about it in the
                [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """
            ),
        ] = None,
        default_response_class: Annotated[
            type[Response],
            Doc(
                """
                The default response class to be used.

                Read more in the
                [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
                """
            ),
        ] = Default(JSONResponse),
        responses: Annotated[
            dict[int | str, dict[str, Any]] | None,
            Doc(
                """
                Additional responses to be shown in OpenAPI.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).

                And in the
                [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """
            ),
        ] = None,
        callbacks: Annotated[
            list[BaseRoute] | None,
            Doc(
                """
                OpenAPI callbacks that should apply to all *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
                """
            ),
        ] = None,
        routes: Annotated[
            list[BaseRoute] | None,
            Doc(
                """
                **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.

                ---

                A list of routes to serve incoming HTTP and WebSocket requests.
                """
            ),
            deprecated(
                """
                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
                """
            ),
        ] = None,
        redirect_slashes: Annotated[
            bool,
            Doc(
                """
                Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
                """
            ),
        ] = True,
        default: Annotated[
            ASGIApp | None,
            Doc(
                """
                Default function handler for this router. Used to handle
                404 Not Found errors.
                """
            ),
        ] = None,
        dependency_overrides_provider: Annotated[
            Any | None,
            Doc(
                """
                Only used internally by FastAPI to handle dependency overrides.

                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
                """
            ),
        ] = None,
        route_class: Annotated[
            type[APIRoute],
            Doc(
                """
                Custom route (*path operation*) class to be used by this router.

                Read more about it in the
                [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
                """
            ),
        ] = APIRoute,
        on_startup: Annotated[
            Sequence[Callable[[], Any]] | None,
            Doc(
                """
                A list of startup event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        on_shutdown: Annotated[
            Sequence[Callable[[], Any]] | None,
            Doc(
                """
                A list of shutdown event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        # the generic to Lifespan[AppType] is the type of the top level application
        # which the router cannot know statically, so we use typing.Any
        lifespan: Annotated[
            Lifespan[Any] | None,
            Doc(
                """
                A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        deprecated: Annotated[
            bool | None,
            Doc(
                """
                Mark all *path operations* in this router as deprecated.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc(
                """
                To include (or not) all the *path operations* in this router in the
                generated OpenAPI.

                This affects the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-parameters-from-openapi).
                """
            ),
        ] = True,
        generate_unique_id_function: Annotated[
            Callable[[APIRoute], str],
            Doc(
                """
                Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.

                This is particularly useful when automatically generating clients or
                SDKs for your API.

                Read more about it in the
                [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
                """
            ),
        ] = Default(generate_unique_id),
        # extra parameters
        app_runtime_dependency: AppRuntimeDependencyPort,
        idempotency_config: Optional[RouterIdempotencyConfig] = None,
        idempotency_dependency: Optional[IdempotencyDependencyPort] = None,
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
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            lifespan=lifespan,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function,
        )

        if idempotency_dependency is None:
            self.__idempotency_dependency = None

        else:
            self.__idempotency_dependency = _idempotency_dependency(
                app_runtime_dependency, idempotency_dependency
            )

        self.__idempotency_config = idempotency_config or {}
        self.__idempotency_header_dependency = _idem_header_dependency(
            self.__idempotency_config.get("header_key", IDEMPOTENCY_KEY_HEADER)
        )

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
        response_model_exclude_none: bool = False,  # overriden below by default `True`
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
        # extra parameters
        idempotent: bool = False,
        idempotency_config: Optional[RouteIdempotencyConfig] = None,
    ) -> None:
        idempotency_config = idempotency_config or self.__idempotency_config

        if idempotent and methods and "POST" in methods:
            if operation_id is None:
                raise CoreError("Operation ID is required for idempotent routes")

            hints = get_type_hints(endpoint)
            resp_model = response_model or hints.get("return")

            if resp_model is None or not isinstance(resp_model, type):
                raise CoreError(
                    "Response model or return annotation is required for idempotent routes"
                )

            adapter = TypeAdapter[Any](resp_model)
            status_code = status_code or 200
            dto_name = idempotency_config.get("dto_param") or self.__guess_dto_param(
                endpoint
            )

            endpoint = self.__wrap_idempotent_route(
                endpoint,
                ttl=idempotency_config.get("ttl", timedelta(seconds=30)),
                dto_param=dto_name,
                operation=operation_id,
                adapter=adapter,
                status_code=status_code,
            )

        return super().add_api_route(
            path,
            endpoint,
            response_model=response_model,
            status_code=status_code,
            tags=tags,
            dependencies=dependencies,
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
        sig = inspect.signature(endpoint)

        for name, p in sig.parameters.items():
            ann = p.annotation

            if isinstance(ann, type) and issubclass(ann, BaseModel):
                return name

        raise RuntimeError(  #!? Replace with CoreError or so
            "Cannot infer DTO param for idempotent route; pass it explicitly"
        )

    # ....................... #

    def __wrap_idempotent_route(
        self,
        endpoint: Callable[..., Any],
        *,
        ttl: timedelta,
        dto_param: str,
        operation: str,
        adapter: TypeAdapter[Any],
        status_code: int,
    ):
        if self.__idempotency_dependency is None:
            raise CoreError("Idempotency dependency is not set")

        @wraps(endpoint)
        async def wrapper(
            *args: Any,
            __idem: IdempotencyPort = Dependency(
                partial(self.__idempotency_dependency, ttl=ttl)
            ),
            __idem_key: str = Dependency(self.__idempotency_header_dependency),
            **kwargs: Any,
        ) -> Any:
            dto = kwargs.get(dto_param)

            if not isinstance(dto, BaseModel):
                bound = inspect.signature(endpoint).bind_partial(*args, **kwargs)
                dto = bound.arguments.get(dto_param)

            if not isinstance(dto, BaseModel):
                return await endpoint(*args, **kwargs)

            h = pydantic_model_hash(dto)
            snap = await __idem.begin(operation, __idem_key, h)

            if snap is not None:
                data = orjson.loads(snap["body"])
                return adapter.validate_python(data)

            out = await endpoint(*args, **kwargs)

            body = orjson.dumps(pydantic_dump(out, exclude={"none": True}))
            await __idem.commit(
                operation,
                __idem_key,
                h,
                {
                    "code": status_code,
                    "content_type": "application/json",
                    "body": body,
                },
            )

            return out

        return wrapper
