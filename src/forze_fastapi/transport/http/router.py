"""Forze HTTP router, route class, and route registration decorator."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Literal, Sequence, TypeVar

from fastapi import APIRouter
from fastapi.routing import APIRoute

from forze.base.primitives import StrKey
from forze_fastapi.transport.http.policies import Policy, merge_policies

# ----------------------- #

_F = TypeVar("_F", bound=Callable[..., Any])

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

# ....................... #


class ForzeAPIRoute(APIRoute):
    """Extension point for response-capturing policies (idempotency, ETag).

    PR2 delegates to the default handler unchanged. A later PR may wrap
    ``get_route_handler()`` using :attr:`forze_policies`.
    """

    def __init__(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        forze_policies: tuple[Policy, ...] = (),
        **kwargs: Any,
    ) -> None:
        self.forze_policies = forze_policies
        super().__init__(path, endpoint, **kwargs)

    def get_route_handler(self) -> Callable[..., Any]:
        # TODO(PR4+): compose policy wrappers around the base handler here.
        return super().get_route_handler()


# ....................... #


class ForzeRouter(APIRouter):
    """APIRouter with Forze policy defaults and :meth:`forze_route` registration."""

    def __init__(
        self,
        *,
        policies: Sequence[Policy] = (),
        route_class: type[APIRoute] = ForzeAPIRoute,
        **kwargs: Any,
    ) -> None:
        self._forze_policies: tuple[Policy, ...] = tuple(policies)
        merged = merge_policies(self._forze_policies)

        existing_deps = list(kwargs.pop("dependencies", None) or [])
        existing_deps.extend(merged.dependencies)
        kwargs["dependencies"] = existing_deps

        super().__init__(route_class=route_class, **kwargs)

    # ....................... #

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        forze_policies: tuple[Policy, ...] = (),
        route_class: type[APIRoute] | None = None,
        **kwargs: Any,
    ) -> None:
        """Register a route, passing ``forze_policies`` into :class:`ForzeAPIRoute`."""

        resolved_class = route_class or self.route_class
        if resolved_class is ForzeAPIRoute:
            route_kwargs = dict(kwargs)
            route_kwargs.pop("route_class_override", None)
            route = ForzeAPIRoute(
                self.prefix + path,
                endpoint,
                forze_policies=forze_policies,
                **route_kwargs,
            )
            self.routes.append(route)
            return

        super().add_api_route(
            path,
            endpoint,
            route_class_override=resolved_class,
            **kwargs,
        )

    # ....................... #

    def forze_route(
        self,
        method: HttpMethod,
        path: str,
        *,
        operation: StrKey | None = None,
        policies: Sequence[Policy] = (),
        response_model: Any = None,
        status_code: int | None = None,
        include_in_schema: bool = True,
        **route_kwargs: Any,
    ) -> Callable[[_F], _F]:
        """Register a function-first route with merged Forze policies."""

        def decorator(endpoint: _F) -> _F:
            merged = merge_policies(self._forze_policies, policies)
            route_class = merged.route_class or ForzeAPIRoute

            per_route_deps = list(merged.dependencies)
            openapi_extra = dict(merged.openapi_extra) if merged.openapi_extra else None

            op_id = str(operation) if operation is not None else endpoint.__name__

            if endpoint.__doc__ is None and operation is not None:
                endpoint.__doc__ = f"Operation ``{operation}``."

            add_kwargs: dict[str, Any] = {
                "path": path,
                "endpoint": endpoint,
                "methods": [method],
                "response_model": response_model,
                "status_code": status_code,
                "include_in_schema": include_in_schema,
                "operation_id": op_id,
                "dependencies": per_route_deps,
                "route_class_override": route_class,
            }

            if openapi_extra:
                add_kwargs["openapi_extra"] = openapi_extra

            add_kwargs.update(route_kwargs)

            if route_class is ForzeAPIRoute:
                self.add_api_route(
                    **add_kwargs,
                    forze_policies=merged.policies,
                )
            else:
                self.add_api_route(**add_kwargs)

            return endpoint

        return decorator


# ....................... #


def forze_route(
    method: HttpMethod,
    path: str,
    *,
    router: ForzeRouter,
    operation: StrKey | None = None,
    policies: Sequence[Policy] = (),
    response_model: Any = None,
    status_code: int | None = None,
    include_in_schema: bool = True,
    **route_kwargs: Any,
) -> Callable[[_F], _F]:
    """Register a route on *router* (alias for :meth:`ForzeRouter.forze_route`)."""

    return router.forze_route(
        method,
        path,
        operation=operation,
        policies=policies,
        response_model=response_model,
        status_code=status_code,
        include_in_schema=include_in_schema,
        **route_kwargs,
    )
