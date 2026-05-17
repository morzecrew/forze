"""Factories for :class:`MiddlewareSpec` used by registry stage authoring."""

from typing import Any

from ..context import ExecutionContext
from ..middleware import (  # type: ignore[import-not-found]
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    SuccessHookMiddleware,
)
from .types import (  # type: ignore[import-not-found]
    FinallyFactory,
    GuardFactory,
    MiddlewareFactory,
    OnFailureFactory,
    SuccessHookFactory,
)

# ----------------------- #


def guard_middleware_factory(guard: GuardFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
        return GuardMiddleware[Any, Any](guard=guard(ctx))

    return factory


# ....................... #


def success_hook_middleware_factory(hook: SuccessHookFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> SuccessHookMiddleware[Any, Any]:
        return SuccessHookMiddleware[Any, Any](hook=hook(ctx))

    return factory


# ....................... #


def finally_middleware_factory(hook: FinallyFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> FinallyMiddleware[Any, Any]:
        return FinallyMiddleware[Any, Any](hook=hook(ctx))

    return factory


# ....................... #


def on_failure_middleware_factory(hook: OnFailureFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> OnFailureMiddleware[Any, Any]:
        return OnFailureMiddleware[Any, Any](hook=hook(ctx))

    return factory
