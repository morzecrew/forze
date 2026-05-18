"""Factories for :class:`MiddlewareSpec` used by registry stage authoring."""

from typing import Any

from ..context import ExecutionContext
from ..middlewares import (  # type: ignore[import-not-found]
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    OnSuccessMiddleware,
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
        return GuardMiddleware[Any, Any](inner=guard(ctx))

    return factory


# ....................... #


def success_hook_middleware_factory(hook: SuccessHookFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> OnSuccessMiddleware[Any, Any]:
        return OnSuccessMiddleware[Any, Any](inner=hook(ctx))

    return factory


# ....................... #


def finally_middleware_factory(hook: FinallyFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> FinallyMiddleware[Any, Any]:
        return FinallyMiddleware[Any, Any](inner=hook(ctx))

    return factory


# ....................... #


def on_failure_middleware_factory(hook: OnFailureFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> OnFailureMiddleware[Any, Any]:
        return OnFailureMiddleware[Any, Any](inner=hook(ctx))

    return factory
