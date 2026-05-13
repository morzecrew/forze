"""Factories for :class:`MiddlewareSpec` used by :class:`UsecasePlan` builders."""

from typing import Any

from ..context import ExecutionContext
from ..middleware import (
    EffectMiddleware,
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
)
from .types import (
    EffectFactory,
    FinallyFactory,
    GuardFactory,
    MiddlewareFactory,
    OnFailureFactory,
)

# ----------------------- #


def guard_middleware_factory(guard: GuardFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
        return GuardMiddleware[Any, Any](guard=guard(ctx))

    return factory


# ....................... #


def effect_middleware_factory(effect: EffectFactory) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
        return EffectMiddleware[Any, Any](effect=effect(ctx))

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
