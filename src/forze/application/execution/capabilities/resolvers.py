"""Resolve guard/effect callables from scheduled specs."""

from typing import Any

from forze.base.errors import CoreError

from ..context import ExecutionContext
from ..middleware import Effect, EffectMiddleware, Guard, GuardMiddleware
from ..plan.spec import MiddlewareSpec

# ----------------------- #


def resolve_guard_steps(
    ctx: ExecutionContext,
    specs: tuple[MiddlewareSpec, ...],
    *,
    bucket: str,
) -> tuple[tuple[Guard[Any], MiddlewareSpec], ...]:
    out: list[tuple[Guard[Any], MiddlewareSpec]] = []

    for spec in specs:
        mw = spec.factory(ctx)

        if not isinstance(mw, GuardMiddleware):
            raise CoreError(
                f"Expected GuardMiddleware in capability bucket {bucket!r}, got {type(mw)}"
            )

        out.append(
            (
                mw.guard,  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
                spec,
            )
        )

    return tuple(out)


# ....................... #


def resolve_effect_steps(
    ctx: ExecutionContext,
    specs: tuple[MiddlewareSpec, ...],
    *,
    bucket: str,
) -> tuple[tuple[Effect[Any, Any], MiddlewareSpec], ...]:
    out: list[tuple[Effect[Any, Any], MiddlewareSpec]] = []

    for spec in specs:
        mw = spec.factory(ctx)

        if not isinstance(mw, EffectMiddleware):
            raise CoreError(
                f"Expected EffectMiddleware in capability bucket {bucket!r}, got {type(mw)}"
            )

        out.append(
            (
                mw.effect,  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
                spec,
            )
        )

    return tuple(out)
