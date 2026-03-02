from typing import Awaitable, Callable, Protocol, Self

import attrs

from .context import ExecutionContext

# ----------------------- #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]


class Middleware[Args, R](Protocol):  # pragma: no cover
    async def __call__(self, next: NextCall[Args, R], args: Args) -> R: ...


class Effect[Args, R](Protocol):  # pragma: no cover
    async def __call__(self, args: Args, res: R) -> R: ...


class Guard[Args](Protocol):  # pragma: no cover
    async def __call__(self, args: Args) -> None: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardMiddleware[Args, R](Middleware[Args, R]):
    guard: Guard[Args]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        await self.guard(args)
        return await next(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectMiddleware[Args, R](Middleware[Args, R]):
    effect: Effect[Args, R]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        res = await next(args)
        return await self.effect(args, res)


# ....................... #
# Pre-defined middlewares


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxMiddleware[Args, R](Middleware[Args, R]):
    ctx: ExecutionContext
    after_commit: tuple[Effect[Args, R], ...] = attrs.field(factory=tuple)

    # ....................... #

    def with_after_commit(self, *effects: Effect[Args, R]) -> Self:
        return attrs.evolve(self, after_commit=(*self.after_commit, *effects))

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        async with self.ctx.transaction():
            res = await next(args)

        for eff in self.after_commit:
            res = await eff(args, res)

        return res
