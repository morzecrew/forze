from __future__ import annotations

from typing import Awaitable, Callable, Protocol, Self, override

import attrs

from ..contracts.tx import TxManagerPort
from .context import ExecutionContext

# ----------------------- #

#! most likely ctx should be part of effect and guard


class Effect[Args, R](Protocol):  # pragma: no cover
    """Effect to run after the usecase execution.

    Effects can transform the result or perform asynchronous side effects
    (logging, auditing, indexing, etc.) based on the input arguments and the
    produced result.
    """

    async def __call__(self, args: Args, res: R) -> R: ...


# ....................... #


class Guard[Args](Protocol):  # pragma: no cover
    """Guard to run before the usecase execution.

    Guards are responsible for validating or enriching arguments before the
    main usecase logic runs. They may raise exceptions on failure.
    """

    async def __call__(self, args: Args) -> None: ...


# ....................... #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]


class Middleware[Args, R](Protocol):  # pragma: no cover
    async def __call__(self, next: NextCall[Args, R], args: Args) -> R: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Usecase[Args, R]:
    """Base class for asynchronous application usecases.

    A usecase encapsulates business logic and is invoked as an async callable.
    It can be decorated with guards and effects that run before and after the
    main execution, respectively.
    """

    guards: tuple[Guard[Args], ...] = attrs.field(factory=tuple)
    """Guards to run before the usecase."""

    middlewares: tuple[Middleware[Args, R], ...] = attrs.field(factory=tuple)
    """Middlewares to run before the usecase."""

    effects: tuple[Effect[Args, R], ...] = attrs.field(factory=tuple)
    """Effects to run after the usecase."""

    # ....................... #

    def with_middlewares(self, *middlewares: Middleware[Args, R]) -> Self:
        """Return a new usecase with additional middlewares appended."""

        if not middlewares:
            return self

        return attrs.evolve(self, middlewares=(*self.middlewares, *middlewares))

    # ....................... #

    def with_effects(self, *effects: Effect[Args, R]) -> Self:
        """Return a new usecase with additional effects appended."""

        if not effects:
            return self

        return attrs.evolve(self, effects=(*self.effects, *effects))

    # ....................... #

    def with_guards(self, *guards: Guard[Args]) -> Self:
        """Return a new usecase with additional guards appended."""

        if not guards:
            return self

        return attrs.evolve(self, guards=(*self.guards, *guards))

    # ....................... #

    async def main(self, args: Args) -> R:
        """Main implementation of the usecase.

        Subclasses must override this method to implement their behavior.
        """

        raise NotImplementedError

    # ....................... #

    async def _run_guards(self, args: Args) -> None:
        """Run guards before the usecase execution."""

        for guard in self.guards:
            await guard(args)

    # ....................... #

    async def _run_effects(self, args: Args, res: R) -> R:
        """Run effects after the usecase execution."""

        for effect in self.effects:
            res = await effect(args, res)

        return res

    # ....................... #

    async def _core(self, args: Args) -> R:
        """Execute the core of the usecase with the configured guards and effects."""

        await self._run_guards(args)
        res = await self.main(args)
        return await self._run_effects(args, res)

    # ....................... #

    def _build_chain(self):
        async def last(args: Args) -> R:
            return await self._core(args)

        fn: NextCall[Args, R] = last

        #! TODO: check order, because we sort middlewares by priority in descending order
        #! in the usecase plan
        for mw in reversed(self.middlewares):
            prev = fn

            async def wrapped(
                a: Args,
                *,
                _mw: Middleware[Args, R] = mw,
                _prev: NextCall[Args, R] = prev,
            ) -> R:
                return await _mw(_prev, a)

            fn = wrapped

        return fn

    # ....................... #

    async def __call__(self, args: Args) -> R:
        """Execute the usecase with the configured guards and effects."""

        chain = self._build_chain()

        return await chain(args)


# ....................... #
#! Need to add transaction options attribute for this generic with default parameters


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxUsecase[Args, R](Usecase[Args, R]):
    """Usecase that runs within a transaction boundary.

    Transactional usecases support both "inner" guards/effects executed
    inside the transaction and "side" variants that run outside the
    transaction (e.g. integration events, notifications).
    """

    ctx: ExecutionContext
    """Execution context to use for the usecase."""

    txmanager: TxManagerPort
    """Transaction manager to use for the usecase."""

    side_guards: tuple[Guard[Args], ...] = attrs.field(factory=tuple)
    """Guards to run before the usecase outside the transaction."""

    side_effects: tuple[Effect[Args, R], ...] = attrs.field(factory=tuple)
    """Effects to run after the usecase outside the transaction."""

    # ....................... #

    def with_side_effects(self, *effects: Effect[Args, R]) -> Self:
        """Return a new usecase with additional side effects appended."""

        if not effects:
            return self

        return attrs.evolve(self, side_effects=(*self.side_effects, *effects))

    # ....................... #

    def with_side_guards(self, *guards: Guard[Args]) -> Self:
        """Return a new usecase with additional side guards appended."""

        if not guards:
            return self

        return attrs.evolve(self, side_guards=(*self.side_guards, *guards))

    # ....................... #

    async def _run_side_guards(self, args: Args) -> None:
        """Run side guards before the usecase execution outside the transaction."""

        for guard in self.side_guards:
            await guard(args)

    # ....................... #

    async def _run_side_effects(self, args: Args, res: R) -> R:
        """Run side effects after the usecase execution outside the transaction."""

        for effect in self.side_effects:
            res = await effect(args, res)

        return res

    # ....................... #

    @override
    async def _core(self, args: Args) -> R:
        """Execute the core of the usecase inside a transaction with side hooks."""

        await self._run_side_guards(args)

        async with self.ctx.transaction(self.txmanager):
            await self._run_guards(args)
            res = await self.main(args)
            final_res = await self._run_effects(args, res)

        await self._run_side_effects(args, final_res)

        return final_res
