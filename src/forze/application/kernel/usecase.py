from typing import Protocol, Self, override

import attrs

from .ports import AppRuntimePort

# ----------------------- #


class Effect[Args, R](Protocol):
    async def __call__(self, args: Args, res: R) -> R: ...


class Guard[Args](Protocol):
    async def __call__(self, args: Args) -> None: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Usecase[Args, R]:
    """Usecase base class."""

    runtime: AppRuntimePort
    """Application runtime."""

    guards: tuple[Guard[Args], ...] = ()
    """Guards to run before the usecase."""

    effects: tuple[Effect[Args, R], ...] = ()
    """Effects to run after the usecase."""

    # ....................... #

    def with_effects(self, *effects: Effect[Args, R]) -> Self:
        return attrs.evolve(self, effects=(*self.effects, *effects))

    # ....................... #

    def with_guards(self, *guards: Guard[Args]) -> Self:
        return attrs.evolve(self, guards=(*self.guards, *guards))

    # ....................... #

    async def main(self, args: Args) -> R:
        raise NotImplementedError

    # ....................... #

    async def _run_guards(self, args: Args) -> None:
        for guard in self.guards:
            await guard(args)

    # ....................... #

    async def _run_effects(self, args: Args, res: R) -> R:
        for effect in self.effects:
            res = await effect(args, res)

        return res

    # ....................... #

    async def __call__(self, args: Args) -> R:
        await self._run_guards(args)
        res = await self.main(args)
        return await self._run_effects(args, res)


# ....................... #
# ? Composition ?#


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxUsecase[Args, R](Usecase[Args, R]):
    """Transactional usecase base class."""

    side_guards: tuple[Guard[Args], ...] = ()
    """Guards to run before the usecase outside the transaction."""

    side_effects: tuple[Effect[Args, R], ...] = ()
    """Effects to run after the usecase outside the transaction."""

    # ....................... #

    def with_side_effects(self, *effects: Effect[Args, R]) -> Self:
        return attrs.evolve(self, side_effects=(*self.side_effects, *effects))

    # ....................... #

    def with_side_guards(self, *guards: Guard[Args]) -> Self:
        return attrs.evolve(self, side_guards=(*self.side_guards, *guards))

    # ....................... #

    async def _run_side_guards(self, args: Args) -> None:
        for guard in self.side_guards:
            await guard(args)

    # ....................... #

    async def _run_side_effects(self, args: Args, res: R) -> R:
        for effect in self.side_effects:
            res = await effect(args, res)

        return res

    # ....................... #

    @override
    async def __call__(self, args: Args) -> R:
        await self._run_side_guards(args)

        async with self.runtime.transaction():
            await self._run_guards(args)
            res = await self.main(args)
            final_res = await self._run_effects(args, res)

        await self._run_side_effects(args, final_res)

        return final_res
