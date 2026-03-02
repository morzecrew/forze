from typing import Self

import attrs

from .context import ExecutionContext
from .middleware import Middleware, NextCall

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Usecase[Args, R]:
    """Base class for asynchronous application usecases.

    Subclasses implement :meth:`main`. Middlewares wrap the usecase in a chain
    (guards run before, effects after; order is reversed so middlewares added
    first run outermost). Invoke via :meth:`__call__` to run the full chain.
    """

    ctx: ExecutionContext
    """Execution context for resolving ports and transactions."""

    middlewares: tuple[Middleware[Args, R], ...] = attrs.field(factory=tuple)
    """Middlewares wrapping the usecase; first added runs outermost."""

    # ....................... #

    def with_middlewares(self, *middlewares: Middleware[Args, R]) -> Self:
        """Return a new usecase with additional middlewares appended.

        :param middlewares: Middlewares to append.
        :returns: New usecase instance.
        """
        if not middlewares:
            return self

        return attrs.evolve(self, middlewares=(*self.middlewares, *middlewares))

    # ....................... #

    async def main(self, args: Args) -> R:
        """Main implementation of the usecase.

        Subclasses must override this method to implement their behavior.
        """
        raise NotImplementedError

    # ....................... #

    def _build_chain(self):
        async def last(args: Args) -> R:
            return await self.main(args)

        fn: NextCall[Args, R] = last

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
        """Execute the usecase with the configured middlewares.

        Builds the middleware chain and runs it with the given args.
        """
        chain = self._build_chain()

        return await chain(args)
