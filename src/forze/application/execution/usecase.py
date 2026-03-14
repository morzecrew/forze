from typing import Self, final

import attrs

from forze.base.logging import getLogger

from .context import ExecutionContext
from .middleware import EffectMiddleware, GuardMiddleware, Middleware, NextCall

# ----------------------- #

logger = getLogger(__name__).bind(scope="usecase")

# ....................... #


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

    @final
    def with_middlewares(self, *middlewares: Middleware[Args, R]) -> Self:
        """Return a new usecase with additional middlewares appended.

        :param middlewares: Middlewares to append.
        :returns: New usecase instance.
        """
        if not middlewares:
            return self

        logger.trace(
            "Appending %d middleware(s) to usecase %s",
            len(middlewares),
            type(self).__qualname__,
        )

        return attrs.evolve(self, middlewares=(*self.middlewares, *middlewares))

    # ....................... #

    async def main(self, args: Args) -> R:
        """Main implementation of the usecase.

        Subclasses must override this method to implement their behavior.
        """

        raise NotImplementedError

    # ....................... #

    @final
    def _build_chain(self) -> NextCall[Args, R]:
        logger.trace(
            "Building middleware chain with %d middleware(s)",
            len(self.middlewares),
        )

        async def last(args: Args) -> R:
            logger.debug("Calling main: %s", type(self).__qualname__)

            with logger.section():
                return await self.main(args)

        fn: NextCall[Args, R] = last

        with logger.section():
            for mw in reversed(self.middlewares):
                prev = fn

                if isinstance(mw, GuardMiddleware):
                    qualname = type(mw.guard).__qualname__

                elif isinstance(mw, EffectMiddleware):
                    qualname = type(mw.effect).__qualname__

                else:
                    qualname = type(mw).__qualname__

                logger.trace("Wrapping with %s", qualname)

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

    @final
    async def __call__(self, args: Args) -> R:
        """Execute the usecase with the configured middlewares.

        Builds the middleware chain on first call and caches it for reuse.
        """

        logger.debug("Starting usecase execution: %s", type(self).__qualname__)

        with logger.section():
            chain = self._build_chain()
            result = await chain(args)

        logger.debug("Usecase execution completed: %s", type(self).__qualname__)

        return result
