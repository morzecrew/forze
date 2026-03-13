from typing import Self

import attrs

from forze.base.logging import getLogger, safe_preview

from .context import ExecutionContext
from .middleware import Middleware, NextCall

# ----------------------- #

logger = getLogger(__name__)

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

    def _build_chain(self) -> NextCall[Args, R]:
        logger.trace(
            "Building middleware chain with %d middleware(s)",
            len(self.middlewares),
        )

        async def last(args: Args) -> R:
            safe_args = safe_preview(args)
            logger.debug("Calling main with args: %s", safe_args)
            return await self.main(args)

        fn: NextCall[Args, R] = last

        for mw in reversed(self.middlewares):
            prev = fn

            logger.trace("Wrapping with middleware %s", type(mw).__qualname__)

            async def wrapped(
                a: Args,
                *,
                _mw: Middleware[Args, R] = mw,
                _prev: NextCall[Args, R] = prev,
            ) -> R:
                logger.debug("Calling middleware %s", type(mw).__qualname__)
                return await _mw(_prev, a)

            fn = wrapped

        return fn

    # ....................... #

    async def __call__(self, args: Args) -> R:
        """Execute the usecase with the configured middlewares.

        Builds the middleware chain on first call and caches it for reuse.
        """

        with logger.contextualize(scope=type(self).__qualname__):
            logger.debug("Starting usecase execution")

            with logger.section():
                chain = self._build_chain()
                result = await chain(args)

            logger.debug("Usecase execution completed")

        return result

    # ....................... #
    # Convenient methods

    def log_delegation(self, target: object) -> None:
        logger.debug("Delegating to %s", type(target).__qualname__)
