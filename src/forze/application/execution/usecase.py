import logging
from typing import Self

import attrs

from forze.base.logging import log_section

from .context import ExecutionContext
from .middleware import Middleware, NextCall

# ----------------------- #

logger = logging.getLogger(__name__)

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

    _chain: NextCall[Args, R] | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
        alias="_chain",
    )

    # ....................... #

    def with_middlewares(self, *middlewares: Middleware[Args, R]) -> Self:
        """Return a new usecase with additional middlewares appended.

        :param middlewares: Middlewares to append.
        :returns: New usecase instance.
        """
        if not middlewares:
            return self

        logger.debug(
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
        logger.debug(
            "Building middleware chain for %s with %d middleware(s)",
            type(self).__qualname__,
            len(self.middlewares),
        )

        async def last(args: Args) -> R:
            logger.debug("Calling main() of %s", type(self).__qualname__)

            with log_section():
                return await self.main(args)

        fn: NextCall[Args, R] = last

        for mw in reversed(self.middlewares):
            prev = fn

            logger.debug(
                "Wrapping %s with middleware %s",
                type(self).__qualname__,
                type(mw).__qualname__,
            )

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

        Builds the middleware chain on first call and caches it for reuse.
        """

        logger.debug("Calling usecase %s", type(self).__qualname__)

        with log_section():
            chain = self._chain

            if chain is None:
                logger.debug("Middleware chain is not built; building now")
                chain = self._build_chain()
                object.__setattr__(self, "_chain", chain)

            else:
                logger.debug("Reusing cached middleware chain")

            result = await chain(args)

            logger.debug("Usecase %s completed", type(self).__qualname__)

        return result
