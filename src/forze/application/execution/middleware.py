"""Middleware protocols and implementations for usecase chains.

Provides :class:`Middleware`, :class:`Guard`, :class:`Effect` protocols and
concrete implementations: :class:`GuardMiddleware`, :class:`EffectMiddleware`,
:class:`TxMiddleware`. Middlewares wrap usecases in a chain; guards run before,
effects after.
"""

import logging
from typing import Awaitable, Callable, Protocol, Self

import attrs

from forze.base.logging import log_section

from .context import ExecutionContext

# ----------------------- #

logger = logging.getLogger(__name__)

# ....................... #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or usecase in the chain."""


class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain.

    Receives the next callable and args; may run logic before or after
    invoking next. Order is outermost-first (first added runs first).
    """

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        """Invoke the middleware with the next callable and args."""
        ...


class Effect[Args, R](Protocol):  # pragma: no cover
    """Protocol for an effect that runs after the usecase returns.

    Receives args and result; may transform or side-effect the result.
    """

    async def __call__(self, args: Args, res: R) -> R:
        """Run the effect with args and result; may return modified result."""
        ...


class Guard[Args](Protocol):  # pragma: no cover
    """Protocol for a guard that runs before the usecase.

    Receives args; may raise to abort or return to proceed.
    """

    async def __call__(self, args: Args) -> None:
        """Validate or authorize; raises to abort the chain."""
        ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a guard before invoking the next call."""

    guard: Guard[Args]
    """Guard to run before the next call."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug("Entering guard middleware %s", type(self.guard).__qualname__)

        with log_section():
            await self.guard(args)
            logger.debug("Guard %s passed", type(self.guard).__qualname__)
            result = await next(args)

        logger.debug("Leaving guard middleware %s", type(self.guard).__qualname__)

        return result


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs an effect after the next call returns."""

    effect: Effect[Args, R]
    """Effect to run after the next call."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug("Entering effect middleware %s", type(self.effect).__qualname__)

        with log_section():
            res = await next(args)
            logger.debug("Running effect %s", type(self.effect).__qualname__)
            res = await self.effect(args, res)

        logger.debug("Leaving effect middleware %s", type(self.effect).__qualname__)

        return res


# ....................... #
# Pre-defined middlewares


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that wraps the next call in a transaction.

    Enters :meth:`ExecutionContext.transaction` before invoking next; runs
    :attr:`after_commit` effects after a successful commit.
    """

    ctx: ExecutionContext
    """Execution context for the transaction."""

    after_commit: tuple[Effect[Args, R], ...] = attrs.field(factory=tuple)
    """Effects to run after commit (e.g. outbox dispatch)."""

    # ....................... #

    def with_after_commit(self, *effects: Effect[Args, R]) -> Self:
        """Return a new middleware with additional after-commit effects.

        :param effects: Effects to append.
        :returns: New middleware instance.
        """

        logger.debug(
            "Appending %d after-commit effect(s) to %s",
            len(effects),
            type(self).__qualname__,
        )

        return attrs.evolve(self, after_commit=(*self.after_commit, *effects))

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug(
            "Entering transaction middleware with %d after-commit effect(s)",
            len(self.after_commit),
        )

        with log_section():
            async with self.ctx.transaction():
                res = await next(args)

            if self.after_commit:
                logger.debug(
                    "Running %d after-commit effect(s)", len(self.after_commit)
                )

                with log_section():
                    for eff in self.after_commit:
                        logger.debug(
                            "Running after-commit effect %s",
                            type(eff).__qualname__,
                        )
                        res = await eff(args, res)

        logger.debug("Leaving transaction middleware")

        return res
