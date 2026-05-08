"""Middleware protocols and implementations for usecase chains.

Provides :class:`Middleware`, :class:`Guard`, :class:`Effect` protocols and
concrete implementations: :class:`GuardMiddleware`, :class:`EffectMiddleware`,
:class:`OnFailureMiddleware`, :class:`FinallyMiddleware`, :class:`TxMiddleware`.
Conditional helpers :class:`ConditionalGuard`, :class:`ConditionalEffect`,
:class:`WhenGuard`, and :class:`WhenEffect` implement optional predicates without
changing the middleware chain. Middlewares wrap usecases in a chain; guards
run before, effects after.
"""

from enum import StrEnum
from typing import Awaitable, Callable, Protocol, Self, final

import attrs

from forze.application._logger import logger

from .context import ExecutionContext

# ----------------------- #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or usecase in the chain."""

# ....................... #


class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain.

    Receives the next callable and args; may run logic before or after
    invoking next. Order is outermost-first (first added runs first).
    """

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        """Invoke the middleware with the next callable and args."""
        ...


# ....................... #


class Effect[Args, R](Protocol):  # pragma: no cover
    """Protocol for an effect that runs after the usecase returns.

    Receives args and result; may transform or side-effect the result.
    """

    async def __call__(self, args: Args, res: R) -> R:
        """Run the effect with args and result; may return modified result."""
        ...


# ....................... #


class Guard[Args](Protocol):  # pragma: no cover
    """Protocol for a guard that runs before the usecase.

    Receives args; may raise to abort or return to proceed.
    """

    async def __call__(self, args: Args) -> None:
        """Validate or authorize; raises to abort the chain."""
        ...


# ....................... #


class ConditionalGuard[Args](Guard[Args]):
    """Guard that runs :meth:`main` only when :meth:`condition` is true.

    For a cheap synchronous predicate on *args*, override :meth:`condition`.
    Implement :meth:`main` with the real validation or authorization logic.

    Satisfies :class:`Guard` structurally via :meth:`__call__`.
    """

    def condition(self, args: Args) -> bool:
        return True

    # ....................... #

    async def main(self, args: Args) -> None:
        """Run when :meth:`condition` is true; may raise to abort."""

        raise NotImplementedError

    # ....................... #

    async def __call__(self, args: Args) -> None:
        if self.condition(args):
            await self.main(args)


# ....................... #


class ConditionalEffect[Args, R](Effect[Args, R]):
    """Effect that runs :meth:`main` only when :meth:`condition` is true.

    When the condition is false, the incoming result is returned unchanged.

    Satisfies :class:`Effect` structurally via :meth:`__call__`.
    """

    def condition(self, args: Args, res: R) -> bool:
        return True

    # ....................... #

    async def main(self, args: Args, res: R) -> R:
        """Run when :meth:`condition` is true; return the (possibly updated) result."""

        raise NotImplementedError

    # ....................... #

    async def __call__(self, args: Args, res: R) -> R:
        if self.condition(args, res):
            return await self.main(args, res)
        return res


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class WhenGuard[Args](Guard[Args]):
    """Wrap a :class:`Guard` and invoke it only when ``when(args)`` is true.

    Use this at wiring time to reuse an existing guard under a predicate.
    Subclass :class:`ConditionalGuard` when the condition is intrinsic to one
    guard type.
    """

    guard: Guard[Args]
    when: Callable[[Args], bool]

    # ....................... #

    async def __call__(self, args: Args) -> None:
        if self.when(args):
            await self.guard(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class WhenEffect[Args, R](Effect[Args, R]):
    """Wrap an :class:`Effect` and invoke it only when ``when(args, res)`` is true.

    When the predicate is false, *res* is returned unchanged.
    """

    effect: Effect[Args, R]
    when: Callable[[Args, R], bool]

    # ....................... #

    async def __call__(self, args: Args, res: R) -> R:
        if self.when(args, res):
            return await self.effect(args, res)

        return res


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Successful[R_co]:
    """Successful usecase outcome passed to :class:`Finally` hooks."""

    value: R_co


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Failed:
    """Failed usecase outcome passed to :class:`Finally` hooks."""

    exc: Exception


# ....................... #

type UsecaseOutcome[R] = Successful[R] | Failed
"""Discriminated outcome for :class:`Finally` middleware."""

# ....................... #


class OnFailure[Args](Protocol):  # pragma: no cover
    """Hook invoked when an :class:`Exception` escapes the inner chain.

    Does not run for :class:`BaseException` subclasses such as
    :class:`KeyboardInterrupt`. The original exception is re-raised after the
    hook returns unless the hook raises.
    """

    async def __call__(self, args: Args, exc: Exception) -> None:
        """Handle failure; may raise to replace the error."""
        ...


# ....................... #


class Finally[Args, R](Protocol):  # pragma: no cover
    """Hook invoked after the inner chain finishes (success or :class:`Exception`).

    Receives a :class:`Successful` or :class:`Failed` outcome. On failure, runs
    after any inner :class:`OnFailureMiddleware` hooks on that path. Does not
    run for :class:`BaseException` subclasses escaping the inner chain.
    """

    async def __call__(
        self,
        args: Args,
        outcome: Successful[R] | Failed,  # noqa: F841
    ) -> None:
        """Observe completion; may raise."""
        ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a guard before invoking the next call."""

    guard: Guard[Args]
    """Guard to run before the next call."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug("Running guard: '%s'", type(self.guard).__qualname__)
        await self.guard(args)

        result = await next(args)

        return result


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs an effect after the next call returns."""

    effect: Effect[Args, R]
    """Effect to run after the next call."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        res = await next(args)

        logger.debug("Running effect: '%s'", type(self.effect).__qualname__)
        res = await self.effect(args, res)

        return res


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OnFailureMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a hook when the inner chain raises :class:`Exception`."""

    hook: OnFailure[Args]
    """Called with the exception before it is re-raised."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        try:
            return await next(args)

        except Exception as exc:
            logger.debug("Running on_failure: '%s'", type(self.hook).__qualname__)
            await self.hook(args, exc)

            raise


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FinallyMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a hook after success or handled failure of the inner chain."""

    hook: Finally[Args, R]
    """Called with :class:`Successful` or :class:`Failed` for each completion."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        try:
            res = await next(args)

        except Exception as exc:
            logger.debug(
                "Running finally (failure path): '%s'",
                type(self.hook).__qualname__,
            )
            await self.hook(args, Failed(exc=exc))

            raise

        logger.debug(
            "Running finally (success path): '%s'",
            type(self.hook).__qualname__,
        )
        await self.hook(args, Successful(value=res))

        return res


# ....................... #
# Pre-defined middlewares


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TxMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that wraps the next call in a transaction.

    Enters :meth:`ExecutionContext.transaction` before invoking next. After a
    successful root commit, callbacks queued via :meth:`~ExecutionContext.defer_after_commit`
    run (inside :meth:`~ExecutionContext.transaction`, FIFO) before this
    middleware runs :attr:`after_commit` effects.
    """

    ctx: ExecutionContext
    """Execution context for the transaction."""

    route: str | StrEnum
    """Routing key for the transaction."""

    after_commit: tuple[Effect[Args, R], ...] = attrs.field(factory=tuple)
    """Effects to run after commit (e.g. outbox dispatch)."""

    # ....................... #

    def with_after_commit(self, *effects: Effect[Args, R]) -> Self:
        """Return a new middleware with additional after-commit effects.

        :param effects: Effects to append.
        :returns: New middleware instance.
        """

        logger.trace(
            "Appending %s after-commit effect(s) to %s",
            len(effects),
            type(self).__qualname__,
        )

        return attrs.evolve(self, after_commit=(*self.after_commit, *effects))

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug("Running transaction middleware: '%s'", type(self).__qualname__)

        async with self.ctx.transaction(self.route):
            res = await next(args)

        if self.after_commit:
            for eff in self.after_commit:
                logger.debug("Running after-commit effect '%s'", type(eff).__qualname__)
                res = await eff(args, res)

        return res
