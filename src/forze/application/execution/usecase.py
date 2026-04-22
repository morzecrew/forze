from __future__ import annotations

from typing import Any, Protocol, Self, final, runtime_checkable

import attrs

from forze.application._logger import logger
from forze.base.errors import CoreError

from .context import ExecutionContext
from .middleware import (
    EffectMiddleware,
    FinallyMiddleware,
    GuardMiddleware,
    Middleware,
    NextCall,
    OnFailureMiddleware,
)

# ----------------------- #


@runtime_checkable
class UsecaseFactory(Protocol):
    """Factory for building usecases."""

    def __call__(self, ctx: ExecutionContext) -> Usecase[Any, Any]:
        """Build a usecase from the execution context."""

        ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Usecase[Args, R]:
    """Base class for asynchronous application usecases.

    Subclasses implement :meth:`main`. The ``middlewares`` tuple is built by
    :class:`~forze.application.execution.plan.UsecasePlan` so that priority and
    pipeline list order have consistent semantics; see :meth:`_build_chain` for
    how that tuple is wrapped. Invoke via :meth:`__call__`.
    """

    ctx: ExecutionContext
    """Execution context for resolving ports and transactions."""

    middlewares: tuple[Middleware[Args, R], ...] = attrs.field(factory=tuple)
    """Wrapping middlewares, outer-to-inner in resolve order; see :meth:`_build_chain`."""

    operation_id: str | None = attrs.field(default=None)
    """The operation id assigned to the usecase."""

    # ....................... #

    @final
    def with_operation_id(self, operation_id: str) -> Self:
        """Set an operation id for the usecase.

        :param operation_id: The operation id to assign.
        :returns: New usecase instance.
        """

        if self.operation_id:
            raise CoreError("Operation id already set")

        if not operation_id:
            raise CoreError("Operation id cannot be empty")

        return attrs.evolve(self, operation_id=operation_id)

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
            "Appending %s middleware(s) to usecase '%s'",
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
            "Building middleware chain with %s middleware(s)",
            len(self.middlewares),
        )

        async def last(args: Args) -> R:
            logger.debug("Calling main")

            return await self.main(args)

        fn: NextCall[Args, R] = last

        for mw in reversed(self.middlewares):
            prev = fn

            if isinstance(mw, GuardMiddleware):
                qualname = type(mw.guard).__qualname__

            elif isinstance(mw, EffectMiddleware):
                qualname = type(mw.effect).__qualname__

            elif isinstance(mw, OnFailureMiddleware):
                qualname = type(mw.hook).__qualname__

            elif isinstance(mw, FinallyMiddleware):
                qualname = type(mw.hook).__qualname__

            else:
                qualname = type(mw).__qualname__

            logger.trace("Wrapping with '%s'", qualname)

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

        logger.debug("Starting usecase execution")

        chain = self._build_chain()
        result = await chain(args)

        logger.debug("Usecase execution completed")

        return result
