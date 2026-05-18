from typing import TYPE_CHECKING, Any

import attrs

from forze.application._logger import logger

from .protocols import (
    Finally,
    FinallyFactory,
    Guard,
    GuardFactory,
    Middleware,
    MiddlewareFactory,
    NextCall,
    OnFailure,
    OnFailureFactory,
    OnSuccess,
    OnSuccessFactory,
    validate_guard_output,
    validate_success_hook_output,
)
from .value_objects import Failure, Success

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a guard before invoking the next call."""

    inner: Guard[Args]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        logger.debug("Running guard: '%s'", type(self.inner).__qualname__)
        out = await self.inner(args)

        validate_guard_output(out)

        return await next(args)

    # ....................... #

    @classmethod
    def mw_factory(cls, inner: GuardFactory) -> MiddlewareFactory:
        def factory(ctx: "ExecutionContext") -> GuardMiddleware[Any, Any]:
            return GuardMiddleware(inner=inner(ctx))

        return factory


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OnSuccessMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a success hook after the next call."""

    inner: OnSuccess[Args, R]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        result = await next(args)

        logger.debug("Running on success: '%s'", type(self.inner).__qualname__)
        out = await self.inner(args, result)

        validate_success_hook_output(out)

        return result

    # ....................... #

    @classmethod
    def mw_factory(cls, inner: OnSuccessFactory) -> MiddlewareFactory:
        def factory(ctx: "ExecutionContext") -> OnSuccessMiddleware[Any, Any]:
            return OnSuccessMiddleware(inner=inner(ctx))

        return factory


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OnFailureMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a failure hook after the next call."""

    inner: OnFailure[Args]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        try:
            return await next(args)

        except Exception as exc:
            logger.debug("Running on failure: '%s'", type(self.inner).__qualname__)
            await self.inner(args, exc)
            raise

    # ....................... #

    @classmethod
    def mw_factory(cls, inner: OnFailureFactory) -> MiddlewareFactory:
        def factory(ctx: "ExecutionContext") -> OnFailureMiddleware[Any, Any]:
            return OnFailureMiddleware(inner=inner(ctx))

        return factory


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FinallyMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that runs a finally hook after the next call."""

    inner: Finally[Args, R]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        try:
            result = await next(args)

        except Exception as exc:
            logger.debug("Running finally: '%s'", type(self.inner).__qualname__)
            await self.inner(args, Failure(exc=exc))
            raise

        logger.debug("Running finally: '%s'", type(self.inner).__qualname__)
        await self.inner(args, Success(value=result))

        return result

    # ....................... #

    @classmethod
    def mw_factory(cls, inner: FinallyFactory) -> MiddlewareFactory:
        def factory(ctx: "ExecutionContext") -> FinallyMiddleware[Any, Any]:
            return FinallyMiddleware(inner=inner(ctx))

        return factory
