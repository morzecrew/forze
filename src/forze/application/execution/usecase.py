from typing import Any, Self

import attrs

from forze.base.logging import getLogger

from .context import ExecutionContext
from .middleware import Middleware, NextCall

# ----------------------- #

logger = getLogger(__name__)

# ....................... #

_QUALNAME_CACHE_MAXSIZE = 256
_qualname_cache: dict[type[Any], str] = {}
_qualname_cache_keys: list[type[Any]] = []


def _qualname_for_type(t: type[Any]) -> str:
    """Return qualname for a type, cached to avoid repeated introspection."""

    if t in _qualname_cache:
        return _qualname_cache[t]

    result = getattr(t, "__qualname__", getattr(t, "__name__", repr(t)))

    if len(_qualname_cache) >= _QUALNAME_CACHE_MAXSIZE:
        evict = _qualname_cache_keys.pop(0)
        del _qualname_cache[evict]

    _qualname_cache[t] = result
    _qualname_cache_keys.append(t)
    return result


def _args_safe_for_logging_impl(args: Any) -> str:
    """Return a logging-safe string representation of *args*.

    Handles list (recursive on first element), dict (first level only),
    and plain objects. Uses type qualnames only; never captures values.
    """

    if isinstance(args, list):
        if not args:
            return "list (empty)"

        first_qual = _args_safe_for_logging_impl(args[0])
        return f"list[{first_qual}]"

    if isinstance(args, dict):
        if not args:
            return "dict (empty)"

        parts = (
            f"{k}: {_qualname_for_type(type(v))}"  # pyright: ignore[reportUnknownArgumentType]
            for k, v in args.items()  # pyright: ignore[reportUnknownVariableType]
        )
        return "{" + ", ".join(parts) + "}"

    return _qualname_for_type(type(args))  # pyright: ignore[reportUnknownArgumentType]


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
            safe_args = self._args_safe_for_logging(args)
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

    def log_parameters(self, parameters: dict[str, Any]) -> None:
        logger.debug("Parameters: %s", parameters)

    def log_delegation(self, target: object) -> None:
        logger.debug("Delegating to %s", type(target).__qualname__)

    def _args_safe_for_logging(self, args: Any) -> str:
        return _args_safe_for_logging_impl(args)
