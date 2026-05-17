from typing import Awaitable, Callable

import attrs

from forze.base.asyncio import maybe_await

from .protocols import Guard, OnSuccess
from .value_objects import Skip

# ----------------------- #
#! Can predicate return Skip?


@attrs.define(slots=True, kw_only=True, frozen=True)
class Conditional[**P, R]:
    """Conditional wrapper that runs inner callable only when predicate is true."""

    inner: Callable[P, Awaitable[R | None]]
    predicate: Callable[P, bool | Awaitable[bool]]

    # ....................... #

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R | None:
        predicate_res = await maybe_await(self.predicate(*args, **kwargs))

        if predicate_res:
            return await self.inner(*args, **kwargs)

        return None


# ....................... #
# Built-in wiring


@attrs.define(slots=True, kw_only=True, frozen=True)
class ConditionalGuard[Args](Conditional[[Args], Skip], Guard[Args]):
    """Guard that runs only only when predicate is true."""

    async def __call__(self, args: Args) -> None | Skip:
        return await super().__call__(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ConditionalOnSuccess[Args, R](Conditional[[Args, R], Skip], OnSuccess[Args, R]):
    """Success hook that runs only when predicate is true."""

    async def __call__(self, args: Args, result: R) -> None | Skip:
        return await super().__call__(args, result)
