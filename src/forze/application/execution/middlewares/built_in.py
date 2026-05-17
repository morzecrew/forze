from contextlib import AbstractAsyncContextManager
from typing import Callable, Self, Sequence

import attrs

from forze.base.primitives import StrKey

from .protocols import Middleware, NextCall, OnSuccess

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that scopes the next call in a transaction."""

    route: StrKey
    """Transaction route."""

    runnable: Callable[[StrKey], AbstractAsyncContextManager[None]]
    """Callable that returns an async context manager that scopes a transaction."""

    after_commit: Sequence[OnSuccess[Args, R]] = attrs.field(factory=tuple)
    """Callbacks to run after the transaction commits."""

    # ....................... #

    def with_after_commit(self, *hooks: OnSuccess[Args, R]) -> Self:
        return attrs.evolve(self, after_commit=(*self.after_commit, *hooks))

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        async with self.runnable(self.route):
            result = await next(args)

        for hook in self.after_commit:
            await hook(args, result)

        return result
