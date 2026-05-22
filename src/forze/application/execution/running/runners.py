from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Callable

import attrs

from forze.application.contracts.execution import Handler
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from ..planning.plans import ResolvedOperationPlan

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRunner:
    """Operation runner."""

    op: StrKey
    """Operation key."""

    plan: "ResolvedOperationPlan"
    """Resolved operation plan."""

    tx_runner: Callable[[StrKey], AbstractAsyncContextManager[None]]
    """Callable that returns an async context manager that scopes a transaction."""

    # ....................... #

    async def run[Args, R](self, handler: Handler[Args, R], args: Args) -> R:
        """Run the operation."""

        return await handler(args)
