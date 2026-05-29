from contextlib import AbstractAsyncContextManager
from typing import Callable

import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.primitives import StrKey

from ..planning.plans import ResolvedOperationPlan
from .plan_runner import run_resolved_operation_plan

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRunner:
    """Operation runner."""

    op: StrKey
    """Operation key."""

    plan: ResolvedOperationPlan
    """Resolved operation plan."""

    tx_runner: Callable[[StrKey], AbstractAsyncContextManager[None]]
    """Callable that returns an async context manager that scopes a transaction."""

    defer_after_commit: AfterCommitPort
    """Defer work until after a successful root transaction commit."""

    # ....................... #

    async def run[Args, R](self, handler: Handler[Args, R], args: Args) -> R:
        """Run the operation."""

        return await run_resolved_operation_plan(
            self.plan,
            handler,
            args,
            tx_runner=self.tx_runner,
            defer_after_commit=self.defer_after_commit,
        )
