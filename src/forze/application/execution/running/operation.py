from typing import TYPE_CHECKING, Any, Callable, final

import attrs

from forze.application.contracts.execution import Handler, OnSuccess
from forze.base.primitives import StrKey

from .runners import OperationRunner

if TYPE_CHECKING:
    from ..context import ExecutionContext
    from ..registry import FrozenOperationRegistry

# ----------------------- #
#! Mb move into "operation" folder


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedOperation[Args, R](Handler[Args, R]):
    """Resolved operation."""

    op: StrKey
    """Operation key."""

    handler: Handler[Args, R]
    """Handler."""

    runner: OperationRunner
    """Runner."""

    # ....................... #

    async def __call__(self, args: Args) -> R:
        """Call the operation."""

        return await self.runner.run(self.handler, args)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchedOperation[Args, R](OnSuccess[Args, R]):
    """Resolved operation dispatcher."""

    resolved: ResolvedOperation[Any, Any]
    """Resolved operation."""

    mapper: Callable[[Args, R], Any]
    """Mapper function to transform the result of the target operation."""

    # ....................... #

    async def __call__(self, args: Args, result: R) -> None:
        """Call the operation dispatcher."""

        op_args = self.mapper(args, result)

        return await self.resolved(op_args)


# ....................... #


async def run_operation(
    registry: "FrozenOperationRegistry",
    op: StrKey,
    args: Any,
    ctx: "ExecutionContext",
) -> Any:
    """Run an operation from a frozen registry (resolve + full plan)."""

    resolved = registry.resolve(op, ctx)

    return await resolved(args)
