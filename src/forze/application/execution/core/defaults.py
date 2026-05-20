from typing import TYPE_CHECKING

from forze.base.conformity import static_fn_conformity

from .contracts import LifecycleHook

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@static_fn_conformity(LifecycleHook)
async def noop_lifecycle_hook(ctx: "ExecutionContext") -> None:
    """No-op lifecycle hook."""

    return
