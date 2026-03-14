from typing import Any, Optional

import attrs

from .context import ExecutionContext
from .plan import OpKey
from .registry import UsecaseRegistry
from .usecase import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasesFacade:
    """Usecases facade."""

    ctx: ExecutionContext
    """Execution context for resolving usecases."""

    reg: UsecaseRegistry
    """Registry with plan merged; used to resolve usecases."""

    # ....................... #

    def resolve(self, op: OpKey) -> Usecase[Any, Any]:
        """Resolve a usecase for the given operation."""

        return self.reg.resolve(op, self.ctx)


# ....................... #


@attrs.define(slots=True, frozen=True)
class facade_op[Args, R]:
    """Simple descriptor for a facade operation."""

    op: OpKey
    """Operation key."""

    uc: Optional[type[Usecase[Args, R]]] = attrs.field(default=None, kw_only=True)
    """Optional usecase type to infer from."""

    # ....................... #

    def __get__(
        self,
        obj: Optional[UsecasesFacade],
        objtype: Optional[type[Any]] = None,
    ) -> Usecase[Args, R]:
        if obj is None:
            raise AttributeError("facade_op is available only on facade instances")

        return obj.resolve(self.op)
