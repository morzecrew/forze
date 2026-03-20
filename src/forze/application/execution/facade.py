from typing import Any

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
    """Аacade operation descriptor."""

    op: OpKey
    """Operation key."""

    uc: type[Usecase[Args, R]] | None = attrs.field(default=None, kw_only=True)
    """Optional usecase type to infer annotations from."""

    # ....................... #

    def __get__(
        self,
        obj: UsecasesFacade | None,
        objtype: type[Any] | None = None,
    ) -> Usecase[Args, R]:
        if obj is None:
            raise AttributeError("facade_op is available only on facade instances")

        return obj.resolve(self.op)
