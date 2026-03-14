from typing import Any, Optional

import attrs

from forze.base.errors import CoreError

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

    @classmethod
    def declared_ops(cls) -> set[OpKey]:
        """Return all operation keys declared on the facade."""

        result: set[OpKey] = set()

        for base in reversed(cls.__mro__):
            for _, value in base.__dict__.items():
                if isinstance(value, facade_op):
                    result.add(value.op)

        return result

    # ....................... #

    @classmethod
    def validate_registry(cls, reg: UsecaseRegistry) -> None:
        """Ensure that all facade-declared operations exist in provided registry."""

        missing = [op for op in cls.declared_ops() if not reg.exists(op)]

        if missing:
            raise CoreError(
                f"Facade {cls.__name__} requires missing operations: {sorted(map(str, missing))}"
            )


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


# ....................... #


def build_usecases_facade[F: UsecasesFacade](
    facade: type[F],
    reg: UsecaseRegistry,
    ctx: ExecutionContext,
    *,
    validate: bool = True,
) -> F:
    """Build usecases facade for a given context."""

    if validate:
        facade.validate_registry(reg)

    return facade(ctx=ctx, reg=reg)
