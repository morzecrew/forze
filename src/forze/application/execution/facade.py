from typing import Any, Self, overload

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
class FacadeOpRef[Args, R]:
    """Reference to a facade operation."""

    op: OpKey
    """Operation key."""

    uc: type[Usecase[Args, R]] | None = attrs.field(default=None, kw_only=True)
    """Optional usecase type to infer annotations from."""

    name: str | None = None
    """Attribute name assigned on facade class."""

    # ....................... #

    def bind(self, facade: UsecasesFacade) -> Usecase[Args, R]:
        """Bind the reference to a facade instance and resolve the usecase."""

        return facade.resolve(self.op)


# ....................... #


@attrs.define(slots=True, frozen=True)
class facade_op[Args, R]:
    """Аacade operation descriptor."""

    op: OpKey
    """Operation key."""

    uc: type[Usecase[Args, R]] | None = attrs.field(default=None, kw_only=True)
    """Optional usecase type to infer annotations from."""

    name: str | None = attrs.field(default=None, init=False, repr=False)
    """Attribute name assigned on facade class."""

    # ....................... #

    def __set_name__(self, owner: type[Any], name: str) -> None:
        object.__setattr__(self, "name", name)

    # ....................... #

    @overload
    def __get__(
        self,
        obj: None,
        objtype: type[Any] | None = None,
    ) -> Self:
        """Return the descriptor itself when accessed on the class."""
        ...

    @overload
    def __get__(
        self,
        obj: UsecasesFacade,
        objtype: type[Any] | None = None,
    ) -> Usecase[Args, R]:
        """Return the resolved usecase when accessed on a facade instance."""
        ...

    def __get__(
        self,
        obj: UsecasesFacade | None,
        objtype: type[Any] | None = None,
    ) -> Usecase[Args, R] | Self:
        if obj is None:
            return self

        return obj.resolve(self.op)

    # ....................... #

    def ref(self) -> FacadeOpRef[Args, R]:
        """Create a reference to the operation."""

        return FacadeOpRef(op=self.op, uc=self.uc, name=self.name)


# ....................... #


def facade_call[Args, R](d: facade_op[Args, R]) -> FacadeOpRef[Args, R]:
    """Create a reference to the operation."""

    return d.ref()
