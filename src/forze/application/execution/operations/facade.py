from typing import TYPE_CHECKING, Any, ClassVar, Self, overload

import attrs

from forze.application.contracts.execution import Handler
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyNamespace

if TYPE_CHECKING:
    from ..context import ExecutionContext, ExecutionContextFactory
    from .registry.registries import FrozenOperationRegistry
    from .run import ResolvedOperation

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationFacade:
    """Facade for operations."""

    namespace_required: ClassVar[bool] = False
    """Whether the facade requires a namespace."""

    # ....................... #

    ctx: "ExecutionContext"
    """Execution context for operation resolution."""

    registry: "FrozenOperationRegistry"
    """Frozen operation registry."""

    namespace: StrKeyNamespace | None = None
    """Namespace for operations, optional."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if type(self).namespace_required and self.namespace is None:
            raise exc.configuration(
                f"{type(self).__name__} requires namespace=... at runtime",
            )

    # ....................... #

    def resolve(self, op: StrKey) -> "ResolvedOperation[Any, Any]":
        """Resolve an operation."""

        if self.namespace is not None:
            op = self.namespace.key(op)

        return self.registry.resolve(op, self.ctx)


# ....................... #


@attrs.define(slots=True, frozen=True)
class facade_op[Args, R]:
    """Descriptor that resolves an operation from a facade instance."""

    op: StrKey
    """Operation key."""

    uc: type[Handler[Args, R]] | None = attrs.field(default=None, kw_only=True)
    """Operation type for type hints."""

    # ....................... #

    @overload
    def __get__(
        self,
        obj: None,
        objtype: type[Any] | None = None,
    ) -> Self: ...

    @overload
    def __get__(
        self,
        obj: OperationFacade,
        objtype: type[Any] | None = None,
    ) -> "ResolvedOperation[Args, R]": ...

    def __get__(
        self,
        obj: OperationFacade | None,
        objtype: type[Any] | None = None,
    ) -> "ResolvedOperation[Args, R] | Self":
        return self if obj is None else obj.resolve(self.op)


# ....................... #


def namespaced_facade[X: OperationFacade](cls: type[X]) -> type[X]:
    """Decorator that makes an operation facade namespace-aware."""

    cls.namespace_required = True

    return cls


# ....................... #


@attrs.define(slots=True, frozen=True)
class OperationFacadeFactory[F: OperationFacade]:
    """Factory for creating :class:`OperationFacade` instances."""

    type: type[F]
    """Type of the facade to create."""

    registry: "FrozenOperationRegistry"
    """Frozen operation registry."""

    ctx_factory: "ExecutionContextFactory"
    """Factory for creating :class:`ExecutionContext` instances."""

    ns: StrKeyNamespace | None = None
    """Namespace for the facade."""

    # ....................... #

    def __call__(self) -> F:
        if self.type.namespace_required and self.ns is None:
            raise exc.configuration(
                f"{self.type.__name__} requires namespace at runtime"
            )

        return self.type(
            ctx=self.ctx_factory(),
            registry=self.registry,
            namespace=self.ns,
        )
