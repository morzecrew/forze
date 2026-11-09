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

    _reusable: bool = attrs.field(
        default=attrs.Factory(lambda self: _holds_no_call_state(self.type), takes_self=True),
        init=False,
        eq=False,
        repr=False,
    )
    """Whether a built facade may be handed out again for the same context."""

    _last: "list[tuple[ExecutionContext, F]]" = attrs.field(
        factory=list,
        init=False,
        eq=False,
        repr=False,
    )
    """At most one ``(context, facade)`` pair: the facade built for the last context
    :attr:`ctx_factory` returned.

    Reused while that context stays current, and never across contexts: holding the
    facade keeps its context alive, so the identity check cannot match a recycled object.
    The pair is replaced as one item, so a concurrent reader never mixes two pairs.
    """

    # ....................... #

    def __call__(self) -> F:
        if self.type.namespace_required and self.ns is None:
            raise exc.configuration(f"{self.type.__name__} requires namespace at runtime")

        ctx = self.ctx_factory()

        for last_ctx, last_facade in self._last:
            if last_ctx is ctx:
                return last_facade

        facade = self.type(ctx=ctx, registry=self.registry, namespace=self.ns)

        if self._reusable:
            self._last[:] = ((ctx, facade),)

        return facade


# ....................... #

_FACADE_FIELDS: frozenset[str] = frozenset(a.name for a in attrs.fields(OperationFacade))
"""What every facade holds: its context, registry and namespace."""


def _holds_no_call_state(facade_type: type[OperationFacade]) -> bool:
    """Whether *facade_type* holds only :data:`_FACADE_FIELDS` — frozen, so one instance
    serves every call on its context. A subclass that adds fields might keep per-call
    state, so it gets a fresh instance each call."""

    return {a.name for a in attrs.fields(facade_type)} == _FACADE_FIELDS
