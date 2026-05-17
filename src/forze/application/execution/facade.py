from __future__ import annotations

from typing import Any, ClassVar, overload

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from .context import ExecutionContext
from .registry import UsecaseRegistry
from .registry.ops import OperationNamespace, OperationRef, operation_namespace_for
from .usecase import Usecase

# ----------------------- #


def _normalize_descriptor_suffix(suffix: StrKey) -> str:
    raw = str(suffix)

    if not raw:
        raise CoreError("Facade operation suffix must be non-empty")

    if "." in raw:
        raise CoreError(
            f"Facade operation suffix must not contain '.', got {raw!r}",
        )

    return raw


# ....................... #
#! Maybe rename back to "facade_op"


@attrs.define(slots=True, frozen=True)
class FacadeOperationDescriptor[Args, R]:
    """Descriptor that resolves a namespaced usecase from a facade instance."""

    suffix: str = attrs.field(converter=_normalize_descriptor_suffix)
    """Suffix of the operation."""

    uc: type[Usecase[Args, R]] | None = attrs.field(default=None, kw_only=True)
    """Usecase type."""

    name: str | None = attrs.field(default=None, kw_only=True)
    """Name of the operation."""

    # ....................... #

    def __set_name__(self, owner: type[Any], name: str) -> None:
        if self.name is None:
            object.__setattr__(self, "name", name)

    # ....................... #

    @overload
    def __get__(
        self,
        obj: None,
        objtype: type[Any] | None = None,
    ) -> FacadeOperationDescriptor[Args, R]: ...

    @overload
    def __get__(
        self,
        obj: UsecasesFacade,
        objtype: type[Any] | None = None,
    ) -> Usecase[Args, R]: ...

    def __get__(
        self,
        obj: UsecasesFacade | None,
        objtype: type[Any] | None = None,
    ) -> Usecase[Args, R] | FacadeOperationDescriptor[Args, R]:
        if obj is None:
            return self

        namespace = obj.namespace or obj.registry.namespace

        if namespace is None:
            raise CoreError(
                f"Facade operation {self.name or self.suffix!r} requires an operation namespace",
            )

        return obj.resolve(
            OperationRef(
                namespace.key(self.suffix),
                uc=self.uc,
                name=self.name,
            )
        )


# ....................... #
#! Need to rely on registry namespace for resolve instead ... (?)


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasesFacade:
    """Usecases facade."""

    require_namespace: ClassVar[bool] = False
    """Whether the facade requires a namespace."""

    # ....................... #

    ctx: ExecutionContext
    """Execution context for resolving usecases."""

    registry: UsecaseRegistry
    """Registry used to resolve usecases."""

    namespace: OperationNamespace | None = None
    """Operation namespace used for facade descriptors."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if type(self).require_namespace and self.namespace is None:
            raise CoreError(
                f"{type(self).__name__} requires namespace=... at runtime",
            )

        registry_namespace = self.registry.namespace

        if (
            self.namespace is not None
            and registry_namespace is not None
            and self.namespace != registry_namespace
        ):
            raise CoreError(
                f"{type(self).__name__} namespace must match registry.namespace",
            )

    # ....................... #

    def resolve(self, op: StrKey | OperationRef[Any, Any]) -> Usecase[Any, Any]:
        """Resolve a usecase for the given operation."""

        if isinstance(op, OperationRef):
            return self.registry.resolve(op.op, self.ctx)

        return self.registry.resolve(op, self.ctx)


# ....................... #


def namespaced_facade[F: UsecasesFacade](cls: type[F]) -> type[F]:
    """Decorator to mark a facade as namespaced."""

    cls.require_namespace = True
    return cls


# ....................... #

__all__ = [
    "FacadeOperationDescriptor",
    "OperationNamespace",
    "OperationRef",
    "UsecasesFacade",
    "operation_namespace_for",
    "namespaced_facade",
]
