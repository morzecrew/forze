from typing import TYPE_CHECKING, Any, Protocol, TypeVar, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .base.specs import BaseSpec

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is used for diagnostics and error messages; type information
    is carried through the type parameter ``T`` for static resolution.
    """

    name: str
    """Human-readable name for diagnostics and error messages."""


# ....................... #


class ConfigurableDepPort[S: BaseSpec, Port](Protocol):
    """Configurable protocol for building resource ports."""

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: S,
    ) -> Port: ...  # pragma: no cover


# ....................... #


class SimpleDepPort[T](Protocol):
    """Simple dependency port."""

    def __call__(self, ctx: "ExecutionContext") -> T:
        """Build a dependency port instance."""
        ...


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ConvenientDeps:
    """Convenient wrapper for dependencies."""

    ctx: "ExecutionContext | None" = attrs.field(default=None)
    """Execution context."""

    _locked: bool = attrs.field(default=False, init=False)
    """Whether the dependencies are locked and cannot be modified."""

    # ....................... #

    def lock(self, ctx: "ExecutionContext") -> None:
        if self._locked:
            raise exc.internal("Convenience layer already locked")

        self._locked = True
        self.ctx = ctx

    # ....................... #

    def _require_ctx(self) -> "ExecutionContext":
        if self.ctx is None:
            raise exc.internal("Execution context is not set")

        return self.ctx

    # ....................... #

    def _resolve_configurable(
        self,
        key: DepKey[Any],
        spec: BaseSpec,
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a configurable port via :attr:`ctx` deps."""

        ctx = self._require_ctx()
        return ctx.deps.resolve_configurable(ctx, key, spec, route=route)

    # ....................... #

    def _resolve_command(
        self,
        key: DepKey[Any],
        spec: BaseSpec,
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a command (write) port — forbidden in a read-only (``QUERY``) operation.

        The single guard point for write ports: a ``QUERY`` operation cannot acquire one,
        by construction. Query/read accessors keep using :meth:`_resolve_configurable`.
        """

        ctx = self._require_ctx()

        if ctx.inv_ctx.is_read_only():
            raise exc.precondition(
                f"Cannot acquire command (write) port {key} in a read-only (QUERY) "
                "operation."
            )

        return ctx.deps.resolve_configurable(ctx, key, spec, route=route)

    # ....................... #

    def _resolve_simple(
        self,
        key: DepKey[Any],
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a simple port via :attr:`ctx` deps."""

        ctx = self._require_ctx()
        return ctx.deps.resolve_simple(ctx, key, route=route)
