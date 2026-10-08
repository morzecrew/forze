import functools
import inspect
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..base.specs import BaseSpec

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


@final
@attrs.define(slots=True, frozen=True)
class GuardedWritePort:
    """What a command (write) accessor returns while the operation is read-only (``QUERY``).

    Holding it is allowed, so a service built with a write port it never calls can serve a
    query. Anything reached for on it is refused while the operation reaching is read-only;
    outside one it is the real port, resolved then. A handler or port built during a query
    and cached is therefore not left broken for the commands that reuse it.

    A probe counts as a use: ``hasattr`` and ``getattr`` with a default raise inside a
    read-only operation rather than answer, and a protocol ``isinstance`` check is false.
    """

    ctx: "ExecutionContext" = attrs.field(repr=False, eq=False)
    """The context the port is resolved in."""

    key: DepKey[Any]
    """The command port this stands in for."""

    spec: BaseSpec
    """The spec the port was asked for; reading it writes nothing."""

    route: StrKey | None = None
    """The route the port resolves on."""

    # ....................... #

    def __getattr__(self, name: str) -> Any:
        # A dunder probe (``copy``, ``pickle``, a protocol check) is not a use; it sees a
        # plain object with nothing on it.
        if name.startswith("__"):
            raise AttributeError(name)

        self.__refuse_if_read_only()
        port = self.ctx.deps.resolve_configurable(self.ctx, self.key, self.spec, route=self.route)
        attribute = getattr(port, name)

        if not callable(attribute):
            return attribute

        # A method taken outside a read-only operation and kept is checked again when called.
        @functools.wraps(attribute)
        def guarded(*args: Any, **kwargs: Any) -> Any:
            self.__refuse_if_read_only()
            return attribute(*args, **kwargs)

        if inspect.iscoroutinefunction(attribute):
            inspect.markcoroutinefunction(guarded)

        return guarded

    # ....................... #

    def __refuse_if_read_only(self) -> None:
        if self.ctx.inv_ctx.is_read_only():
            raise exc.precondition(
                f"Cannot use command (write) port {self.key} in a read-only (QUERY) operation."
            )


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
        """Resolve a command (write) port — unusable in a read-only (``QUERY``) operation.

        The single guard point for write ports: a ``QUERY`` operation gets a
        :class:`GuardedWritePort`, which it may hold but not use, so a write is refused
        where it is made. Query/read accessors keep using :meth:`_resolve_configurable`.
        """

        ctx = self._require_ctx()

        if ctx.inv_ctx.is_read_only():
            return GuardedWritePort(ctx=ctx, key=key, spec=spec, route=route)

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
