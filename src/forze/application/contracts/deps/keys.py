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


def _refuse_if_read_only(ctx: "ExecutionContext", key: DepKey[Any]) -> None:
    if ctx.inv_ctx.is_read_only():
        raise exc.precondition(
            f"Cannot use command (write) port {key} in a read-only (QUERY) operation."
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class GuardedWritePort:
    """What a command (write) accessor returns: the port resolved at acquisition, which
    refuses any use while the operation using it is read-only (``QUERY``).

    Holding it is allowed, so a service built with a write port it never calls can serve a
    query. Whichever operation builds a handler or port that holds it, and whatever later
    reuses that from a cache, a write is judged by the operation making it; outside a
    read-only operation it is the port it stands in for.

    A probe counts as a use: ``hasattr`` and ``getattr`` with a default raise inside a
    read-only operation rather than answer, and a protocol ``isinstance`` check is false. A
    guard against a write a query makes by mistake, not a boundary against code set on one:
    :func:`forze.testing.unguarded` reaches the port, and so does resolving it directly.
    """

    ctx: "ExecutionContext" = attrs.field(repr=False, eq=False)
    """The context the port is used in."""

    key: DepKey[Any]
    """The command port this stands in for."""

    spec: BaseSpec
    """The spec the port was asked for; reading it writes nothing."""

    route: StrKey | None = None
    """The route the port resolves on."""

    _target: Any = attrs.field(default=None, repr=False, eq=False, alias="target")
    """The port resolved at acquisition."""

    _methods: dict[str, Any] = attrs.field(factory=dict, init=False, repr=False, eq=False)
    """Each method name's guarded callable, built on first use."""

    # ....................... #

    def __getattr__(self, name: str) -> Any:
        # A dunder probe (``copy``, ``pickle``, a protocol check) is not a use; it sees a
        # plain object with nothing on it.
        if name.startswith("__"):
            raise AttributeError(name)

        _refuse_if_read_only(self.ctx, self.key)

        if (method := self._methods.get(name)) is not None:
            return method

        attribute = getattr(self._target, name)

        if not callable(attribute):
            return attribute

        method = _guarded(self.ctx, self.key, self._target, name, attribute)
        self._methods[name] = method

        return method


# ....................... #


def _guarded(ctx: "ExecutionContext", key: DepKey[Any], target: Any, name: str, sample: Any) -> Any:
    """*target*'s method *name*, checking the read-only flag when called: one taken outside a
    read-only operation and kept is checked again. It looks the method up on each call, so
    one patched onto the port later is the one called. Closes over the port, not its guard,
    so a guard holding it is freed by reference counting."""

    def guarded(*args: Any, **kwargs: Any) -> Any:
        _refuse_if_read_only(ctx, key)
        return getattr(target, name)(*args, **kwargs)

    # Named like the port's method, but without ``__wrapped__``: unwrapping it must not hand
    # out the real port's method.
    guarded.__name__ = name
    guarded.__qualname__ = getattr(sample, "__qualname__", name)
    guarded.__doc__ = getattr(sample, "__doc__", None)

    if inspect.iscoroutinefunction(sample):
        inspect.markcoroutinefunction(guarded)

    return guarded


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

        The single guard point for write ports. The port is resolved here, so a wiring error
        surfaces at acquisition, and handed out inside a :class:`GuardedWritePort`: a handler
        or adapter built by a command and cached would otherwise hold the bare port, and a
        query reusing it could write. Query/read accessors keep using
        :meth:`_resolve_configurable`.
        """

        ctx = self._require_ctx()
        target = ctx.deps.resolve_configurable(ctx, key, spec, route=route)

        return GuardedWritePort(ctx=ctx, key=key, spec=spec, route=route, target=target)

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
