"""Dependency resolution helpers for the application kernel.

The kernel does not depend directly on concrete infrastructure. Instead it
uses small keys and callables to lazily construct ports such as
``DocumentPort`` or ``CounterPort`` from a :class:`UsecaseContext`.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import (
    Any,
    Iterator,
    Literal,
    Optional,
    Protocol,
    Self,
    TypeVar,
    cast,
    final,
    overload,
    runtime_checkable,
)

import attrs

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from .ports import (
    CounterPort,
    DocumentCachePort,
    DocumentPort,
    IdempotencyPort,
    TxManagerPort,
)
from .specs import DocumentSpec

# ----------------------- #
#! TODO: split into files

T = TypeVar("T")

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DependencyKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is only used for diagnostics; type information is carried
    through the type parameter ``T``.
    """

    name: str
    """Name of the dependency."""


# ....................... #


class DependenciesPort(Protocol):
    """Abstract access to dependency resolution."""

    def provide(self, key: DependencyKey[T]) -> T:
        """Return the dependency instance registered under ``key``."""
        ...

    def exists(self, key: DependencyKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""
        ...


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class Dependencies(DependenciesPort):
    """In-memory dependency container used by the kernel."""

    deps: dict[DependencyKey[Any], Any] = attrs.field(
        factory=dict,
        on_setattr=attrs.setters.frozen,
    )
    """Dependencies by key (type-parameterized)."""

    # ....................... #

    @overload
    def register(
        self,
        key: DependencyKey[T],
        dep: T,
        *,
        inplace: Literal[True],
    ) -> None:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    @overload
    def register(
        self,
        key: DependencyKey[T],
        dep: T,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    def register(
        self,
        key: DependencyKey[T],
        dep: T,
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """

        if key in self.deps:
            raise CoreError(f"Dependency {key.name} already registered")

        new = dict(self.deps)
        new[key] = dep

        if inplace:
            self.deps = new
            return

        else:
            new_instance = type(self)(deps=new)
            return new_instance

    # ....................... #

    @overload
    def register_many(
        self,
        deps: dict[DependencyKey[T], T],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    @overload
    def register_many(
        self,
        deps: dict[DependencyKey[T], T],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    def register_many(
        self,
        deps: dict[DependencyKey[T], T],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """

        already_registered = set(self.deps.keys()).intersection(deps.keys())

        if already_registered:
            raise CoreError(
                f"Dependencies are already registered for keys: {already_registered}"
            )

        new = dict(self.deps)
        new.update(deps)

        if inplace:
            self.deps = new
            return

        else:
            new_instance = type(self)(deps=new)
            return new_instance

    # ....................... #

    def provide(self, key: DependencyKey[T]) -> T:
        """Return a dependency value for the given key.

        :param key: Dependency key identifying the provider.
        :returns: Cached or newly constructed instance of the dependency.
        :raises CoreError: If the dependency is not registered.
        """

        dep = self.deps.get(key)

        if not dep:
            raise CoreError(f"Dependency {key.name} not found")

        return cast(T, dep)

    # ....................... #

    def exists(self, key: DependencyKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""

        return key in self.deps


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionContext:
    """Execution context shared by usecases and factories.

    The context provides access to the application runtime and to a
    :class:`DependenciesPort` used to resolve infrastructure-specific ports.
    """

    deps: DependenciesPort
    """Dependencies container."""

    # Non initable fields
    __resolve_stack: ContextVar[tuple[DependencyKey[Any], ...]] = attrs.field(
        factory=lambda: ContextVar("resolve_stack", default=tuple()),
        init=False,
        repr=False,
    )
    """Per-task dependency resolution stack used to detect cycles."""

    # ....................... #

    @contextmanager
    def __resolving(self, key: DependencyKey[Any]) -> Iterator[None]:
        stack = self.__resolve_stack.get()

        if key in stack:
            chain = " -> ".join(k.name for k in (*stack, key))
            raise CoreError(f"Dependency cycle detected: {chain}")

        token = self.__resolve_stack.set(stack + (key,))

        try:
            yield

        finally:
            self.__resolve_stack.reset(token)

    # ....................... #

    def dep(self, key: DependencyKey[T]) -> T:
        """Resolve a dependency by key using the underlying container."""

        with self.__resolving(key):
            return self.deps.provide(key)

    # ....................... #

    def __dep_exists(self, key: DependencyKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""

        return self.deps.exists(key)

    # ....................... #

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Return a document port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentDependencyPort`
        that binds the current :class:`AppRuntimePort` and document spec.
        """

        with self.__resolving(DocumentDependencyKey):
            cache = self.cache(spec)
            return self.dep(DocumentDependencyKey)(self, spec, cache)

    # ....................... #

    def cache(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> Optional[DocumentCachePort]:
        """Return a document cache port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentCacheDependencyPort`
        that binds the current :class:`AppRuntimePort` and document spec.
        """

        if not self.__dep_exists(DocumentCacheDependencyKey):
            return None

        with self.__resolving(DocumentCacheDependencyKey):
            return self.dep(DocumentCacheDependencyKey)(self, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Return a counter port bound to a namespace.

        The namespace is used by implementations to partition counters.
        """

        with self.__resolving(CounterDependencyKey):
            return self.dep(CounterDependencyKey)(self, namespace)

    # ....................... #

    def txmanager(self) -> TxManagerPort:
        """Return a transaction manager port bound to the current context."""

        with self.__resolving(TxManagerDependencyKey):
            return self.dep(TxManagerDependencyKey)(self)


# ....................... #


@runtime_checkable
class DocumentCacheDependencyPort(Protocol):
    """Factory protocol for building :class:`DocumentCachePort` instances."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentCachePort:
        """Build a document cache port bound to the given context and spec."""
        ...


DocumentCacheDependencyKey: DependencyKey[DocumentCacheDependencyPort] = DependencyKey(
    "document_cache"
)
"""Key used to register the :class:`DocumentCacheDependencyPort` implementation."""

# ....................... #


@runtime_checkable
class DocumentDependencyPort(Protocol):
    """Factory protocol for building :class:`DocumentPort` instances."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: Optional[DocumentCachePort] = None,
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Build a document port bound to the given context, spec,
        and optional cache.
        """
        ...


#! TODO: routed document dependency (need general support for routed dependencies mb)

"""
@attrs.define(slots=True, frozen=True)
class RoutedDocumentDependency(DocumentDependencyPort):
    routes: dict[str, DocumentDependencyPort]  # namespace -> provider
    default: DocumentDependencyPort

    def __call__(self, context: ExecutionContext, spec: DocumentSpec[..., ...], cache=None):
        provider = self.routes.get(spec.namespace, self.default)
        return provider(context, spec, cache=cache)
"""


DocumentDependencyKey: DependencyKey[DocumentDependencyPort] = DependencyKey("document")
"""Key used to register the :class:`DocumentDependencyPort` implementation."""

# ....................... #


@runtime_checkable
class CounterDependencyPort(Protocol):
    """Factory protocol for building :class:`CounterPort` instances."""

    def __call__(
        self,
        context: ExecutionContext,
        namespace: str,
    ) -> CounterPort:
        """Build a counter port bound to the given context and namespace."""
        ...


CounterDependencyKey: DependencyKey[CounterDependencyPort] = DependencyKey("counter")
"""Key used to register the :class:`CounterDependencyPort` implementation."""

# ....................... #


@runtime_checkable
class IdempotencyDependencyPort(Protocol):
    """Factory protocol for building :class:`IdempotencyPort` instances."""

    def __call__(
        self,
        context: ExecutionContext,
        ttl: timedelta = timedelta(seconds=30),
    ) -> IdempotencyPort:
        """Build an idempotency port bound to the given context and TTL."""
        ...


# Dependency key is not implemented as we typically don't need to use idempotency dependency
# within the application code, only in interfaces (e.g. HTTP API).

# ....................... #


@runtime_checkable
class TxManagerDependencyPort(Protocol):
    """Factory protocol for building :class:`TxManagerPort` instances."""

    def __call__(self, context: ExecutionContext) -> TxManagerPort:
        """Build a transaction manager port bound to the given context."""
        ...


TxManagerDependencyKey: DependencyKey[TxManagerDependencyPort] = DependencyKey(
    "tx_manager"
)
"""Key used to register the :class:`TxManagerDependencyPort` implementation."""
