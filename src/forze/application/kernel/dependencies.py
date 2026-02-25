"""Dependency resolution helpers for the application kernel.

The kernel does not depend directly on concrete infrastructure. Instead it
uses small keys and callables to lazily construct ports such as
``DocumentPort`` or ``CounterPort`` from a :class:`UsecaseContext`.
"""

from datetime import timedelta
from typing import (
    Any,
    Callable,
    Generic,
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
    AppRuntimePort,
    CounterPort,
    DocumentCachePort,
    DocumentPort,
    IdempotencyPort,
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
class DependencyKey(Generic[T]):
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
    """In-memory dependency container used by the kernel.

    Dependencies are registered as zero-argument provider callables and are
    cached after the first use to avoid repeated construction. The container
    is small on purpose and is not meant to be a full-featured DI framework.
    """

    providers: dict[DependencyKey[Any], Callable[[], Any]] = attrs.field(
        factory=dict,
        on_setattr=attrs.setters.frozen,
    )
    """Providers of dependencies by key (type-parameterized)."""

    cache: dict[DependencyKey[Any], Any] = attrs.field(factory=dict, init=False)
    """Cache of dependencies by key (type-parameterized)."""

    # ....................... #

    @overload
    def register(
        self,
        key: DependencyKey[T],
        provider: Callable[[], T],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the provider.
        :param provider: Callable that builds the dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    @overload
    def register(
        self,
        key: DependencyKey[T],
        provider: Callable[[], T],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the provider.
        :param provider: Callable that builds the dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    def register(
        self,
        key: DependencyKey[T],
        provider: Callable[[], T],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the provider.
        :param provider: Callable that builds the dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """

        if key in self.providers:
            raise CoreError(f"Dependency {key.name} already registered")

        new = dict(self.providers)
        new[key] = provider

        if inplace:
            self.providers = new
            return

        else:
            new_instance = type(self)(providers=new)
            return new_instance

    # ....................... #

    @overload
    def register_many(
        self,
        providers: dict[DependencyKey[T], Callable[[], T]],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register multiple dependency providers at once.

        :param providers: Mapping from dependency key to provider callable.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    @overload
    def register_many(
        self,
        providers: dict[DependencyKey[T], Callable[[], T]],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register multiple dependency providers at once.

        :param providers: Mapping from dependency key to provider callable.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    def register_many(
        self,
        providers: dict[DependencyKey[T], Callable[[], T]],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register multiple dependency providers at once.

        :param providers: Mapping from dependency key to provider callable.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """

        new = dict(self.providers)
        new.update(providers)

        if inplace:
            self.providers = new
            return

        else:
            new_instance = type(self)(providers=new)
            return new_instance

    # ....................... #

    def provide(self, key: DependencyKey[T]) -> T:
        """Return a dependency value for the given key.

        :param key: Dependency key identifying the provider.
        :returns: Cached or newly constructed instance of the dependency.
        :raises CoreError: If the dependency is not registered.
        """

        if key in self.cache:
            return cast(T, self.cache[key])

        prov = self.providers.get(key)

        if not prov:
            raise CoreError(f"Dependency {key.name} not found")

        val = prov()
        self.cache[key] = val

        return cast(T, val)

    # ....................... #

    def exists(self, key: DependencyKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""

        return key in self.providers


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecaseContext:
    """Execution context shared by usecases and factories.

    The context provides access to the application runtime and to a
    :class:`DependenciesPort` used to resolve infrastructure-specific ports.
    """

    runtime: AppRuntimePort
    """Application runtime."""

    deps: DependenciesPort
    """Dependencies container."""

    # ....................... #

    def dep(self, key: DependencyKey[T]) -> T:
        """Resolve a dependency by key using the underlying container."""

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

        cache = self.doc_cache(spec)

        return self.dep(DocumentDependencyKey)(self.runtime, spec, cache)

    # ....................... #

    def doc_cache(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> Optional[DocumentCachePort]:
        """Return a document cache port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentCacheDependencyPort`
        that binds the current :class:`AppRuntimePort` and document spec.
        """

        if not self.__dep_exists(DocumentCacheDependencyKey):
            return None

        return self.dep(DocumentCacheDependencyKey)(self.runtime, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Return a counter port bound to a namespace.

        The namespace is used by implementations to partition counters.
        """

        return self.dep(CounterDependencyKey)(self.runtime, namespace)


# ....................... #


@runtime_checkable
class DocumentCacheDependencyPort(Protocol):
    """Factory protocol for building :class:`DocumentCachePort` instances."""

    def __call__(
        self,
        runtime: AppRuntimePort,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentCachePort:
        """Build a document cache port bound to the given runtime and spec."""
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
        runtime: AppRuntimePort,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: Optional[DocumentCachePort] = None,
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Build a document port bound to the given runtime, spec,
        and optional cache.
        """
        ...


#! TODO: routed document dependency (need general support for routed dependencies mb)

"""
@attrs.define(slots=True, frozen=True)
class RoutedDocumentDependency(DocumentDependencyPort):
    routes: dict[str, DocumentDependencyPort]  # namespace -> provider
    default: DocumentDependencyPort

    def __call__(self, runtime: AppRuntimePort, spec: DocumentSpec[..., ...], cache=None):
        provider = self.routes.get(spec.namespace, self.default)
        return provider(runtime, spec, cache=cache)
"""


DocumentDependencyKey: DependencyKey[DocumentDependencyPort] = DependencyKey("document")
"""Key used to register the :class:`DocumentDependencyPort` implementation."""

# ....................... #


@runtime_checkable
class CounterDependencyPort(Protocol):
    """Factory protocol for building :class:`CounterPort` instances."""

    def __call__(
        self,
        runtime: AppRuntimePort,
        namespace: str,
    ) -> CounterPort:
        """Build a counter port bound to the given runtime and namespace."""
        ...


CounterDependencyKey: DependencyKey[CounterDependencyPort] = DependencyKey("counter")
"""Key used to register the :class:`CounterDependencyPort` implementation."""

# ....................... #


@runtime_checkable
class IdempotencyDependencyPort(Protocol):
    """Factory protocol for building :class:`IdempotencyPort` instances."""

    def __call__(
        self,
        runtime: AppRuntimePort,
        ttl: timedelta = timedelta(seconds=30),
    ) -> IdempotencyPort:
        """Build an idempotency port bound to the given runtime and TTL."""
        ...


# Dependency key is not implemented as we typically don't need to use idempotency dependency
# within the application code, only in interfaces (e.g. HTTP API)
