"""Dependency resolution helpers for the application kernel.

The kernel does not depend directly on concrete infrastructure. Instead it
uses small keys and callables to lazily construct ports such as
``DocumentPort`` or ``CounterPort`` from a :class:`UsecaseContext`.
"""

from typing import Any, Callable, Generic, Protocol, TypeVar, cast, runtime_checkable

import attrs

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from .ports import AppRuntimePort, CounterPort, DocumentPort
from .specs import DocumentSpec

# ----------------------- #

T = TypeVar("T")

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@attrs.define(slots=True, frozen=True)
class DependencyKey(Generic[T]):
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is only used for diagnostics; type information is carried
    through the type parameter ``T``.
    """

    name: str


class DependenciesPort(Protocol):
    """Abstract access to dependency resolution."""

    def provide(self, key: DependencyKey[T]) -> T:
        """Return the dependency instance registered under ``key``."""
        ...


# ....................... #


@attrs.define(slots=True, frozen=True)
class Dependencies(DependenciesPort):
    """In-memory dependency container used by the kernel.

    Dependencies are registered as zero-argument provider callables and are
    cached after the first use to avoid repeated construction. The container
    is small on purpose and is not meant to be a full-featured DI framework.
    """

    providers: dict[DependencyKey[Any], Callable[[], Any]] = attrs.field(factory=dict)
    cache: dict[DependencyKey[Any], Any] = attrs.field(factory=dict)

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


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecaseContext:
    """Execution context shared by usecases and factories.

    The context provides access to the application runtime and to a
    :class:`DependenciesPort` used to resolve infrastructure-specific ports.
    """

    runtime: AppRuntimePort
    deps: DependenciesPort

    # ....................... #

    def dep(self, key: DependencyKey[T]) -> T:
        """Resolve a dependency by key using the underlying container."""

        return self.deps.provide(key)

    # ....................... #

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Return a document port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentDependencyPort`
        that binds the current :class:`AppRuntimePort` and document spec.
        """

        return self.dep(DocumentDependencyKey)(self.runtime, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Return a counter port bound to a namespace.

        The namespace is used by implementations to partition counters.
        """

        return self.dep(CounterDependencyKey)(self.runtime, namespace)


# ....................... #


@runtime_checkable
class DocumentDependencyPort(Protocol):
    """Factory protocol for building :class:`DocumentPort` instances."""

    def __call__(
        self,
        runtime: AppRuntimePort,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Build a document port bound to the given runtime and spec."""
        ...


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
