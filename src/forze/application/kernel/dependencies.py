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
    name: str


class DependenciesPort(Protocol):
    def provide(self, key: DependencyKey[T]) -> T: ...


# ....................... #


@attrs.define(slots=True, frozen=True)
class Dependencies(DependenciesPort):
    providers: dict[DependencyKey[Any], Callable[[], Any]] = attrs.field(factory=dict)
    cache: dict[DependencyKey[Any], Any] = attrs.field(factory=dict)

    # ....................... #

    def provide(self, key: DependencyKey[T]) -> T:
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
    runtime: AppRuntimePort
    deps: DependenciesPort

    # ....................... #

    def dep(self, key: DependencyKey[T]) -> T:
        return self.deps.provide(key)

    # ....................... #

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Shortcut for document dependency."""

        return self.dep(DocumentDependencyKey)(self.runtime, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Shortcut for counter dependency."""

        return self.dep(CounterDependencyKey)(self.runtime, namespace)


# ....................... #


@runtime_checkable
class DocumentDependencyPort(Protocol):
    def __call__(
        self,
        runtime: AppRuntimePort,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]: ...


DocumentDependencyKey: DependencyKey[DocumentDependencyPort] = DependencyKey("document")

# ....................... #


@runtime_checkable
class CounterDependencyPort(Protocol):
    def __call__(
        self,
        runtime: AppRuntimePort,
        namespace: str,
    ) -> CounterPort: ...


CounterDependencyKey: DependencyKey[CounterDependencyPort] = DependencyKey("counter")
