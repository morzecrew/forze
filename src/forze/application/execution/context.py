"""Execution context for dependency resolution and transactions.

Provides :class:`ExecutionContext` with :meth:`dep`, :meth:`transaction`, and
convenience methods (:meth:`doc`, :meth:`counter`, :meth:`txmanager`,
:meth:`storage`). Uses context variables for per-task transaction state and
dependency cycle detection.
"""

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, AsyncIterator, Iterator, Optional, final

import attrs

from forze.base.errors import CoreError

from ..contracts.cache import CacheDepKey, CachePort, CacheSpec
from ..contracts.counter import CounterDepKey, CounterPort
from ..contracts.deps import DepKey, DepsPort
from ..contracts.document import DocumentDepKey, DocumentPort, DocumentSpec
from ..contracts.search import SearchReadDepKey, SearchReadPort, SearchSpec
from ..contracts.storage import StorageDepKey, StoragePort
from ..contracts.tx import TxHandle, TxManagerDepKey, TxManagerPort, TxScopedPort

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionContext:
    """Execution context shared by usecases and factories.

    The context provides access to the application runtime and to a
    :class:`DepsPort` used to resolve infrastructure-specific ports.
    """

    deps: DepsPort
    """Dependencies container."""

    # Non initable fields
    __resolve_stack: ContextVar[tuple[DepKey[Any], ...]] = attrs.field(
        factory=lambda: ContextVar("resolve_stack", default=tuple()),
        init=False,
        repr=False,
    )
    """Per-task dependency resolution stack used to detect cycles."""

    __tx_handle: ContextVar[Optional[TxHandle]] = attrs.field(
        factory=lambda: ContextVar("tx_handle", default=None),
        init=False,
        repr=False,
    )
    """Current active transaction handle."""

    __tx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("tx_depth", default=0),
        init=False,
        repr=False,
    )
    """Current transaction depth."""

    # ....................... #

    def active_tx(self) -> Optional[TxHandle]:
        """Return the current active transaction handle.

        Returns ``None`` when no transaction is active.
        """
        return self.__tx_handle.get()

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Enter a transaction scope.

        Nested calls reuse the same transaction (savepoints when supported).
        Raises :exc:`CoreError` on scope mismatch (e.g. different tx manager).
        """
        tx = self.txmanager()

        scope = tx.scope_key()
        depth = self.__tx_depth.get()
        cur = self.__tx_handle.get()

        if depth > 0:
            if (  # protect against different kind (implementations) of tx opened simultaneously
                cur is None or cur.scope != scope
            ):
                raise CoreError(
                    f"Nested tx scope mismatch: active={cur.scope.name if cur else None} "
                    f"requested={scope.name}"
                )

            token_d = self.__tx_depth.set(depth + 1)

            try:
                async with tx.transaction():
                    yield

            finally:
                self.__tx_depth.reset(token_d)

            return

        token_h = self.__tx_handle.set(TxHandle(scope=scope))
        token_d = self.__tx_depth.set(1)

        try:
            async with tx.transaction():
                yield

        finally:
            self.__tx_handle.reset(token_h)
            self.__tx_depth.reset(token_d)

    # ....................... #

    def __validate_tx_scope(self, instance: Any) -> None:
        h = self.active_tx()

        if (
            h is not None
            and isinstance(instance, TxScopedPort)
            and h.scope != instance.tx_scope
        ):
            raise CoreError(
                f"Port tx scope mismatch: active={h.scope.name}, requested={instance.tx_scope.name}"
            )

    # ....................... #

    @contextmanager
    def __resolving(self, key: DepKey[Any]) -> Iterator[None]:
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

    def dep[T](self, key: DepKey[T]) -> T:
        """Resolve a dependency by key using the underlying container.

        :param key: Dependency key.
        :returns: Resolved instance.
        :raises CoreError: If the dependency is not registered or a cycle is detected.
        """
        with self.__resolving(key):
            return self.deps.provide(key)

    # ....................... #
    # Convenient namespace methods for resolving ports

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Resolve a document port for the given spec.

        :param spec: Document specification.
        :returns: Document port instance.
        """

        cache = None

        if spec.cache is not None and spec.cache.get("enabled", False):
            cache_spec = CacheSpec(
                namespace=spec.namespace,
                ttl=spec.cache.get("ttl", timedelta(seconds=300)),
            )
            cache = self.cache(cache_spec)

        dep = self.dep(DocumentDepKey)(self, spec, cache=cache)
        self.__validate_tx_scope(dep)

        return dep

    # ....................... #

    def cache(self, spec: CacheSpec) -> CachePort:
        """Resolve a cache port for the given spec.

        :param spec: Cache specification.
        :returns: Cache port instance.
        """

        return self.dep(CacheDepKey)(self, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Resolve a counter port for the given namespace.

        :param namespace: Counter namespace.
        :returns: Counter port instance.
        """

        return self.dep(CounterDepKey)(self, namespace)

    # ....................... #

    def txmanager(self) -> TxManagerPort:
        """Resolve the transaction manager port."""

        return self.dep(TxManagerDepKey)(self)

    # ....................... #

    def storage(self, bucket: str) -> StoragePort:
        """Resolve a storage port for the given bucket.

        :param bucket: Storage bucket name.
        :returns: Storage port instance.
        """

        return self.dep(StorageDepKey)(self, bucket)

    # ....................... #

    def search(self, spec: SearchSpec) -> SearchReadPort[Any]:
        """Resolve a search port."""

        return self.dep(SearchReadDepKey)(self, spec)
