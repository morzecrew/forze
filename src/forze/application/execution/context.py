"""Execution context for dependency resolution and transactions."""

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, AsyncIterator, Iterator, Optional, final

import attrs

from forze.base.errors import CoreError
from forze.base.logging import getLogger

from ..contracts.cache import CacheDepKey, CachePort, CacheSpec
from ..contracts.counter import CounterDepKey, CounterPort
from ..contracts.deps import DepKey, DepsPort
from ..contracts.document import (
    DocumentReadDepKey,
    DocumentReadPort,
    DocumentSpec,
    DocumentWriteDepKey,
    DocumentWritePort,
)
from ..contracts.search import SearchReadDepKey, SearchReadPort, SearchSpec
from ..contracts.storage import StorageDepKey, StoragePort
from ..contracts.tx import TxHandle, TxManagerDepKey, TxManagerPort, TxScopedPort

# ----------------------- #

logger = getLogger(__name__).bind(scope="context")

# ....................... #


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

        logger.debug("Entering transaction scope")

        with logger.section():
            tx = self.txmanager()
            scope = tx.scope_key()
            depth = self.__tx_depth.get()
            cur = self.__tx_handle.get()

            logger.trace(
                "Transaction state: requested_scope=%s depth=%d active_scope=%s",
                scope.name,
                depth,
                cur.scope.name if cur else None,
            )

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
                    logger.trace("Reusing nested transaction scope %s", scope.name)

                    async with tx.transaction():
                        yield

                finally:
                    self.__tx_depth.reset(token_d)
                    logger.trace("Leaving nested transaction scope %s", scope.name)

                return

            token_h = self.__tx_handle.set(TxHandle(scope=scope))
            token_d = self.__tx_depth.set(1)

            try:
                logger.trace("Starting root transaction scope %s", scope.name)

                async with tx.transaction():
                    yield

            finally:
                self.__tx_handle.reset(token_h)
                self.__tx_depth.reset(token_d)
                logger.trace("Leaving root transaction scope %s", scope.name)

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
            with logger.section():
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
            dep = self.deps.provide(key)
            return dep

    # ....................... #
    # Convenient namespace methods for resolving ports

    def doc_read(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentReadPort[Any]:
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
            logger.trace(
                "Resolving cache for document read namespace '%s' with ttl=%s",
                spec.namespace,
                cache_spec.ttl,
            )
            cache = self.cache(cache_spec)

        dep = self.dep(DocumentReadDepKey)(self, spec, cache=cache)
        self.__validate_tx_scope(dep)

        logger.trace(
            "Resolved document read port for namespace '%s' -> %s",
            spec.namespace,
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def doc_write(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentWritePort[Any, Any, Any, Any]:
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
            logger.trace(
                "Resolving cache for document write namespace '%s' with ttl=%s",
                spec.namespace,
                cache_spec.ttl,
            )
            cache = self.cache(cache_spec)

        dep = self.dep(DocumentWriteDepKey)(self, spec, cache=cache)
        self.__validate_tx_scope(dep)

        logger.trace(
            "Resolved document write port for namespace '%s' -> %s",
            spec.namespace,
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def cache(self, spec: CacheSpec) -> CachePort:
        """Resolve a cache port for the given spec.

        :param spec: Cache specification.
        :returns: Cache port instance.
        """

        dep = self.dep(CacheDepKey)(self, spec)

        logger.trace(
            "Resolved cache port for namespace '%s' -> %s",
            spec.namespace,
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Resolve a counter port for the given namespace.

        :param namespace: Counter namespace.
        :returns: Counter port instance.
        """

        dep = self.dep(CounterDepKey)(self, namespace)

        logger.trace(
            "Resolved counter port for namespace '%s' -> %s",
            namespace,
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def txmanager(self) -> TxManagerPort:
        """Resolve the transaction manager port."""

        dep = self.dep(TxManagerDepKey)(self)

        logger.trace(
            "Resolved transaction manager port -> %s",
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def storage(self, bucket: str) -> StoragePort:
        """Resolve a storage port for the given bucket.

        :param bucket: Storage bucket name.
        :returns: Storage port instance.
        """

        dep = self.dep(StorageDepKey)(self, bucket)

        logger.trace(
            "Resolved storage port for bucket '%s' -> %s",
            bucket,
            type(dep).__qualname__,
        )

        return dep

    # ....................... #

    def search(self, spec: SearchSpec[Any]) -> SearchReadPort[Any]:
        """Resolve a search port."""

        dep = self.dep(SearchReadDepKey)(self, spec)

        logger.trace(
            "Resolved search port -> %s",
            type(dep).__qualname__,
        )

        return dep
