from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Iterator, Optional, final

import attrs

from forze.base.errors import CoreError

from ..contracts.counter import CounterDepKey, CounterPort
from ..contracts.deps import DepKey, DepsPort
from ..contracts.document import (
    DocumentCacheDepKey,
    DocumentDepKey,
    DocumentPort,
    DocumentSpec,
)
from ..contracts.storage import StorageDepKey, StoragePort
from ..contracts.tx import (
    TxHandle,
    TxManagerDepKey,
    TxManagerPort,
    TxScopedPort,
)

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
        """Return the current active transaction handle."""

        return self.__tx_handle.get()

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Enter a transaction scope."""

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
        """Resolve a dependency by key using the underlying container."""

        with self.__resolving(key):
            return self.deps.provide(key)

    # ....................... #
    # Convenient namespace methods for resolving ports

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        cache = self.dep(DocumentCacheDepKey)(self, spec)
        dep = self.dep(DocumentDepKey)(self, spec, cache=cache)
        self.__validate_tx_scope(dep)

        return dep

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        return self.dep(CounterDepKey)(self, namespace)

    # ....................... #

    def txmanager(self) -> TxManagerPort:
        return self.dep(TxManagerDepKey)(self)

    # ....................... #

    def storage(self, bucket: str) -> StoragePort:
        return self.dep(StorageDepKey)(self, bucket)
