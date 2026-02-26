from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Concatenate,
    Iterator,
    Optional,
    ParamSpec,
    Protocol,
    TypeVar,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import uuid7

from .deps.base import DepKey, DepsPort
from .ports import (
    CounterPort,
    DocumentCachePort,
    DocumentPort,
    StoragePort,
    TxManagerPort,
    TxScopeKey,
)
from .specs import DocumentSpec

# ----------------------- #

T = TypeVar("T")
P = ParamSpec("P")
TxCsp = TypeVar("TxCsp", bound="TxContextScopedPort")

# ....................... #


@attrs.define(slots=True, frozen=True)
class TxHandle:
    """Opaque capability token for transactional execution."""

    scope: TxScopeKey
    """The scope of the transaction."""

    id: UUID = attrs.field(factory=uuid7, init=False)
    """The unique identifier of the transaction."""


# ....................... #


@runtime_checkable
class TxContextScopedPort(Protocol):
    ctx: ExecutionContext
    tx_scope: TxScopeKey


# ....................... #


def require_tx_scope_match(
    method: Callable[Concatenate[TxCsp, P], Awaitable[T]],
) -> Callable[Concatenate[TxCsp, P], Awaitable[T]]:
    @wraps(method)
    async def async_wrapper(
        self: TxCsp,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        if self.ctx.active_tx() is not None:
            self.ctx.require_tx(self.tx_scope)

        return await method(self, *args, **kwargs)

    return async_wrapper


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
        """Return the current active transaction handle."""

        return self.__tx_handle.get()

    # ....................... #

    def require_tx(self, scope: TxScopeKey) -> TxHandle:
        """Require a transaction handle for the given scope.

        This method raises :exc:`CoreError` if no transaction is active or if the active transaction is not for the given scope.
        """

        h = self.__tx_handle.get()

        if h is None:
            raise CoreError(
                f"Transactional context is required for scope: {scope.name}"
            )

        if h.scope != scope:
            raise CoreError(
                f"Transaction scope mismatch: active={h.scope.name}, requested={scope.name}"
            )

        return h

    # ....................... #

    @asynccontextmanager
    async def transaction(self, tx: TxManagerPort) -> AsyncIterator[None]:
        """Enter a transaction scope."""

        scope = tx.scope_key()
        depth = self.__tx_depth.get()
        cur = self.__tx_handle.get()

        if depth > 0:
            if cur is None or cur.scope != scope:
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
            and isinstance(instance, TxContextScopedPort)
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

    def dep(self, key: DepKey[T]) -> T:
        """Resolve a dependency by key using the underlying container."""

        with self.__resolving(key):
            return self.deps.provide(key)

    # ....................... #

    def doc(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Return a document port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentDepPort`
        that binds the current :class:`ExecutionContext` and document spec.
        """

        from .deps.document import DocumentDepKey

        cache = self.doc_cache(spec)
        dep = self.dep(DocumentDepKey)(self, spec, cache=cache)
        self.__validate_tx_scope(dep)

        return dep

    # ....................... #

    def doc_cache(self, spec: DocumentSpec[Any, Any, Any, Any]) -> DocumentCachePort:
        """Return a document cache port for the given :class:`DocumentSpec`.

        This is a convenience wrapper around :class:`DocumentCacheDepPort`
        that binds the current :class:`ExecutionContext` and document spec.
        """

        from .deps.document import DocumentCacheDepKey

        return self.dep(DocumentCacheDepKey)(self, spec)

    # ....................... #

    def counter(self, namespace: str) -> CounterPort:
        """Return a counter port bound to a namespace.

        The namespace is used by implementations to partition counters.
        """

        from .deps.counter import CounterDepKey

        return self.dep(CounterDepKey)(self, namespace)

    # ....................... #

    def txmanager(self) -> TxManagerPort:
        """Return a transaction manager port bound to the current context."""

        from .deps.txmanager import TxManagerDepKey

        return self.dep(TxManagerDepKey)(self)

    # ....................... #

    def storage(self, bucket: str) -> StoragePort:
        """Return a storage port bound to the current context."""

        from .deps.storage import StorageDepKey

        return self.dep(StorageDepKey)(self, bucket)
