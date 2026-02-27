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
    TypeVar,
    final,
)

import attrs

from forze.base.errors import CoreError

from ..contracts.deps import DepKey, DepsPort
from ..contracts.tx import TxContextScopedPort, TxHandle, TxManagerPort, TxScopeKey

# ----------------------- #

T = TypeVar("T")
P = ParamSpec("P")

# ....................... #


def require_tx_scope_match[**P, T, S: TxContextScopedPort](
    method: Callable[Concatenate[S, P], Awaitable[T]],
) -> Callable[Concatenate[S, P], Awaitable[T]]:
    @wraps(method)
    async def async_wrapper(
        self: S,
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

    def validate_tx_scope(self, instance: Any) -> None:
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
