from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Awaitable, Callable

import attrs

from forze.application.contracts.transaction import (
    TransactionHandle,
    TransactionManagerPort,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..deps.tx_tracer import NOOP_TX_TRACER, TxTracer

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class TransactionContext:
    """Transaction context."""

    resolver: Callable[[StrKey], TransactionManagerPort] | None = None
    """Callable to resolve the transaction manager port."""

    _tx_tracer: TxTracer = attrs.field(default=NOOP_TX_TRACER, init=False)
    """Optional observer for root scope enter/exit (noop by default)."""

    # ....................... #

    _locked: bool = attrs.field(default=False, init=False)
    """Whether the transaction context is locked and cannot be modified."""

    __tx_handle: ContextVar[TransactionHandle | None] = attrs.field(
        factory=lambda: ContextVar("tx_handle", default=None),
        init=False,
        repr=False,
        on_setattr=attrs.setters.frozen,
    )
    """Current active transaction handle."""

    __tx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("tx_depth", default=0),
        init=False,
        repr=False,
        on_setattr=attrs.setters.frozen,
    )
    """Current transaction depth."""

    __cb_stack: ContextVar[list[Callable[[], Awaitable[None]]] | None] = attrs.field(
        factory=lambda: ContextVar("cb_stack", default=None),
        init=False,
        repr=False,
        on_setattr=attrs.setters.frozen,
    )
    """Queued async callables run after a successful root transaction exit."""

    # ....................... #

    def lock(
        self,
        resolver: Callable[[StrKey], TransactionManagerPort],
        *,
        tx_tracer: TxTracer | None = None,
    ) -> None:
        """Lock the transaction context."""

        if self._locked:
            raise exc.internal("Transaction context already locked")

        self._locked = True
        self.resolver = resolver
        self._tx_tracer = tx_tracer if tx_tracer is not None else NOOP_TX_TRACER

    # ....................... #

    def depth(self) -> int:
        """Return transaction nesting depth (``0`` outside any transaction)."""

        return self.__tx_depth.get()

    # ....................... #

    def _defer(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Defer a callback to run after the current root transaction commits successfully."""

        q = self.__cb_stack.get()

        if q is None:
            raise exc.internal(
                "defer_callback requires an active TransactionContext.scope scope"
            )

        q.append(cb)

    # ....................... #

    async def run_or_defer(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Defer a callback to run after the current root transaction commits successfully, or run it immediately if outside a transaction."""

        if self.depth() == 0:
            await cb()
            return

        self._defer(cb)

    # ....................... #

    @asynccontextmanager
    async def scope(self, route: StrKey) -> AsyncGenerator[None]:
        """Enter a transaction scope"""

        if self.resolver is None:
            raise exc.internal("Transaction resolver is not set")

        tx = self.resolver(route)

        depth = self.depth()
        cur_scope = self.__tx_handle.get()

        if depth > 0:
            if cur_scope is None or cur_scope.scope != tx.scope_key:
                raise exc.internal(
                    f"Nested tx scope mismatch: active={cur_scope.scope.name if cur_scope else None} "
                    f"requested={tx.scope_key.name}"
                )

            token_d = self.__tx_depth.set(depth + 1)

            try:
                async with tx.transaction():
                    yield

            finally:
                self.__tx_depth.reset(token_d)

            return

        token_h = self.__tx_handle.set(TransactionHandle(scope=tx.scope_key))
        token_d = self.__tx_depth.set(1)
        token_cb = self.__cb_stack.set([])
        route_name = str(getattr(route, "value", route))

        self._tx_tracer.on_scope_enter(route=route_name, depth=1)

        deferred: list[Callable[[], Awaitable[None]]] | None = None

        try:
            async with tx.transaction():
                yield

        except BaseException:
            raise

        else:
            deferred = self.__cb_stack.get()

        finally:
            self._tx_tracer.on_scope_exit(
                route=route_name,
                depth=self.__tx_depth.get(),
            )
            self.__cb_stack.reset(token_cb)
            self.__tx_handle.reset(token_h)
            self.__tx_depth.reset(token_d)

        if deferred is not None:
            for cb in deferred:
                await cb()
