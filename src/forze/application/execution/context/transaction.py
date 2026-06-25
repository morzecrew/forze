from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncContextManager, AsyncGenerator, Awaitable, Callable, cast

import attrs

from forze.application._logger import logger
from forze.application.contracts.transaction import (
    IsolationAware,
    IsolationLevel,
    TransactionHandle,
    TransactionManagerPort,
)
from forze.base.asyncio import run_to_completion
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..tracing import NOOP_TX_TRACER, TxTracer, next_tx_id

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

    __tx_id: ContextVar[int | None] = attrs.field(
        factory=lambda: ContextVar("tx_id", default=None),
        init=False,
        repr=False,
        on_setattr=attrs.setters.frozen,
    )
    """Run-global id of the active root transaction (``None`` outside one, and in production where
    no run counter is bound). Minted at root entry, inherited by nested scopes, and stamped on the
    trace so the oracle can group port calls by transaction — see :func:`tx_id`."""

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

    def tx_id(self) -> int | None:
        """Return the active root transaction's run-global id (``None`` outside one).

        Minted at root entry from the run counter (``None`` in production, where no counter is
        bound), inherited by nested scopes. Read by the tracer to group port calls by transaction.
        """

        return self.__tx_id.get()

    # ....................... #

    def _defer(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Defer a callback to run after the current root transaction commits successfully.

        Deferred callbacks run *post-commit*: every registered callback runs even when
        earlier ones fail, and a callback failure does **not** roll back the (already
        committed) transaction. Failures are logged individually and re-raised as a
        single aggregated ``after_commit_failed`` internal error after all callbacks ran.
        """

        q = self.__cb_stack.get()

        if q is None:
            raise exc.internal(
                "defer_callback requires an active TransactionContext.scope scope"
            )

        q.append(cb)

    # ....................... #

    async def run_or_defer(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Defer a callback to run after the current root transaction commits successfully, or run it immediately if outside a transaction.

        See :meth:`_defer` for the post-commit failure semantics: all deferred
        callbacks run regardless of individual failures, and a failure does not
        roll back the committed transaction.
        """

        if self.depth() == 0:
            await cb()
            return

        self._defer(cb)

    # ....................... #

    @staticmethod
    def _open_root(
        tx: TransactionManagerPort,
        *,
        read_only: bool,
        isolation: IsolationLevel | None,
    ) -> AsyncContextManager[None]:
        """Open the root transaction, forwarding ``isolation`` only to IsolationAware managers.

        The fail-closed check in :meth:`scope` guarantees an ``IsolationAware`` manager
        whenever ``isolation`` is set, so non-isolation managers keep their original
        single-argument call (no signature break).
        """

        if isolation is None:
            return tx.transaction(read_only=read_only)

        return cast(IsolationAware, tx).transaction(
            read_only=read_only, isolation=isolation
        )

    # ....................... #

    @asynccontextmanager
    async def scope(
        self,
        route: StrKey,
        *,
        read_only: bool | None = None,
        isolation: IsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        """Enter a transaction scope.

        ``read_only`` opens a read-only transaction where the backend supports it (a
        ``QUERY`` operation passes this), so the database rejects writes.

        ``isolation`` requests an explicit :class:`IsolationLevel` (honored at root only,
        like ``read_only``). It is checked fail-closed at first resolve: the resolved manager
        must be :class:`IsolationAware` and report the level in its
        :class:`TxCapabilities`, else ``exc.configuration`` — a manager that cannot guarantee
        the requested isolation never silently runs weaker. ``None`` leaves the manager's
        default.

        Transaction options are honored only at the **root** scope: nested scopes are
        savepoints that inherit the root's options, so ``read_only`` is never forwarded
        to a nested ``transaction()`` call. A nested scope explicitly requesting a
        ``read_only`` value that conflicts with the root's raises a precondition error;
        the same value or ``None`` (unspecified) is fine.

        Cancellation semantics: cancellation during the body follows the
        rollback path as usual. Once the root transaction has committed, the
        deferred post-commit callbacks are a critical section — they run to
        completion even if the task is cancelled, and the cancellation is
        re-raised afterwards. Cancellation landing inside the driver commit
        itself is outside the engine's control (the adapter rolls back
        best-effort; the server-side outcome can be ambiguous).
        """

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

            if read_only is not None and read_only != cur_scope.read_only:
                raise exc.precondition(
                    f"Nested tx scope requested read_only={read_only} but the root "
                    f"scope is read_only={cur_scope.read_only}; transaction options "
                    "are honored only at root",
                    code="tx_nested_read_only_conflict",
                )

            if isolation is not None and isolation != cur_scope.isolation:
                raise exc.precondition(
                    f"Nested tx scope requested isolation={isolation.name} but the root "
                    f"scope is isolation="
                    f"{cur_scope.isolation.name if cur_scope.isolation else None}; "
                    "transaction options are honored only at root",
                    code="tx_nested_isolation_conflict",
                )

            token_d = self.__tx_depth.set(depth + 1)

            try:
                async with tx.transaction():
                    yield

            finally:
                self.__tx_depth.reset(token_d)

            return

        root_read_only = bool(read_only)
        route_name = str(getattr(route, "value", route))

        # Fail-closed isolation check (root only): only managers that report capabilities can
        # honor an explicit level; a non-reporting manager cannot guarantee any, so reject.
        if isolation is not None:
            if not isinstance(tx, IsolationAware):
                raise exc.configuration(
                    f"Operation requires isolation={isolation.name} on route "
                    f"{route_name!r}, but its transaction manager does not report isolation "
                    "capabilities (not IsolationAware)",
                    code="tx_isolation_unsupported",
                )

            supported = tx.capabilities().isolation

            if isolation not in supported:
                raise exc.configuration(
                    f"Operation requires isolation={isolation.name} on route "
                    f"{route_name!r}, but its transaction manager supports only "
                    f"{sorted(level.name for level in supported)}",
                    code="tx_isolation_unsupported",
                )

        token_h = self.__tx_handle.set(
            TransactionHandle(
                scope=tx.scope_key, read_only=root_read_only, isolation=isolation
            )
        )
        token_d = self.__tx_depth.set(1)
        token_cb = self.__cb_stack.set([])
        # A run-global id for this root transaction (``None`` in production). Stamped on the trace
        # so the oracle groups port calls by transaction — operation spans cannot, since concurrent
        # transactions interleave. A clean exit emits a tx ``exit`` event under this id (the commit
        # signal); a rollback raises and emits none.
        tx_id = next_tx_id()
        token_id = self.__tx_id.set(tx_id)

        self._tx_tracer.on_scope_enter(route=route_name, depth=1, tx_id=tx_id)

        deferred: list[Callable[[], Awaitable[None]]] | None = None

        try:
            async with self._open_root(tx, read_only=root_read_only, isolation=isolation):
                yield

            # Reached only on a clean exit (no exception thrown into the scope) — capture the
            # after-commit callbacks to drain below. An escaping exception skips this, leaving
            # ``deferred`` as ``None`` so nothing is run after a rollback.
            deferred = self.__cb_stack.get()

        finally:
            self._tx_tracer.on_scope_exit(
                route=route_name,
                depth=self.__tx_depth.get(),
                tx_id=tx_id,
            )
            self.__cb_stack.reset(token_cb)
            self.__tx_handle.reset(token_h)
            self.__tx_depth.reset(token_d)
            self.__tx_id.reset(token_id)

        if deferred:
            # The transaction is committed: the drain is a critical section.
            # Run it to completion even if the surrounding task is cancelled
            # (client disconnect, deadline) — otherwise idempotency commits and
            # after-commit dispatch would be silently skipped after a commit.
            # The cancellation is re-raised once the drain finishes. A
            # cancellation landing during the driver commit itself is adapter
            # territory and follows the rollback path above.
            await run_to_completion(self._run_deferred(deferred))

    # ....................... #

    async def _run_deferred(
        self, deferred: list[Callable[[], Awaitable[None]]]
    ) -> None:
        """Run post-commit callbacks, isolating individual failures.

        Every callback runs even when earlier ones fail; each failure is logged.
        If any failed, a single aggregated internal error is raised afterwards —
        the transaction is already committed and is **not** rolled back.
        """

        first_error: Exception | None = None
        failed: list[dict[str, str]] = []

        for index, cb in enumerate(deferred):
            try:
                await cb()

            except Exception as e:
                name = getattr(cb, "__qualname__", None) or repr(cb)
                logger.exception(
                    "After-commit callback %s/%s (%s) failed "
                    "(transaction already committed)",
                    index + 1,
                    len(deferred),
                    name,
                )

                if first_error is None:
                    first_error = e

                failed.append({"index": str(index), "callback": name, "error": str(e)})

        if first_error is not None:
            raise exc.internal(
                "After-commit callbacks failed (transaction already committed)",
                code="after_commit_failed",
                details={"failed": failed},
            ) from first_error
