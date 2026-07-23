import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from typing import cast

import attrs

from forze.application._logger import logger
from forze.application.contracts.transaction import (
    COMMIT_AMBIGUOUS_CODE,
    IsolationAware,
    IsolationLevel,
    TransactionallyEnlistable,
    TransactionHandle,
    TransactionManagerPort,
)
from forze.base.asyncio import run_to_completion
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..tracing import NOOP_TX_TRACER, TxTracer, next_tx_id
from .active_operation import active_operation_var, continue_operation_on_task
from .commit_state import mark_commit_started

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class AfterCommitFailure:
    """One post-commit callback that raised after the transaction had committed."""

    index: int
    """Position of the callback in the deferred queue."""

    callback: str
    """A human-readable name of the callback (its ``__qualname__`` or ``repr``)."""

    error: str
    """The stringified exception the callback raised."""


@attrs.define(slots=True, frozen=True, kw_only=True)
class AfterCommitError:
    """Reported to a wired handler when post-commit callbacks fail on an already-committed transaction.

    The transaction is committed and its result is returned to the caller unchanged; this
    is an out-of-band signal so an app can alert or reconcile the failed effects (e.g. an
    idempotency-record write or an eager after-commit dispatch) without a committed
    operation surfacing as a caller-visible error.
    """

    route: str
    """The root transaction's route."""

    tx_id: int | None
    """The root transaction's run-global id (``None`` in production)."""

    failures: tuple[AfterCommitFailure, ...]
    """Every callback that failed, in queue order."""


AfterCommitErrorHandler = Callable[[AfterCommitError], None]
"""Out-of-band handler for post-commit callback failures. Must not raise (a raising handler
is logged and suppressed — it can never turn a committed operation into a failed one)."""


_DeferredCallback = tuple[Callable[[], Awaitable[None]], bool]
"""A queued post-commit callback and whether its failure is ``fatal``.

A **fatal** callback is a deliberate domain check (e.g. detective invariant enforcement) whose
failure must surface to the caller — it re-raises after every callback has run. A **non-fatal**
callback is a best-effort effect (cache invalidation, event dispatch, an idempotency-record
write) whose failure must **not** turn an already-committed operation into a caller-visible
error — it is logged and reported to the wired :class:`AfterCommitErrorHandler` instead."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class TransactionContext:
    """Transaction context."""

    resolver: Callable[[StrKey], TransactionManagerPort] | None = None
    """Callable to resolve the transaction manager port."""

    _tx_tracer: TxTracer = attrs.field(default=NOOP_TX_TRACER, init=False)
    """Optional observer for root scope enter/exit (noop by default)."""

    _after_commit_error_handler: "AfterCommitErrorHandler | None" = attrs.field(
        default=None, init=False
    )
    """Optional out-of-band handler invoked when post-commit callbacks fail (noop by default)."""

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

    __cb_stack: ContextVar[list[_DeferredCallback] | None] = attrs.field(
        factory=lambda: ContextVar("cb_stack", default=None),
        init=False,
        repr=False,
        on_setattr=attrs.setters.frozen,
    )
    """Queued ``(callback, fatal)`` pairs run after a successful root transaction exit."""

    # ....................... #

    def lock(
        self,
        resolver: Callable[[StrKey], TransactionManagerPort],
        *,
        tx_tracer: TxTracer | None = None,
        after_commit_error_handler: "AfterCommitErrorHandler | None" = None,
    ) -> None:
        """Lock the transaction context."""

        if self._locked:
            raise exc.internal("Transaction context already locked")

        self._locked = True
        self.resolver = resolver
        self._tx_tracer = tx_tracer if tx_tracer is not None else NOOP_TX_TRACER
        self._after_commit_error_handler = after_commit_error_handler

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

    def current_isolation(self) -> IsolationLevel | None:
        """The active root transaction's declared :class:`IsolationLevel`, inherited by nested scopes.

        ``None`` outside any transaction, or when the root scope left the manager's default (no
        explicit level requested). Lets a caller verify it is running under a sufficient isolation
        *floor* before relying on it — e.g. preventive cross-aggregate invariant enforcement, which
        is only correct at or above the level its conflict mode needs. An explicit non-``None`` level
        here also implies the fail-closed capability check at root entry already passed for it.
        """

        handle = self.__tx_handle.get()

        return handle.isolation if handle is not None else None

    # ....................... #

    def assert_enlisted(self, resource: object, *, what: str) -> None:
        """Fail closed if *resource* does not commit in the active transaction.

        Requires an open scope. If *resource* can report enlistment (implements
        :class:`~forze.application.contracts.transaction.TransactionallyEnlistable`) and its
        writes go through a *different* client/pool than the one this scope opened its
        transaction on, its write commits on a separate connection — silently breaking any
        "commit atomically" guarantee (e.g. an inbox dedup mark that must be atomic with the
        handler's writes for exactly-once). Raises ``configuration`` in that case. A resource
        that cannot report enlistment is left unchecked (best-effort).
        """

        if self.depth() == 0:
            raise exc.internal(f"assert_enlisted({what}) requires an active transaction scope")

        if isinstance(resource, TransactionallyEnlistable) and (
            not resource.is_transactionally_enlisted()
        ):
            raise exc.configuration(
                f"{what} is not enlisted in the active transaction: its writes commit on a "
                "different connection/pool than the transaction was opened on, silently "
                "breaking atomic (exactly-once) commit. Wire it to the same client/route as "
                "the transaction.",
                code="core.tx.not_enlisted",
            )

    # ....................... #

    def _defer(self, cb: Callable[[], Awaitable[None]], *, fatal: bool = False) -> None:
        """Defer a callback to run after the current root transaction commits successfully.

        Deferred callbacks run *post-commit*: every registered callback runs even when
        earlier ones fail, and a callback failure does **not** roll back the (already
        committed) transaction. A ``fatal=False`` (default) callback is a best-effort effect
        — its failure is logged and reported to a wired :class:`AfterCommitErrorHandler`
        out-of-band, never raised, so the committed operation returns its result unchanged.
        A ``fatal=True`` callback is a deliberate domain check (e.g. detective invariant
        enforcement) — its failure re-raises once every callback has run (see
        :meth:`_run_deferred`).
        """

        q = self.__cb_stack.get()

        if q is None:
            raise exc.internal("defer_callback requires an active TransactionContext.scope scope")

        q.append((cb, fatal))

    # ....................... #

    async def run_or_defer(self, cb: Callable[[], Awaitable[None]], *, fatal: bool = False) -> None:
        """Defer a callback to run after the current root transaction commits successfully, or run it immediately if outside a transaction.

        See :meth:`_defer` for the post-commit failure semantics. ``fatal`` distinguishes a
        best-effort effect (default — a failure is reported out-of-band, not raised) from a
        deliberate domain check (``fatal=True`` — a failure re-raises after all callbacks
        run). Outside a transaction the callback runs immediately and any failure propagates.
        """

        if self.depth() == 0:
            await cb()
            return

        self._defer(cb, fatal=fatal)

    # ....................... #

    @staticmethod
    def _open_root(
        tx: TransactionManagerPort,
        *,
        read_only: bool,
        isolation: IsolationLevel | None,
    ) -> AbstractAsyncContextManager[None]:
        """Open the root transaction, forwarding ``isolation`` only to IsolationAware managers.

        The fail-closed check in :meth:`scope` guarantees an ``IsolationAware`` manager
        whenever ``isolation`` is set, so non-isolation managers keep their original
        single-argument call (no signature break).
        """

        if isolation is None:
            return tx.transaction(read_only=read_only)

        return cast(IsolationAware, tx).transaction(read_only=read_only, isolation=isolation)

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
        rollback path as usual (safely retryable — nothing committed). Once the
        body completes, the scope marks the commit imminent (see
        :func:`commit_started`); the deferred post-commit callbacks are a
        critical section — they run to completion even if the task is cancelled.
        A *non-fatal* post-commit callback that fails is logged and reported to
        a wired :class:`AfterCommitErrorHandler` but never raised — the
        transaction committed, so the operation returns its result unchanged; a
        *fatal* one (a deliberate domain check) re-raises (see
        :meth:`_run_deferred`). Cancellation landing inside the driver commit
        itself is still outside the engine's control (the adapter rolls back
        best-effort; the server-side outcome can be ambiguous), so inside an
        operation the scope surfaces it — like a cancellation re-raised after
        the post-commit drain — as a non-retryable ``commit_ambiguous`` error
        rather than a retryable deadline or a raw cancellation, so an
        at-least-once caller does not double-execute; a deadline landing after
        the scope exits gets the same classification at the invocation boundary
        via the commit mark. Outside an operation (an inbox consumer's
        per-message transaction, a saga step runner) the cancellation is
        re-raised untouched: a shutdown cancel must reach the owning loop as a
        cancel, never be reclassified into an error a poison-handling loop
        would swallow.
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
            TransactionHandle(scope=tx.scope_key, read_only=root_read_only, isolation=isolation)
        )
        token_d = self.__tx_depth.set(1)
        token_cb = self.__cb_stack.set([])
        # A run-global id for this root transaction (``None`` in production). Stamped on the trace
        # so the oracle groups port calls by transaction — operation spans cannot, since concurrent
        # transactions interleave. The scope always emits a tx ``exit`` event under this id (from the
        # ``finally`` below); its ``outcome`` is ``commit`` on a clean exit and ``rollback`` when an
        # exception escaped, so a rolled-back scope is never mistaken for a commit.
        tx_id = next_tx_id()
        token_id = self.__tx_id.set(tx_id)

        self._tx_tracer.on_scope_enter(route=route_name, depth=1, tx_id=tx_id)

        deferred: list[_DeferredCallback] | None = None
        committed = False
        commit_reached = False

        try:
            try:
                async with self._open_root(tx, read_only=root_read_only, isolation=isolation):
                    yield

                    # Body completed cleanly; leaving this block runs the driver commit.
                    # No await sits between the yield returning and here, so this always
                    # runs *before* the commit. Marking now means a cancellation
                    # (deadline / disconnect) landing inside that commit — or the shielded
                    # post-commit drain below — surfaces as a non-retryable
                    # ``commit_ambiguous`` (below, or at the boundary via the mark) rather
                    # than a retryable deadline, so an at-least-once caller cannot
                    # double-execute an operation that may have (or has) committed. A
                    # body failure/cancel throws into the yield, skips this, and follows
                    # the rollback path (safely retryable). Not reset here: it must
                    # survive to the boundary.
                    mark_commit_started()
                    commit_reached = True

                # Reached only on a clean exit (no exception thrown into the scope) — capture the
                # after-commit callbacks to drain below, and mark the transaction committed. An escaping
                # exception skips this, leaving ``deferred`` as ``None`` (nothing runs after a rollback)
                # and ``committed`` False, so the exit event records a rollback, not a commit.
                deferred = self.__cb_stack.get()
                committed = True

            finally:
                self._tx_tracer.on_scope_exit(
                    route=route_name,
                    depth=self.__tx_depth.get(),
                    tx_id=tx_id,
                    committed=committed,
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
                # The cancellation is re-raised once the drain finishes.
                #
                # ``route_name``/``tx_id`` are passed explicitly: the tx ContextVars
                # were already reset in the ``finally`` above, so reading them here
                # would report the *outer* transaction, not this one.
                #
                # ``run_to_completion`` runs the drain on its own task; that task is
                # an engine-internal continuation of the admitted operation (awaited
                # right here), so adopt the operation onto it — a dispatch a
                # post-commit callback makes rides the admitted drain slot instead of
                # being re-admitted (and, mid-shutdown, rejected as ``draining``).
                await run_to_completion(
                    continue_operation_on_task(
                        self._run_deferred(deferred, route=route_name, tx_id=tx_id)
                    )
                )

        except asyncio.CancelledError as error:
            # Before the commit point, a cancellation followed the rollback path
            # above — nothing committed, safely retryable — so it propagates
            # untouched. At or after it (a torn driver commit, or a cancellation
            # re-raised once the shielded post-commit drain finished) the commit
            # may have (or has) landed: whatever requested the cancel — deadline,
            # client disconnect, drain — the caller must not see a plain
            # cancellation it could retry into a duplicate, so surface the same
            # non-retryable ``commit_ambiguous`` the deadline path reports.
            #
            # Converted only inside an operation owned by the current task (the
            # marker the invoke boundary stamps): a transaction the engine runs
            # outside one — an inbox consumer's per-message scope, a saga step
            # runner — keeps raw cancellation semantics, so a shutdown cancel
            # still exits the owning loop instead of being reclassified into an
            # error a poison-handling loop would swallow.
            task = asyncio.current_task()

            if not commit_reached or task is None or active_operation_var.get() is not task:
                raise

            # The conversion absorbs exactly the cancellation it caught, so
            # balance the task's cancel count the way ``asyncio.timeout`` does
            # when it converts one: an enclosing expiring timeout then treats its
            # own cancel as handled and lets this error propagate. The task still
            # terminates with this error, so a drain-initiated cancel cannot
            # wedge shutdown.
            task.uncancel()

            raise exc.internal(
                f"Cancelled at or after the transaction commit on route "
                f"{route_name!r}; the commit outcome is ambiguous (it may have "
                "committed). Surfaced as non-retryable so a retry cannot "
                "double-execute — reconcile before re-running.",
                code=COMMIT_AMBIGUOUS_CODE,
            ) from error

    # ....................... #

    async def _run_deferred(
        self,
        deferred: list[_DeferredCallback],
        *,
        route: str,
        tx_id: int | None,
    ) -> None:
        """Run post-commit callbacks, isolating individual failures.

        Every callback runs even when earlier ones fail; each failure is logged. The
        transaction is already committed, so no failure rolls it back.

        A **non-fatal** callback (a best-effort effect — cache invalidation, dispatch, an
        idempotency-record write) that fails does **not** raise: it is reported to the wired
        :class:`AfterCommitErrorHandler` out-of-band so the committed operation returns its
        result unchanged. A **fatal** callback (a deliberate domain check, e.g. detective
        invariant enforcement) that fails re-raises its original exception once every callback
        has run — the caller must see it, even though the transaction committed. A raising
        handler is itself logged and suppressed (it can never fail a committed operation).
        """

        first_fatal_error: Exception | None = None
        effect_failures: list[AfterCommitFailure] = []

        for index, (cb, fatal) in enumerate(deferred):
            try:
                await cb()

            except Exception as error:
                name = getattr(cb, "__qualname__", None) or repr(cb)
                logger.exception(
                    "After-commit callback %s/%s (%s) failed (transaction already committed)",
                    index + 1,
                    len(deferred),
                    name,
                )

                if fatal:
                    if first_fatal_error is None:
                        first_fatal_error = error

                else:
                    effect_failures.append(
                        AfterCommitFailure(index=index, callback=name, error=str(error))
                    )

        if effect_failures:
            logger.error(
                "%s of %s after-commit effect(s) failed on route %r "
                "(transaction already committed; the operation result is returned unchanged)",
                len(effect_failures),
                len(deferred),
                route,
            )

            handler = self._after_commit_error_handler

            if handler is not None:
                try:
                    handler(
                        AfterCommitError(route=route, tx_id=tx_id, failures=tuple(effect_failures))
                    )

                except Exception:
                    logger.exception(
                        "After-commit error handler raised (suppressed — a committed "
                        "operation must not fail on its post-commit reporting)"
                    )

        # A deliberate domain check (fatal) must surface even though the tx committed.
        if first_fatal_error is not None:
            raise first_fatal_error
