"""Orchestrate execution of a resolved operation plan."""

import asyncio
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, cast

import attrs

from forze.application.contracts.execution import (
    Failure,
    Handler,
    Success,
    TwoPhaseHandler,
)
from forze.application.contracts.transaction import AfterCommitPort, IsolationLevel
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from ...context.invocation import InvocationContext

# ----------------------- #


@attrs.define(slots=True)
class _PrepareOnce:
    """Per-invocation holder for the single ``prepare`` future of a two-phase op.

    Bound above the wrap chain so retries (same context) and hedge attempts
    (child contexts that share this object by reference via ``copy_context``) all
    await the same future — ``prepare`` runs exactly once per logical invocation.
    """

    future: "asyncio.Future[Any] | None" = None


_prepare_once: ContextVar[_PrepareOnce | None] = ContextVar(
    "two_phase_prepare_once", default=None
)

from ..planning.plans import OperationKind, ResolvedOperationPlan
from ..planning.scopes import ResolvedScope, ResolvedTransactionScope
from .stages import (
    run_graph_before,
    run_graph_on_success,
    run_pipeline_finally,
    run_pipeline_on_failure,
    run_pipeline_on_success,
    run_wrap_pipeline,
)

# ----------------------- #


class TransactionRunner(Protocol):
    """Open a transaction scope on a route, optionally read-only (a QUERY operation).

    ``read_only=None`` leaves the option unspecified: a root scope defaults to
    read-write and a nested scope inherits the root's value. An explicit value on a
    nested scope that conflicts with the root raises a precondition error (see
    :meth:`~forze.application.execution.context.transaction.TransactionContext.scope`).
    """

    def __call__(
        self,
        route: StrKey,
        *,
        read_only: bool | None = None,
        isolation: IsolationLevel | None = None,
    ) -> AbstractAsyncContextManager[None]: ...


# ....................... #


async def _run_scope_body[Args, R](
    scope: ResolvedScope,
    args: Args,
    *,
    inner: Callable[[Args], Awaitable[R]],
) -> R:
    """Run before, wrap, on_success, dispatch, on_failure, and finally for a scope.

    Hook phase semantics:

    - ``finally`` hooks always run once the scope is entered — including when a
      ``before`` hook raises (e.g. an authn/authz/tenancy denial), so audit and
      metrics hooks observe denials as a :class:`Failure` outcome.
    - ``on_failure`` hooks run when the wrap chain / handler fails **or** when an
      ``on_success``/dispatch hook raises after a successful handler (the
      operation fails as a whole either way); they do **not** run when a
      ``before`` guard denies the operation.
    """

    result: R | None = None
    failure: Exception | None = None

    try:
        await run_graph_before(scope.before, args)

        try:
            result = await run_wrap_pipeline(scope.wrap, args, inner)
            await run_graph_on_success(scope.on_success, args, result)
            await run_pipeline_on_success(scope.dispatch, args, result)

        except Exception as e:
            # Covers handler/wrap failures AND on_success/dispatch hooks raising
            # after a successful handler; before-hook errors bypass it (see above).
            await run_pipeline_on_failure(scope.on_failure, args, e)
            raise

    except Exception as e:
        failure = e
        raise

    finally:
        # Only materialize the outcome when a finally hook will observe it.
        if not scope.finally_empty:
            outcome = Success(value=result) if failure is None else Failure(exc=failure)
            await run_pipeline_finally(scope.finally_, args, outcome)

    return cast(R, result)  # type: ignore[redundant-cast]


# ....................... #


async def run_resolved_scope[R](
    scope: ResolvedScope,
    inner: Callable[[], Awaitable[R]],
    args: Any,
) -> R:
    """Run a resolved scope around an inner callable."""

    # Fast path: a scope with no body stages adds no behavior, so invoke the inner
    # callable directly and skip the wrap/finally machinery and its allocations.
    # ``body_empty`` is precomputed at plan-resolution time (the scope is frozen).
    if scope.body_empty:
        return await inner()

    async def _wrapped(a: Any) -> R:
        return await inner()

    return await _run_scope_body(scope, args, inner=_wrapped)


# ....................... #


async def run_resolved_tx_scope[Args, R](
    tx: ResolvedTransactionScope,
    handler: Handler[Args, R],
    args: Args,
    *,
    tx_runner: TransactionRunner,
    defer_after_commit: AfterCommitPort,
    read_only: bool | None = None,
    isolation: IsolationLevel | None = None,
) -> R:
    """Run the transaction scope around the handler."""

    route = tx.route

    if route is None:
        raise exc.internal("Transaction route is required to run a transaction scope")

    async with tx_runner(route, read_only=read_only, isolation=isolation):
        if tx.body_empty:
            # No transaction-scope body hooks: run the handler directly inside the
            # transaction (after-commit stages are still handled below).
            result = await handler(args)

        else:

            async def _handler_call(a: Args) -> R:
                return await handler(a)

            result = await _run_scope_body(tx, args, inner=_handler_call)

        # The resolved after-commit stages are frozen at plan-resolution time, so
        # emptiness here (after the handler ran) is final: skip registering the
        # deferred callback when there is nothing to run. Callbacks deferred by the
        # handler itself go through the transaction context directly.
        if not tx.after_commit_empty:
            captured_result = result

            async def _after_commit() -> None:
                await run_graph_on_success(tx.after_commit, args, captured_result)
                await run_pipeline_on_success(
                    tx.dispatch_after_commit,
                    args,
                    captured_result,
                )

            await defer_after_commit(_after_commit)

        return result


# ....................... #


async def run_resolved_operation_plan[Args, R](
    plan: ResolvedOperationPlan,
    handler: Handler[Args, R] | TwoPhaseHandler[Args, Any, R],
    args: Args,
    *,
    tx_runner: TransactionRunner,
    defer_after_commit: AfterCommitPort,
    inv_ctx: "InvocationContext",
) -> R:
    """Run handler through outer and transaction scopes in plan order.

    For a two-phase plan the handler is a :class:`TwoPhaseHandler`: ``prepare``
    runs here — inside the outer scope's ``before``/``wrap`` but **before** the
    transaction opens, under the read-only flag — and its payload is bound into an
    ``apply`` closure that runs inside the transaction as an ordinary handler body.
    ``prepare`` runs **exactly once** per invocation even when a retry/hedge wrap
    re-enters the body: a once-box bound here (above the wrap chain) memoizes its
    future, shared by retries and inherited by hedge child contexts.
    """

    async def transactional_core() -> R:
        if plan.two_phase:
            inner = await _prepare_apply_handler(
                cast(TwoPhaseHandler[Args, Any, R], handler), args, inv_ctx
            )
        else:
            inner = cast(Handler[Args, R], handler)

        if plan.tx.route is None:
            # A route-less tx scope with stages is rejected at plan-resolution time
            # (``ResolvedTransactionScope.__attrs_post_init__``); two-phase without a
            # route is rejected at freeze. So no re-validation is needed per call.
            return await inner(args)

        return await run_resolved_tx_scope(
            plan.tx,
            inner,
            args,
            tx_runner=tx_runner,
            defer_after_commit=defer_after_commit,
            # QUERY explicitly requests a read-only transaction; other kinds leave
            # the option unspecified so a nested scope inherits the root's value.
            read_only=True if plan.kind is OperationKind.QUERY else None,
            isolation=plan.tx.isolation,
        )

    if not plan.two_phase:
        return await run_resolved_scope(plan.outer, transactional_core, args)

    # Bind the prepare once-box above the wrap chain (run_resolved_scope runs the
    # retry/hedge wraps). On a clean or early exit, cancel an in-flight prepare so
    # a cancelled operation never leaks its prepare task.
    token = _prepare_once.set(_PrepareOnce())

    try:
        return await run_resolved_scope(plan.outer, transactional_core, args)

    finally:
        box = _prepare_once.get()

        if box is not None and box.future is not None and not box.future.done():
            box.future.cancel()

        _prepare_once.reset(token)


# ....................... #


async def _run_prepare[Args, Payload](
    two_phase: TwoPhaseHandler[Args, Payload, Any],
    args: Args,
    inv_ctx: "InvocationContext",
) -> Payload:
    """Run ``prepare`` under the read-only flag.

    The flag bars ``prepare`` from acquiring a command (write) port (best-effort —
    same coverage as a QUERY operation: lazily-resolved ports are caught, eagerly
    injected ones are not). Runs in the prepare future's own context, so the flag
    is scoped to ``prepare`` and never leaks into ``apply``.
    """

    ro_token = inv_ctx.set_read_only()

    try:
        return await two_phase.prepare(args)

    finally:
        inv_ctx.reset_read_only(ro_token)


# ....................... #


async def _prepare_apply_handler[Args, R](
    handler: TwoPhaseHandler[Args, Any, R],
    args: Args,
    inv_ctx: "InvocationContext",
) -> Handler[Args, R]:
    """Run ``prepare`` once via the invocation's once-box and return an ``apply``
    closure to run inside the transaction.

    The first caller schedules ``prepare``; retries reuse the completed future and
    concurrent hedge attempts await the same one (the box is shared across copied
    contexts), so ``prepare`` runs exactly once. ``apply`` then runs per attempt
    with the shared payload — a QUERY two-phase op stays read-only into ``apply``,
    a COMMAND op is write-capable.
    """

    once = _prepare_once.get()

    if once is None:  # pragma: no cover - set by run_resolved_operation_plan
        raise exc.internal("Two-phase prepare invoked without a once-box")

    if once.future is None:
        once.future = asyncio.create_task(
            _run_prepare(handler, args, inv_ctx), name="two-phase-prepare"
        )

    payload = await once.future

    async def _apply(args: Args) -> R:
        return await handler.apply(args, payload)

    return _apply
