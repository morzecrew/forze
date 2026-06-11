"""Orchestrate execution of a resolved operation plan."""

from contextlib import AbstractAsyncContextManager
from typing import Any, Awaitable, Callable, Protocol, cast

from forze.application.contracts.execution import Failure, Handler, Success
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

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
        self, route: StrKey, *, read_only: bool | None = None
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
    - ``on_failure`` hooks are handler-only: they run when the wrap chain / handler
      fails, **not** when a ``before`` guard denies the operation.
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
            # on_failure is handler-only: before-hook errors bypass it (see above).
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
) -> R:
    """Run the transaction scope around the handler."""

    route = tx.route

    if route is None:
        raise exc.internal("Transaction route is required to run a transaction scope")

    async with tx_runner(route, read_only=read_only):
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
    handler: Handler[Args, R],
    args: Args,
    *,
    tx_runner: TransactionRunner,
    defer_after_commit: AfterCommitPort,
) -> R:
    """Run handler through outer and transaction scopes in plan order."""

    async def transactional_core() -> R:
        if plan.tx.route is None:
            # A route-less tx scope with stages is rejected at plan-resolution time
            # (``ResolvedTransactionScope.__attrs_post_init__``), so no re-validation
            # is needed per call.
            return await handler(args)

        return await run_resolved_tx_scope(
            plan.tx,
            handler,
            args,
            tx_runner=tx_runner,
            defer_after_commit=defer_after_commit,
            # QUERY explicitly requests a read-only transaction; other kinds leave
            # the option unspecified so a nested scope inherits the root's value.
            read_only=True if plan.kind is OperationKind.QUERY else None,
        )

    return await run_resolved_scope(plan.outer, transactional_core, args)
