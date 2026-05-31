"""Orchestrate execution of a resolved operation plan."""

from contextlib import AbstractAsyncContextManager
from typing import Any, Awaitable, Callable, cast

from forze.application.contracts.execution import Failure, Handler, Success
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..planning.plans import ResolvedOperationPlan
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

TransactionRunner = Callable[[StrKey], AbstractAsyncContextManager[None]]

# ....................... #


def _assert_tx_configured(tx: ResolvedTransactionScope) -> None:
    if tx.route is not None or tx.is_empty():
        return

    raise exc.internal("Transaction scope has stages but no route set")


# ....................... #


async def _run_scope_body[Args, R](
    scope: ResolvedScope,
    args: Args,
    *,
    inner: Callable[[Args], Awaitable[R]],
) -> R:
    """Run before, wrap, on_success, dispatch, on_failure, and finally for a scope."""

    await run_graph_before(scope.before, args)

    result: R | None = None
    exc: Exception | None = None

    try:
        result = await run_wrap_pipeline(scope.wrap, args, inner)
        await run_graph_on_success(scope.on_success, args, result)
        await run_pipeline_on_success(scope.dispatch, args, result)

    except Exception as e:
        exc = e
        await run_pipeline_on_failure(scope.on_failure, args, e)
        raise

    finally:
        outcome = Success(value=result) if exc is None else Failure(exc=exc)
        await run_pipeline_finally(scope.finally_, args, outcome)

    return cast(R, result)  # type: ignore[redundant-cast]


# ....................... #


async def run_resolved_scope[R](
    scope: ResolvedScope,
    inner: Callable[[], Awaitable[R]],
    args: Any,
) -> R:
    """Run a resolved scope around an inner callable."""

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
) -> R:
    """Run the transaction scope around the handler."""

    route = tx.route

    if route is None:
        raise exc.internal("Transaction route is required to run a transaction scope")

    async with tx_runner(route):

        async def _handler_call(a: Args) -> R:
            return await handler(a)

        result = await _run_scope_body(tx, args, inner=_handler_call)
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
            _assert_tx_configured(plan.tx)
            return await handler(args)

        return await run_resolved_tx_scope(
            plan.tx,
            handler,
            args,
            tx_runner=tx_runner,
            defer_after_commit=defer_after_commit,
        )

    return await run_resolved_scope(plan.outer, transactional_core, args)
