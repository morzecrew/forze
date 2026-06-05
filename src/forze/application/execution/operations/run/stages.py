"""Low-level executors for resolved scope stages."""

from typing import Any, Awaitable, Callable

from forze.application.contracts.execution import (
    Before,
    ExecutionGraph,
    ExecutionPipeline,
    Finally,
    Middleware,
    OnFailure,
    OnSuccess,
    Outcome,
)

from ...graph_run import run_graph_waves_forward

# ----------------------- #


async def run_graph_before[Args](
    graph: ExecutionGraph[Before[Args]],
    args: Args,
) -> None:
    """Run before hooks in topological wave order."""

    async def _run_hook(hook: Before[Args]) -> None:
        await hook(args)

    await run_graph_waves_forward(graph, _run_hook, concurrent=False)


# ....................... #


async def run_graph_on_success[Args, R](
    graph: ExecutionGraph[OnSuccess[Args, R]],
    args: Args,
    result: R,
) -> None:
    """Run on-success hooks in topological wave order."""

    async def _run_hook(hook: OnSuccess[Args, R]) -> None:
        await hook(args, result)

    await run_graph_waves_forward(graph, _run_hook, concurrent=False)


# ....................... #


async def run_pipeline_on_failure[Args](
    pipeline: ExecutionPipeline[OnFailure[Args]],
    args: Args,
    exc: Exception,
) -> None:
    """Run on-failure hooks in pipeline order."""

    for hook in pipeline.steps:
        await hook(args, exc)


# ....................... #


async def run_pipeline_finally[Args, R](
    pipeline: ExecutionPipeline[Finally[Args, R]],
    args: Args,
    outcome: Outcome[R],
) -> None:
    """Run finally hooks in pipeline order."""

    for hook in pipeline.steps:
        await hook(args, outcome)


# ....................... #


async def run_pipeline_on_success[Args, R](
    pipeline: ExecutionPipeline[OnSuccess[Args, R]],
    args: Args,
    result: R,
) -> None:
    """Run on-success pipeline hooks (including dispatch) in order."""

    for hook in pipeline.steps:
        await hook(args, result)


# ....................... #


def _wrap_step[Args, R](
    middleware: Middleware[Any, Any],
    nxt: Callable[[Args], Awaitable[R]],
) -> Callable[[Args], Awaitable[R]]:
    async def _call(call_args: Args) -> R:
        return await middleware(nxt, call_args)

    return _call


async def run_wrap_pipeline[Args, R](
    pipeline: ExecutionPipeline[Middleware[Any, Any]],
    args: Args,
    inner: Callable[[Args], Awaitable[R]],
) -> R:
    """Run middleware wrap chain; higher priority is closer to the handler.

    Folds the chain iteratively from the innermost handler outward instead of
    recursing per request, so there is no recursion frame or repeated ``len()`` per
    middleware, and a chain with no middleware calls the handler directly.
    """

    steps = pipeline.steps

    if not steps:
        return await inner(args)

    call = inner

    for middleware in reversed(steps):
        call = _wrap_step(middleware, call)

    return await call(args)
