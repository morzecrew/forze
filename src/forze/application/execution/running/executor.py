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

# ----------------------- #


async def run_graph_before[Args](
    graph: ExecutionGraph[Before[Args]],
    args: Args,
) -> None:
    """Run before hooks in topological wave order."""

    for wave in graph.waves:
        for step_id in wave:
            hook = graph.steps[step_id]
            await hook(args)


# ....................... #


async def run_graph_on_success[Args, R](
    graph: ExecutionGraph[OnSuccess[Args, R]],
    args: Args,
    result: R,
) -> None:
    """Run on-success hooks in topological wave order."""

    for wave in graph.waves:
        for step_id in wave:
            hook = graph.steps[step_id]
            await hook(args, result)


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


async def run_wrap_pipeline[Args, R](
    pipeline: ExecutionPipeline[Middleware[Any, Any]],
    args: Args,
    inner: Callable[[Args], Awaitable[R]],
) -> R:
    """Run middleware wrap chain; higher priority is closer to the handler."""

    async def invoke(index: int, call_args: Args) -> R:
        if index >= len(pipeline.steps):
            return await inner(call_args)

        middleware = pipeline.steps[index]

        async def next_call(next_args: Args) -> R:
            return await invoke(index + 1, next_args)

        return await middleware(next_call, call_args)

    return await invoke(0, args)
