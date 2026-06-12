from typing import Any, Awaitable, Callable

import pytest
from anyio import from_thread, to_thread
from anyio.lowlevel import current_token


@pytest.fixture(scope="session", autouse=True)
def _configure_perf_logging() -> None:
    """Configure logging once for the perf session (level info, json render).

    Without this, the first benchmarks in a session would run with logging
    unconfigured; trace is gated at INFO by default either way, but explicit
    configuration keeps perf numbers honest and independent of test order
    (previously, tests before the one that called ``configure_logging`` paid
    the full unconfigured-trace pipeline cost on every per-item trace call).
    """

    from forze.base.logging import configure_logging

    configure_logging(level="info", render_mode="json")


@pytest.fixture
async def async_benchmark(benchmark: Any) -> Any:
    """
    Repurposes the pytest-benchmark fixture for coroutines.
    This can be used similarly to the sync fixture, e.g.:
    >>> await async_benchmark(coro_to_benchmark, arg1, arg2, ...)

    It executes the pytest-benchmark code in a separate thread while running the coroutine
    in the main event loop. This means AsyncClients will work as expected.
    """
    token = current_token()  # Get a token for the main event loop

    async def benchmarker(f: Callable[..., Awaitable[Any]], *args: Any) -> Any:
        return await to_thread.run_sync(
            lambda: benchmark(from_thread.run, f, *args, token=token)
        )

    return benchmarker
