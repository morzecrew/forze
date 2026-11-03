"""Every analytics read path refuses an option :class:`AnalyticsRunOptions` does not define,
and is bounded by its route's timeout.

Each backend reads its options on each path for itself, so every public method is asked: a
misspelt ``max_rows`` or ``timeout`` that one path dropped would lift the bound the caller meant
to set, and a path that forgot the route's timeout would run unbounded.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


async def _drain(chunks: AsyncIterator[Any]) -> None:
    async for _chunk in chunks:
        pass


async def assert_every_path_refuses_an_unknown_option(
    port: Any, query_key: str, params: BaseModel
) -> None:
    options: Any = {"max_rowz": 1}
    paths: dict[str, Callable[[], Any]] = {
        "run": lambda: port.run(query_key, params, options=options),
        "run_page": lambda: port.run_page(query_key, params, options=options),
        "run_cursor": lambda: port.run_cursor(query_key, params, options=options),
        "run_chunked": lambda: _drain(port.run_chunked(query_key, params, options=options)),
        "project_run": lambda: port.project_run(["x"], query_key, params, options=options),
        "project_run_page": lambda: port.project_run_page(
            ["x"], query_key, params, options=options
        ),
        "project_run_cursor": lambda: port.project_run_cursor(
            ["x"], query_key, params, options=options
        ),
        "project_run_chunked": lambda: _drain(
            port.project_run_chunked(["x"], query_key, params, options=options)
        ),
        "select_run": lambda: port.select_run(BaseModel, query_key, params, options=options),
        "select_run_page": lambda: port.select_run_page(
            BaseModel, query_key, params, options=options
        ),
        "select_run_cursor": lambda: port.select_run_cursor(
            BaseModel, query_key, params, options=options
        ),
        "select_run_chunked": lambda: _drain(
            port.select_run_chunked(BaseModel, query_key, params, options=options)
        ),
    }

    for path, call in paths.items():
        with pytest.raises(CoreException, match="max_rowz") as refused:
            await call()

        assert refused.value.kind is ExceptionKind.PRECONDITION, path


# ....................... #


async def assert_every_read_is_bounded_by_the_route_timeout(
    port: Any,
    query_key: str,
    params: BaseModel,
    *,
    route: timedelta,
    seen: list[Any],
) -> None:
    """Each read path hands the client the route's timeout when the call sets none, clamps a
    per-call timeout above it down to it, and keeps one below it.

    :param seen: The timeouts the test's client was handed, in call order; cleared per path.
    """

    below = route / 2
    cases: list[tuple[Any, timedelta]] = [
        (None, route),
        ({"timeout": route * 10}, route),
        ({"timeout": below}, below),
    ]

    for options, expected in cases:
        paths: dict[str, Callable[[], Any]] = {
            "run": lambda: port.run(query_key, params, options=options),
            "run_page": lambda: port.run_page(query_key, params, options=options),
            "run_cursor": lambda: port.run_cursor(query_key, params, options=options),
            "run_chunked": lambda: _drain(port.run_chunked(query_key, params, options=options)),
        }

        for path, call in paths.items():
            seen.clear()
            await call()

            assert seen, path
            assert all(t == expected for t in seen), (path, options, seen)
