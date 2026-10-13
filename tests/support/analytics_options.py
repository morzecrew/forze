"""Every analytics read path refuses an option :class:`AnalyticsRunOptions` does not define.

Each backend reads its options on each path for itself, so each path is asked: a misspelt
``max_rows`` or ``timeout`` that one path dropped would lift the bound the caller meant to set.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
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
        "select_run_chunked": lambda: _drain(
            port.select_run_chunked(BaseModel, query_key, params, options=options)
        ),
    }

    for path, call in paths.items():
        with pytest.raises(CoreException, match="max_rowz") as refused:
            await call()

        assert refused.value.kind is ExceptionKind.PRECONDITION, path
