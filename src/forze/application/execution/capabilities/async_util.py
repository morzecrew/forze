"""Async helpers for capability segments."""

import inspect
from typing import Any


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value

    return value
