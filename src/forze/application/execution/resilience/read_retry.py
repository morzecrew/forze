"""Lightweight exponential-backoff retry for idempotent client reads.

This is deliberately *not* routed through the full resilience-policy
machinery: warehouse/cache clients use it as a small, config-driven retry
loop around transient transport failures on their read paths.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from forze.base.exceptions import exc

# ----------------------- #

T = TypeVar("T")

DEFAULT_READ_RETRY_EXC: tuple[type[BaseException], ...] = (
    TimeoutError,
    OSError,
    ConnectionError,
)
"""Default transient transport errors retried by :func:`retry_read`."""

# ....................... #


async def retry_read(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    base_delay: float,
    retry_on: tuple[type[BaseException], ...] = DEFAULT_READ_RETRY_EXC,
    on_retry: Callable[[int], None] | None = None,
) -> T:
    """Run *fn*, retrying on *retry_on* with exponential backoff.

    :param fn: Zero-argument coroutine factory for the read operation.
    :param attempts: Number of retries after the initial call (``0`` disables
        retrying); negative values are treated as ``0``.
    :param base_delay: Initial delay in seconds, doubled per retry
        (``base_delay * 2**i``); negative values are treated as ``0``.
    :param retry_on: Exception types considered transient and retried.
    :param on_retry: Optional hook invoked with the 1-based retry number
        before sleeping.
    :returns: The result of the first successful call.
    :raises BaseException: The last transient error once *attempts* is
        exhausted; non-transient errors propagate immediately.
    """

    total = max(0, attempts)
    delay = max(0.0, base_delay)

    for i in range(total + 1):
        try:
            return await fn()

        except retry_on:
            if i >= total:
                raise

            if on_retry is not None:
                on_retry(i + 1)

            await asyncio.sleep(delay * (2**i))

    raise exc.internal("Read retry loop exited without a result.")  # pragma: no cover
