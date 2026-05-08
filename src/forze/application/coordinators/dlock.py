import asyncio
import contextlib
import secrets
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Callable

import attrs

from forze.base.errors import CoreError

from .._logger import logger
from ..contracts.dlock import DistributedLockCommandPort

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DistributedLockCoordinator:
    """Distributed lock coordinator."""

    cmd: DistributedLockCommandPort
    """Distributed lock command port."""

    owner_provider: Callable[[], str]
    """Callable to provide the owner of the lock."""

    extend_interval: timedelta | None = None
    """Interval at which to extend the lock's time-to-live."""

    retry_interval: timedelta = timedelta(milliseconds=100)
    """Interval at which to retry the lock acquisition."""

    retry_jitter: timedelta = timedelta(milliseconds=20)
    """Jitter to add to the retry interval."""

    wait_timeout: timedelta | None = None
    """Timeout to wait for the lock acquisition."""

    # ....................... #

    @asynccontextmanager
    async def scope(self, key: str) -> AsyncIterator[bool]:
        loop = asyncio.get_running_loop()

        deadline = (
            None
            if self.wait_timeout is None
            else loop.time() + self.wait_timeout.total_seconds()
        )

        owner = self.owner_provider()

        async def try_acquire_until_deadline() -> bool:
            nonlocal deadline

            attempt = 0

            while True:
                ok = await self.cmd.acquire(key, owner)

                if ok:
                    return True

                if deadline is None:
                    return False

                remaining = deadline - loop.time()

                if remaining <= 0:
                    return False

                jitter_ms = int(self.retry_jitter.total_seconds() * 1000) + 1

                sleep_for = min(
                    self.retry_interval.total_seconds() + secrets.randbelow(jitter_ms),
                    remaining,
                )

                attempt += 1
                await asyncio.sleep(sleep_for)

        acquired = await try_acquire_until_deadline()

        if not acquired:
            raise CoreError("Failed to acquire distributed lock")

        extend_task: asyncio.Task[Any] | None = None
        stop_event = asyncio.Event()
        extend_errors: list[Exception] = []

        async def extend_lock(
            key: str,
            owner: str,
            interval: timedelta,
        ) -> None:
            interval_s = interval.total_seconds()

            try:
                while not stop_event.is_set():
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
                        break

                    except asyncio.TimeoutError:
                        ok = await self.cmd.reset(key, owner)

                        if not ok:
                            break

            except Exception as e:
                logger.error("Failed to extend distributed lock: %s", e)
                extend_errors.append(e)
                stop_event.set()

        try:
            if self.extend_interval is not None:
                extend_task = asyncio.create_task(
                    extend_lock(key, owner, self.extend_interval)
                )

            yield True

        finally:
            if self.extend_interval is not None and extend_task is not None:
                stop_event.set()

                try:
                    await asyncio.wait_for(extend_task, timeout=0.005)

                except asyncio.TimeoutError:
                    extend_task.cancel()

                    with contextlib.suppress(asyncio.CancelledError):
                        await extend_task

            try:
                await asyncio.shield(self.cmd.release(key, owner))

            except Exception:  # nosec B110
                pass

            if extend_errors:
                raise CoreError(
                    "Failed to extend distributed lock",
                    details={"error": str(extend_errors[0])},
                )
