import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator, Callable, Final

import attrs

from forze_kits.scopes._logger import logger
from forze.application.contracts.dlock import (
    AcquiredLock,
    DistributedLockCommandPort,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import current_entropy_source

# ----------------------- #

EXTEND_TASK_SHUTDOWN_GRACE: Final[timedelta] = timedelta(milliseconds=5)
"""Grace period to let the extend heartbeat observe its stop event before cancelling.

The heartbeat wakes as soon as the stop event is set, so this only needs to cover one
event-loop scheduling hop — not a full ``extend_interval``.
"""


@attrs.define(slots=True, kw_only=True, frozen=True)
class DistributedLockScope:
    """Async context manager for distributed lock acquire/extend/release.

    The protected body is the caller's ``async with`` block, so the scope cannot
    cancel it mid-flight. When the extend heartbeat loses the lock (``reset``
    returns ``False`` — expired or stolen) or fails with an exception, the
    heartbeat stops, the failure is recorded, and the scope raises on exit so the
    loss never goes unnoticed. If the body itself raised, the body's exception
    propagates unchanged and the lock failure is attached as context (a warning
    log plus an exception note) instead of masking it.

    **Fencing.** The scope yields the :class:`~forze.application.contracts.dlock.AcquiredLock`
    handle (``key`` / ``owner`` / ``token``). Tokens are monotonically increasing
    per key across lock generations; the extend heartbeat (``reset``) keeps the
    same generation, so the token never changes mid-scope. Thread the token into
    downstream writes and reject stale ones storage-side::

        async with dlock_scope.scope("invoice:42") as lock:
            # e.g. UPDATE invoice SET ..., fence = :token
            #      WHERE id = 42 AND fence < :token
            await repo.update_invoice(invoice, fence_token=lock.token)

    Without that consumer-side check the scope remains best-effort exclusion
    (a GC- or network-paused holder can resume after expiry while a new holder
    runs); the token check is what upgrades it to fenced exclusion. A ``token``
    of ``None`` means the backend cannot issue fencing tokens at all.
    """

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

    def __attrs_post_init__(self) -> None:
        if self.retry_interval.total_seconds() <= 0:
            raise exc.configuration("Retry interval must be positive")

        if self.retry_jitter.total_seconds() < 0:
            raise exc.configuration("Retry jitter must be non-negative")

        if self.wait_timeout is not None and self.wait_timeout.total_seconds() <= 0:
            raise exc.configuration("Wait timeout must be positive")

        if (
            self.extend_interval is not None
            and self.extend_interval.total_seconds() <= 0
        ):
            raise exc.configuration("Extend interval must be positive")

    # ....................... #

    @asynccontextmanager
    async def scope(self, key: str) -> AsyncGenerator[AcquiredLock]:  # skipcq: PY-R1000
        loop = asyncio.get_running_loop()

        deadline = (
            None
            if self.wait_timeout is None
            else loop.time() + self.wait_timeout.total_seconds()
        )

        owner = self.owner_provider()

        async def try_acquire_until_deadline() -> AcquiredLock | None:
            nonlocal deadline

            while True:
                acquired = await self.cmd.acquire(key, owner)

                if acquired is not None:
                    return acquired

                if deadline is None:
                    return None

                remaining = deadline - loop.time()

                if remaining <= 0:
                    return None

                # Base delay plus optional jitter, all in seconds. ``randrange`` counts
                # integer steps; previously ms-sized steps were added to ``retry_interval``
                # in seconds, inflating sleeps by ~1000x.
                retry_s = self.retry_interval.total_seconds()
                jitter_s = max(0.0, self.retry_jitter.total_seconds())

                if jitter_s > 0:
                    jitter_max_ns = int(jitter_s * 1_000_000_000)

                    extra_s = (
                        current_entropy_source()
                        .as_random()
                        .randrange(jitter_max_ns + 1)
                        / 1_000_000_000
                        if jitter_max_ns > 0
                        else 0.0
                    )

                else:
                    extra_s = 0.0

                sleep_for = min(retry_s + extra_s, remaining)

                await asyncio.sleep(sleep_for)

        acquired = await try_acquire_until_deadline()

        if acquired is None:
            raise exc.internal("Failed to acquire distributed lock")

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
                            # Lock expired or was stolen: the body keeps running
                            # (it cannot be cancelled mid-flight), so record the
                            # loss, stop extending, and raise at scope exit.
                            lost = exc.concurrency(
                                "Distributed lock lost before scope exit "
                                "(expired or stolen)",
                                details={"key": key, "owner": owner},
                            )
                            logger.error(
                                "Distributed lock lost before scope exit",
                                key=key,
                                owner=owner,
                            )
                            extend_errors.append(lost)
                            stop_event.set()
                            break

            except Exception as e:
                logger.error("Failed to extend distributed lock: %s", e)
                extend_errors.append(e)
                stop_event.set()

        body_exc: BaseException | None = None

        try:
            if self.extend_interval is not None:
                extend_task = asyncio.create_task(
                    extend_lock(key, owner, self.extend_interval)
                )

            yield acquired

        except BaseException as e:
            body_exc = e
            raise

        finally:
            if self.extend_interval is not None and extend_task is not None:
                stop_event.set()

                try:
                    await asyncio.wait_for(
                        extend_task,
                        timeout=EXTEND_TASK_SHUTDOWN_GRACE.total_seconds(),
                    )

                except asyncio.TimeoutError:
                    extend_task.cancel()

                    with contextlib.suppress(asyncio.CancelledError):
                        await extend_task

            try:
                await asyncio.shield(self.cmd.release(key, owner))

            except Exception as e:
                logger.warning(
                    "Failed to release distributed lock",
                    key=key,
                    owner=owner,
                    error=str(e),
                )

            if extend_errors:
                first = extend_errors[0]
                lock_exc = (
                    first
                    if isinstance(first, CoreException)
                    else exc.internal(
                        "Failed to extend distributed lock",
                        details={"key": key, "error": str(first)},
                    )
                )

                if body_exc is None:
                    raise lock_exc

                # The body raised: never mask its exception with the lock
                # problem — attach the lock failure as context instead.
                logger.warning(
                    "Distributed lock extend failed while scope body raised; "
                    "propagating the body's exception",
                    key=key,
                    owner=owner,
                    error=str(first),
                )
                body_exc.add_note(f"distributed lock problem during scope: {lock_exc}")
