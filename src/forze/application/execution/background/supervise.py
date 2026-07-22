"""Crash-restart supervision for a forever-running background loop.

The restart discipline every long-running consumer needs, extracted once: run the loop, and
when it crashes, restart it after a **jittered** backoff — a shared downstream outage crashes
every replica's loop at once, and without jitter they all restart (and re-crash) in lockstep.

Three exits, each deliberate:

- **Stop requested** — the loop returned after its stop event was set: a clean shutdown.
- **Configuration error** — wiring does not fix itself by retrying; restarting a loop that
  cannot resolve its route hot-loops a critical log forever. Terminal, logged as such.
- **Crash-loop ceiling** (opt-in) — too many consecutive short-lived runs means the fault is
  not transient. Terminal rather than a quieter kind of down. A run that survives past the
  healthy-uptime threshold resets the streak, so a loop that recovers for hours and then
  trips again is treated as a fresh incident, not the next strike.

A clean return with **no** stop requested is treated as a fault and restarted: these loops run
forever by contract, so a normal exit means the work is silently down — restarting after the
same backoff beats trusting whatever ended it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import Final

from forze.application._logger import logger
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import current_entropy_source

# ----------------------- #


def is_terminal_crash(error: BaseException) -> bool:
    """Whether *error* means restarting the loop is pointless.

    A ``CONFIGURATION``-kind :class:`CoreException` is the framework's marker for a fault
    that retrying cannot clear — bad wiring, but also a dependency the deployment is not
    allowed to use or that no longer exists (a revoked or deleted KMS key). Restarting on
    one of those hot-loops a critical log until a human intervenes.

    Shared so every supervisor agrees on what "terminal" means: :func:`run_supervised` and
    the consumers that run their own restart loop must not drift apart on this.
    """

    return isinstance(error, CoreException) and error.kind is ExceptionKind.CONFIGURATION


# ....................... #

HEALTHY_UPTIME_SECONDS: Final[float] = 30.0
"""A run that survives this long resets the consecutive-crash streak.

Separates "flapping on startup" (the ceiling's target) from "crashed after hours of honest
work" (a fresh incident): only short-lived runs count toward the ceiling.
"""


# ....................... #


async def _sleep_or_stop(stop: asyncio.Event, delay: float) -> bool:
    """Wait out *delay*, interruptibly; ``True`` when a stop was requested meanwhile."""

    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)

    return stop.is_set()


# ----------------------- #


async def run_supervised(
    run: Callable[[], Awaitable[None]],
    *,
    stop: asyncio.Event,
    name: str,
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    on_crash: Callable[[BaseException], None] | None = None,
) -> None:
    """Run *run* until *stop* is set, restarting it on crash after a jittered backoff.

    Pair with :class:`~forze.application.execution.background.loop.BackgroundLoopControl`:
    pass ``control.event`` as *stop* and let the loop honor it at its unit boundaries, so a
    graceful stop ends the run instead of tripping a restart.

    ``CancelledError`` always propagates — structured cancellation is the shutdown backstop,
    never a crash. A ``CONFIGURATION``-kind :class:`CoreException` is terminal (see module
    docstring). *on_crash* is a synchronous observation hook (metrics, per-loop health); it
    runs before the backoff and must not raise.

    :param run: One attempt of the loop; called again after each restart.
    :param stop: The stop signal; checked between runs and during every backoff sleep.
    :param name: Identifies the loop in supervision logs.
    :param restart_backoff: Base backoff between restarts; jittered ×[1.0, 1.5).
    :param max_consecutive_crashes: Terminal ceiling on short-lived runs; ``None`` = restart
        forever (every crash is still logged loudly).
    """

    if restart_backoff.total_seconds() <= 0:
        raise exc.configuration("Restart backoff must be positive")

    if max_consecutive_crashes is not None and max_consecutive_crashes <= 0:
        raise exc.configuration("Crash ceiling must be positive")

    clock = asyncio.get_running_loop()
    crashes = 0

    while True:
        started = clock.time()

        try:
            await run()

        except asyncio.CancelledError:
            raise

        except Exception as error:
            if is_terminal_crash(error):
                logger.critical(
                    "Background loop %s hit a configuration error; supervision stopped — "
                    "wiring does not fix itself, fix it and restart the process",
                    name,
                    exc_info=error,
                )
                return

            if on_crash is not None:
                try:
                    on_crash(error)

                except Exception as observer_error:
                    # An observation hook must never take supervision down with it —
                    # the original crash still gets logged and the loop still restarts.
                    logger.error(
                        "Background loop %s crash observer failed",
                        name,
                        exc_info=observer_error,
                    )

            logger.error(
                "Background loop %s crashed; restarting after backoff", name, exc_info=error
            )

        else:
            if stop.is_set():
                return

            # These loops run forever by contract — a clean return means the work is
            # silently down. Restart it, paced like a crash so a broken loop can't spin.
            logger.warning("Background loop %s returned unexpectedly; restarting", name)

        crashes = 1 if clock.time() - started >= HEALTHY_UPTIME_SECONDS else crashes + 1

        if max_consecutive_crashes is not None and crashes >= max_consecutive_crashes:
            logger.critical(
                "Background loop %s failed %d times in a row; supervision stopped — "
                "the fault is not transient, fix it and restart the process",
                name,
                crashes,
            )
            return

        if await _sleep_or_stop(
            stop,
            # Desynchronization jitter, not security randomness.
            restart_backoff.total_seconds()
            * current_entropy_source().as_random().uniform(1.0, 1.5),
        ):
            return
