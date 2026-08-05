"""The lifetime handle over the providers :func:`bootstrap_telemetry` created."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, final

import attrs

from forze.base._logger import logger
from forze.base.primitives import monotonic

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

# ----------------------- #


@final
@attrs.define(slots=True, eq=False)
class _ShutdownGate:
    """Single-flight state for :meth:`TelemetryHandle.shutdown`.

    Mutable, and deliberately not part of the handle's own frozen value: "has this been
    shut down" is lifecycle, not identity.
    """

    lock: asyncio.Lock = attrs.field(factory=asyncio.Lock)
    """Serializes entry. An ``asyncio.Lock`` binds to no event loop until it is first
    awaited, so building it here is safe."""

    task: asyncio.Future[None] | None = None
    """The one teardown in flight, shared by every caller.

    Held rather than awaited inline because the work runs in a thread and cannot be
    cancelled: if the caller that started it goes away, the thread keeps closing providers
    regardless, so the next caller has to be able to find that work and wait for *it*.
    """

    done: bool = False
    """Set only once the teardown has actually finished — never on the way in."""


@final
@attrs.define(slots=True, frozen=True)
class TelemetryHandle:
    """Owns the providers ``bootstrap_telemetry`` created — and only those.

    A signal the bootstrap deferred on (an SDK the application had already installed) is
    ``None`` here, so :meth:`flush` and :meth:`shutdown` never reach into a provider whose
    lifecycle somebody else owns.

    **Order matters at shutdown.** Call :meth:`shutdown` *before* closing clients and pools:
    the final metric collection runs observable callbacks, and those callbacks read live
    objects (pool stats, keyring stats, bulkhead depths). Tear the clients down first and the
    last collection either raises inside the exporter thread or reports numbers from a
    half-disposed object.
    """

    tracer_provider: TracerProvider | None = None
    """The SDK tracer provider this bootstrap installed, or ``None`` when it deferred."""

    meter_provider: MeterProvider | None = None
    """The SDK meter provider this bootstrap installed, or ``None`` when it deferred."""

    resource: Resource | None = None
    """The resource identity both providers were built with; ``None`` when both deferred."""

    _gate: _ShutdownGate = attrs.field(
        factory=_ShutdownGate,
        init=False,
        repr=False,
        eq=False,
    )

    # ....................... #

    async def flush(self, timeout: float = 5.0) -> None:
        """Force-export everything buffered, without ending the providers' lives.

        Blocking SDK work (the batch processor's handoff, the exporter's HTTP round trip)
        runs off the event loop. Failures are logged, never raised — including the ones the
        SDK signals by *raising*: ``MeterProvider.force_flush`` throws when a reader times
        out rather than returning ``False``, and a telemetry stall must not become the
        exception that fails whatever asked for the flush.

        :param timeout: Budget in seconds, genuinely shared: both signals flush against one
            deadline, so a slow span export eats into what metrics get rather than doubling
            the wall clock.
        """

        await asyncio.to_thread(self._flush_blocking, monotonic() + max(0.0, timeout))

    # ....................... #

    async def shutdown(self, timeout: float = 5.0) -> None:
        """Flush, then shut both providers down. Idempotent and single-flight.

        This is the contract half of "flush is part of drain": wire it into the FastAPI
        lifespan or the runner's shutdown, *after* the drain gate flips, so the last metric
        interval and span batch outlive the pod instead of dying with it. The SDK's own
        ``atexit`` hook stays registered until this runs, as a best-effort backstop for
        processes that exit without draining.

        Concurrent callers — a runner's own hook plus a wrapping ``finally`` — all wait for
        the *same* shutdown to finish. Returning early on a second call would tell that
        caller the providers were closed while the first call was still flushing, and it
        would go on to dispose the very clients the final collection is reading.

        That holds under cancellation too. The teardown runs in a thread, which nothing can
        cancel, so a cancelled caller leaves it running: the completion flag is set by the
        work finishing, never by a caller walking away from it. A retry after a cancelled
        shutdown waits for the original.

        :param timeout: Budget in seconds for the flush and the meter provider's teardown.
            It does **not** bound the tracer provider's own teardown: the SDK's
            ``TracerProvider.shutdown()`` takes no timeout and joins its batch worker on
            its own 30s budget. In practice the flush above has already drained the queue,
            so that join returns immediately unless the exporter itself is wedged.
        """

        gate = self._gate

        async with gate.lock:
            if gate.done:
                return

            if gate.task is None:
                gate.task = asyncio.ensure_future(
                    asyncio.to_thread(self._shutdown_blocking, timeout)
                )

            # Shielded, so a cancelled caller does not cancel the shared teardown out from
            # under everybody else. ``done`` is set *after* the await returns, so being
            # cancelled here leaves the gate open and the next caller waits on the same
            # task rather than being told the providers are closed while they are not.
            await asyncio.shield(gate.task)

            gate.done = True

    # ....................... #

    def _flush_blocking(self, deadline: float) -> None:
        if self.tracer_provider is not None:
            self._guarded(
                "span flush",
                lambda: self.tracer_provider.force_flush(int(_remaining_millis(deadline))),  # type: ignore[union-attr]
            )

        if self.meter_provider is not None:
            self._guarded(
                "metric flush",
                lambda: self.meter_provider.force_flush(_remaining_millis(deadline)),  # type: ignore[union-attr]
            )

    # ....................... #

    def _shutdown_blocking(self, timeout: float) -> None:
        deadline = monotonic() + max(0.0, timeout)

        # Flushing first is what guarantees the tail batch under a bounded budget; the
        # providers' own teardown drains again, but only on the SDK's terms.
        self._flush_blocking(deadline)

        # Each provider is torn down independently: a metric reader that fails to stop must
        # not leave the span processor running (and vice versa), because the process is on
        # its way out and nobody will get a second chance to close either one.
        if self.tracer_provider is not None:
            self._guarded("tracer provider shutdown", self.tracer_provider.shutdown)

        if self.meter_provider is not None:
            self._guarded(
                "meter provider shutdown",
                lambda: self.meter_provider.shutdown(  # type: ignore[union-attr]
                    timeout_millis=_remaining_millis(deadline)
                ),
            )

    # ....................... #

    @staticmethod
    def _guarded(step: str, call: Callable[[], object]) -> None:
        """Run one teardown step, absorbing whatever it does on the way out.

        Both shapes of failure are absorbed: a ``False`` return (the documented "did not
        finish in time") and an exception (what the metrics SDK actually raises — its
        ``force_flush`` throws on a reader timeout and never returns ``False``). Every step
        after this one still has to run: the process is leaving, and a provider left
        running because an earlier one threw is a thread that outlives its own telemetry.
        """

        try:
            if call() is False:
                logger.warning("Telemetry %s did not complete within its budget", step)

        except Exception:
            logger.warning("Telemetry %s failed", step, exc_info=True)


# ....................... #


def _remaining_millis(deadline: float) -> float:
    """Milliseconds left before *deadline*, never negative.

    Zero is a meaningful value to hand the SDK: it means "you are already out of budget",
    and every call site treats it as an immediate timeout rather than as "no limit".
    """

    return max(0.0, deadline - monotonic()) * 1000.0
