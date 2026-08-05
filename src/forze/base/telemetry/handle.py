"""The lifetime handle over the providers :func:`bootstrap_telemetry` created."""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, final

import attrs

from forze.base._logger import logger

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

# ----------------------- #


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

    _closed: threading.Event = attrs.field(
        factory=threading.Event,
        init=False,
        repr=False,
        eq=False,
    )
    """Latches on the first :meth:`shutdown`, so a lifecycle that shuts down twice — the
    runner's own hook plus a wrapping ``finally`` — does not trip the SDK's
    "shutdown can only be called once" warning or block a second time on a dead exporter."""

    # ....................... #

    async def flush(self, timeout: float = 5.0) -> None:
        """Force-export everything buffered, without ending the providers' lives.

        Blocking SDK work (the batch processor's handoff, the exporter's HTTP round trip)
        runs off the event loop. An incomplete flush is logged, not raised: a telemetry
        stall must not fail the operation that asked for the flush.

        :param timeout: Budget in seconds, shared by both signals.
        """

        await asyncio.to_thread(self._flush_blocking, timeout)

    # ....................... #

    async def shutdown(self, timeout: float = 5.0) -> None:
        """Flush, then shut both providers down. Idempotent.

        This is the contract half of "flush is part of drain": wire it into the FastAPI
        lifespan or the runner's shutdown, *after* the drain gate flips, so the last metric
        interval and span batch outlive the pod instead of dying with it. The SDK's own
        ``atexit`` hook stays registered until this runs, as a best-effort backstop for
        processes that exit without draining.

        :param timeout: Budget in seconds, shared by flush and shutdown across both signals.
        """

        if self._closed.is_set():
            return

        self._closed.set()

        await asyncio.to_thread(self._shutdown_blocking, timeout)

    # ....................... #

    def _flush_blocking(self, timeout: float) -> None:
        millis = max(0.0, timeout) * 1000.0

        if self.tracer_provider is not None and not self.tracer_provider.force_flush(int(millis)):
            logger.warning("Telemetry span flush did not complete within %.1fs", timeout)

        if self.meter_provider is not None and not self.meter_provider.force_flush(millis):
            logger.warning("Telemetry metric flush did not complete within %.1fs", timeout)

    # ....................... #

    def _shutdown_blocking(self, timeout: float) -> None:
        self._flush_blocking(timeout)

        # Each provider is torn down independently: a metric reader that fails to stop must
        # not leave the span processor running (and vice versa), because the process is on
        # its way out and nobody will get a second chance to close either one.
        if self.tracer_provider is not None:
            try:
                self.tracer_provider.shutdown()

            except Exception:
                logger.warning("Telemetry tracer provider shutdown failed", exc_info=True)

        if self.meter_provider is not None:
            try:
                self.meter_provider.shutdown(timeout_millis=max(0.0, timeout) * 1000.0)

            except Exception:
                logger.warning("Telemetry meter provider shutdown failed", exc_info=True)
