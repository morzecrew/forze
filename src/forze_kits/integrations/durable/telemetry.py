"""OpenTelemetry spans + metrics for self-hosted durable execution (opt-in).

Pass a :class:`DurableTelemetry` to :class:`~forze_kits.integrations.durable.DurableFunctionRunner`
and :class:`~forze_kits.integrations.durable.DurableScheduler` to emit a span per run
execution plus run-outcome, recovery, and schedule-fire metrics. OpenTelemetry is a core
dependency (imported lazily here); metrics emit via the global providers unless a tracer /
meter is supplied — configure the SDK + exporter in your app.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from opentelemetry.metrics import (  # noqa: F401
        Counter,
        Histogram,
        Meter,
    )
    from opentelemetry.trace import Span, Tracer  # noqa: F401

    from forze.application.contracts.durable.function import DurableRunRecord

# ----------------------- #

DURABLE_RUNS_COUNTER = "forze.durable.runs"
DURABLE_RUN_DURATION_HISTOGRAM = "forze.durable.run.duration"
DURABLE_RECOVERED_COUNTER = "forze.durable.recovered"
DURABLE_SCHEDULE_FIRES_COUNTER = "forze.durable.schedule.fires"

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DurableTelemetry:
    """OpenTelemetry spans + metrics for durable execution.

    Emits: a ``durable.run`` span per run execution (labelled name / run id / attempt /
    tenant, marked error on failure); ``forze.durable.runs`` (counter) +
    ``forze.durable.run.duration`` (histogram, ms) by name and outcome
    (``completed`` / ``failed`` / ``forward_incomplete``); ``forze.durable.recovered``
    (counter) for reclaimed runs; and ``forze.durable.schedule.fires`` (counter) per fire.
    """

    _tracer: "Tracer" = attrs.field(alias="tracer")
    _runs: "Counter" = attrs.field(alias="runs")
    _duration: "Histogram" = attrs.field(alias="duration")
    _recovered: "Counter" = attrs.field(alias="recovered")
    _fires: "Counter" = attrs.field(alias="fires")

    # ....................... #

    @classmethod
    def create(
        cls,
        *,
        tracer: "Tracer | None" = None,
        meter: "Meter | None" = None,
    ) -> "DurableTelemetry":
        """Build the telemetry, using the global OTel providers unless *tracer* / *meter* given."""

        from opentelemetry import metrics, trace

        tracer = tracer or trace.get_tracer("forze")
        meter = meter or metrics.get_meter("forze")

        return cls(
            tracer=tracer,
            runs=meter.create_counter(
                DURABLE_RUNS_COUNTER,
                unit="1",
                description="Durable run executions by outcome.",
            ),
            duration=meter.create_histogram(
                DURABLE_RUN_DURATION_HISTOGRAM,
                unit="ms",
                description="Durable run execution duration in milliseconds.",
            ),
            recovered=meter.create_counter(
                DURABLE_RECOVERED_COUNTER,
                unit="1",
                description="Abandoned durable runs reclaimed by the recovery scanner.",
            ),
            fires=meter.create_counter(
                DURABLE_SCHEDULE_FIRES_COUNTER,
                unit="1",
                description="Recurring durable-schedule fires.",
            ),
        )

    # ....................... #

    def run_span(self, record: "DurableRunRecord") -> "AbstractContextManager[Span]":
        """Open a ``durable.run`` span for executing *record*."""

        attributes: dict[str, str] = {
            "forze.durable.name": record.name,
            "forze.durable.run_id": record.run_id,
            "forze.durable.attempt": str(record.attempts),
        }

        if record.tenant_id is not None:
            attributes["forze.tenant_id"] = str(record.tenant_id)

        return self._tracer.start_as_current_span("durable.run", attributes=attributes)

    # ....................... #

    def mark_error(self, span: "Span", error: BaseException) -> None:
        """Record *error* on *span* and set its status to error."""

        from opentelemetry.trace import Status, StatusCode

        span.record_exception(error)
        span.set_status(Status(StatusCode.ERROR))

    # ....................... #

    def record_run(self, name: str, outcome: str, duration_ms: float) -> None:
        """Count a run execution and record its duration, labelled by name + outcome."""

        labels = {"forze.durable.name": name, "forze.durable.outcome": outcome}
        self._runs.add(1, labels)
        self._duration.record(duration_ms, labels)

    # ....................... #

    def record_recovered(self, count: int) -> None:
        """Count *count* runs reclaimed by a recovery sweep (no-op when zero)."""

        if count:
            self._recovered.add(count)

    # ....................... #

    def record_fire(self, name: str) -> None:
        """Count one schedule fire for function *name*."""

        self._fires.add(1, {"forze.durable.name": name})
