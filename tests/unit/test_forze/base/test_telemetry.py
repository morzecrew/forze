"""Unit tests for the OpenTelemetry SDK bootstrap (`forze.base.telemetry`).

The interesting behavior is all in the seams: what happens when an application already
owns an SDK, whether instruments created *before* the provider still record, and whether
the framework's millisecond bucket ladder lands on exactly the two histograms it should.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.metrics._internal import _ProxyMeterProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricExporter,
    MetricExportResult,
)
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import (
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    SERVICE_VERSION,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.util._once import Once

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.telemetry import (
    FORZE_VERSION_ATTRIBUTE,
    MILLISECOND_HISTOGRAM_BUCKETS,
    MILLISECOND_HISTOGRAM_INSTRUMENTS,
    TelemetryHandle,
    bootstrap_telemetry,
    millisecond_histogram_views,
)

# ----------------------- #


@pytest.fixture(autouse=True)
def _reset_otel_globals() -> Iterator[None]:
    """Return the process to "no SDK installed" around every test.

    Both API packages latch their provider behind a ``Once`` **and** memoize the resolved
    real provider inside a module-level proxy. Clearing only the former would leave the
    proxy handing out meters from the previous test's provider, so the proxies are
    replaced too.
    """

    import opentelemetry.metrics._internal as metrics_internal

    def _reset() -> None:
        trace._TRACER_PROVIDER = None
        trace._TRACER_PROVIDER_SET_ONCE = Once()
        trace._PROXY_TRACER_PROVIDER = trace.ProxyTracerProvider()

        metrics_internal._METER_PROVIDER = None
        metrics_internal._METER_PROVIDER_SET_ONCE = Once()
        metrics_internal._PROXY_METER_PROVIDER = _ProxyMeterProvider()

    _reset()

    yield

    _reset()


# ....................... #


def _bootstrap_with_reader(**kwargs: Any) -> tuple[TelemetryHandle, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    handle = bootstrap_telemetry(
        service_name=kwargs.pop("service_name", "orders-api"),
        exporter="none",
        extra_metric_readers=[reader],
        **kwargs,
    )

    return handle, reader


def _histogram_bounds(reader: InMemoryMetricReader, name: str) -> tuple[float, ...] | None:
    data = reader.get_metrics_data()

    if data is None:
        return None

    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != name:
                    continue

                points = list(metric.data.data_points)  # type: ignore[union-attr]

                return tuple(points[0].explicit_bounds)  # type: ignore[union-attr]

    return None


def _point_types(reader: InMemoryMetricReader, name: str) -> list[str]:
    data = reader.get_metrics_data()
    out: list[str] = []

    if data is None:
        return out

    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == name:
                    out.extend(type(p).__name__ for p in metric.data.data_points)  # type: ignore[union-attr]

    return out


# ----------------------- #


class TestProviderInstallation:
    """Who owns the SDK — decision #3."""

    def test_installs_both_providers_when_the_process_has_none(self) -> None:
        handle, _ = _bootstrap_with_reader()

        assert handle.tracer_provider is not None
        assert handle.meter_provider is not None
        assert trace.get_tracer_provider() is handle.tracer_provider
        assert metrics.get_meter_provider() is handle.meter_provider

    # ....................... #

    def test_defers_to_a_tracer_provider_the_application_installed(self) -> None:
        owned = TracerProvider()
        trace.set_tracer_provider(owned)

        handle, _ = _bootstrap_with_reader()

        assert handle.tracer_provider is None, "must not claim a provider it did not create"
        assert trace.get_tracer_provider() is owned
        assert handle.meter_provider is not None, "the other signal is still bootstrapped"

    # ....................... #

    def test_defers_to_a_meter_provider_the_application_installed(self) -> None:
        owned = MeterProvider()
        metrics.set_meter_provider(owned)

        handle = bootstrap_telemetry(service_name="orders-api", exporter="none")

        assert handle.meter_provider is None
        assert metrics.get_meter_provider() is owned
        assert handle.tracer_provider is not None

    # ....................... #

    @pytest.mark.parametrize("signal", ["traces", "metrics"])
    def test_refuses_to_start_on_an_existing_provider_when_asked(self, signal: str) -> None:
        if signal == "traces":
            trace.set_tracer_provider(TracerProvider())

        else:
            metrics.set_meter_provider(MeterProvider())

        with pytest.raises(CoreException) as caught:
            bootstrap_telemetry(
                service_name="orders-api",
                exporter="none",
                on_existing_provider="error",
            )

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    async def test_a_deferred_provider_is_never_shut_down_by_the_handle(self) -> None:
        """Shutting the handle down must leave the application's own SDK working.

        Asserted by exporting a span *after* the shutdown, because a provider that has
        been shut down still hands out tracers quite happily — it just silently stops
        exporting, which is precisely the failure this has to rule out.
        """

        exporter = _RecordingSpanExporter()
        owned = TracerProvider()
        owned.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(owned)

        handle, _ = _bootstrap_with_reader()

        assert handle.tracer_provider is None

        await handle.shutdown()

        owned.get_tracer("app").start_span("after-shutdown").end()

        assert exporter.names == ["after-shutdown"]
        assert exporter.shutdowns == 0

    # ....................... #

    def test_an_error_on_one_signal_leaves_the_other_uninstalled(self) -> None:
        """Both signals are inspected before either is installed.

        Installing the tracer provider and only then discovering the meter provider is
        taken would strand it: OpenTelemetry's slot is set-once, so nothing can reclaim
        it, and the caller holds an exception rather than a handle that could flush it.
        """

        metrics.set_meter_provider(MeterProvider())

        with pytest.raises(CoreException):
            bootstrap_telemetry(
                service_name="orders-api",
                exporter="none",
                on_existing_provider="error",
            )

        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider), (
            "a failed bootstrap must not leave a provider nothing owns"
        )

    # ....................... #

    def test_a_meter_that_fails_to_build_leaves_no_tracer_provider_behind(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Construction failures must not be half-applied either.

        The meter provider is built after the tracer one, and building is where the
        failures live — a malformed view, an exporter package that is not installed.
        Publishing the tracer first would strand it: the caller gets an exception instead
        of a handle, and the set-once slot cannot be reclaimed to try again.
        """

        import opentelemetry.sdk.metrics as sdk_metrics

        def _explode(*_args: Any, **_kwargs: Any) -> MeterProvider:
            raise RuntimeError("meter provider is unbuildable")

        # Patched at the module the bootstrap imports it from, so the failure lands where
        # a real one would: inside the build step, after the tracer provider exists.
        monkeypatch.setattr(sdk_metrics, "MeterProvider", _explode)

        with pytest.raises(RuntimeError, match="unbuildable"):
            bootstrap_telemetry(service_name="orders-api", exporter="none")

        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider), (
            "the tracer provider must not be published when the meter build fails"
        )

    # ....................... #

    def test_a_failed_meter_build_shuts_down_the_readers_it_created(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Atomic startup has to cover what the *builder itself* allocated.

        A periodic reader starts its ticker thread on construction, so a failure between
        that and the provider leaves a thread running against an exporter nobody will ever
        flush or close.
        """

        import opentelemetry.sdk.metrics as sdk_metrics
        import opentelemetry.sdk.metrics.export as sdk_export

        shutdowns: list[str] = []

        class _SpyReader(sdk_export.PeriodicExportingMetricReader):
            def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
                shutdowns.append("reader")
                super().shutdown(timeout_millis=timeout_millis, **kwargs)

        def _explode(*_args: Any, **_kwargs: Any) -> MeterProvider:
            raise RuntimeError("meter provider is unbuildable")

        monkeypatch.setattr(sdk_export, "PeriodicExportingMetricReader", _SpyReader)
        monkeypatch.setattr(sdk_metrics, "MeterProvider", _explode)

        with pytest.raises(RuntimeError, match="unbuildable"):
            # "console" is enough to make the builder create a reader of its own.
            bootstrap_telemetry(service_name="orders-api", exporter="console")

        assert shutdowns == ["reader"], "the builder must close what the builder opened"

    # ....................... #

    def test_a_reader_that_fails_to_construct_does_not_orphan_its_exporter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The exporter exists before the reader that will own it does.

        An OTLP exporter is an open HTTP session, not an inert value, so a reader whose
        constructor raises would leave it with nobody left to close it.
        """

        import opentelemetry.sdk.metrics.export as sdk_export

        closed: list[str] = []

        class _SpyExporter(sdk_export.ConsoleMetricExporter):
            def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
                closed.append("exporter")
                super().shutdown(timeout_millis=timeout_millis, **kwargs)

        def _refuse(*_args: Any, **_kwargs: Any) -> object:
            raise RuntimeError("reader will not start")

        monkeypatch.setattr(sdk_export, "ConsoleMetricExporter", _SpyExporter)
        monkeypatch.setattr(sdk_export, "PeriodicExportingMetricReader", _refuse)

        with pytest.raises(RuntimeError, match="will not start"):
            bootstrap_telemetry(service_name="orders-api", exporter="console")

        assert closed == ["exporter"]

    # ....................... #

    def test_a_failed_meter_build_is_safe_when_traces_were_never_asked_for(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The rollback path with nothing to roll back — metrics-only processes.

        A worker exporting metrics and no traces still has to surface the build failure
        rather than trip over a tracer provider that was never created.
        """

        import opentelemetry.sdk.metrics as sdk_metrics

        def _explode(*_args: Any, **_kwargs: Any) -> MeterProvider:
            raise RuntimeError("meter provider is unbuildable")

        monkeypatch.setattr(sdk_metrics, "MeterProvider", _explode)

        with pytest.raises(RuntimeError, match="unbuildable"):
            bootstrap_telemetry(service_name="orders-api", exporter="none", traces=False)

        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)

    # ....................... #

    async def test_the_otlp_exporters_are_what_the_default_builds(self) -> None:
        """The blessed path, constructed for real — no endpoint is contacted here.

        Everything else in this file runs on ``exporter="none"``, so without this the
        default configuration would be exercised only by the integration suite.
        """

        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        handle = bootstrap_telemetry(service_name="orders-api", metric_export_interval=600.0)

        try:
            assert handle.tracer_provider is not None
            assert handle.meter_provider is not None

            # Reaching into the SDK's internals is the only way to see which exporter
            # was wired; there is no public accessor for either.
            processors = handle.tracer_provider._active_span_processor._span_processors
            assert any(
                isinstance(getattr(p, "span_exporter", None), OTLPSpanExporter)
                for p in processors
            )

            readers = handle.meter_provider._all_metric_readers
            assert any(
                isinstance(getattr(r, "_exporter", None), OTLPMetricExporter) for r in readers
            )

        finally:
            await handle.shutdown(timeout=1.0)

    # ....................... #

    @pytest.mark.parametrize(
        ("traces", "metrics_on"),
        [(True, False), (False, True), (False, False)],
    )
    def test_each_signal_can_be_left_out(self, traces: bool, metrics_on: bool) -> None:
        handle = bootstrap_telemetry(
            service_name="orders-api",
            exporter="none",
            traces=traces,
            metrics=metrics_on,
        )

        assert (handle.tracer_provider is not None) is traces
        assert (handle.meter_provider is not None) is metrics_on
        assert (handle.resource is not None) is (traces or metrics_on)


# ----------------------- #


class TestResourceIdentity:
    """Decision #4 — a process without a unique instance id collides with its siblings."""

    def test_instance_id_is_fresh_per_call(self) -> None:
        first, _ = _bootstrap_with_reader()

        # A second process would repeat the whole bootstrap; here, reset and redo it.
        trace._TRACER_PROVIDER = None
        trace._TRACER_PROVIDER_SET_ONCE = Once()

        second = bootstrap_telemetry(service_name="orders-api", exporter="none", metrics=False)

        assert first.resource is not None
        assert second.resource is not None
        assert (
            first.resource.attributes[SERVICE_INSTANCE_ID]
            != second.resource.attributes[SERVICE_INSTANCE_ID]
        )

    # ....................... #

    def test_identity_fields_and_overrides(self) -> None:
        handle, _ = _bootstrap_with_reader(
            service_name="orders-api",
            service_version="4.2.0",
            service_instance_id="orders-api-7f9d-worker-3",
            resource_attributes={"deployment.environment": "staging"},
        )

        assert handle.resource is not None
        attributes = dict(handle.resource.attributes)

        assert attributes[SERVICE_NAME] == "orders-api"
        assert attributes[SERVICE_VERSION] == "4.2.0"
        assert attributes[SERVICE_INSTANCE_ID] == "orders-api-7f9d-worker-3"
        assert attributes["deployment.environment"] == "staging"

    # ....................... #

    def test_framework_version_is_always_present_and_backs_the_version_default(self) -> None:
        handle, _ = _bootstrap_with_reader()

        assert handle.resource is not None
        attributes = dict(handle.resource.attributes)

        assert attributes[FORZE_VERSION_ATTRIBUTE]
        assert attributes[SERVICE_VERSION] == attributes[FORZE_VERSION_ATTRIBUTE]

    # ....................... #

    def test_caller_attributes_win_over_the_defaults(self) -> None:
        handle, _ = _bootstrap_with_reader(
            resource_attributes={SERVICE_NAME: "renamed", FORZE_VERSION_ATTRIBUTE: "pinned"},
        )

        assert handle.resource is not None
        assert handle.resource.attributes[SERVICE_NAME] == "renamed"
        assert handle.resource.attributes[FORZE_VERSION_ATTRIBUTE] == "pinned"


# ----------------------- #


class TestHistogramViews:
    """Decision #8 — the SDK's second-oriented ladder is wrong for millisecond values."""

    @pytest.mark.parametrize("instrument", MILLISECOND_HISTOGRAM_INSTRUMENTS)
    def test_framework_duration_histograms_get_the_millisecond_ladder(
        self,
        instrument: str,
    ) -> None:
        _, reader = _bootstrap_with_reader()

        metrics.get_meter("forze").create_histogram(instrument, unit="ms").record(3.5)

        assert _histogram_bounds(reader, instrument) == MILLISECOND_HISTOGRAM_BUCKETS

    # ....................... #

    def test_an_application_histogram_of_the_same_name_is_left_alone(self) -> None:
        _, reader = _bootstrap_with_reader()

        instrument = MILLISECOND_HISTOGRAM_INSTRUMENTS[0]
        metrics.get_meter("my-app").create_histogram(instrument, unit="ms").record(3.5)

        # Views are scoped to the ``forze`` meter; a same-named instrument recorded under a
        # different instrumentation scope keeps the SDK's defaults.
        assert _histogram_bounds(reader, instrument) != MILLISECOND_HISTOGRAM_BUCKETS

    # ....................... #

    def test_other_framework_histograms_are_not_reshaped(self) -> None:
        _, reader = _bootstrap_with_reader()

        metrics.get_meter("forze").create_histogram("forze.something.else", unit="ms").record(1.0)

        assert _histogram_bounds(reader, "forze.something.else") != MILLISECOND_HISTOGRAM_BUCKETS

    # ....................... #

    def test_exponential_aggregation_is_opt_in(self) -> None:
        _, reader = _bootstrap_with_reader(exponential_histograms=True)

        instrument = MILLISECOND_HISTOGRAM_INSTRUMENTS[0]
        metrics.get_meter("forze").create_histogram(instrument, unit="ms").record(3.5)

        assert _point_types(reader, instrument) == ["ExponentialHistogramDataPoint"]

    # ....................... #

    def test_supplied_views_replace_the_defaults(self) -> None:
        custom = View(
            instrument_name=MILLISECOND_HISTOGRAM_INSTRUMENTS[0],
            meter_name="forze",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=(7.0, 42.0)),
        )
        _, reader = _bootstrap_with_reader(histogram_views=[custom])

        metrics.get_meter("forze").create_histogram(
            MILLISECOND_HISTOGRAM_INSTRUMENTS[0], unit="ms"
        ).record(3.5)

        assert _histogram_bounds(reader, MILLISECOND_HISTOGRAM_INSTRUMENTS[0]) == (7.0, 42.0)

    # ....................... #

    def test_the_defaults_can_be_composed_back_in(self) -> None:
        views = [
            *millisecond_histogram_views(),
            View(
                instrument_name="app.latency",
                aggregation=ExplicitBucketHistogramAggregation(boundaries=(7.0,)),
            ),
        ]
        _, reader = _bootstrap_with_reader(histogram_views=views)

        meter = metrics.get_meter("forze")
        meter.create_histogram(MILLISECOND_HISTOGRAM_INSTRUMENTS[0], unit="ms").record(3.5)
        meter.create_histogram("app.latency", unit="ms").record(3.5)

        assert (
            _histogram_bounds(reader, MILLISECOND_HISTOGRAM_INSTRUMENTS[0])
            == MILLISECOND_HISTOGRAM_BUCKETS
        )
        assert _histogram_bounds(reader, "app.latency") == (7.0,)

    # ....................... #

    def test_views_are_absent_when_the_bootstrap_deferred(self) -> None:
        reader = InMemoryMetricReader()
        owned = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(owned)

        bootstrap_telemetry(service_name="orders-api", exporter="none")

        metrics.get_meter("forze").create_histogram(
            MILLISECOND_HISTOGRAM_INSTRUMENTS[0], unit="ms"
        ).record(3.5)

        # An application that owns its SDK owns its aggregation too — decision #3 wins over #8.
        assert (
            _histogram_bounds(reader, MILLISECOND_HISTOGRAM_INSTRUMENTS[0])
            != MILLISECOND_HISTOGRAM_BUCKETS
        )


# ----------------------- #


class TestLateBinding:
    """The edge decision #3's ordering advice exists to make safe — pinned, not assumed.

    Every ``instrument_*`` helper creates its instruments at assembly time. If the API's
    proxies did not rebind, an application that assembled before bootstrapping would emit
    into a void — silently, forever.
    """

    def test_an_instrument_created_before_the_provider_still_records(self) -> None:
        counter = metrics.get_meter("forze").create_counter("forze.test.preexisting")

        _, reader = _bootstrap_with_reader()

        counter.add(3)
        data = reader.get_metrics_data()

        assert data is not None
        recorded = [
            point.value
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
            if metric.name == "forze.test.preexisting"
            for point in metric.data.data_points  # type: ignore[union-attr]
        ]

        assert recorded == [3]

    # ....................... #

    def test_a_tracer_taken_before_the_provider_produces_recording_spans(self) -> None:
        tracer = trace.get_tracer("forze")

        _bootstrap_with_reader()

        with tracer.start_as_current_span("late-bound") as span:
            assert span.is_recording()


# ----------------------- #


class _RecordingSpanExporter(SpanExporter):
    """Stands in for a collector, so "the tail batch got out" is observable."""

    def __init__(self) -> None:
        self.names: list[str] = []
        self.shutdowns = 0

    def export(self, spans: Any) -> SpanExportResult:
        self.names.extend(span.name for span in spans)

        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True

    def shutdown(self) -> None:
        self.shutdowns += 1


class _RecordingMetricExporter(MetricExporter):
    """Same idea on the metrics side: counts export rounds the handle forced."""

    def __init__(self) -> None:
        super().__init__()
        self.exports = 0
        self.shutdowns = 0

    def export(self, metrics_data: Any, timeout_millis: float = 10_000, **kwargs: Any) -> Any:
        self.exports += 1

        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        self.shutdowns += 1


# ....................... #

_NEVER_ON_ITS_OWN_MS = 600_000
"""Batch/export delay long enough that anything exported was exported *because we asked*."""


def _bootstrap_with_recorders() -> tuple[
    TelemetryHandle,
    _RecordingSpanExporter,
    _RecordingMetricExporter,
]:
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    span_exporter = _RecordingSpanExporter()
    metric_exporter = _RecordingMetricExporter()

    handle = bootstrap_telemetry(
        service_name="orders-api",
        exporter="none",
        extra_metric_readers=[
            PeriodicExportingMetricReader(
                metric_exporter,
                export_interval_millis=_NEVER_ON_ITS_OWN_MS,
            )
        ],
    )

    assert handle.tracer_provider is not None
    handle.tracer_provider.add_span_processor(
        BatchSpanProcessor(span_exporter, schedule_delay_millis=_NEVER_ON_ITS_OWN_MS)
    )

    return handle, span_exporter, metric_exporter


class TestHandleLifecycle:
    """Decision #9 — flush belongs to drain, and drain is what saves the last interval."""

    async def test_flush_pushes_the_pending_batch_without_ending_the_providers(self) -> None:
        handle, spans, metric_exporter = _bootstrap_with_recorders()

        trace.get_tracer("forze").start_span("checkout").end()
        metrics.get_meter("forze").create_counter("forze.test.flush").add(1)

        assert spans.names == [], "nothing should have left on its own yet"

        await handle.flush()

        assert spans.names == ["checkout"]
        assert metric_exporter.exports >= 1
        assert spans.shutdowns == 0
        assert metric_exporter.shutdowns == 0

    # ....................... #

    async def test_shutdown_exports_the_tail_and_then_stops_both_providers(self) -> None:
        handle, spans, metric_exporter = _bootstrap_with_recorders()

        trace.get_tracer("forze").start_span("checkout").end()

        await handle.shutdown()

        assert spans.names == ["checkout"], "the last batch must outlive the process"
        assert spans.shutdowns == 1
        assert metric_exporter.shutdowns == 1

    # ....................... #

    async def test_shutdown_is_idempotent(self) -> None:
        handle, spans, metric_exporter = _bootstrap_with_recorders()

        await handle.shutdown()
        await handle.shutdown()

        assert spans.shutdowns == 1
        assert metric_exporter.shutdowns == 1

    # ....................... #

    async def test_an_empty_handle_is_safe_to_drain(self) -> None:
        handle = TelemetryHandle()

        await handle.flush()
        await handle.shutdown()


# ----------------------- #


class _SlowShutdownExporter(_RecordingSpanExporter):
    """Makes the shutdown window wide enough for a second caller to race into it."""

    def __init__(self, delay: float = 0.3) -> None:
        super().__init__()
        self._delay = delay
        self.finished = False

    def shutdown(self) -> None:
        time.sleep(self._delay)
        super().shutdown()
        self.finished = True


class _RaisingFlushMetricExporter(_RecordingMetricExporter):
    """The failure mode the metrics SDK actually has.

    ``MeterProvider.force_flush`` collects its readers' errors and **raises** — it never
    returns ``False``, so the "log a warning on a falsy result" branch is not the path that
    matters. The span side is not usable for this: a batch processor's ``force_flush``
    drains its own queue without ever calling the exporter's.
    """

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        raise RuntimeError("collector unreachable")


def _handle_with(exporter: SpanExporter) -> TelemetryHandle:
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    handle = bootstrap_telemetry(service_name="orders-api", exporter="none", metrics=False)

    assert handle.tracer_provider is not None
    handle.tracer_provider.add_span_processor(BatchSpanProcessor(exporter))

    return handle


def _handle_with_metric_exporter(exporter: MetricExporter) -> TelemetryHandle:
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    return bootstrap_telemetry(
        service_name="orders-api",
        exporter="none",
        traces=False,
        extra_metric_readers=[
            PeriodicExportingMetricReader(exporter, export_interval_millis=600_000)
        ],
    )


class TestShutdownIsHostileToCallers:
    """The three ways a naive drain hook gets this wrong."""

    async def test_a_second_caller_waits_for_the_first_to_finish(self) -> None:
        """Not merely "returns once" — *returns only when the work is done*.

        A latch claimed up front would let the second caller believe the providers were
        closed while the first was still flushing. That caller then goes on to dispose the
        clients whose stats the final collection is in the middle of reading.
        """

        exporter = _SlowShutdownExporter()
        handle = _handle_with(exporter)

        observed: list[bool] = []

        async def _drain() -> None:
            await handle.shutdown()
            observed.append(exporter.finished)

        await asyncio.gather(_drain(), _drain())

        assert observed == [True, True], "a caller returned before shutdown had finished"
        assert exporter.shutdowns == 1, "the work itself must still happen exactly once"

    # ....................... #

    async def test_a_raising_flush_does_not_strand_the_providers(self) -> None:
        """The SDK signals a failed flush by *raising*, not by returning ``False``.

        Unguarded, that exception escapes shutdown with the latch already claimed — so the
        providers are never closed and a retry is a no-op. The process leaves with its
        exporter threads still running.
        """

        exporter = _RaisingFlushMetricExporter()
        handle = _handle_with_metric_exporter(exporter)

        await handle.shutdown()

        assert exporter.shutdowns == 1, "teardown must survive a failed flush"

    # ....................... #

    async def test_flush_never_raises_at_the_caller(self) -> None:
        """`flush()` documents "logged, not raised", and the drain path depends on it."""

        handle = _handle_with_metric_exporter(_RaisingFlushMetricExporter())

        await handle.flush()

    # ....................... #

    async def test_a_cancelled_caller_does_not_mark_the_teardown_complete(self) -> None:
        """Walking away from a shutdown is not the same as finishing one.

        The work runs in a thread and cannot be cancelled, so a cancelled caller leaves it
        running. Marking the gate closed on the way out would let the *next* caller return
        immediately — while providers are still being flushed — and go on to dispose the
        clients the final collection is reading.
        """

        exporter = _SlowShutdownExporter(delay=0.4)
        handle = _handle_with(exporter)

        cancelled = asyncio.ensure_future(handle.shutdown())
        await asyncio.sleep(0.05)  # let it reach the thread
        cancelled.cancel()

        with pytest.raises(asyncio.CancelledError):
            await cancelled

        assert not exporter.finished, "the test needs the teardown to still be in flight"

        # The retry must wait for the original teardown, not sail past it.
        await handle.shutdown()

        assert exporter.finished
        assert exporter.shutdowns == 1

    # ....................... #

    async def test_an_exhausted_budget_still_tears_everything_down(self) -> None:
        """Steps now run against one deadline, so later ones can find it already spent.

        A spent budget has to mean "you are out of time", not a negative timeout handed to
        an SDK that never agreed to receive one — and every remaining step still has to
        run, because the process is on its way out.
        """

        exporter = _RecordingSpanExporter()
        reader = InMemoryMetricReader()

        handle = bootstrap_telemetry(
            service_name="orders-api",
            exporter="none",
            extra_metric_readers=[reader],
        )
        assert handle.tracer_provider is not None

        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        handle.tracer_provider.add_span_processor(BatchSpanProcessor(exporter))

        await handle.shutdown(timeout=0.0)

        assert exporter.shutdowns == 1
        assert handle.meter_provider is not None


# ----------------------- #


class TestConfigurationErrors:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"service_name": "   "},
            {"service_name": "orders-api", "metric_export_interval": 0.0},
            {"service_name": "orders-api", "metric_export_interval": -1.0},
        ],
    )
    def test_invalid_arguments_fail_at_the_call(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as caught:
            bootstrap_telemetry(exporter="none", **kwargs)

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    @pytest.mark.parametrize(
        "module",
        [
            "opentelemetry.exporter.otlp.proto.http.trace_exporter",
            "opentelemetry.exporter.otlp.proto.http.metric_exporter",
        ],
    )
    def test_a_missing_exporter_package_names_the_extra(
        self,
        module: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # ``None`` in sys.modules is the documented way to make an import raise.
        monkeypatch.setitem(sys.modules, module, None)

        with pytest.raises(CoreException) as caught:
            bootstrap_telemetry(service_name="orders-api")

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "observability" in str(caught.value)
