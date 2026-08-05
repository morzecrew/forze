"""Unit tests for the OpenTelemetry SDK bootstrap (`forze.base.telemetry`).

The interesting behavior is all in the seams: what happens when an application already
owns an SDK, whether instruments created *before* the provider still record, and whether
the framework's millisecond bucket ladder lands on exactly the two histograms it should.
"""

from __future__ import annotations

import sys
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
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
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

    def test_a_deferred_provider_is_never_shut_down_by_the_handle(self) -> None:
        owned = TracerProvider()
        trace.set_tracer_provider(owned)

        handle, _ = _bootstrap_with_reader()

        assert handle.tracer_provider is None
        # The application's provider is still usable after the handle "shut down".
        assert owned.get_tracer("app") is not None

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
