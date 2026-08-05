"""One-call OpenTelemetry SDK setup — the twin of ``bootstrap_logging``.

Everything in Forze emits through the *global* OpenTelemetry providers, and that stays
true: this module owns none of the instrumentation, only the last mile between it and a
collector. It exists because the same fifteen lines of SDK wiring get hand-written in
every deploying application, and get the same four things wrong every time — no unique
``service.instance.id`` (per-worker series collide and ``rate()`` flaps), no flush on
shutdown (the last export interval of every deploy dies with the pod), second-oriented
histogram buckets applied to millisecond values, and a provider installed after the
instruments that should have used it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal

from forze.base._logger import logger
from forze.base.exceptions import exc
from forze.base.primitives import uuid4

from .constants import (
    FORZE_METER_NAME,
    MILLISECOND_HISTOGRAM_BUCKETS,
    MILLISECOND_HISTOGRAM_INSTRUMENTS,
)
from .handle import TelemetryHandle

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import MetricExporter, MetricReader
    from opentelemetry.sdk.metrics.view import View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SpanExporter

# ----------------------- #

ExporterChoice = Literal["otlp", "console", "none"]

FORZE_VERSION_ATTRIBUTE = "forze.version"
"""Resource attribute carrying the framework's own version, always populated."""

_OBSERVABILITY_EXTRA_HINT = (
    "install the 'observability' extra (pip install 'forze[observability]') or pass "
    "exporter='console' / exporter='none'"
)

# ....................... #


def millisecond_histogram_views(*, exponential: bool = False) -> tuple[View, ...]:
    """The framework's own duration-histogram views, for callers composing their own set.

    ``bootstrap_telemetry(histogram_views=...)`` replaces the defaults outright; pass
    ``[*millisecond_histogram_views(), *my_views]`` to keep them and add to them.

    :param exponential: Use exponential-bucket aggregation (Prometheus native histograms)
        instead of the explicit millisecond ladder.
    """

    from opentelemetry.sdk.metrics.view import (
        ExplicitBucketHistogramAggregation,
        ExponentialBucketHistogramAggregation,
        View,
    )

    aggregation = (
        ExponentialBucketHistogramAggregation()
        if exponential
        else ExplicitBucketHistogramAggregation(boundaries=MILLISECOND_HISTOGRAM_BUCKETS)
    )

    return tuple(
        View(
            instrument_name=name,
            # Scoped to the framework's own instrumentation scope: an application histogram
            # that happens to share a name keeps whatever aggregation the application chose.
            meter_name=FORZE_METER_NAME,
            aggregation=aggregation,
        )
        for name in MILLISECOND_HISTOGRAM_INSTRUMENTS
    )


# ....................... #


def bootstrap_telemetry(
    *,
    service_name: str,
    service_version: str | None = None,
    service_instance_id: str | None = None,
    resource_attributes: Mapping[str, str] | None = None,
    traces: bool = True,
    metrics: bool = True,
    exporter: ExporterChoice = "otlp",
    metric_export_interval: float = 15.0,
    exponential_histograms: bool = False,
    histogram_views: Sequence[View] | None = None,
    extra_metric_readers: Sequence[MetricReader] | None = None,
    on_existing_provider: Literal["defer", "error"] = "defer",
) -> TelemetryHandle:
    """Install OpenTelemetry SDK providers for this process and return their handle.

    Call **once, at process start, before assembly** — the ``instrument_*`` helpers create
    their instruments while the registry is built, and although the API's proxy tracer and
    meter do late-bind to a provider installed afterwards, ordering it this way keeps the
    first spans of a slow startup from being dropped on the floor.

    Endpoint, headers, protocol timeouts and the head sampler all come from the standard
    ``OTEL_EXPORTER_OTLP_*`` / ``OTEL_TRACES_SAMPLER`` environment variables, which the
    exporter and the SDK read themselves. This function deliberately adds no parallel
    configuration vocabulary for them.

    :param service_name: ``service.name`` — the only genuinely required identity.
    :param service_version: ``service.version``. Pass your application's version. Defaults
        to the *Forze* version so the resource is never version-less; the framework version
        is always available separately as ``forze.version``.
    :param service_instance_id: ``service.instance.id``. Defaults to a fresh UUID per
        process, which is what keeps each worker's cumulative counters (pools, crypto, L1,
        realtime, signing) a distinct series under pre-fork multi-worker servers. Override
        only with something equally unique per process — a pod name is not, a pod name plus
        worker index is.
    :param resource_attributes: Extra resource attributes; these win over the defaults above.
    :param traces: Install a tracer provider.
    :param metrics: Install a meter provider.
    :param exporter: ``"otlp"`` exports over OTLP http/protobuf (needs the ``observability``
        extra); ``"console"`` prints to stdout for local dev; ``"none"`` installs real
        providers with no exporter, which is what tests and in-memory readers want.
    :param metric_export_interval: Seconds between metric exports.
    :param exponential_histograms: Switch the framework's duration histograms to
        exponential-bucket aggregation (Prometheus native histograms). The explicit
        millisecond ladder is the default because it is portable to every Prometheus.
    :param histogram_views: Replace the framework's default views entirely. See
        :func:`millisecond_histogram_views` to extend them instead.
    :param extra_metric_readers: Additional readers to register on the meter provider — a
        ``PrometheusMetricReader`` for the pull endpoint, or an in-memory reader in tests.
    :param on_existing_provider: ``"defer"`` leaves an application-installed SDK completely
        alone for that signal (the returned handle will not flush or shut it down);
        ``"error"`` refuses to start instead of silently doing nothing.

    :raises CoreException: ``configuration`` — a non-positive export interval, an empty
        service name, a missing exporter package, or an existing provider under
        ``on_existing_provider="error"``.
    """

    if not service_name.strip():
        raise exc.configuration("Telemetry service_name must be a non-empty string")

    if metric_export_interval <= 0:
        raise exc.configuration("Telemetry metric_export_interval must be positive")

    resource = _build_resource(
        service_name=service_name,
        service_version=service_version,
        service_instance_id=service_instance_id,
        resource_attributes=resource_attributes,
    )

    # Both signals are inspected *before* either is installed. Deciding per signal as we go
    # would let the first one be set globally and the second one raise — and a provider set
    # into OpenTelemetry's set-once slot cannot be taken back, so the caller would be left
    # holding an exception, no handle, and a live provider nothing will ever flush.
    defer_traces = traces and _tracer_provider_installed()
    defer_metrics = metrics and _meter_provider_installed()

    for signal, deferred in (("tracer", defer_traces), ("meter", defer_metrics)):
        if deferred:
            _report_existing(signal, on_existing_provider)

    # Build first, publish second. Construction is where the failures live — a missing
    # exporter package, a malformed View, a reader that will not start — and publishing is
    # irreversible. Interleaving them would let a meter that fails to build strand an
    # already-published tracer provider: unreachable through the handle the caller never
    # receives, unreplaceable because OpenTelemetry's slot is set-once, and still holding
    # buffered spans nothing will flush.
    tracer_provider = (
        _build_tracer_provider(resource=resource, exporter=exporter)
        if traces and not defer_traces
        else None
    )

    try:
        meter_provider = (
            _build_meter_provider(
                resource=resource,
                exporter=exporter,
                metric_export_interval=metric_export_interval,
                exponential_histograms=exponential_histograms,
                histogram_views=histogram_views,
                extra_metric_readers=extra_metric_readers,
            )
            if metrics and not defer_metrics
            else None
        )

    except Exception:
        if tracer_provider is not None:
            # Built but never published, so nothing can reach it — including its own batch
            # worker thread and the atexit hook it registered on construction.
            tracer_provider.shutdown()

        raise

    if tracer_provider is not None:
        _publish_tracer_provider(tracer_provider)

    if meter_provider is not None:
        _publish_meter_provider(meter_provider)

    return TelemetryHandle(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        resource=resource if (tracer_provider or meter_provider) else None,
    )


# ....................... #


def _build_resource(
    *,
    service_name: str,
    service_version: str | None,
    service_instance_id: str | None,
    resource_attributes: Mapping[str, str] | None,
) -> Resource:
    from opentelemetry.sdk.resources import (
        SERVICE_INSTANCE_ID,
        SERVICE_NAME,
        SERVICE_VERSION,
        Resource,
    )

    forze_version = _forze_version()

    attributes: dict[str, str] = {
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version or forze_version,
        SERVICE_INSTANCE_ID: service_instance_id or str(uuid4()),
        FORZE_VERSION_ATTRIBUTE: forze_version,
        **dict(resource_attributes or {}),
    }

    # ``Resource.create`` folds in OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME and the SDK's
    # own detectors; what is passed here wins over them.
    return Resource.create(attributes)


# ....................... #


def _forze_version() -> str:
    try:
        from forze._version import __version__

    except ImportError:  # pragma: no cover - only in a source tree without a build
        return "unknown"

    return __version__


# ....................... #


def _build_tracer_provider(
    *,
    resource: Resource,
    exporter: ExporterChoice,
) -> TracerProvider:
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider(resource=resource)
    span_exporter = _build_span_exporter(exporter)

    if span_exporter is not None:
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider.add_span_processor(BatchSpanProcessor(span_exporter))

    return provider


def _publish_tracer_provider(provider: TracerProvider) -> None:
    from opentelemetry import trace

    trace.set_tracer_provider(provider)


# ....................... #


def _build_meter_provider(
    *,
    resource: Resource,
    exporter: ExporterChoice,
    metric_export_interval: float,
    exponential_histograms: bool,
    histogram_views: Sequence[View] | None,
    extra_metric_readers: Sequence[MetricReader] | None,
) -> MeterProvider:
    from opentelemetry.sdk.metrics import MeterProvider

    readers: list[MetricReader] = []
    metric_exporter = _build_metric_exporter(exporter)

    if metric_exporter is not None:
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        readers.append(
            PeriodicExportingMetricReader(
                metric_exporter,
                export_interval_millis=metric_export_interval * 1000.0,
            )
        )

    readers.extend(extra_metric_readers or ())

    views = (
        millisecond_histogram_views(exponential=exponential_histograms)
        if histogram_views is None
        else tuple(histogram_views)
    )

    return MeterProvider(metric_readers=readers, resource=resource, views=views)


def _publish_meter_provider(provider: MeterProvider) -> None:
    from opentelemetry import metrics

    metrics.set_meter_provider(provider)


# ....................... #


def _tracer_provider_installed() -> bool:
    """Whether something already set a tracer provider.

    Unset, the API hands back a ``ProxyTracerProvider``; anything else — including an
    explicitly installed no-op, which is a deliberate "tracing is off here" — is somebody
    else's decision to keep.
    """

    from opentelemetry import trace

    return not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)


def _meter_provider_installed() -> bool:
    """The metrics-side twin of :func:`_tracer_provider_installed`.

    The metrics API does not re-export its proxy type, hence the private import; the
    defer/create behavior it drives is pinned by test, so a rename surfaces as a failure
    rather than as a bootstrap that quietly stops deferring.
    """

    from opentelemetry import metrics
    from opentelemetry.metrics._internal import _ProxyMeterProvider

    return not isinstance(metrics.get_meter_provider(), _ProxyMeterProvider)


# ....................... #


def _report_existing(signal: str, on_existing_provider: Literal["defer", "error"]) -> None:
    if on_existing_provider == "error":
        raise exc.configuration(
            f"An OpenTelemetry {signal} provider is already installed; "
            f"bootstrap_telemetry refuses to replace it "
            f"(pass on_existing_provider='defer' to leave it alone)"
        )

    logger.info(
        "OpenTelemetry %s provider already installed; bootstrap_telemetry deferred to it",
        signal,
    )


# ....................... #


def _build_span_exporter(exporter: ExporterChoice) -> SpanExporter | None:
    if exporter == "none":
        return None

    if exporter == "console":
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        return ConsoleSpanExporter()

    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    except ImportError as error:
        raise exc.configuration(
            f"OTLP span exporter is unavailable: {_OBSERVABILITY_EXTRA_HINT}"
        ) from error

    return OTLPSpanExporter()


# ....................... #


def _build_metric_exporter(exporter: ExporterChoice) -> MetricExporter | None:
    if exporter == "none":
        return None

    if exporter == "console":
        from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

        return ConsoleMetricExporter()

    try:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

    except ImportError as error:
        raise exc.configuration(
            f"OTLP metric exporter is unavailable: {_OBSERVABILITY_EXTRA_HINT}"
        ) from error

    return OTLPMetricExporter()
