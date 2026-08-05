"""`bootstrap_telemetry` against a real OpenTelemetry Collector, over real OTLP/http.

In-memory exporters prove the SDK wiring and nothing about the wire. This is the honest
check that the three things which only exist outside the process actually compose: the
`OTEL_EXPORTER_OTLP_*` environment contract the exporter reads on its own, the resource
identity that arrives attached to every data point, and `shutdown()` pushing the tail out
before the process would have ended.

The collector writes everything it receives to a file, which the test then reads back —
so the assertion is on bytes a separate process decoded, not on anything this one holds.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("opentelemetry.exporter.otlp.proto.http.trace_exporter")

from opentelemetry import metrics, trace
from opentelemetry.metrics._internal import _ProxyMeterProvider
from opentelemetry.util._once import Once
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from forze.base.telemetry import bootstrap_telemetry

# ----------------------- #

COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.130.0"
OTLP_HTTP_PORT = 4318
SINK_DIR = "/data"
SINK_NAME = "otel-sink.json"

COLLECTOR_CONFIG = f"""
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:{OTLP_HTTP_PORT}

exporters:
  file:
    path: {SINK_DIR}/{SINK_NAME}

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file]
    metrics:
      receivers: [otlp]
      exporters: [file]
"""

SPAN_NAME = "forze.integration.checkout"
COUNTER_NAME = "forze.integration.orders"
SERVICE_NAME = "forze-otlp-integration"
INSTANCE_ID = "worker-7"


# ....................... #


def _reset_otel_globals() -> None:
    import opentelemetry.metrics._internal as metrics_internal

    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    trace._PROXY_TRACER_PROVIDER = trace.ProxyTracerProvider()

    metrics_internal._METER_PROVIDER = None
    metrics_internal._METER_PROVIDER_SET_ONCE = Once()
    metrics_internal._PROXY_METER_PROVIDER = _ProxyMeterProvider()


# ....................... #


@pytest.fixture(scope="module")
def collector(
    docker_available: None,  # noqa: ARG001 - session gate
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[DockerContainer, Path]]:
    workdir = tmp_path_factory.mktemp("otelcol")
    config = workdir / "config.yaml"
    config.write_text(COLLECTOR_CONFIG, encoding="utf-8")

    sink_dir = workdir / "sink"
    sink_dir.mkdir()
    # The collector image runs as its own uid; pytest's tmp dir is owned by this user.
    sink_dir.chmod(0o777)

    container = (
        DockerContainer(image=COLLECTOR_IMAGE)
        .with_exposed_ports(OTLP_HTTP_PORT)
        .with_volume_mapping(str(config), "/etc/otelcol-contrib/config.yaml", "ro")
        .with_volume_mapping(str(sink_dir), SINK_DIR, "rw")
    )

    with container:
        wait_for_logs(container, "Everything is ready", timeout=120)

        yield container, sink_dir / SINK_NAME


# ....................... #


def _sink_payloads(sink: Path, *, timeout: float = 30.0) -> list[dict[str, Any]]:
    """Everything the collector has written so far, one JSON document per export."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if sink.exists() and sink.read_text(encoding="utf-8").strip():
            return [
                json.loads(line)
                for line in sink.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        time.sleep(1)

    pytest.fail(f"the collector never wrote anything to {sink}")


def _resource_attributes(payload: dict[str, Any], key: str) -> dict[str, str]:
    out: dict[str, str] = {}

    for entry in payload.get(key, ()):
        for attribute in entry.get("resource", {}).get("attributes", ()):
            out[attribute["key"]] = attribute["value"].get("stringValue", "")

    return out


# ----------------------- #


class TestOtlpExportOverTheWire:
    async def test_spans_and_metrics_reach_a_real_collector(
        self,
        collector: tuple[DockerContainer, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        container, sink = collector
        endpoint = (
            f"http://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(OTLP_HTTP_PORT)}"
        )
        # The exporter reads this itself — that it does is half of what this test checks.
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)

        _reset_otel_globals()

        try:
            handle = bootstrap_telemetry(
                service_name=SERVICE_NAME,
                service_version="9.9.9",
                service_instance_id=INSTANCE_ID,
                resource_attributes={"deployment.environment": "integration"},
                # Long enough that nothing leaves on a timer: whatever arrives, arrives
                # because shutdown pushed it.
                metric_export_interval=600.0,
            )

            with trace.get_tracer("forze").start_as_current_span(SPAN_NAME):
                metrics.get_meter("forze").create_counter(COUNTER_NAME).add(5)

            await handle.shutdown(timeout=20.0)

        finally:
            _reset_otel_globals()

        payloads = _sink_payloads(sink)

        span_names = {
            span["name"]
            for payload in payloads
            for resource_spans in payload.get("resourceSpans", ())
            for scope_spans in resource_spans.get("scopeSpans", ())
            for span in scope_spans.get("spans", ())
        }
        metric_names = {
            metric["name"]
            for payload in payloads
            for resource_metrics in payload.get("resourceMetrics", ())
            for scope_metrics in resource_metrics.get("scopeMetrics", ())
            for metric in scope_metrics.get("metrics", ())
        }

        assert SPAN_NAME in span_names, "the tail span batch did not survive shutdown"
        assert COUNTER_NAME in metric_names, "the final metric interval did not survive shutdown"

        # Resource identity is what keeps one worker's cumulative counters apart from the
        # next one's; assert it arrived rather than trusting that it was set locally.
        for key in ("resourceSpans", "resourceMetrics"):
            attributes = {
                name: value
                for payload in payloads
                for name, value in _resource_attributes(payload, key).items()
            }

            assert attributes["service.name"] == SERVICE_NAME
            assert attributes["service.version"] == "9.9.9"
            assert attributes["service.instance.id"] == INSTANCE_ID
            assert attributes["deployment.environment"] == "integration"
            assert attributes["forze.version"]
