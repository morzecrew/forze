"""The shipped Grafana-stack assets, checked against the tools that actually consume them.

Two things a unit test cannot do.

**Tier 1 — do the files load?** `alerts.yml` and `prometheus.yml` are read by Prometheus,
`config.alloy` by Alloy. A syntax error in any of them is invisible to Python and fatal at
`docker compose up`, and the unit parity gate cannot see it: it checks that expressions
name real *metrics*, not that they are valid PromQL.

**Tier 2 — is the name mapping real?** Every dashboard panel and alert rule in this
repository is written against the OTel name with dots turned into underscores, and the
unit gate checks them against exactly that spelling. But the spelling is a *consequence*
of `add_metric_suffixes = false` in the Alloy config, and nothing anywhere proved that the
setting produces it. If the assumption were wrong, every shipped asset would be
consistently, silently wrong — and the parity gate would stay green, because it only ever
compared the assets to themselves.

So the last test here puts a real Alloy between `bootstrap_telemetry` and a real
Prometheus and asks Prometheus what the series is called.

The full five-container compose stack stays a manual gate. What it would
add beyond this is mostly proof that Grafana's own provisioning works.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("opentelemetry.exporter.otlp.proto.http.trace_exporter")

import httpx
from opentelemetry import metrics, trace
from opentelemetry.metrics._internal import _ProxyMeterProvider
from opentelemetry.util._once import Once
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from forze.base.telemetry import bootstrap_telemetry

# ----------------------- #

PROMETHEUS_IMAGE = "prom/prometheus:v3.6.0"
ALLOY_IMAGE = "grafana/alloy:v1.10.0"

_ASSETS = (
    Path(__file__).resolve().parents[3]
    / "pages"
    / "docs"
    / "running-in-prod"
    / "assets"
    / "grafana"
)

# Where the shipped compose file mounts each asset. Checking them at these paths is what
# makes the check faithful: `prometheus.yml` refers to `alerts.yml` by container path, so
# validating it anywhere else fails for a reason that says nothing about the file.
PROMETHEUS_CONFIG_MOUNT = "/etc/prometheus/prometheus.yml"
ALERTS_MOUNT = "/etc/prometheus/alerts.yml"


# ....................... #


def _run_tool(
    image: str,
    *,
    entrypoint: str | None,
    args: list[str],
    mounts: dict[str, str],
) -> tuple[int, str]:
    """Run a one-shot CLI in *image*; return its exit status and combined output.

    A non-zero exit is a result, not an error: these tests assert on it, so the output has
    to survive the failure rather than disappear into a traceback.
    """

    import docker
    from docker.errors import ContainerError

    client = docker.from_env()
    volumes = {host: {"bind": target, "mode": "ro"} for host, target in mounts.items()}

    try:
        raw = client.containers.run(
            image,
            command=args,
            entrypoint=entrypoint,
            volumes=volumes,
            stderr=True,
            remove=True,
        )

        return 0, raw.decode("utf-8", errors="replace")

    except ContainerError as failure:
        return failure.exit_status, (failure.stderr or b"").decode("utf-8", errors="replace")


# ----------------------- #


class TestAssetsLoadInTheirOwnTools:
    """Tier 1: the files parse for the programs that read them."""

    def test_the_alert_pack_loads_into_prometheus(self, docker_available: None) -> None:
        status, output = _run_tool(
            PROMETHEUS_IMAGE,
            entrypoint="promtool",
            args=["check", "rules", ALERTS_MOUNT],
            mounts={str(_ASSETS / "alerts.yml"): ALERTS_MOUNT},
        )

        assert status == 0, output
        assert "SUCCESS" in output, output

    # ....................... #

    def test_the_prometheus_config_loads_with_its_rule_file(
        self,
        docker_available: None,
    ) -> None:
        """Checked with `alerts.yml` at the path the compose file mounts it to.

        `prometheus.yml` names its rule file by container path, so this only means
        anything when the layout matches the one the recipe ships.
        """

        status, output = _run_tool(
            PROMETHEUS_IMAGE,
            entrypoint="promtool",
            args=["check", "config", PROMETHEUS_CONFIG_MOUNT],
            mounts={
                str(_ASSETS / "prometheus.yml"): PROMETHEUS_CONFIG_MOUNT,
                str(_ASSETS / "alerts.yml"): ALERTS_MOUNT,
            },
        )

        assert status == 0, output
        assert "SUCCESS" in output, output

    # ....................... #

    def test_the_alloy_config_parses(self, docker_available: None) -> None:
        status, output = _run_tool(
            ALLOY_IMAGE,
            entrypoint=None,
            args=["fmt", "/w/config.alloy"],
            mounts={str(_ASSETS / "config.alloy"): "/w/config.alloy"},
        )

        assert status == 0, output


# ----------------------- #


ALLOY_METRICS_ONLY_CONFIG = """
otelcol.receiver.otlp "default" {
  http {
    endpoint = "0.0.0.0:4318"
  }

  output {
    metrics = [otelcol.exporter.prometheus.default.input]
  }
}

otelcol.exporter.prometheus "default" {
  // The line under test. Everything the repository ships assumes what it produces.
  add_metric_suffixes = false

  forward_to = [prometheus.remote_write.default.receiver]
}

prometheus.remote_write "default" {
  endpoint {
    url = "http://prometheus:9090/api/v1/write"
  }

  // Push promptly: the test is waiting on this, not on a scrape interval.
  wal {
    truncate_frequency = "5s"
  }
}
"""


def _wait_ready(url: str, name: str, *, timeout: float = 120.0) -> None:
    """Poll a readiness endpoint. Log-scraping is version-specific; this is not."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return

        except httpx.HTTPError:
            pass

        time.sleep(1)

    pytest.fail(f"{name} was not ready at {url} within {timeout:.0f}s")


@pytest.fixture(scope="module")
def alloy_to_prometheus(
    docker_available: None,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[str, str]]:
    """Alloy in front of Prometheus, wired the way the recipe wires them.

    Two containers rather than the recipe's five: Loki, Tempo and Grafana have nothing to
    say about what a metric is called.
    """

    config = tmp_path_factory.mktemp("alloy") / "config.alloy"
    config.write_text(ALLOY_METRICS_ONLY_CONFIG, encoding="utf-8")

    network = Network()
    network.create()

    prometheus = (
        DockerContainer(image=PROMETHEUS_IMAGE)
        .with_exposed_ports(9090)
        .with_network(network)
        .with_network_aliases("prometheus")
        .with_command(
            "--web.enable-remote-write-receiver "
            "--config.file=/etc/prometheus/prometheus.yml "
            "--storage.tsdb.retention.time=1h"
        )
    )

    alloy = (
        DockerContainer(image=ALLOY_IMAGE)
        .with_exposed_ports(4318, 12345)
        .with_network(network)
        .with_volume_mapping(str(config), "/etc/alloy/config.alloy", "ro")
        .with_command(
            "run --server.http.listen-addr=0.0.0.0:12345 "
            "--storage.path=/tmp/alloy /etc/alloy/config.alloy"
        )
    )

    try:
        with prometheus:
            prometheus_url = (
                f"http://{prometheus.get_container_host_ip()}:"
                f"{prometheus.get_exposed_port(9090)}"
            )
            _wait_ready(f"{prometheus_url}/-/ready", "Prometheus")

            with alloy:
                host = alloy.get_container_host_ip()
                _wait_ready(f"http://{host}:{alloy.get_exposed_port(12345)}/-/ready", "Alloy")

                yield f"http://{host}:{alloy.get_exposed_port(4318)}", prometheus_url

    finally:
        network.remove()


# ....................... #


def _reset_otel_globals() -> None:
    import opentelemetry.metrics._internal as metrics_internal

    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    trace._PROXY_TRACER_PROVIDER = trace.ProxyTracerProvider()

    metrics_internal._METER_PROVIDER = None
    metrics_internal._METER_PROVIDER_SET_ONCE = Once()
    metrics_internal._PROXY_METER_PROVIDER = _ProxyMeterProvider()


def _series_names(prometheus_url: str, *, match: str, timeout: float = 60.0) -> set[str]:
    """Every series name Prometheus knows matching *match*, waited for."""

    query = urlencode({"match[]": match})
    deadline = time.monotonic() + timeout
    seen: set[str] = set()

    while time.monotonic() < deadline:
        response = httpx.get(f"{prometheus_url}/api/v1/label/__name__/values?{query}")

        if response.status_code == 200:
            seen = set(response.json().get("data", ()))

            if seen:
                return seen

        time.sleep(1)

    return seen


def _query_one(prometheus_url: str, query: str, *, timeout: float = 60.0) -> dict[str, Any]:
    """Labels of the first series matching *query*, waited for. Empty when none arrives."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = httpx.get(f"{prometheus_url}/api/v1/query", params={"query": query})
        result = response.json().get("data", {}).get("result", ())

        if result:
            return dict(result[0]["metric"])

        time.sleep(1)

    return {}


class TestTheNameMappingTheAssetsAssume:
    """Tier 2: what `add_metric_suffixes = false` actually produces, end to end."""

    async def test_metrics_arrive_under_the_names_the_dashboards_query(
        self,
        alloy_to_prometheus: tuple[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        alloy_url, prometheus_url = alloy_to_prometheus

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", alloy_url)
        _reset_otel_globals()

        try:
            handle = bootstrap_telemetry(
                service_name="forze-name-mapping",
                traces=False,
                metric_export_interval=600.0,
            )

            # A counter and a histogram: the two shapes whose Prometheus spelling differs,
            # and the two the shipped panels actually select on.
            meter = metrics.get_meter("forze")
            meter.create_counter("forze.operations", unit="1").add(1)
            meter.create_histogram("forze.operation.duration", unit="ms").record(12.0)

            await handle.shutdown(timeout=30.0)

        finally:
            _reset_otel_globals()

        names = _series_names(prometheus_url, match='{__name__=~"forze_.*"}')

        assert names, "nothing reached Prometheus through Alloy at all"

        # The exact spelling every shipped panel and alert rule is written against.
        assert "forze_operations" in names
        assert "forze_operation_duration_bucket" in names

        # And the spelling the collector default would have produced. If these ever show
        # up, the assets in this repository match nothing and the unit parity gate cannot
        # tell, because it only compares the assets to the constants in src/.
        assert "forze_operations_total" not in names
        assert not {name for name in names if "milliseconds" in name}, sorted(names)

    # ....................... #

    async def test_attribute_names_map_the_same_way(
        self,
        alloy_to_prometheus: tuple[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Panels group by ``forze_operation`` / ``forze_outcome``; labels convert too."""

        alloy_url, prometheus_url = alloy_to_prometheus

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", alloy_url)
        _reset_otel_globals()

        try:
            handle = bootstrap_telemetry(
                service_name="forze-label-mapping",
                traces=False,
                metric_export_interval=600.0,
            )

            metrics.get_meter("forze").create_counter("forze.operations", unit="1").add(
                1, {"forze.operation": "orders.place", "forze.outcome": "success"}
            )

            await handle.shutdown(timeout=30.0)

        finally:
            _reset_otel_globals()

        labels = _query_one(
            prometheus_url, 'forze_operations{forze_operation="orders.place"}'
        )

        assert labels, "no series carried the converted label"
        assert labels["forze_outcome"] == "success"
        # Alloy derives these from the resource; the dashboards' `$service` variable and
        # every per-process counter rate depend on them.
        assert labels["job"] == "forze-label-mapping"
        assert labels["instance"]
