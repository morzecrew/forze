"""The soak: the ``/metrics`` parser, counter rates, drift, the one-process rule, and the run."""

from __future__ import annotations

import asyncio
import json
import math
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import attrs
import httpx
import pytest
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, UntypedMetricFamily
from prometheus_client.registry import Collector

from forze.application.execution.observability import instrument_process
from forze.base.exceptions import CoreException
from forze_fastapi.routes._snapshots import worker_metrics_reader
from forze_load import (
    Call,
    LoadPlan,
    Principal,
    RunFile,
    Scenario,
    Soak,
    compare_runs,
    read_run,
    run,
    summarize,
)
from forze_load.soak import ServerSeries, fit, parse_metrics, series_key

pytestmark = pytest.mark.unit

# ----------------------- #


def _server() -> ServerSeries:
    return ServerSeries(None, "/metrics")  # type: ignore[arg-type]


def _expo(
    gauges: dict[str, float] | None = None,
    *,
    counters: dict[str, float] | None = None,
    instance: str | None = "w-1",
) -> str:
    """A small exposition: a ``target_info`` naming *instance*, then gauges and counters."""

    lines = []

    if instance is not None:
        lines += [
            "# TYPE target_info gauge",
            f'target_info{{service_instance_id="{instance}",service_name="app"}} 1.0',
        ]

    for series, value in (gauges or {}).items():
        lines += [f"# TYPE {series.partition('{')[0]} gauge", f"{series} {value}"]

    for series, value in (counters or {}).items():
        lines += [f"# TYPE {series.partition('{')[0]} counter", f"{series} {value}"]

    return "\n".join(lines) + "\n"


def _feed(server: ServerSeries, texts: Sequence[str], window_s: float = 1.0) -> None:
    """Scrape *texts* in order: the baseline at t = 0, then one per window."""

    for window, text in enumerate(texts):
        server.add(window, window * window_s, parse_metrics(text))


def _render(reader: PrometheusMetricReader, registry: CollectorRegistry) -> str:
    provider = MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": "app", "service.instance.id": "w-1"}),
    )
    meter = provider.get_meter("forze")
    instrument_process(meter=meter)
    meter.create_counter("forze.operations", unit="1").add(3, {"forze.operation": 'a"b\\c\nd'})
    meter.create_histogram("forze.operation.duration", unit="ms").record(12.5)
    meter.create_up_down_counter("forze.realtime.hub.subscribers", unit="1").add(2)
    meter.create_observable_gauge(
        "forze.cache.l1.size", callbacks=[lambda _options: [Observation(5)]], unit="1"
    )

    try:
        return generate_latest(registry).decode()

    finally:
        provider.shutdown()


# ....................... #


class TestParser:
    def test_it_reads_the_exporter_s_own_output(self) -> None:
        registry = CollectorRegistry()
        text = _render(PrometheusMetricReader(registry=registry), registry)

        samples = {sample.name: sample for sample in parse_metrics(text)}

        assert samples["forze_operations_total"].type == "counter"
        assert samples["forze_operations_total"].value == 3.0
        # `\"`, `\\` and `\n` are unescaped.
        assert samples["forze_operations_total"].labels["forze_operation"] == 'a"b\\c\nd'
        assert samples["forze_operation_duration_milliseconds_bucket"].type == "histogram"
        assert samples["forze_operation_duration_milliseconds_count"].type == "histogram"
        assert samples["forze_realtime_hub_subscribers"].type == "gauge"
        assert samples["target_info"].labels["service_instance_id"] == "w-1"
        assert samples["process_cpu_time_seconds_total"].type == "counter"

    def test_the_kept_series_are_forze_s_and_the_process_gauges(self) -> None:
        registry = CollectorRegistry()
        server = _server()

        row = server.add(
            0, 0.0, parse_metrics(_render(PrometheusMetricReader(registry=registry), registry))
        )
        names = {key.partition("{")[0] for key in row["series"]}

        expected = {
            "forze_operations_total",
            "forze_realtime_hub_subscribers",
            "forze_cache_l1_size",
            "process_cpu_time_seconds_total",
        }

        # `instrument_process` exports resident memory on Linux only.
        if sys.platform.startswith("linux"):
            expected.add("process_memory_usage_bytes")

        # Histograms and `target_info` are not fitted.
        assert names == expected
        assert row["instances"] == ["w-1"]

    def test_a_pre_fork_union_labels_every_series_with_its_instance(self) -> None:
        reader = worker_metrics_reader()
        server = _server()

        server.add(0, 0.0, parse_metrics(_render(reader, reader._registry)))  # pyright: ignore[reportPrivateUsage]

        assert server.labelled == set(server.kinds)
        assert all('service_instance_id="w-1"' in key for key in server.kinds)

    def test_untyped_families_and_created_timestamps(self) -> None:
        class Families(Collector):
            def collect(self) -> Iterator[Any]:
                untyped = UntypedMetricFamily("forze_custom", "", labels=["k"])
                untyped.add_metric(["v"], 7.0)
                counter = CounterMetricFamily("forze_jobs", "", labels=[])
                counter.add_metric([], 4.0, created=1_700_000_000.0)

                yield untyped
                yield counter

        registry = CollectorRegistry()
        registry.register(Families())
        text = generate_latest(registry).decode()

        samples = {sample.name: sample for sample in parse_metrics(text)}
        row = _server().add(0, 0.0, list(samples.values()))

        assert samples["forze_custom"].type == "untyped"
        assert "forze_jobs_created" in samples
        # An untyped series is fitted as a gauge; a `_created` timestamp is not a value.
        assert row["series"] == {'forze_custom{k="v"}': 7.0, "forze_jobs_total": None}

    def test_spacing_trailing_commas_timestamps_and_special_values(self) -> None:
        text = (
            "# HELP forze_a some help\n"
            "# a free comment\n"
            "\n"
            'forze_a { b = "1" , } 2.5 1700000000000\n'
            "forze_b +Inf\n"
            "forze_c NaN\n"
            "forze_d{} -1e3\n"
        )

        samples = parse_metrics(text)

        assert [(s.name, dict(s.labels), s.type) for s in samples] == [
            ("forze_a", {"b": "1"}, "untyped"),
            ("forze_b", {}, "untyped"),
            ("forze_c", {}, "untyped"),
            ("forze_d", {}, "untyped"),
        ]
        assert samples[0].value == 2.5
        assert samples[1].value == math.inf
        assert math.isnan(samples[2].value)
        assert samples[3].value == -1000.0

    @pytest.mark.parametrize(
        "line",
        [
            "forze_x",
            "forze_x 1 2 3",
            "forze_x abc",
            '{a="b"} 1',
            'forze_x{a="1" b="2"} 1',
            "forze_x{a=1} 1",
            'forze_x{a="1" 1',
        ],
    )
    def test_a_malformed_line_refuses_the_whole_exposition(self, line: str) -> None:
        with pytest.raises(ValueError):
            parse_metrics(f"forze_ok 1\n{line}\n")

    def test_series_keys_sort_labels_drop_empty_ones_and_escape(self) -> None:
        assert series_key("m", {}) == "m"
        assert series_key("m", {"b": "2", "a": "1", "e": ""}) == 'm{a="1",b="2"}'
        assert series_key("m", {"a": 'x"y\\z\nw'}) == 'm{a="x\\"y\\\\z\\nw"}'


# ....................... #


def _line(values: Sequence[float], window_s: float = 1.0) -> list[tuple[float, float]]:
    return [(i * window_s, value) for i, value in enumerate(values)]


_UNION_KEY = 'forze_ops_total{service_instance_id="w-1"}'


def _union(sent: float, *, interval: float, latency: float = 0.05) -> str:
    """A union scrape sent at *sent*: one worker whose snapshot, written every *interval*
    seconds, holds a counter growing a steady 100/s, served with its age."""

    read = sent + latency / 2  # when the route reads the snapshot's age
    written = math.floor(read / interval) * interval

    return (
        "# TYPE target_info gauge\n"
        'target_info{service_instance_id="w-1"} 1\n'
        "# TYPE forze_ops_total counter\n"
        f"{_UNION_KEY} {100.0 * written}\n"
        "# TYPE forze_metrics_snapshot_age_seconds gauge\n"
        f'forze_metrics_snapshot_age_seconds{{service_instance_id="w-1"}} {read - written:.3f}\n'
    )


class TestFit:
    def test_a_flat_series_is_not_flagged(self) -> None:
        assert fit(_line([100.0] * 12), 12.0)["status"] == "steady"

    def test_a_linear_leak_is_flagged(self) -> None:
        result = fit(_line([100.0 + 5 * i for i in range(12)]), 12.0)

        assert result["status"] == "flagged"
        assert result["slope_per_s"] == pytest.approx(5.0)
        assert result["change"] == pytest.approx(60.0)

    def test_a_sawtooth_is_not_flagged(self) -> None:
        # A GC sawtooth: rises, drops, rises, with no trend across the run.
        values = [100.0 + 40 * (i % 4) for i in range(16)]

        assert fit(_line(values), 16.0)["status"] == "steady"

    def test_a_falling_series_is_not_flagged(self) -> None:
        assert fit(_line([200.0 - 5 * i for i in range(12)]), 12.0)["status"] == "steady"

    def test_a_rise_from_zero_is_flagged(self) -> None:
        assert fit(_line([0, 0, 0, 1, 2, 3, 4, 5]), 8.0)["status"] == "flagged"

    def test_one_event_after_a_zero_first_quarter_is_sparse_not_flagged(self) -> None:
        assert fit(_line([0, 0, 0, 0, 0, 0, 0, 1 / 60]), 8.0)["status"] == "sparse"

    def test_a_rise_from_zero_held_through_the_last_quarter_is_flagged(self) -> None:
        assert fit(_line([0, 0, 0, 0, 0, 0, 1, 1]), 8.0)["status"] == "flagged"

    def test_a_rise_under_the_threshold_is_not_flagged(self) -> None:
        # 1 % over the run, against a 10 % threshold.
        values = [1000.0 + 10.0 * i / 11 for i in range(12)]

        assert fit(_line(values), 12.0)["status"] == "steady"

    def test_fewer_points_than_quarters_are_not_fitted(self) -> None:
        assert fit(_line([1.0, 2.0, 3.0]), 3.0) == {"points": 3, "status": "too_few"}

    @pytest.mark.parametrize("window_s", [1.0, 5.0, 10.0, 30.0, 60.0])
    @pytest.mark.parametrize(("per_s", "status"), [(0.5, "flagged"), (0.005, "steady")])
    def test_the_flag_does_not_depend_on_window_s(
        self, window_s: float, per_s: float, status: str
    ) -> None:
        duration = 600.0
        warm = 2
        times = [window_s * k for k in range(warm + 1, int(duration // window_s) + 1)]

        result = fit([(t, 100.0 + per_s * t) for t in times], duration)

        assert result["status"] == status
        assert result["change"] == pytest.approx(per_s * duration)

    def test_the_flag_does_not_depend_on_the_time_unit(self) -> None:
        values = [100.0 + 3 * i for i in range(10)]
        seconds = fit(_line(values), 10.0)
        millis = fit([(t * 1000, v) for t, v in _line(values)], 10_000.0)

        assert millis["status"] == seconds["status"] == "flagged"
        assert millis["change"] == pytest.approx(seconds["change"])


# ....................... #


class TestServerSeries:
    def test_a_counter_is_fitted_as_its_rate(self) -> None:
        server = _server()
        # 10/s, steady, scraped every 2 s: cumulative, so its level rises forever.
        _feed(server, [_expo(counters={"forze_ops_total": 20.0 * w}) for w in range(10)], 2.0)

        report = server.drift(warm_windows=2, duration_s=18.0)["forze_ops_total"]

        assert report["type"] == "counter"
        assert report["status"] == "steady"
        assert report["first_quarter"] == pytest.approx(10.0)
        assert server.points["forze_ops_total"][5] == (10.0, 10.0)

    def test_a_counter_whose_rate_climbs_is_flagged(self) -> None:
        server = _server()
        levels = [sum(range(w + 1)) * 10.0 for w in range(10)]
        _feed(server, [_expo(counters={"forze_dropped_total": level}) for level in levels])

        assert server.drift(2, 9.0)["forze_dropped_total"]["status"] == "flagged"

    def test_the_row_shows_a_counter_as_a_rate_none_before_the_second_scrape(self) -> None:
        server = _server()
        first = server.add(0, 0.0, parse_metrics(_expo(counters={"forze_x_total": 5.0})))
        second = server.add(1, 2.0, parse_metrics(_expo(counters={"forze_x_total": 9.0})))
        reset = server.add(2, 4.0, parse_metrics(_expo(counters={"forze_x_total": 1.0})))

        assert first["series"] == {"forze_x_total": None}
        assert second["series"] == {"forze_x_total": 2.0}
        # A counter that went down was reset: no rate across it.
        assert reset["series"] == {"forze_x_total": None}

    def test_a_non_finite_gauge_has_no_point(self) -> None:
        server = _server()
        row = server.add(1, 1.0, parse_metrics(_expo({"forze_g": math.nan})))

        assert row["series"] == {"forze_g": None}
        assert "forze_g" not in server.points

    def test_a_leaking_gauge_is_flagged_and_a_flat_one_is_not(self) -> None:
        server = _server()
        _feed(server, [_expo({"forze_leak": 100.0 + 10 * w, "forze_flat": 7.0}) for w in range(10)])

        report = server.drift(2, 9.0)

        assert report["forze_leak"]["status"] == "flagged"
        assert report["forze_flat"]["status"] == "steady"

    def test_a_gauge_that_fills_to_its_capacity_is_not_flagged(self) -> None:
        server = _server()
        texts = [
            _expo(
                {
                    'forze_cache_l1_size{forze_document="a"}': min(10.0 * w, 50.0),
                    'forze_cache_l1_capacity{forze_document="a"}': 50.0,
                    # Rising, but its capacity is at other labels: a leak.
                    'forze_cache_l1_size{forze_document="b"}': 10.0 * w,
                }
            )
            for w in range(10)
        ]
        _feed(server, texts)

        report = server.drift(2, 9.0)

        assert report['forze_cache_l1_size{forze_document="a"}']["status"] == "at_capacity"
        assert report['forze_cache_l1_size{forze_document="b"}']["status"] == "flagged"

    def test_a_gauge_short_of_its_capacity_is_still_flagged(self) -> None:
        server = _server()
        texts = [
            _expo(
                {
                    'forze_connection_pool_in_use{forze_client="pg"}': 2.0 + w,
                    'forze_connection_pool_max{forze_client="pg"}': 100.0,
                }
            )
            for w in range(10)
        ]
        _feed(server, texts)

        report = server.drift(2, 9.0)

        assert report['forze_connection_pool_in_use{forze_client="pg"}']["status"] == "flagged"

    def test_scrapes_of_different_processes_are_interleaved_and_not_fitted(self) -> None:
        server = _server()
        texts = [
            _expo({"forze_leak": 100.0 + 10 * w}, instance="w-1" if w % 2 else "w-2")
            for w in range(10)
        ]
        _feed(server, texts)

        assert server.drift(2, 9.0) == {"forze_leak": {"type": "gauge", "status": "interleaved"}}

    def test_a_restart_mid_run_interleaves_too(self) -> None:
        server = _server()
        texts = [
            _expo({"forze_leak": 100.0 + 10 * w}, instance="w-1" if w < 6 else "w-9")
            for w in range(10)
        ]
        _feed(server, texts)

        assert server.drift(2, 9.0)["forze_leak"]["status"] == "interleaved"

    def test_scrapes_that_name_no_instance_are_not_fitted(self) -> None:
        server = _server()
        _feed(server, [_expo({"forze_leak": 100.0 + 10 * w}, instance=None) for w in range(10)])

        assert server.drift(2, 9.0)["forze_leak"]["status"] == "unattributed"

    def test_series_labelled_by_instance_are_fitted_per_process(self) -> None:
        # A pre-fork union: every scrape names both workers, and each series names its own.
        server = _server()
        texts = [
            "# TYPE target_info gauge\n"
            'target_info{service_instance_id="w-1"} 1\n'
            'target_info{service_instance_id="w-2"} 1\n'
            "# TYPE forze_hub gauge\n"
            f'forze_hub{{service_instance_id="w-1"}} {100.0 + 10 * w}\n'
            'forze_hub{service_instance_id="w-2"} 5\n'
            "# TYPE forze_metrics_snapshot_age_seconds gauge\n"
            f'forze_metrics_snapshot_age_seconds{{service_instance_id="w-1"}} {0.25 * (w % 2)}\n'
            for w in range(10)
        ]
        _feed(server, texts)

        report = server.drift(2, 9.0)

        assert report['forze_hub{service_instance_id="w-1"}']["status"] == "flagged"
        assert report['forze_hub{service_instance_id="w-2"}']["status"] == "steady"
        # The union route's own age gauge describes the scrape, not the app.
        assert set(report) == {
            'forze_hub{service_instance_id="w-1"}',
            'forze_hub{service_instance_id="w-2"}',
        }

    def test_a_point_is_stamped_when_the_scrape_was_sent(self) -> None:
        server = _server()
        server.add(1, 10.0, parse_metrics(_expo({"forze_g": 1.0})), answered=12.5)

        assert server.points["forze_g"][1] == (10.0, 1.0)

    def test_a_union_snapshot_is_stamped_when_its_worker_wrote_it(self) -> None:
        # Snapshots every 7 s, scrapes every 60 s: each scrape reads a snapshot of another
        # age, so a point stamped at the scrape gives a steady counter a rate that wanders.
        server = _server()

        for k in range(10):
            sent = 60.0 * k + 0.5
            server.add(k, sent, parse_metrics(_union(sent, interval=7.0)), answered=sent + 0.05)

        rates = [value for _, value in server.points[_UNION_KEY].values()]

        assert rates == pytest.approx([100.0] * 9, rel=1e-3)
        assert server.drift(2, 540.0)[_UNION_KEY]["status"] == "steady"

    def test_a_snapshot_read_twice_gives_no_second_point(self) -> None:
        # Scrapes every 5 s, snapshots every 10 s; the latency varies, so a second read of
        # one snapshot is stamped a little later than the first.
        server = _server()
        rows = []

        for k in range(12):
            sent, latency = 5.0 * k + 0.5, 0.15 if k % 2 == 0 else 0.05
            text = _union(sent, interval=10.0, latency=latency)
            rows.append(server.add(k, sent, parse_metrics(text), answered=sent + latency))

        unchanged = [row["series"][_UNION_KEY] is None for row in rows[1:]]
        rates = [value for _, value in server.points[_UNION_KEY].values()]

        assert unchanged == [True, False] * 5 + [True]
        assert rates == pytest.approx([100.0] * 5, rel=1e-3)

    @pytest.mark.parametrize(
        ("text", "attribution"),
        [
            (_expo({"forze_g": 1.0}), "instance"),
            (_expo({"forze_g": 1.0}, instance=None), "none"),
            (
                'target_info{service_instance_id="a"} 1\n'
                'target_info{service_instance_id="b"} 1\n'
                "forze_g 1\n",
                "interleaved",
            ),
            (
                'forze_g{service_instance_id="a"} 1\nforze_g{service_instance_id="b"} 1\n',
                "per_series",
            ),
        ],
    )
    def test_the_first_scrape_says_how_it_ties_series_to_a_process(
        self, text: str, attribution: str
    ) -> None:
        server = _server()
        server.add(0, 0.0, parse_metrics(text))

        assert server.attribution() == attribution

    def test_warm_up_windows_are_left_out_of_the_fit(self) -> None:
        server = _server()
        # A start-up climb over the warm-up, flat after it.
        _feed(server, [_expo({"forze_g": 1000.0 if w > 2 else 100.0 * w}) for w in range(10)])

        report = server.drift(2, 9.0)["forze_g"]

        assert report["status"] == "steady"
        assert report["points"] == 7


# ....................... #


async def _get(call: Call) -> httpx.Response:
    return await call.request("GET", "/items")


def _plan(**kw: Any) -> LoadPlan:
    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=[Principal(name="viewer", mix={"get": 1})],
        scenarios=[Scenario("get", _get, writes=False)],
        **kw,
    )


def _app(metrics: Any) -> httpx.MockTransport:
    """An app whose ``/items`` answers 200 and whose ``/metrics`` is *metrics(scrape)*."""

    scrapes = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal scrapes

        if request.url.path == "/metrics":
            scrapes += 1
            return metrics(scrapes)

        await asyncio.sleep(0.001)

        return httpx.Response(200)

    return httpx.MockTransport(handler)


def _leaking(scrape: int) -> httpx.Response:
    return httpx.Response(200, text=_expo({"forze_leak": 100.0 + 50 * scrape, "forze_flat": 7.0}))


def _refused(_scrape: int) -> httpx.Response:
    raise httpx.ConnectError("refused")


def _undecodable(_scrape: int) -> httpx.Response:
    return httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=b"not gzip")


def _token(_scrape: int) -> httpx.Response:
    return httpx.Response(200, text="forze_x secret-token-123\n")


_SOAK = Soak(duration_s=0.3, window_s=0.05, workers=1)


class TestSoakRun:
    async def test_a_window_row_per_window_and_drift_at_the_end(self, tmp_path: Path) -> None:
        out = tmp_path / "soak.jsonl"

        soak = Soak(duration_s=1.2, window_s=0.2, workers=1)

        result = await run(_plan(), soak, out=out, transport=_app(_leaking))

        assert [row["window"] for row in result.rows] == [1, 2, 3, 4, 5, 6]
        assert all(row["kind"] == "window" and row["workers"] == 1 for row in result.rows)
        assert [row["warm"] for row in result.rows] == [True, True, False, False, False, False]
        times = [row["t_s"] for row in result.rows]
        assert times == sorted(set(times))
        # Goodput beside latency, so a slowdown can be told from more data.
        assert all("goodput_rps" in row and "p99" in row["ms"] for row in result.rows)
        assert any(row["goodput_rps"] > 0 for row in result.rows if not row["warm"])
        assert result.rows[0]["server"]["instances"] == ["w-1"]
        assert set(result.rows[0]["server"]["series"]) == {"forze_leak", "forze_flat"}

        drift = result.summary["drift"]
        assert drift["server"]["forze_leak"]["status"] == "flagged"
        assert drift["server"]["forze_flat"]["status"] == "steady"
        assert "forze_leak" in drift["flagged"]
        assert set(drift["client"]) == {"p50", "p95", "p99"}
        # A flag never fails the run.
        assert result.summary["stopped"] is None
        assert result.summary["windows"] == 6
        assert "knee" not in result.summary
        assert result.meta["model"] == "closed"
        assert result.meta["profile"]["window_s"] == 0.2
        assert result.meta["server"] == {"attribution": "instance"}

        # The file reads back for `compare`, window rows included.
        assert len(read_run(out).rows) == 6

    async def test_an_open_soak_runs_at_its_rate(self) -> None:
        profile = Soak(duration_s=0.3, window_s=0.05, rate=100)

        result = await run(_plan(metrics=None), profile, transport=_app(_leaking))

        assert result.meta["model"] == "open"
        assert all(row["rate"] == 100.0 and row["latency"] == "open" for row in result.rows)
        assert "server" not in result.rows[0]
        assert result.summary["drift"]["server"] is None

    async def test_a_missing_metrics_route_refuses_the_run_before_sign_in(
        self, tmp_path: Path
    ) -> None:
        signed_in = 0

        async def credential(_client: httpx.AsyncClient) -> dict[str, str]:
            nonlocal signed_in
            signed_in += 1
            return {}

        plan = attrs.evolve(
            _plan(), principals=[Principal(name="viewer", mix={"get": 1}, auth=credential)]
        )
        out = tmp_path / "refused.jsonl"

        with pytest.raises(CoreException) as caught:
            await run(plan, _SOAK, out=out, transport=_app(lambda _n: httpx.Response(404)))

        assert caught.value.code == "load_metrics_unavailable"
        assert "answered 404" in caught.value.summary
        assert signed_in == 0
        assert not out.exists()

    @pytest.mark.parametrize(
        "answer",
        [
            lambda _n: httpx.Response(200, text="forze_x{oops} 1\n"),
            _refused,
            _undecodable,
            _token,
        ],
        ids=["not-prometheus", "transport", "undecodable", "token"],
    )
    async def test_an_unreadable_metrics_route_refuses_the_run(self, answer: Any) -> None:
        with pytest.raises(CoreException) as caught:
            await run(_plan(), _SOAK, transport=_app(answer))

        assert caught.value.code == "load_metrics_unavailable"
        # The reason never carries the route's body.
        assert "secret-token-123" not in caught.value.summary

    @pytest.mark.parametrize(
        ("answer", "error"),
        [
            (lambda _n: httpx.Response(503), "answered 503"),
            (_undecodable, "failed (DecodingError)"),
            (_token, "is not Prometheus text (forze_x has a value that is not a number)"),
        ],
        ids=["status", "undecodable", "token"],
    )
    async def test_a_scrape_that_fails_mid_run_is_recorded_and_the_soak_goes_on(
        self, answer: Any, error: str
    ) -> None:
        def flaky(scrape: int) -> httpx.Response:
            return answer(scrape) if scrape == 3 else _leaking(scrape)

        result = await run(_plan(), _SOAK, transport=_app(flaky))

        # Scrape 1 is the baseline, so scrape 3 closes window 2.
        assert result.rows[1]["server"] == {"error": error}
        assert len(result.rows) == 6
        assert result.summary["drift"]["server"]["forze_leak"]["points"] == 4
        assert "secret-token-123" not in json.dumps(result.rows)

    async def test_server_errors_stop_a_soak_after_its_warm_up(self) -> None:
        async def failing(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.001)

            return httpx.Response(503)

        result = await run(_plan(metrics=None), _SOAK, transport=httpx.MockTransport(failing))
        stopped = result.summary["stopped"]

        # The warm-up never stops a soak; the first later window that got a call does.
        assert stopped["reason"] == "errors"
        assert stopped["window"] == len(result.rows) > _SOAK.warm_windows
        assert stopped["workers"] == 1

    async def test_a_cold_start_burst_in_the_warm_up_does_not_stop_a_soak(self) -> None:
        scrapes = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal scrapes

            if request.url.path == "/metrics":
                scrapes += 1
                return _leaking(scrapes)

            await asyncio.sleep(0.001)

            # Scrape 3 closes window 2: every call of the warm-up answers 503.
            return httpx.Response(503 if scrapes < 3 else 200)

        result = await run(_plan(), _SOAK, transport=httpx.MockTransport(handler))

        assert len(result.rows) == 6
        assert result.summary["stopped"] is None
        assert not any(row["outcomes"]["ok"] for row in result.rows[:2])

    async def test_a_scrape_that_names_no_process_is_in_the_meta_line_before_sign_in(
        self,
    ) -> None:
        signed_in = 0
        seen: list[tuple[Any, int]] = []

        async def credential(_client: httpx.AsyncClient) -> dict[str, str]:
            nonlocal signed_in
            signed_in += 1
            return {}

        def nameless(_scrape: int) -> httpx.Response:
            return httpx.Response(200, text=_expo({"forze_g": 1.0}, instance=None))

        plan = attrs.evolve(
            _plan(), principals=[Principal(name="viewer", mix={"get": 1}, auth=credential)]
        )

        result = await run(
            plan,
            _SOAK,
            transport=_app(nameless),
            on_meta=lambda meta: seen.append((meta.get("server"), signed_in)),
        )

        # A warning, not a refusal: the soak runs, and its client numbers still count.
        assert seen == [({"attribution": "none"}, 0)]
        assert result.summary["stopped"] is None
        assert result.summary["drift"]["server"]["forze_g"]["status"] == "unattributed"

    def test_a_metrics_url_with_a_host_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            asyncio.run(run(_plan(metrics="http://evil.example/metrics"), _SOAK))

        assert caught.value.code == "load_plan_invalid"


class TestSoakProfile:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({}, "exactly one"),
            ({"workers": 1, "rate": 5.0}, "exactly one"),
            ({"workers": 0}, "whole number of at least 1"),
            ({"workers": True}, "whole number of at least 1"),
            ({"rate": -1.0}, "positive, finite"),
            ({"workers": 1, "window_s": 0.0}, "window_s"),
            ({"workers": 1, "warm_windows": 1}, "at least 2"),
            ({"workers": 1, "warm_windows": 2.5}, "whole number"),
            ({"workers": 1, "duration_s": 300.0}, "at least 6 windows"),
            ({"workers": 1, "duration_s": math.nan}, "at least 6 windows"),
            ({"workers": 1, "think_s": -1.0}, "think_s"),
            ({"workers": 1, "stop_error_rate": 2.0}, "stop_error_rate"),
        ],
    )
    def test_a_profile_that_cannot_soak_is_refused(
        self, kwargs: dict[str, Any], message: str
    ) -> None:
        with pytest.raises(CoreException, match=message):
            Soak(**{"duration_s": 3600.0, **kwargs})

    def test_duration_rounds_down_to_whole_windows(self) -> None:
        soak = Soak(duration_s=3659.0, workers=4)

        assert soak.windows == 60
        assert (soak.step_s, soak.warm_s) == (60.0, 0.0)


# ....................... #


def _window(window: int, p95: float, *, warm: bool = False) -> dict[str, Any]:
    return {
        "kind": "window",
        "window": window,
        "workers": 4,
        "warm": warm,
        "goodput_rps": 100.0,
        "ms": {"p50": 1.0, "p95": p95, "p99": p95},
    }


def _soak_file(p95s: Sequence[float]) -> RunFile:
    rows = [_window(n, p95, warm=n <= 2) for n, p95 in enumerate(p95s, 1)]

    return RunFile(meta={"kind": "meta", "schema": 1}, rows=rows, summary=None)


class TestSoakCompare:
    def test_windows_are_matched_by_index_and_gated(self) -> None:
        comparison = compare_runs(
            _soak_file([1, 1, 10, 10]), _soak_file([1, 1, 10, 20]), fail_on={"p95": 0.15}
        )

        assert [line.split(":")[0] for line in comparison.lines] == [
            "window=1",
            "window=2",
            "window=3",
            "window=4",
        ]
        assert comparison.failures == ["window=4: p95 worse by 100.0% (limit 15%)"]

    def test_warm_up_windows_are_reported_and_never_gated(self) -> None:
        comparison = compare_runs(
            _soak_file([1, 1, 10]), _soak_file([50, 50, 10]), fail_on={"p95": 0.15}
        )

        assert comparison.failures == []
        assert "window=1: warm-up, not gated" in comparison.warnings

    def test_a_gate_with_only_warm_up_windows_compared_nothing(self) -> None:
        with pytest.raises(CoreException) as caught:
            compare_runs(_soak_file([1, 1]), _soak_file([1, 1]), fail_on={"p95": 0.15})

        assert caught.value.code == "load_compare_empty"

    def test_a_soak_file_round_trips(self, tmp_path: Path) -> None:
        path = tmp_path / "s.jsonl"
        records = [{"kind": "meta", "schema": 1}, _window(1, 2.0, warm=True), _window(3, 2.0)]
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n")

        assert [row["window"] for row in read_run(path).rows] == [1, 3]

    def test_the_summary_names_flagged_drift(self) -> None:
        row = {**_window(1, 2.0), "outcomes": {}, "client": {"saturated": False}}
        row["misconfigured"] = False

        flagged = summarize([row], {"drift": {"flagged": ["process_memory_usage_bytes"]}})
        quiet = summarize([row], {"drift": {"flagged": []}})

        assert flagged == [
            "window=1: goodput 100.0/s, p95 2 ms, p99 2 ms, errors 0",
            "drift: process_memory_usage_bytes",
        ]
        assert quiet[-1] == "drift: none flagged"
