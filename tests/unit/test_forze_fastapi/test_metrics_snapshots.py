"""Per-worker metric snapshots and the union route that serves them (RFC 0083)."""

from __future__ import annotations

import asyncio
import os
import stat
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import prometheus_client
import pytest
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from prometheus_client import Counter, generate_latest
from prometheus_client.parser import text_string_to_metric_families
from starlette.testclient import TestClient

from forze.application.execution import build_runtime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import bind_time_source
from forze_fastapi.routes import MetricsSnapshots, attach_metrics_route, worker_metrics_reader
from forze_fastapi.routes._snapshots import (  # pyright: ignore[reportPrivateUsage]
    RenderGate,
    _Writer,
)

# ----------------------- #

INTERVAL = timedelta(seconds=1)
TTL = timedelta(seconds=3)


def _snapshots(directory: Path, **kwargs: object) -> MetricsSnapshots:
    return MetricsSnapshots(directory=directory, interval=INTERVAL, ttl=TTL, **kwargs)  # type: ignore[arg-type]


def _writer(directory: Path, render: Callable[[], str], **kwargs: object) -> _Writer:
    return _Writer(
        snapshots=_snapshots(directory, **kwargs),
        gate=RenderGate(render=lambda: render().encode()),
    )


def _body(instance: str, *, hits: float = 3.0) -> str:
    return (
        "# HELP forze_operations_total \n"
        "# TYPE forze_operations_total counter\n"
        f'forze_operations_total{{forze_outcome="success",service_instance_id="{instance}"}} '
        f"{hits}\n"
        "# HELP forze_tenancy_pool_size Live pools\n"
        "# TYPE forze_tenancy_pool_size gauge\n"
        f'forze_tenancy_pool_size{{forze_client="pg",service_instance_id="{instance}"}} 2.0\n'
    )


def _write(
    directory: Path,
    instance: str,
    *,
    pid: int = 4242,
    age: float = 0.5,
    wall: float | None = None,
    mono: float | None = None,
    body: str | None = None,
    header: str | None = None,
) -> Path:
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / f"{pid}-{uuid.uuid4().hex}.prom"
    written_at = time.time() - age if wall is None else wall
    monotonic_at = time.monotonic() - age if mono is None else mono
    head = header or (
        f"# forze-metrics-snapshot v1 written_at={written_at:.3f} mono={monotonic_at:.3f} "
        f"pid={pid} instance={instance}"
    )
    path.write_text(f"{head}\n{body if body is not None else _body(instance)}", "utf-8")

    return path


def _families(body: bytes) -> dict[str, list[tuple[str, dict[str, str], float]]]:
    return {
        family.name: [(s.name, dict(s.labels), s.value) for s in family.samples]
        for family in text_string_to_metric_families(body.decode())
    }


def _served(body: bytes) -> set[str]:
    return {labels["service_instance_id"] for _, labels, _ in _families(body)["forze_operations"]}


def _ages(body: bytes) -> dict[str, float]:
    return {
        labels["service_instance_id"]: value
        for _, labels, value in _families(body)["forze_metrics_snapshot_age_seconds"]
    }


@pytest.fixture
def provider() -> Iterator[tuple[MeterProvider, PrometheusMetricReader]]:
    reader = worker_metrics_reader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": "orders", "service.instance.id": "w-1"}),
    )

    try:
        yield provider, reader

    finally:
        provider.shutdown()


def _dead_pid() -> int:
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()

    return child.pid


class _Clock:
    """A time source with a settable wall clock and monotonic clock."""

    def __init__(self, wall: float, mono: float) -> None:
        self.wall = wall
        self.mono = mono

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.wall, UTC)

    def uuid(self) -> uuid.UUID:
        return uuid.uuid4()

    def monotonic(self) -> float:
        return self.mono


class _Renders:
    """A render that times every call, and can make chosen calls slow."""

    def __init__(self, durations: list[float], produce: Callable[..., bytes]) -> None:
        self.durations = durations
        self.produce = produce
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.starts: list[float] = []
        self.ends: list[float] = []
        self.started = [threading.Event() for _ in range(8)]

    def __call__(self, *args: Any) -> bytes:
        with self.lock:
            n = len(self.starts)
            self.starts.append(time.monotonic())
            self.active += 1
            self.peak = max(self.peak, self.active)

        if n < len(self.started):
            self.started[n].set()

        try:
            time.sleep(self.durations[n] if n < len(self.durations) else 0.0)
            return self.produce(*args)

        finally:
            with self.lock:
                self.active -= 1
                self.ends.append(time.monotonic())

    async def settled(self) -> None:
        for _ in range(250):
            if len(self.ends) == len(self.starts):
                return

            await asyncio.sleep(0.02)


class _Log:
    """Stands in for the module logger and records what each call said."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def __getattr__(self, level: str) -> object:
        return lambda event, *args, **_kw: self.records.append((level, event % args))


# ----------------------- #


class TestConfiguration:
    def test_ttl_defaults_to_three_intervals(self, tmp_path: Path) -> None:
        snapshots = MetricsSnapshots(directory=tmp_path)

        assert snapshots.interval == timedelta(seconds=10)
        assert snapshots.ttl == timedelta(seconds=30)
        assert MetricsSnapshots(directory=tmp_path, interval=INTERVAL).ttl == INTERVAL * 3

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"interval": timedelta(0)},
            {"interval": INTERVAL, "ttl": INTERVAL * 2 - timedelta(milliseconds=1)},
            {"max_snapshot_bytes": 0},
        ],
    )
    def test_refuses_what_could_drop_a_live_worker(
        self, tmp_path: Path, kwargs: dict[str, object]
    ) -> None:
        with pytest.raises(CoreException) as caught:
            MetricsSnapshots(directory=tmp_path, **kwargs)  # type: ignore[arg-type]

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    def test_the_reader_labels_every_series_and_keeps_process_collectors_out(
        self, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        meter_provider.get_meter("forze").create_counter("forze.test.hits").add(2)

        families = _families(generate_latest(reader._registry))  # pyright: ignore[reportPrivateUsage]

        assert families["forze_test_hits"][0][1]["service_instance_id"] == "w-1"
        assert not any(name.startswith(("process_", "python_")) for name in families)

    def test_an_exporter_without_resource_attribute_filter_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import opentelemetry.exporter.prometheus as exporter

        class OldReader:
            def __init__(self, disable_target_info: bool = False, prefix: str = "") -> None:
                pass

        monkeypatch.setattr(exporter, "PrometheusMetricReader", OldReader)

        with pytest.raises(CoreException) as caught:
            worker_metrics_reader()

        assert caught.value.code == "metrics_exporter_too_old"
        assert "0.66b0" in caught.value.summary


# ----------------------- #


class TestWriter:
    @pytest.mark.asyncio
    async def test_startup_writes_a_labelled_snapshot_privately(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        meter_provider.get_meter("forze").create_counter("forze.test.hits").add(5)
        directory = tmp_path / "snaps"
        runtime = build_runtime(lifecycle_steps=_snapshots(directory).writer_step(reader))

        async with runtime.scope():
            files = list(directory.iterdir())

            assert stat.S_IMODE(directory.stat().st_mode) == 0o700
            assert len(files) == 1
            assert stat.S_IMODE(files[0].stat().st_mode) == 0o600
            assert files[0].name.startswith(f"{os.getpid()}-")

            header, _, body = files[0].read_text().partition("\n")

        assert header.startswith("# forze-metrics-snapshot v1 written_at=")
        assert " mono=" in header
        assert header.endswith(f" pid={os.getpid()} instance=w-1")
        assert 'forze_test_hits_total{otel_scope_name="forze"' in body

    @pytest.mark.asyncio
    async def test_shutdown_leaves_a_final_snapshot_and_a_rebuild_overwrites_it(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        hits = meter_provider.get_meter("forze").create_counter("forze.test.hits")
        directory = tmp_path / "snaps"
        snapshots = _snapshots(directory)

        for _ in range(2):
            async with build_runtime(lifecycle_steps=snapshots.writer_step(reader)).scope():
                hits.add(4)

        files = list(directory.iterdir())
        assert len(files) == 1, "a runtime rebuilt in one process leaves one claimant"
        assert "forze_test_hits_total" in files[0].read_text()

        status, body = snapshots.union()
        assert status == 200
        assert _families(body)["forze_test_hits"][0][2] == 8.0

    @pytest.mark.parametrize("problem", ["world_writable", "symlink", "file"])
    @pytest.mark.asyncio
    async def test_an_unsafe_directory_fails_the_boot(
        self,
        tmp_path: Path,
        provider: tuple[MeterProvider, PrometheusMetricReader],
        problem: str,
    ) -> None:
        _, reader = provider
        directory = tmp_path / "snaps"

        if problem == "world_writable":
            directory.mkdir()
            directory.chmod(0o777)

        elif problem == "symlink":
            (tmp_path / "real").mkdir(mode=0o700)
            directory.symlink_to(tmp_path / "real")

        else:
            directory.write_text("")

        runtime = build_runtime(lifecycle_steps=_snapshots(directory).writer_step(reader))

        with pytest.raises(CoreException) as caught:
            async with runtime.scope():
                pass

        assert caught.value.code == "metrics_snapshot_directory_unsafe"

    @pytest.mark.asyncio
    async def test_a_directory_swapped_after_startup_changes_nothing(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        hits = meter_provider.get_meter("forze").create_counter("forze.test.hits")
        hits.add(1)
        directory = tmp_path / "snaps"
        other = tmp_path / "other"
        other.mkdir(mode=0o700)
        snapshots = MetricsSnapshots(
            directory=directory,
            interval=timedelta(milliseconds=50),
            ttl=timedelta(milliseconds=150),
        )

        async with build_runtime(lifecycle_steps=snapshots.writer_step(reader)).scope():
            # Whoever can write to the parent moves the checked directory away and puts a
            # symlink to a directory of their own at its path.
            directory.rename(tmp_path / "moved")
            directory.symlink_to(other)
            hits.add(1)
            await asyncio.sleep(0.25)

            status, body = snapshots.union()

        assert list(other.iterdir()) == [], "nothing went through the swapped path"
        assert len(list((tmp_path / "moved").glob("*.prom"))) == 1
        assert status == 200
        assert _families(body)["forze_test_hits"][0][2] == 2.0

    @pytest.mark.asyncio
    async def test_a_global_registry_reader_fails_the_boot(self, tmp_path: Path) -> None:
        # The global registry carries the process collectors, with no instance label: in a
        # union every worker's ``process_cpu_seconds_total`` would be one colliding series.
        reader = PrometheusMetricReader()
        meter_provider = MeterProvider(metric_readers=[reader])

        try:
            runtime = build_runtime(
                lifecycle_steps=_snapshots(tmp_path / "snaps").writer_step(reader)
            )

            with pytest.raises(CoreException) as caught:
                async with runtime.scope():
                    pass

        finally:
            meter_provider.shutdown()

        assert caught.value.code == "metrics_snapshot_unlabelled"
        assert "worker_metrics_reader()" in caught.value.summary

    @pytest.mark.asyncio
    async def test_a_sample_naming_another_instance_fails_the_boot(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        hits = meter_provider.get_meter("forze").create_counter("forze.test.hits")
        hits.add(1)
        # A data point's own attribute overrides the label copied from the resource.
        hits.add(1, {"service.instance.id": "someone-else"})
        runtime = build_runtime(lifecycle_steps=_snapshots(tmp_path / "snaps").writer_step(reader))

        with pytest.raises(CoreException) as caught:
            async with runtime.scope():
                pass

        assert caught.value.code == "metrics_snapshot_unlabelled"

    @pytest.mark.asyncio
    async def test_a_family_that_appears_later_without_the_label_is_left_out(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        meter_provider.get_meter("forze").create_counter("forze.test.hits").add(1)
        directory = tmp_path / "snaps"
        registry = reader._registry  # pyright: ignore[reportPrivateUsage]

        async with build_runtime(lifecycle_steps=_snapshots(directory).writer_step(reader)).scope():
            Counter("app_custom", "Unlabelled", registry=registry).inc()

        # The shutdown write is not strict: it drops the family, it does not fail.
        text = next(directory.iterdir()).read_text()
        assert "forze_test_hits_total" in text
        assert "app_custom" not in text

    @pytest.mark.asyncio
    async def test_the_sweep_removes_what_no_live_process_will_refresh(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        _, reader = provider
        directory = tmp_path / "snaps"
        dead = _dead_pid()
        dead_stale = _write(directory, "gone", pid=dead, age=60)
        dead_garbage = _write(directory, "junk", pid=dead, header="# not a snapshot")
        dead_fresh = _write(directory, "recent", pid=dead)
        live_stale = _write(directory, "stalled", pid=os.getppid(), age=60)
        temp = directory / f".tmp-{dead}-leftover"
        temp.write_text("")

        async with build_runtime(lifecycle_steps=_snapshots(directory).writer_step(reader)).scope():
            pass

        assert not dead_stale.exists(), "stale by its header, and its pid is gone"
        assert not dead_garbage.exists(), "a malformed header counts as not fresh"
        assert not temp.exists(), "nothing can finish a dead pid's temp file"
        assert dead_fresh.exists(), "still fresh: it may still be served"
        assert live_stale.exists(), "a stalled worker is withheld, never swept"


# ----------------------- #


class TestWriterLifetime:
    @pytest.mark.asyncio
    async def test_no_file_until_a_render_names_the_worker(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        meter_provider, reader = provider
        hits = meter_provider.get_meter("forze").create_counter("forze.test.hits")
        directory = tmp_path / "snaps"
        snapshots = MetricsSnapshots(
            directory=directory,
            interval=timedelta(milliseconds=50),
            ttl=timedelta(milliseconds=150),
        )

        async with build_runtime(lifecycle_steps=snapshots.writer_step(reader)).scope():
            await asyncio.sleep(0.2)

            # No series yet: nothing to serve, and no instance to name the file by.
            assert list(directory.glob("*.prom")) == []
            assert snapshots.union()[0] == 503

            hits.add(1)

            for _ in range(100):
                if list(directory.glob("*.prom")):
                    break

                await asyncio.sleep(0.02)

            (written,) = directory.glob("*.prom")
            assert written.read_text().partition("\n")[0].endswith(" instance=w-1")

    def test_the_first_instance_a_worker_names_is_its_own_for_good(self, tmp_path: Path) -> None:
        directory = tmp_path / "snaps"
        directory.mkdir(mode=0o700)
        writer = _writer(directory, lambda: "")

        writer.write(_body("w-1"), strict=True)
        writer.write(_body("w-2"), strict=False)

        (written,) = directory.glob("*.prom")
        header, _, body = written.read_text().partition("\n")
        assert header.endswith(" instance=w-1")
        assert "w-2" not in body, "a render naming another instance fails the label check"

        with pytest.raises(CoreException) as caught:
            writer.write(_body("w-2"), strict=True)

        assert caught.value.code == "metrics_snapshot_unlabelled"

    @pytest.mark.parametrize(
        ("slow_tick", "final_render"),
        # The final render has one 0.6 s interval: a 0.3 s tick ends inside it, 1.5 s does not.
        [(0.3, True), (1.5, False)],
    )
    @pytest.mark.asyncio
    async def test_shutdown_during_a_slow_tick_never_renders_twice_at_once(
        self, tmp_path: Path, slow_tick: float, final_render: bool
    ) -> None:
        directory = tmp_path / "snaps"
        directory.mkdir(mode=0o700)
        renders = _Renders([slow_tick], lambda: _body("w-1").encode())
        writer = _Writer(
            snapshots=MetricsSnapshots(directory=directory, interval=timedelta(seconds=0.6)),
            gate=RenderGate(render=renders),
        )

        # The loop's stop cancels a tick mid-render: the render's thread runs on.
        tick = asyncio.ensure_future(writer.tick())
        assert await asyncio.to_thread(renders.started[0].wait, 5)
        tick.cancel()

        with suppress(asyncio.CancelledError):
            await tick

        await writer.final()
        # A skipped final leaves the tick's thread running; let it finish before counting.
        await renders.settled()

        assert renders.peak == 1, "two renders ran at once"
        assert len(list(directory.glob("*.prom"))) == 1

        if final_render:
            assert len(renders.starts) == 2
            assert renders.starts[1] >= renders.ends[0], "the final render waited for the tick"

        else:
            assert len(renders.starts) == 1, "the final render was skipped"

    @pytest.mark.asyncio
    async def test_a_plain_route_waits_for_the_writers_render(
        self,
        tmp_path: Path,
        provider: tuple[MeterProvider, PrometheusMetricReader],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        meter_provider, reader = provider
        meter_provider.get_meter("forze").create_counter("forze.test.hits").add(1)
        # The startup render is quick; the first tick's render takes a second.
        renders = _Renders([0.0, 1.0], generate_latest)
        monkeypatch.setattr(prometheus_client, "generate_latest", renders)
        snapshots = MetricsSnapshots(directory=tmp_path / "snaps", interval=timedelta(seconds=5))
        step = snapshots.writer_step(reader)
        router = APIRouter()
        attach_metrics_route(router, reader)  # a plain route on the writer's reader
        app = FastAPI()
        app.include_router(router)

        async with build_runtime(lifecycle_steps=step).scope():
            assert await asyncio.to_thread(renders.started[1].wait, 5)

            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
                response = await client.get("/metrics")

            renders_at_response = len(renders.starts)

        await renders.settled()

        assert response.status_code == 200
        assert "forze_test_hits_total" in response.text
        assert renders_at_response == 2, "the scrape joined the tick's render"
        assert renders.peak == 1


# ----------------------- #


class TestUnion:
    def test_the_union_keeps_every_series_per_instance(self, tmp_path: Path) -> None:
        for n in range(3):
            _write(tmp_path, f"w-{n}", pid=100 + n, body=_body(f"w-{n}", hits=float(n)))

        status, body = _snapshots(tmp_path).union()

        assert status == 200
        families = _families(body)
        hits = families["forze_operations"]
        assert sorted((labels["service_instance_id"], value) for _, labels, value in hits) == [
            ("w-0", 0.0),
            ("w-1", 1.0),
            ("w-2", 2.0),
        ]
        assert len(families["forze_tenancy_pool_size"]) == 3
        # One HELP/TYPE per family: the parser would split a repeated family in two.
        assert body.decode().count("# TYPE forze_operations_total counter") == 1

    def test_every_served_worker_has_its_monotonic_age(self, tmp_path: Path) -> None:
        _write(tmp_path, "w-0", pid=100, wall=1000.0, mono=500.0)
        _write(tmp_path, "w-1", pid=101, wall=999.0, mono=499.0)

        with bind_time_source(_Clock(wall=1001.0, mono=501.25)):
            _, body = _snapshots(tmp_path).union()

        assert _ages(body) == {"w-0": 1.25, "w-1": 2.25}

    @pytest.mark.parametrize("step", [-100.0, 100.0])
    def test_a_wall_clock_step_withholds_old_snapshots_until_the_next_write(
        self, tmp_path: Path, step: float
    ) -> None:
        _write(tmp_path, "w-0", pid=100, wall=1000.0, mono=500.0)
        snapshots = _snapshots(tmp_path)

        with bind_time_source(_Clock(wall=1001.0, mono=501.0)):
            assert _served(snapshots.union()[1]) == {"w-0"}

        # The wall clock jumps; the monotonic clock does not. The old snapshot is withheld,
        # never served for longer than its monotonic age allows.
        with bind_time_source(_Clock(wall=1001.5 + step, mono=501.5)):
            assert snapshots.union()[0] == 503

        _write(tmp_path, "w-0", pid=100, wall=1001.5 + step, mono=501.5)

        with bind_time_source(_Clock(wall=1002.5 + step, mono=502.5)):
            status, body = snapshots.union()

        assert status == 200
        assert _ages(body) == {"w-0": 1.0}

    def test_a_snapshot_from_before_a_reboot_is_not_served(self, tmp_path: Path) -> None:
        # Its wall-clock age is fine, but its monotonic stamp is ahead of the clock now.
        _write(tmp_path, "w-0", wall=1000.0, mono=900.0)

        with bind_time_source(_Clock(wall=1001.0, mono=10.0)):
            assert _snapshots(tmp_path).union()[0] == 503

    def test_a_snapshot_older_than_ttl_is_not_served(self, tmp_path: Path) -> None:
        _write(tmp_path, "live", pid=100)
        _write(tmp_path, "dead", pid=101, age=TTL.total_seconds() + 1)

        _, body = _snapshots(tmp_path).union()

        assert _served(body) == {"live"}

    def test_two_processes_sharing_an_instance_id_are_both_withheld(self, tmp_path: Path) -> None:
        _write(tmp_path, "shared", pid=100)
        _write(tmp_path, "shared", pid=101)
        _write(tmp_path, "own", pid=102)

        status, body = _snapshots(tmp_path).union()

        assert status == 200
        assert _served(body) == {"own"}

    def test_nothing_to_serve_answers_503(self, tmp_path: Path) -> None:
        assert _snapshots(tmp_path / "missing").union()[0] == 503

        _write(tmp_path, "shared", pid=100)
        _write(tmp_path, "shared", pid=101)

        assert _snapshots(tmp_path).union()[0] == 503, "every fresh snapshot is a duplicate"

    def test_a_family_whose_type_changed_keeps_the_first(self, tmp_path: Path) -> None:
        _write(tmp_path, "w-0", pid=100)
        _write(
            tmp_path,
            "w-1",
            pid=101,
            body=(
                "# HELP forze_operations_total \n"
                "# TYPE forze_operations_total gauge\n"
                'forze_operations_total{service_instance_id="w-1"} 9.0\n'
            ),
        )

        status, body = _snapshots(tmp_path).union()

        assert status == 200
        text = body.decode()
        assert text.count("# TYPE forze_operations_total") == 1
        assert text.count("forze_operations_total{") == 1

    def test_a_family_named_like_the_age_gauge_is_left_out(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "w-0",
            body=_body("w-0")
            + "# HELP forze_metrics_snapshot_age_seconds Not yours\n"
            + "# TYPE forze_metrics_snapshot_age_seconds gauge\n"
            + 'forze_metrics_snapshot_age_seconds{service_instance_id="w-0"} 999.0\n',
        )

        status, body = _snapshots(tmp_path).union()

        assert status == 200
        assert body.decode().count("# TYPE forze_metrics_snapshot_age_seconds gauge") == 1
        assert _ages(body)["w-0"] < 999.0

    def test_a_label_value_holding_a_help_marker_does_not_split_a_family(
        self, tmp_path: Path
    ) -> None:
        body = (
            "# HELP forze_operations_total \n"
            "# TYPE forze_operations_total counter\n"
            'forze_operations_total{forze_operation="a # HELP b",service_instance_id="w-0"} 1.0\n'
        )
        _write(tmp_path, "w-0", body=body)

        _, merged = _snapshots(tmp_path).union()

        ((_, labels, _),) = _families(merged)["forze_operations"]
        assert labels["forze_operation"] == "a # HELP b"

    @pytest.mark.parametrize("problem", ["header", "family", "utf8", "size", "symlink", "name"])
    def test_a_bad_file_is_skipped_and_the_rest_served(self, tmp_path: Path, problem: str) -> None:
        _write(tmp_path, "good", pid=100)

        if problem == "header":
            _write(tmp_path, "bad", pid=101, header="# not a snapshot")

        elif problem == "family":
            _write(tmp_path, "bad", pid=101, body="forze_operations_total 1.0\n")

        elif problem == "utf8":
            (tmp_path / f"101-{uuid.uuid4().hex}.prom").write_bytes(b"\xff\xfe")

        elif problem == "size":
            _write(tmp_path, "bad", pid=101, body=_body("bad") + "#" * 10_000 + "\n")

        elif problem == "symlink":
            target = _write(tmp_path.parent / f"{tmp_path.name}-outside", "bad", pid=101)
            (tmp_path / f"101-{uuid.uuid4().hex}.prom").symlink_to(target)

        else:
            (tmp_path / "bad.prom").write_text(_body("bad"))

        status, body = _snapshots(tmp_path, max_snapshot_bytes=4096).union()

        assert status == 200
        assert _served(body) == {"good"}

    def test_a_file_owned_by_another_user_is_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write(tmp_path, "w-0")
        snapshots = _snapshots(tmp_path)
        assert snapshots.union()[0] == 200  # the directory is now held open

        # Without root no test can chown a file, so the process changes users instead.
        real = os.geteuid()
        monkeypatch.setattr(os, "geteuid", lambda: real + 1)

        assert snapshots.union()[0] == 503


# ----------------------- #


class TestRoute:
    def test_the_route_serves_the_union_from_any_worker(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        _, reader = provider
        _write(tmp_path, "w-0", pid=100)
        _write(tmp_path, "w-1", pid=101)
        router = APIRouter()
        attach_metrics_route(router, reader, snapshots=_snapshots(tmp_path))
        app = FastAPI()
        app.include_router(router)

        response = TestClient(app).get("/metrics")

        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
        assert 'service_instance_id="w-1"' in response.text
        assert "/metrics" not in app.openapi()["paths"]

    def test_the_route_answers_503_with_nothing_fresh(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        _, reader = provider
        router = APIRouter()
        attach_metrics_route(router, reader, snapshots=_snapshots(tmp_path))
        app = FastAPI()
        app.include_router(router)

        assert TestClient(app).get("/metrics").status_code == 503


# ----------------------- #


_MIXED = (
    "# HELP forze_operations_total \n"
    "# TYPE forze_operations_total counter\n"
    'forze_operations_total{forze_outcome="success",service_instance_id="w-1"} 1.0\n'
    'forze_operations_total{forze_outcome="failed"} 2.0\n'
    "# HELP forze_tenancy_pool_size Live pools\n"
    "# TYPE forze_tenancy_pool_size gauge\n"
    'forze_tenancy_pool_size{forze_client="pg",service_instance_id="w-1"} 2.0\n'
    'forze_tenancy_pool_size{forze_client="redis",service_instance_id="w-9"} 1.0\n'
    "# HELP forze_kept Kept\n"
    "# TYPE forze_kept gauge\n"
    'forze_kept{service_instance_id="w-1"} 1.0\n'
)


class TestHardening:
    def test_a_fifo_named_like_a_snapshot_does_not_block_the_scrape(self, tmp_path: Path) -> None:
        _write(tmp_path, "good", pid=100)
        fifo = tmp_path / f"101-{uuid.uuid4().hex}.prom"
        os.mkfifo(fifo)
        results: list[tuple[int, bytes]] = []
        scrape = threading.Thread(
            target=lambda: results.append(_snapshots(tmp_path).union()), daemon=True
        )
        scrape.start()
        scrape.join(5)

        try:
            assert not scrape.is_alive(), "the scrape blocked opening a FIFO"

        finally:
            if scrape.is_alive():
                # Release the blocked open() so the thread can end.
                with suppress(OSError):
                    os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))

        status, body = results[0]
        assert status == 200
        assert _served(body) == {"good"}

    def test_a_stale_snapshot_is_skipped_without_reading_its_body(self, tmp_path: Path) -> None:
        stamp = f"written_at={time.time() - 60:.3f} mono={time.monotonic() - 60:.3f}"
        tmp_path.joinpath(f"100-{uuid.uuid4().hex}.prom").write_bytes(
            f"# forze-metrics-snapshot v1 {stamp} pid=100 instance=old\n".encode()
            + b"\xff\xfe not text"
        )
        snapshots = _snapshots(tmp_path)

        assert snapshots.union()[0] == 503
        seen = snapshots._log_once.seen  # pyright: ignore[reportPrivateUsage]
        assert not any("UTF-8" in key for key in seen), "the body was read and decoded"

    @pytest.mark.asyncio
    async def test_concurrent_scrapes_share_one_read(
        self,
        tmp_path: Path,
        provider: tuple[MeterProvider, PrometheusMetricReader],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _, reader = provider
        _write(tmp_path, "w-0")
        reads: list[float] = []
        union = MetricsSnapshots.union

        def slow_union(self: MetricsSnapshots) -> tuple[int, bytes]:
            reads.append(time.monotonic())
            time.sleep(0.3)
            return union(self)

        monkeypatch.setattr(MetricsSnapshots, "union", slow_union)
        router = APIRouter()
        attach_metrics_route(router, reader, snapshots=_snapshots(tmp_path))
        app = FastAPI()
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            responses = await asyncio.gather(*(client.get("/metrics") for _ in range(10)))

        assert [response.status_code for response in responses] == [200] * 10
        assert len(reads) == 1

    def test_a_comment_line_among_samples_drops_only_that_family(self, tmp_path: Path) -> None:
        body = (
            "# HELP forze_operations_total \n"
            "# TYPE forze_operations_total counter\n"
            'forze_operations_total{service_instance_id="w-0"} 1.0\n'
            "# TYPE forze_stray gauge\n"
            'forze_stray{service_instance_id="w-0"} 5.0\n'
            "# HELP forze_tenancy_pool_size Live pools\n"
            "# TYPE forze_tenancy_pool_size gauge\n"
            'forze_tenancy_pool_size{forze_client="pg",service_instance_id="w-0"} 2.0\n'
        )
        _write(tmp_path, "w-0", body=body)

        status, merged = _snapshots(tmp_path).union()

        assert status == 200
        assert b"forze_stray" not in merged
        families = _families(merged)
        assert "forze_operations" not in families
        assert len(families["forze_tenancy_pool_size"]) == 1

    def test_a_family_with_any_unlabelled_or_foreign_sample_is_left_out(
        self, tmp_path: Path
    ) -> None:
        directory = tmp_path / "snaps"
        directory.mkdir(mode=0o700)

        _writer(directory, lambda: "").write(_MIXED, strict=False)

        (written,) = directory.glob("*.prom")
        body = written.read_text().partition("\n")[2]
        assert "forze_kept" in body
        assert "forze_operations_total" not in body
        assert "forze_tenancy_pool_size" not in body

        with pytest.raises(CoreException) as caught:
            _writer(directory, lambda: "").write(_MIXED, strict=True)

        assert caught.value.code == "metrics_snapshot_unlabelled"

    def test_nothing_kept_writes_nothing(self, tmp_path: Path) -> None:
        directory = tmp_path / "snaps"
        directory.mkdir(mode=0o700)
        writer = _writer(directory, lambda: "")
        writer.write(_body("w-1"), strict=True)
        (written,) = directory.glob("*.prom")
        before = written.read_text()

        # Every family now carries another worker's id: all are left out.
        writer.write(_body("w-2"), strict=False)

        assert written.read_text() == before, "a header alone would show an age, no series"

        fresh = tmp_path / "fresh"
        fresh.mkdir(mode=0o700)
        unlabelled = _body("x").replace('service_instance_id="x"', 'other="x"')
        _writer(fresh, lambda: "").write(unlabelled, strict=False)

        assert list(fresh.glob("*.prom")) == []

    @pytest.mark.asyncio
    async def test_a_persistent_tick_failure_is_logged_once_per_cause(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_fastapi.routes import _snapshots as module

        log = _Log()
        monkeypatch.setattr(module, "_logger", log)
        directory = tmp_path / "snaps"
        directory.mkdir(mode=0o700)
        oversized = _writer(directory, lambda: _body("w-1"), max_snapshot_bytes=64)

        for _ in range(3):
            await oversized.tick()

        assert [level for level, _ in log.records] == ["warning"]
        assert "over max_snapshot_bytes" in log.records[0][1]
        assert list(directory.glob("*.prom")) == []

        log.records.clear()
        writer = _writer(directory, lambda: _body("w-1"))
        writer.prepare()
        directory.chmod(0o500)

        try:
            for _ in range(3):
                await writer.tick()

        finally:
            directory.chmod(0o700)

        assert [level for level, _ in log.records] == ["error"]

        # Recovered: the cause is forgotten, so its next occurrence is logged again.
        await writer.tick()
        directory.chmod(0o500)

        try:
            await writer.tick()

        finally:
            directory.chmod(0o700)

        assert [level for level, _ in log.records] == ["error", "error"]
