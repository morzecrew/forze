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
from collections.abc import Iterator
from contextlib import suppress
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi import APIRouter, FastAPI
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from prometheus_client import Counter, generate_latest
from prometheus_client.parser import text_string_to_metric_families
from starlette.testclient import TestClient

from forze.application.execution import build_runtime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi.routes import MetricsSnapshots, attach_metrics_route, worker_metrics_reader
from forze_fastapi.routes._snapshots import _Writer  # pyright: ignore[reportPrivateUsage]

# ----------------------- #

INTERVAL = timedelta(seconds=1)
TTL = timedelta(seconds=3)


def _snapshots(directory: Path, **kwargs: object) -> MetricsSnapshots:
    return MetricsSnapshots(directory=directory, interval=INTERVAL, ttl=TTL, **kwargs)  # type: ignore[arg-type]


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
    body: str | None = None,
    header: str | None = None,
) -> Path:
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / f"{pid}-{uuid.uuid4().hex}.prom"
    written_at = time.time() - age
    head = header or (
        f"# forze-metrics-snapshot v1 written_at={written_at:.3f} pid={pid} instance={instance}"
    )
    path.write_text(f"{head}\n{body if body is not None else _body(instance)}", "utf-8")

    return path


def _families(body: bytes) -> dict[str, list[tuple[str, dict[str, str], float]]]:
    return {
        family.name: [(s.name, dict(s.labels), s.value) for s in family.samples]
        for family in text_string_to_metric_families(body.decode())
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
    async def test_the_sweep_removes_only_old_files_of_dead_processes(
        self, tmp_path: Path, provider: tuple[MeterProvider, PrometheusMetricReader]
    ) -> None:
        _, reader = provider
        directory = tmp_path / "snaps"
        dead = _dead_pid()
        old = time.time() - 60
        dead_old = _write(directory, "gone", pid=dead)
        dead_fresh = _write(directory, "recent", pid=dead)
        live_old = _write(directory, "stalled", pid=os.getppid())
        temp_old = directory / f".tmp-{dead}-leftover"
        temp_old.write_text("")

        for path in (dead_old, live_old, temp_old):
            os.utime(path, (old, old))

        async with build_runtime(lifecycle_steps=_snapshots(directory).writer_step(reader)).scope():
            pass

        assert not dead_old.exists()
        assert not temp_old.exists()
        assert dead_fresh.exists()
        assert live_old.exists(), "a stalled worker is withheld, never swept"


# ----------------------- #


class _Renders:
    """A writer's render: times every call, and can make the first one slow."""

    def __init__(self, first: float) -> None:
        self.first = first
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.starts: list[float] = []
        self.ends: list[float] = []
        self.entered = threading.Event()

    def __call__(self) -> str:
        with self.lock:
            n = len(self.starts)
            self.starts.append(time.monotonic())
            self.active += 1
            self.peak = max(self.peak, self.active)

        self.entered.set()

        try:
            time.sleep(self.first if n == 0 else 0.0)
            return _body("w-1")

        finally:
            with self.lock:
                self.active -= 1
                self.ends.append(time.monotonic())


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
        rendered = [_body("w-1")]
        writer = _Writer(snapshots=_snapshots(directory), render=lambda: rendered[0])

        writer.write(strict=True)
        rendered[0] = _body("w-2")
        writer.write(strict=False)

        (written,) = directory.glob("*.prom")
        header, _, body = written.read_text().partition("\n")
        assert header.endswith(" instance=w-1")
        assert "w-2" not in body, "a render naming another instance fails the label check"

        with pytest.raises(CoreException) as caught:
            writer.write(strict=True)

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
        renders = _Renders(slow_tick)
        writer = _Writer(
            snapshots=MetricsSnapshots(directory=directory, interval=timedelta(seconds=0.6)),
            render=renders,
        )

        # The loop's stop cancels a tick mid-render: the render's thread runs on.
        tick = asyncio.ensure_future(writer.tick())
        assert await asyncio.to_thread(renders.entered.wait, 5)
        tick.cancel()

        with suppress(asyncio.CancelledError):
            await tick

        await writer.final()

        # A skipped final leaves the tick's thread running; let it finish before counting.
        for _ in range(200):
            if len(renders.ends) == len(renders.starts):
                break

            await asyncio.sleep(0.02)

        assert renders.peak == 1, "two renders ran at once"
        assert len(list(directory.glob("*.prom"))) == 1

        if final_render:
            assert len(renders.starts) == 2
            assert renders.starts[1] >= renders.ends[0], "the final render waited for the tick"

        else:
            assert len(renders.starts) == 1, "the final render was skipped"


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

    def test_every_served_worker_has_an_age(self, tmp_path: Path) -> None:
        _write(tmp_path, "w-0", pid=100, age=1.0)
        _write(tmp_path, "w-1", pid=101, age=2.0)

        _, body = _snapshots(tmp_path).union()

        ages = {
            labels["service_instance_id"]: value
            for _, labels, value in _families(body)["forze_metrics_snapshot_age_seconds"]
        }
        assert set(ages) == {"w-0", "w-1"}
        assert 1.0 <= ages["w-0"] < 1.5
        assert 2.0 <= ages["w-1"] < 2.5

    def test_a_clock_step_backwards_clamps_the_age_at_zero(self, tmp_path: Path) -> None:
        _write(tmp_path, "w-0", age=-30.0)

        _, body = _snapshots(tmp_path).union()

        assert _families(body)["forze_metrics_snapshot_age_seconds"][0][2] == 0.0

    def test_a_snapshot_older_than_ttl_is_not_served(self, tmp_path: Path) -> None:
        _write(tmp_path, "live", pid=100)
        _write(tmp_path, "dead", pid=101, age=TTL.total_seconds() + 1)

        _, body = _snapshots(tmp_path).union()

        served = {
            labels["service_instance_id"] for _, labels, _ in _families(body)["forze_operations"]
        }
        assert served == {"live"}

    def test_two_processes_sharing_an_instance_id_are_both_withheld(self, tmp_path: Path) -> None:
        _write(tmp_path, "shared", pid=100)
        _write(tmp_path, "shared", pid=101)
        _write(tmp_path, "own", pid=102)

        status, body = _snapshots(tmp_path).union()

        assert status == 200
        served = {
            labels["service_instance_id"] for _, labels, _ in _families(body)["forze_operations"]
        }
        assert served == {"own"}

    def test_nothing_to_serve_answers_503(self, tmp_path: Path) -> None:
        assert _snapshots(tmp_path / "missing").union()[0] == 503

        _write(tmp_path, "shared", pid=100)
        _write(tmp_path, "shared", pid=101)

        assert _snapshots(tmp_path).union()[0] == 503

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
        served = {
            labels["service_instance_id"] for _, labels, _ in _families(body)["forze_operations"]
        }
        assert served == {"good"}


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
