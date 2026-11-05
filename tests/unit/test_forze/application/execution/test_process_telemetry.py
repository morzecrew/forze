"""`instrument_process` exports the process's CPU time and resident memory as OTel metrics."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.tenancy import TenantPoolStats
from forze.application.execution import observability
from forze.application.execution.observability import (
    PROCESS_CPU_TIME_COUNTER,
    PROCESS_MEMORY_USAGE_COUNTER,
    instrument_process,
    instrument_tenant_pools,
)

# ----------------------- #


def _meter() -> tuple[Any, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _collect(reader: InMemoryMetricReader) -> dict[str, list[tuple[dict[str, Any], Any]]]:
    out: dict[str, list[tuple[dict[str, Any], Any]]] = {}
    data = reader.get_metrics_data()

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                out[metric.name] = [
                    (dict(dp.attributes), dp.value) for dp in metric.data.data_points
                ]

    return out


def _statm(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "statm"
    path.write_text(content)
    return path


class _Warnings(list[str]):
    def warning(self, event: str, *_args: Any, **_kw: Any) -> None:
        self.append(event)


def _cpu(points: dict[str, list[tuple[dict[str, Any], Any]]]) -> dict[str, float]:
    return {attrs["cpu.mode"]: value for attrs, value in points[PROCESS_CPU_TIME_COUNTER]}


# ....................... #


def test_cpu_time_is_reported_per_mode_and_never_decreases() -> None:
    meter, reader = _meter()
    instrument_process(meter=meter)

    first = _cpu(_collect(reader))
    sum(i * i for i in range(200_000))  # burn a little CPU between collections
    second = _cpu(_collect(reader))

    assert set(first) == {"user", "system"}
    assert first["user"] > 0
    assert first["system"] >= 0
    assert second["user"] >= first["user"]
    assert second["system"] >= first["system"]
    assert second["user"] <= os.times().user


@pytest.mark.skipif(sys.platform != "linux", reason="resident memory is read from /proc")
def test_resident_memory_is_plausible_on_linux() -> None:
    meter, reader = _meter()
    instrument_process(meter=meter)

    [(attrs, resident)] = _collect(reader)[PROCESS_MEMORY_USAGE_COUNTER]
    physical = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")

    assert attrs == {}
    assert 1 << 20 < resident < physical


@pytest.mark.skipif(not hasattr(os, "sysconf"), reason="needs os.sysconf for the page size")
def test_resident_memory_is_statm_resident_pages_times_page_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statm = _statm(tmp_path, "5000 1234 300 10 0 900 0\n")
    monkeypatch.setattr(observability, "_PROC_STATM", str(statm))

    meter, reader = _meter()
    instrument_process(meter=meter)

    [(_, resident)] = _collect(reader)[PROCESS_MEMORY_USAGE_COUNTER]

    assert resident == 1234 * os.sysconf("SC_PAGE_SIZE")


def test_memory_instrument_is_absent_without_statm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No current RSS outside Linux: skip the instrument rather than export the peak."""

    monkeypatch.setattr(observability, "_PROC_STATM", str(tmp_path / "missing"))

    meter, reader = _meter()
    instrument_process(meter=meter)

    points = _collect(reader)

    assert PROCESS_CPU_TIME_COUNTER in points
    assert PROCESS_MEMORY_USAGE_COUNTER not in points


@pytest.mark.skipif(not hasattr(os, "sysconf"), reason="needs os.sysconf for the page size")
def test_a_second_call_adds_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SDK keeps the first instruments on a meter and drops the repeat's callbacks."""

    monkeypatch.setattr(observability, "_PROC_STATM", str(_statm(tmp_path, "5000 1234 0\n")))

    meter, reader = _meter()
    instrument_process(meter=meter)
    instrument_process(meter=meter)

    points = _collect(reader)

    assert len(points[PROCESS_CPU_TIME_COUNTER]) == 2
    assert len(points[PROCESS_MEMORY_USAGE_COUNTER]) == 1


@pytest.mark.parametrize("content", ["", "5000\n", "5000 lots 0\n"])
def test_a_malformed_statm_at_registration_skips_the_memory_instrument(
    content: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(observability, "_PROC_STATM", str(_statm(tmp_path, content)))

    meter, reader = _meter()
    instrument_process(meter=meter)

    points = _collect(reader)

    assert PROCESS_CPU_TIME_COUNTER in points
    assert PROCESS_MEMORY_USAGE_COUNTER not in points


@pytest.mark.skipif(not hasattr(os, "sysconf"), reason="needs os.sysconf for the page size")
@pytest.mark.parametrize("breakage", ["vanish", "garble"])
def test_statm_failing_after_registration_yields_no_point_and_warns_once(
    breakage: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    statm = _statm(tmp_path, "5000 1234 0\n")
    monkeypatch.setattr(observability, "_PROC_STATM", str(statm))
    warnings = _Warnings()
    monkeypatch.setattr(observability, "logger", warnings, raising=False)

    meter, reader = _meter()
    instrument_process(meter=meter)

    if breakage == "vanish":
        statm.unlink()
    else:
        statm.write_text("garbage\n")

    first = _collect(reader)
    second = _collect(reader)

    assert PROCESS_MEMORY_USAGE_COUNTER not in first
    assert PROCESS_MEMORY_USAGE_COUNTER not in second
    assert PROCESS_CPU_TIME_COUNTER in second
    assert len(warnings) == 1
    assert not [r for r in caplog.records if "Callback failed" in r.getMessage()]


@pytest.mark.skipif(not hasattr(os, "sysconf"), reason="needs os.sysconf for the page size")
def test_the_call_registers_both_names_and_nothing_appears_without_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(observability, "_PROC_STATM", str(_statm(tmp_path, "5000 1234 0\n")))

    class _Client:
        def pool_stats(self) -> TenantPoolStats:
            return TenantPoolStats(size=1, capacity=2, created=1, disposed=0, evicted_explicit=0)

    called, called_reader = _meter()
    instrument_process(meter=called)

    uncalled, uncalled_reader = _meter()
    instrument_tenant_pools({"pg": _Client()}, meter=uncalled)

    with_call = set(_collect(called_reader))
    without_call = set(_collect(uncalled_reader))

    assert {PROCESS_CPU_TIME_COUNTER, PROCESS_MEMORY_USAGE_COUNTER} <= with_call
    assert without_call
    assert not {n for n in without_call if n.startswith("process.")}
