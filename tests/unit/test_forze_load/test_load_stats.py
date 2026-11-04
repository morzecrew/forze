"""Outcome classes, step summaries and the knee rule, on synthetic numbers."""

from __future__ import annotations

from typing import Any

import pytest

from forze_load.stats import StepRecorder, classify, error_share, find_knee

pytestmark = pytest.mark.unit

# ----------------------- #


def _closed(workers: int, goodput: float, *, saturated: bool = False, **kw: Any) -> dict:
    return {
        "workers": workers,
        "goodput_rps": goodput,
        "misconfigured": kw.get("misconfigured", False),
        "client": {"saturated": saturated},
        "ms": {"p99": kw.get("p99", 10.0)},
    }


def _open(rate: float, answered: float, p99: float | None) -> dict:
    return {
        "rate": rate,
        "goodput_rps": answered,
        "answered_rps": answered,
        "misconfigured": False,
        "client": {"saturated": False},
        "ms": {"p99": p99},
    }


# ....................... #


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (200, "ok"),
        (201, "ok"),
        (204, "ok"),
        (409, "conflict"),
        (429, "shed"),
        (504, "deadline"),
        (401, "forbidden"),
        (403, "forbidden"),
        (404, "not_found"),
        (422, "client_error"),
        (500, "server_error"),
        (503, "server_error"),
    ],
)
def test_classify(status: int, outcome: str) -> None:
    assert classify(status) == outcome


class TestStepRecorder:
    def test_summary_counts_goodput_and_latency(self) -> None:
        recorder = StepRecorder()

        for latency in range(1, 101):
            recorder.record("read", "ok", float(latency))

        recorder.record("write", "conflict", 50.0)
        recorder.record("write", "transport", None)
        summary = recorder.summary(2.0)

        assert summary["calls"] == 102
        assert summary["goodput_rps"] == 50.0
        assert summary["answered_rps"] == 50.5
        assert summary["outcomes"]["conflict"] == 1
        assert summary["outcomes"]["transport"] == 1
        assert summary["ms"]["max"] == 100.0
        assert summary["ms"]["p50"] == pytest.approx(50, rel=0.03)
        assert summary["scenarios"]["write"]["calls"] == 2
        assert not summary["misconfigured"]
        assert error_share(summary) == pytest.approx(1 / 102)

    def test_a_deadline_counts_as_an_error(self) -> None:
        recorder = StepRecorder()
        recorder.record("read", "deadline", 30.0)
        recorder.record("read", "ok", 1.0)

        assert error_share(recorder.summary(1.0)) == 0.5

    def test_a_step_answering_403_is_misconfigured(self) -> None:
        recorder = StepRecorder()

        for _ in range(10):
            recorder.record("read", "forbidden", 1.0)

        assert recorder.summary(1.0)["misconfigured"]

    def test_an_empty_step_has_no_latency(self) -> None:
        summary = StepRecorder().summary(1.0)

        assert summary["ms"]["p99"] is None
        assert summary["ms"]["max"] is None
        assert error_share(summary) == 0.0


class TestClosedKnee:
    def test_the_knee_is_where_goodput_stops_rising(self) -> None:
        rows = [
            _closed(1, 100),
            _closed(2, 200),
            _closed(4, 390),
            _closed(8, 400),
            _closed(16, 401),
        ]

        assert find_knee(rows, model="closed") == {
            "knee": {"workers": 8},
            "confirmed": True,
            "note": None,
        }

    def test_one_dip_is_not_a_knee(self) -> None:
        rows = [_closed(1, 100), _closed(2, 101), _closed(4, 300), _closed(8, 600)]

        assert find_knee(rows, model="closed")["knee"] is None

    def test_a_flat_last_step_is_unconfirmed(self) -> None:
        knee = find_knee([_closed(1, 100), _closed(2, 200), _closed(4, 201)], model="closed")

        assert knee["knee"] == {"workers": 4}
        assert not knee["confirmed"]

    def test_a_saturated_step_is_never_named(self) -> None:
        rows = [_closed(1, 100), _closed(2, 200), _closed(4, 200, saturated=True)]
        knee = find_knee(rows, model="closed")

        assert knee["knee"] is None
        assert knee["note"] == "client saturated at {'workers': 4}"

    def test_a_knee_before_saturation_stands(self) -> None:
        rows = [
            _closed(1, 100),
            _closed(2, 101),
            _closed(4, 102),
            _closed(8, 102, saturated=True),
        ]

        assert find_knee(rows, model="closed")["knee"] == {"workers": 2}

    def test_a_misconfigured_step_stops_the_search(self) -> None:
        knee = find_knee([_closed(1, 100), _closed(2, 900, misconfigured=True)], model="closed")

        assert knee["knee"] is None
        assert "misconfigured" in knee["note"]


class TestOpenKnee:
    def test_p99_doubling_is_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 20, 22.0), _open(40, 40, 90.0)]

        assert find_knee(rows, model="open")["knee"] == {"rate": 40}

    def test_answers_falling_behind_the_offer_is_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 17, 21.0)]

        assert find_knee(rows, model="open")["knee"] == {"rate": 20}

    def test_a_healthy_ramp_has_no_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 20, None), _open(40, 40, 25.0)]

        assert find_knee(rows, model="open")["knee"] is None
