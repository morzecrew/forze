"""Outcome classes, step summaries and the knee rule, on synthetic numbers."""

from __future__ import annotations

from typing import Any

import pytest

from forze_load.stats import KNEE_MIN_SAMPLES, StepRecorder, classify, error_share, find_knee

pytestmark = pytest.mark.unit

# ----------------------- #


def _closed(workers: int, goodput: float, **kw: Any) -> dict[str, Any]:
    return {
        "workers": workers,
        "goodput_rps": goodput,
        "misconfigured": kw.get("misconfigured", False),
        "client": {"saturated": kw.get("saturated", False)},
        "ms": {"p99": 10.0},
        "sampled": 1000,
    }


def _open(rate: float, goodput: float, p99: float | None, **kw: Any) -> dict[str, Any]:
    outcomes = {"conflict": kw.get("conflict", 0), "not_found": kw.get("not_found", 0)}

    return {
        "rate": rate,
        "goodput_rps": goodput,
        "duration_s": 1.0,
        "outcomes": outcomes,
        "misconfigured": False,
        "client": {"saturated": kw.get("saturated", False)},
        "ms": {"p99": p99},
        "sampled": kw.get("sampled", 1000),
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
    def test_summary_counts_goodput_and_answered_latency(self) -> None:
        recorder = StepRecorder(warm_until=0.0)

        for latency in range(1, 101):
            recorder.record("read", "ok", float(latency))

        recorder.record("write", "conflict", 50.0)
        recorder.record("write", "transport", None)
        recorder.add_auth(0.25)
        summary = recorder.summary(2.0)

        assert summary["calls"] == 102
        assert summary["goodput_rps"] == 50.0
        assert summary["sampled"] == 101  # the transport failure has no latency
        assert summary["outcomes"]["conflict"] == 1
        assert summary["outcomes"]["transport"] == 1
        assert summary["ms"]["max"] == 100.0
        assert summary["ms"]["p50"] == pytest.approx(50, rel=0.03)
        assert summary["scenarios"]["write"]["calls"] == 2
        assert summary["auth"] == {"total_s": 0.25, "max_ms": 250.0}
        assert not summary["misconfigured"]
        assert error_share(summary) == pytest.approx(1 / 102)

    def test_a_deadline_counts_as_an_error(self) -> None:
        recorder = StepRecorder(warm_until=0.0)
        recorder.record("read", "deadline", 30.0)
        recorder.record("read", "ok", 1.0)

        assert error_share(recorder.summary(1.0)) == 0.5

    def test_a_step_answering_403_is_misconfigured(self) -> None:
        recorder = StepRecorder(warm_until=0.0)

        for _ in range(10):
            recorder.record("read", "forbidden", 1.0)

        assert recorder.summary(1.0)["misconfigured"]

    def test_an_empty_step_has_no_latency(self) -> None:
        summary = StepRecorder(warm_until=0.0).summary(1.0)

        assert summary["ms"]["p99"] is None
        assert summary["ms"]["max"] is None
        assert error_share(summary) == 0.0

    async def test_drain_waits_for_the_calls_the_step_sent(self) -> None:
        recorder = StepRecorder(warm_until=0.0)
        recorder.enter()
        recorder.enter()
        recorder.leave()

        await recorder.drain(0.01)  # one still in flight: gives up after the timeout
        assert recorder.inflight == 1

        recorder.leave()
        await recorder.drain(0.01)
        assert recorder.summary(1.0)["unfinished"] == 0


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

    def test_zero_goodput_is_not_read_as_rising(self) -> None:
        knee = find_knee([_closed(1, 0), _closed(2, 0), _closed(4, 0)], model="closed")

        assert knee == {"knee": None, "confirmed": False, "note": "no goodput at {'workers': 1}"}

    def test_one_step_has_no_comparison(self) -> None:
        assert find_knee([_closed(1, 100)], model="closed")["note"] == "one step, no comparison"

    def test_a_saturated_step_is_never_named(self) -> None:
        rows = [_closed(1, 100), _closed(2, 200), _closed(4, 200, saturated=True)]
        knee = find_knee(rows, model="closed")

        assert knee["knee"] is None
        assert knee["note"] == "client saturated at {'workers': 4}"

    def test_a_saturated_step_never_confirms_a_knee(self) -> None:
        rows = [_closed(1, 100), _closed(2, 200), _closed(4, 201), _closed(8, 201, saturated=True)]
        knee = find_knee(rows, model="closed")

        assert knee["knee"] == {"workers": 4}
        assert not knee["confirmed"]
        assert knee["note"] == "unconfirmed: the next step was client saturated at {'workers': 8}"

    def test_a_confirmed_knee_before_saturation_stands(self) -> None:
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
    def test_p99_doubling_confirmed_by_the_next_rate_is_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 20, 50.0), _open(40, 40, 90.0)]

        assert find_knee(rows, model="open") == {
            "knee": {"rate": 20},
            "confirmed": True,
            "note": None,
        }

    def test_a_p99_spike_the_next_rate_does_not_repeat_is_not_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 20, 50.0), _open(40, 40, 25.0)]

        assert find_knee(rows, model="open")["knee"] is None

    def test_p99_is_not_compared_below_the_minimum_sample(self) -> None:
        few = KNEE_MIN_SAMPLES - 1
        rows = [_open(10, 10, 20.0, sampled=few), _open(20, 20, 90.0), _open(40, 40, 95.0)]

        assert find_knee(rows, model="open")["knee"] is None

    def test_goodput_behind_the_offer_confirmed_by_the_next_rate_is_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 17, 21.0), _open(40, 30, 22.0)]

        assert find_knee(rows, model="open") == {
            "knee": {"rate": 20},
            "confirmed": True,
            "note": None,
        }

    def test_a_shortfall_at_the_first_rate_is_found(self) -> None:
        rows = [_open(100, 80, 20.0), _open(200, 80, 21.0)]

        assert find_knee(rows, model="open") == {
            "knee": {"rate": 100},
            "confirmed": True,
            "note": None,
        }

    def test_a_shortfall_at_the_only_rate_is_unconfirmed(self) -> None:
        knee = find_knee([_open(100, 80, 20.0)], model="open")

        assert knee == {"knee": {"rate": 100}, "confirmed": False, "note": "last step, unconfirmed"}

    def test_a_shortfall_the_next_rate_does_not_repeat_is_not_a_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 17, 21.0), _open(40, 40, 22.0)]

        assert find_knee(rows, model="open")["knee"] is None

    def test_a_shortfall_at_the_last_rate_is_unconfirmed(self) -> None:
        knee = find_knee([_open(10, 10, 20.0), _open(20, 17, 21.0)], model="open")

        assert knee == {"knee": {"rate": 20}, "confirmed": False, "note": "last step, unconfirmed"}

    def test_expected_conflicts_are_not_a_knee(self) -> None:
        rows = [_open(rate, rate * 0.94, 20.0, conflict=int(rate * 0.06)) for rate in (50, 100)]

        assert find_knee(rows, model="open")["knee"] is None

    def test_a_healthy_ramp_has_no_knee(self) -> None:
        rows = [_open(10, 10, 20.0), _open(20, 20, None), _open(40, 40, 25.0)]

        assert find_knee(rows, model="open")["knee"] is None


class TestMixChange:
    def test_a_step_whose_mix_changed_stops_the_search(self) -> None:
        retired = [{"principal": "ada", "scenario": "products.get"}]
        rows = [
            _closed(1, 100),
            _closed(2, 200),
            {**_closed(4, 201), "sample_exhausted": retired},
            _closed(8, 201),
        ]
        knee = find_knee(rows, model="closed")

        assert knee["knee"] is None
        assert knee["note"] == "mix changed at {'workers': 4}"

    def test_a_mix_change_never_confirms_a_knee(self) -> None:
        retired = [{"principal": "ada", "scenario": "products.get"}]
        rows = [_closed(1, 100), _closed(2, 101), {**_closed(4, 101), "sample_exhausted": retired}]
        knee = find_knee(rows, model="closed")

        assert knee["knee"] == {"workers": 2}
        assert knee["note"] == "unconfirmed: the next step was mix changed at {'workers': 4}"
