"""The reservation-TTL example: DST fast-forwards virtual time to find a time bug.

Backs ``examples/recipes/dst_reservation_ttl/app.py``. The confirm operation takes longer
(virtual time) than the reservation's TTL, so it confirms an expired reservation. DST
fast-forwards the latency instantly and the test-side invariant flags it — no real waiting.
"""

from __future__ import annotations

from typer.testing import CliRunner

from forze_cli.app import app
from forze_dst import SimulationConfig, Strategy

from examples.recipes.dst_reservation_ttl.app import AUTH_LATENCY, TTL, simulation

# ----------------------- #

runner = CliRunner()

_TARGET = "examples.recipes.dst_reservation_ttl.app:simulation"


def test_latency_exceeds_ttl() -> None:
    # The premise of the bug: the slow work outlasts the hold.
    assert AUTH_LATENCY > TTL


def test_dst_finds_the_expired_confirmation() -> None:
    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=3, concurrency=2, seeds=range(3)
        ),
        scenario=simulation.derive_scenario(),
    )
    assert report is not None
    assert "confirmed after it expired" in report.violations[0].message
    # A single confirm already triggers it — the bug is time, not concurrency.
    assert [op for op, _ in report.workload] == ["confirm"]


def test_clock_fast_forwarded_past_the_ttl() -> None:
    report = simulation.run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=3, concurrency=2, seeds=range(3)
        ),
        scenario=simulation.derive_scenario(),
    )
    assert report is not None
    # The violation is stamped at virtual time past the TTL (the latency elapsed), proving
    # the clock advanced — in real wall-clock the run took milliseconds.
    breach = report.violations[0].events[0]
    assert breach.at >= TTL.total_seconds()


def test_handler_has_no_artificial_sleep() -> None:
    # The time passes because the *simulated* payment downstream is slow (configured
    # test-side), not because the handler sleeps. The handler is ordinary forze code.
    import inspect

    from examples.recipes.dst_reservation_ttl import app as example

    source = inspect.getsource(example._Confirm)
    assert "sleep(" not in source
    assert "asyncio" not in inspect.getsource(example)


def test_via_cli() -> None:
    result = runner.invoke(app, ["dst", "run", _TARGET])
    assert result.exit_code == 1
    assert "confirmed after it expired" in result.stdout
