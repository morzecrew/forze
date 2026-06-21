"""CPU-offload (run_cpu) under Deterministic Simulation Testing.

A raw ``loop.run_in_executor`` raises ``RealIOForbidden`` under simulation
(see test_simulation.py); ``run_cpu`` instead runs inline, so handlers that
offload CPU work stay deterministic and simulatable.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import bind_deadline, monotonic, run_cpu, run_cpu_map
from forze_dst.runtime import run_simulation


def _parse(x: int) -> int:
    return x * 2


# ----------------------- #


class TestRunCpuUnderSimulation:
    def test_run_cpu_runs_inline_not_forbidden(self) -> None:
        # The headline: a run_cpu offload does NOT trip RealIOForbidden.
        async def scenario() -> int:
            return await run_cpu(_parse, 21)

        assert run_simulation(scenario) == 42

    def test_deterministic_across_runs(self) -> None:
        async def scenario() -> list[int]:
            return await run_cpu_map(range(6), _parse, chunk_size=2)

        first = run_simulation(scenario, seed=7)
        second = run_simulation(scenario, seed=7)
        assert first == second == [0, 2, 4, 6, 8, 10]

    def test_cost_model_advances_virtual_time(self) -> None:
        seen: dict[str, float] = {}

        async def scenario() -> None:
            seen["before"] = monotonic()
            await run_cpu(_parse, 1)
            seen["after"] = monotonic()

        run_simulation(scenario, cpu_cost=lambda _label: 2.5)
        assert seen["after"] - seen["before"] == pytest.approx(2.5)

    def test_cost_model_keyed_by_call_site(self) -> None:
        def heavy() -> None:
            return None

        def light() -> None:
            return None

        async def scenario() -> tuple[float, float]:
            t0 = monotonic()
            await run_cpu(heavy)
            t1 = monotonic()
            await run_cpu(light)
            t2 = monotonic()
            return (t1 - t0, t2 - t1)

        def cost(label: str | None) -> float:
            if label and label.endswith("heavy"):
                return 4.0
            if label and label.endswith("light"):
                return 1.0
            return 0.0

        heavy_cost, light_cost = run_simulation(scenario, cpu_cost=cost)
        assert heavy_cost == pytest.approx(4.0)
        assert light_cost == pytest.approx(1.0)

    def test_deadline_fires_during_modeled_slow_offload(self) -> None:
        # A 5s modeled parse under a 1s deadline must raise deterministically.
        async def scenario() -> str:
            with bind_deadline(1.0):
                return await run_cpu(_parse, 1)

        with pytest.raises(CoreException) as ei:
            run_simulation(scenario, cpu_cost=lambda _label: 5.0)

        assert ei.value.code == "cpu_offload_deadline"

    def test_offload_within_deadline_succeeds(self) -> None:
        async def scenario() -> int:
            with bind_deadline(10.0):
                return await run_cpu(_parse, 5)

        assert run_simulation(scenario, cpu_cost=lambda _label: 2.0) == 10
