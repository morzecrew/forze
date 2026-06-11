"""Tests for :mod:`forze.application.execution.graph_run`."""

import pytest

from forze.application.contracts.execution import ExecutionGraph
from forze.application.execution.graph_run import (
    run_graph_waves_forward,
    run_graph_waves_reverse,
    run_wave_forward,
    run_wave_reverse,
)
class TestRunWaveForward:
    @pytest.mark.asyncio
    async def test_empty_wave_is_noop(self) -> None:
        calls: list[int] = []

        async def run_step(step: int) -> None:
            calls.append(step)

        await run_wave_forward((), {}, run_step, concurrent=False)
        assert calls == []

    @pytest.mark.asyncio
    async def test_sequential_order(self) -> None:
        steps = {"a": 1, "b": 2}
        order: list[int] = []

        async def run_step(step: int) -> None:
            order.append(step)

        await run_wave_forward(
            ("a", "b"),
            steps,
            run_step,
            concurrent=False,
        )
        assert order == [1, 2]

    @pytest.mark.asyncio
    async def test_concurrent_propagates_exception(self) -> None:
        async def run_step(_step: int) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await run_wave_forward(("x",), {"x": 1}, run_step, concurrent=True)

    @pytest.mark.asyncio
    async def test_concurrent_single_failure_raises_directly(self) -> None:
        seen: list[int] = []

        async def run_step(step: int) -> None:
            if step == 1:
                raise ValueError("boom")

            seen.append(step)

        with pytest.raises(ValueError, match="boom"):
            await run_wave_forward(
                ("a", "b"),
                {"a": 1, "b": 2},
                run_step,
                concurrent=True,
            )

        assert seen == [2]

    @pytest.mark.asyncio
    async def test_concurrent_multiple_failures_raise_exception_group(self) -> None:
        async def run_step(step: int) -> None:
            raise ValueError(f"boom-{step}")

        with pytest.raises(ExceptionGroup) as exc_info:
            await run_wave_forward(
                ("a", "b"),
                {"a": 1, "b": 2},
                run_step,
                concurrent=True,
            )

        messages = sorted(str(e) for e in exc_info.value.exceptions)
        assert messages == ["boom-1", "boom-2"]


class TestRunWaveReverse:
    @pytest.mark.asyncio
    async def test_reverse_order(self) -> None:
        steps = {"a": 1, "b": 2}
        order: list[int] = []

        async def run_step(step: int) -> None:
            order.append(step)

        await run_wave_reverse(
            ("a", "b"),
            steps,
            run_step,
            concurrent=False,
        )
        assert order == [2, 1]


class TestRunGraphWaves:
    @pytest.mark.asyncio
    async def test_forward_graph_runs_waves_in_order(self) -> None:
        graph = ExecutionGraph(
            steps={"a": "A", "b": "B"},
            waves=(("a",), ("b",)),
        )
        seen: list[str] = []

        async def run_step(label: str) -> None:
            seen.append(label)

        await run_graph_waves_forward(graph, run_step, concurrent=False)
        assert seen == ["A", "B"]

    @pytest.mark.asyncio
    async def test_reverse_graph_runs_waves_backwards(self) -> None:
        graph = ExecutionGraph(
            steps={"a": "A", "b": "B"},
            waves=(("a",), ("b",)),
        )
        seen: list[str] = []

        async def run_step(label: str) -> None:
            seen.append(label)

        await run_graph_waves_reverse(graph, run_step, concurrent=False)
        assert seen == ["B", "A"]

    @pytest.mark.asyncio
    async def test_forward_concurrent_runs_all_steps_in_wave(self) -> None:
        steps = {"a": 1, "b": 2}
        seen: list[int] = []

        async def run_step(step: int) -> None:
            seen.append(step)

        await run_wave_forward(("a", "b"), steps, run_step, concurrent=True)
        assert sorted(seen) == [1, 2]

    @pytest.mark.asyncio
    async def test_reverse_empty_wave_is_noop(self) -> None:
        calls: list[int] = []

        async def run_step(step: int) -> None:
            calls.append(step)

        await run_wave_reverse((), {}, run_step, concurrent=False)
        assert calls == []

    @pytest.mark.asyncio
    async def test_reverse_concurrent_propagates_exception(self) -> None:
        async def run_step(_step: int) -> None:
            raise RuntimeError("reverse")

        with pytest.raises(RuntimeError, match="reverse"):
            await run_wave_reverse(("x",), {"x": 1}, run_step, concurrent=True)

    @pytest.mark.asyncio
    async def test_reverse_concurrent_multiple_failures_raise_exception_group(
        self,
    ) -> None:
        async def run_step(step: int) -> None:
            raise RuntimeError(f"reverse-{step}")

        with pytest.raises(ExceptionGroup) as exc_info:
            await run_wave_reverse(
                ("a", "b"),
                {"a": 1, "b": 2},
                run_step,
                concurrent=True,
            )

        messages = sorted(str(e) for e in exc_info.value.exceptions)
        assert messages == ["reverse-1", "reverse-2"]

    @pytest.mark.asyncio
    async def test_graph_forward_concurrent(self) -> None:
        graph = ExecutionGraph(
            steps={"a": 1, "b": 2},
            waves=(("a", "b"),),
        )
        seen: list[int] = []

        async def run_step(step: int) -> None:
            seen.append(step)

        await run_graph_waves_forward(graph, run_step, concurrent=True)
        assert sorted(seen) == [1, 2]
