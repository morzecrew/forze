"""The sandbox seam under simulation: the mock answers, the real adapter fails loud.

# covers: forze_mock.adapters.sandbox (deterministic answers inside a simulation)
# covers: forze.application.integrations.sandbox.process (the real adapter is
#         out-of-boundary by definition and the loop says so)

RFC 0021 puts the cut here on purpose: out-of-process execution is real, off-loop,
wall-clock work, which is exactly what the simulated loop refuses. The refusal is not an
obstacle to route around — it is the specification. An author who wired the real adapter
under simulation gets told, in the same breath, that they meant to bind the mock.
"""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from datetime import timedelta

import pytest

from forze.application.contracts.sandbox import (
    CapturedStream,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
)
from forze.application.contracts.storage import StorageSpec
from forze.application.integrations.sandbox import SubprocessSandbox, SubprocessSandboxConfig
from forze.testing import context_from_modules
from forze_dst.runtime import RealIOForbidden, run_simulation
from forze_mock import MockDepsModule, MockSandboxRegistry, MockState

pytestmark = pytest.mark.unit

# ----------------------- #

_SPEC = SandboxSpec(name="jobs", provenance="trusted")


def _request() -> SandboxRequest:
    return SandboxRequest(command=("python3", "-c", "print(1)"))


# ----------------------- #


class TestTheMockIsSimulable:
    def test_a_scripted_run_replays_identically_across_seeds(self) -> None:
        # Determinism is the whole reason the seam exists: the same scenario twice must
        # produce the same story, which real out-of-process work never does.
        def scenario_factory() -> object:
            async def scenario() -> tuple[str, int | None]:
                registry = MockSandboxRegistry().on(
                    "jobs",
                    lambda _r: SandboxResult(
                        outcome="exited", exit_code=0, stdout=CapturedStream(text="1\n")
                    ),
                )
                ctx = context_from_modules(MockDepsModule(state=MockState(), sandboxes=registry))
                result = await ctx.sandbox.run(_SPEC).run(_request())

                return result.outcome, result.exit_code

            return scenario

        assert run_simulation(scenario_factory(), seed=11) == ("exited", 0)
        assert run_simulation(scenario_factory(), seed=99) == ("exited", 0)

    @pytest.mark.parametrize("outcome", ["killed_oom", "killed_timeout", "spawn_failed"])
    def test_the_hard_outcomes_are_drivable_under_simulation(self, outcome: str) -> None:
        # The failures a real sandbox produces once a quarter, on demand and in order.
        async def scenario() -> str:
            registry = MockSandboxRegistry().on(
                "jobs",
                lambda _r: SandboxResult(outcome=outcome),  # type: ignore[arg-type]
            )
            ctx = context_from_modules(MockDepsModule(state=MockState(), sandboxes=registry))

            return (await ctx.sandbox.run(_SPEC).run(_request())).outcome

        assert run_simulation(scenario) == outcome


class TestTheRealAdapterIsOutOfBounds:
    def test_the_adapter_is_refused_before_it_can_spawn_anything(self) -> None:
        # Not a limitation to work around: the loop refusing this is what tells an author
        # their simulation was about to include real wall-clock work. It lands one step
        # earlier than "spawning a subprocess" — the workspace is created off the loop, so
        # the thread offload is refused first — and the fail-loud is the same either way.
        async def scenario() -> None:
            ctx = context_from_modules(MockDepsModule(state=MockState()))
            sandbox = SubprocessSandbox(
                spec=_SPEC,
                config=SubprocessSandboxConfig(
                    provenance="trusted",
                    wall_clock_ceiling=timedelta(seconds=1),
                    max_output_bytes=1024,
                    acknowledge_network_egress=True,
                    storage=StorageSpec(name="files"),
                ),
                ctx=ctx,
            )

            await sandbox.run(_request())

        with pytest.raises(RealIOForbidden) as caught:
            run_simulation(scenario)

        assert "use an in-memory mock adapter" in str(caught.value)

    def test_the_streamed_call_is_refused_on_the_same_ground(self) -> None:
        # The seam is the port, not one of its two methods. A streamed run is the same
        # off-loop wall-clock work, so a simulation reaching for it fails loud in the same
        # place — otherwise `run_stream` would be the way round the cut.
        async def scenario() -> None:
            ctx = context_from_modules(MockDepsModule(state=MockState()))
            sandbox = SubprocessSandbox(
                spec=_SPEC,
                config=SubprocessSandboxConfig(
                    provenance="trusted",
                    wall_clock_ceiling=timedelta(seconds=1),
                    max_output_bytes=1024,
                    acknowledge_network_egress=True,
                    storage=StorageSpec(name="files"),
                ),
                ctx=ctx,
            )

            async with aclosing(sandbox.run_stream(_request())) as events:
                async for _ in events:
                    pass

        with pytest.raises(RealIOForbidden) as caught:
            run_simulation(scenario)

        assert "use an in-memory mock adapter" in str(caught.value)

    def test_the_loop_refuses_the_spawn_itself(self) -> None:
        # The guard the adapter's own refusal rests on, pinned separately: were the
        # workspace ever prepared on the loop, this is what would stop the run.
        async def scenario() -> None:
            await asyncio.create_subprocess_exec("python3", "-c", "print(1)")

        with pytest.raises(RealIOForbidden) as caught:
            run_simulation(scenario)

        assert "subprocess" in str(caught.value)
