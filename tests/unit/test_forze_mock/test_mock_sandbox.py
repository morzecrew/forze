"""The in-memory sandbox: the seam a simulation cuts.

# covers: forze_mock.adapters.sandbox (registered answers, the unprogrammed refusal,
#         borrowed capabilities, streamed replay, recorded requests)

The mock runs nothing, and that is the property under test. Real out-of-process work is
wall-clock work off the loop; a scripted answer is what lets a simulation reach
``killed_oom`` on purpose rather than on a bad day.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.sandbox import (
    CapturedStream,
    ProgramPayload,
    ResourceRequest,
    SandboxCapabilities,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockSandboxRegistry, MockState

pytestmark = pytest.mark.unit

# ----------------------- #

_SPEC = SandboxSpec(name="jobs", provenance="trusted")


def _ctx(registry: MockSandboxRegistry | None = None) -> Any:
    return context_from_modules(MockDepsModule(state=MockState(), sandboxes=registry))


def _request(**kwargs: Any) -> SandboxRequest:
    return SandboxRequest(command=("echo", "hi"), **kwargs)


def _ok(_request: SandboxRequest) -> SandboxResult:
    return SandboxResult(outcome="exited", exit_code=0, stdout=CapturedStream(text="hi\n"))


# ----------------------- #


class TestScriptedOutcomes:
    @pytest.mark.asyncio
    async def test_a_registered_answer_is_returned(self) -> None:
        registry = MockSandboxRegistry().on("jobs", _ok)

        result = await _ctx(registry).sandbox.run(_SPEC).run(_request())

        assert result.succeeded
        assert result.stdout.text == "hi\n"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "outcome",
        ["killed_oom", "killed_timeout", "killed_cancel", "killed_resource", "spawn_failed"],
    )
    async def test_the_hard_cases_are_reachable_on_demand(self, outcome: Any) -> None:
        # The reason the plane is simulable at all: a real child OOMs when it feels like
        # it, and a test needs that path on the first call, every time.
        registry = MockSandboxRegistry().on("jobs", lambda _r: SandboxResult(outcome=outcome))

        result = await _ctx(registry).sandbox.run(_SPEC).run(_request())

        assert result.outcome == outcome
        assert not result.succeeded

    @pytest.mark.asyncio
    async def test_an_answer_may_vary_by_call_without_reading_a_clock(self) -> None:
        calls: list[int] = []

        def flaky(_request: SandboxRequest) -> SandboxResult:
            calls.append(1)

            return SandboxResult(outcome="exited", exit_code=0 if len(calls) > 1 else 1)

        port = _ctx(MockSandboxRegistry().on("jobs", flaky)).sandbox.run(_SPEC)

        assert (await port.run(_request())).exit_code == 1
        assert (await port.run(_request())).exit_code == 0

    @pytest.mark.asyncio
    async def test_an_unprogrammed_route_refuses_rather_than_inventing_an_answer(self) -> None:
        with pytest.raises(CoreException) as caught:
            await _ctx().sandbox.run(_SPEC).run(_request())

        assert caught.value.code == "mock.sandbox.unprogrammed"


class TestWhatTheRouteSaw:
    @pytest.mark.asyncio
    async def test_requests_are_recorded_in_order(self) -> None:
        registry = MockSandboxRegistry().on("jobs", _ok)
        port = _ctx(registry).sandbox.run(_SPEC)

        await port.run(_request(timeout=timedelta(seconds=1)))
        await port.run(
            SandboxRequest(program=ProgramPayload(interpreter=("python3",), source="pass"))
        )

        seen = registry.requests_for("jobs")

        assert len(seen) == 2
        assert seen[0].command == ("echo", "hi")
        assert seen[1].program is not None


class TestBorrowedCapabilities:
    @pytest.mark.asyncio
    async def test_the_default_surface_is_the_full_one(self) -> None:
        # A mock narrower than production makes gated code fail here and only here; a mock
        # that advertises everything lets a route stand in for any backend.
        port = _ctx(MockSandboxRegistry().on("jobs", _ok)).sandbox.run(_SPEC)

        assert port.sandbox_capabilities.isolation == "vm"
        assert port.sandbox_capabilities.supports_stream

    @pytest.mark.asyncio
    async def test_a_route_can_stand_in_for_a_narrower_backend(self) -> None:
        registry = MockSandboxRegistry().on(
            "jobs",
            _ok,
            capabilities=SandboxCapabilities(isolation="none", supports_stream=False),
        )
        port = _ctx(registry).sandbox.run(_SPEC)

        with pytest.raises(CoreException) as caught:
            async for _ in port.run_stream(_request()):
                pass

        assert caught.value.code == "sandbox_feature_unsupported"

    @pytest.mark.asyncio
    async def test_re_registering_without_capabilities_drops_the_narrow_declaration(
        self,
    ) -> None:
        # A stale declaration lingering after a re-registration would make a later test
        # refuse for a reason nobody wrote down.
        registry = MockSandboxRegistry().on(
            "jobs", _ok, capabilities=SandboxCapabilities(supports_stream=False)
        )
        registry.on("jobs", _ok)

        assert registry.capabilities_for("jobs") is None


    @pytest.mark.asyncio
    async def test_a_narrow_route_refuses_a_ceiling_its_backend_would_not_impose(self) -> None:
        # The differential property: a request that would run uncapped in production is
        # refused here, instead of passing against the oracle and only failing deployed.
        registry = MockSandboxRegistry().on(
            "jobs", _ok, capabilities=SandboxCapabilities(enforces_memory=False)
        )

        with pytest.raises(CoreException) as caught:
            await _ctx(registry).sandbox.run(_SPEC).run(
                _request(resources=ResourceRequest(memory_bytes=1024))
            )

        assert caught.value.code == "sandbox_feature_unsupported"

    @pytest.mark.asyncio
    async def test_the_full_surface_accepts_what_it_claims_to_enforce(self) -> None:
        result = await _ctx(MockSandboxRegistry().on("jobs", _ok)).sandbox.run(_SPEC).run(
            _request(resources=ResourceRequest(memory_bytes=1024))
        )

        assert result.succeeded


class TestStreamedReplay:
    @pytest.mark.asyncio
    async def test_output_arrives_before_the_result(self) -> None:
        registry = MockSandboxRegistry().on(
            "jobs",
            lambda _r: SandboxResult(
                outcome="exited",
                exit_code=0,
                stdout=CapturedStream(text="out"),
                stderr=CapturedStream(text="err"),
            ),
        )

        events = [event async for event in _ctx(registry).sandbox.run(_SPEC).run_stream(_request())]

        assert [event.kind for event in events] == ["stdout", "stderr", "result"]
        assert events[-1].result is not None
        assert events[-1].result.succeeded


class TestThePlaneIsCommandOnly:
    @pytest.mark.asyncio
    async def test_a_read_only_operation_cannot_acquire_a_sandbox(self) -> None:
        # Spawning a process is an effect on the world whatever the child then does, so a
        # QUERY handler cannot get one at all.
        ctx = _ctx(MockSandboxRegistry().on("jobs", _ok))

        with ctx.inv_ctx.bind_read_only():
            with pytest.raises(CoreException) as caught:
                ctx.sandbox.run(_SPEC)

        assert caught.value.kind is ExceptionKind.PRECONDITION
