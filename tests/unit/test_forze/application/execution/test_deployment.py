"""Deployment posture, drain-state exposure, and singleton lifecycle steps."""

from __future__ import annotations

from typing import Any

import attrs
import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import DeploymentProfile, build_runtime
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.lifecycle import singleton_lifecycle_step

# ----------------------- #


def _noop_step(step_id: str = "s", **kw: Any) -> LifecycleStep:
    async def _start(_ctx: Any) -> None:
        return None

    return LifecycleStep(id=step_id, startup=_start, **kw)


@attrs.define(slots=True)
class _FakeLock:
    """Single-holder lock double implementing the dlock command port shape."""

    holder: str | None = None
    acquires: int = 0
    releases: int = 0

    async def acquire(self, key: str, owner: str) -> Any:
        self.acquires += 1

        if self.holder is not None:
            return None

        self.holder = owner

        return attrs.make_class("L", ["key", "owner", "token"])(key, owner, 1)

    async def release(self, key: str, owner: str) -> bool:
        self.releases += 1
        self.holder = None
        return True

    async def reset(self, key: str, owner: str) -> bool:
        return True


@attrs.define(slots=True)
class _FakeDlockDeps:
    """``ctx.dlock`` double: hands back the same lock for any spec."""

    lock: _FakeLock

    def command(self, _spec: DistributedLockSpec) -> _FakeLock:
        return self.lock


@attrs.define(slots=True)
class _FakeCtx:
    """Minimal execution-context double exposing only ``dlock``."""

    dlock: _FakeDlockDeps


def _ctx_for(lock: _FakeLock) -> Any:
    return _FakeCtx(dlock=_FakeDlockDeps(lock=lock))


# ----------------------- #


class TestDeploymentProfile:
    def test_fleet_rejects_unguarded_mutating_step(self) -> None:
        plan = LifecyclePlan.from_steps(
            _noop_step("migrate", mutates_shared_state=True)
        ).freeze()

        with pytest.raises(CoreException) as ei:
            ExecutionRuntime(lifecycle=plan, deployment=DeploymentProfile.FLEET)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert "migrate" in ei.value.summary

    def test_fleet_accepts_guarded_mutating_step(self) -> None:
        plan = LifecyclePlan.from_steps(
            _noop_step("migrate", mutates_shared_state=True, singleton_guarded=True)
        ).freeze()

        ExecutionRuntime(lifecycle=plan, deployment=DeploymentProfile.FLEET)

    def test_single_process_skips_validation(self) -> None:
        plan = LifecyclePlan.from_steps(
            _noop_step("migrate", mutates_shared_state=True)
        ).freeze()

        ExecutionRuntime(lifecycle=plan)

    def test_build_runtime_passes_deployment(self) -> None:
        rt = build_runtime(deployment=DeploymentProfile.FLEET)

        assert rt.deployment is DeploymentProfile.FLEET


class TestDrainStateExposure:
    def test_not_ready_outside_scope(self) -> None:
        rt = ExecutionRuntime()

        assert rt.ready is False
        assert rt.draining is False

    @pytest.mark.asyncio
    async def test_ready_inside_scope_draining_observable(self) -> None:
        rt = ExecutionRuntime()

        async with rt.scope():
            assert rt.ready is True
            assert rt.draining is False

            await rt.get_context().drain_gate.drain(0.0)

            assert rt.draining is True
            assert rt.ready is False

        assert rt.ready is False


class TestSingletonLifecycleStep:
    @pytest.mark.asyncio
    async def test_leader_runs_and_releases(self) -> None:
        ran: list[str] = []

        async def _start(_ctx: Any) -> None:
            ran.append("start")

        async def _shut(_ctx: Any) -> None:
            ran.append("shut")

        lock = _FakeLock()
        step = singleton_lifecycle_step(
            LifecycleStep(id="seed", startup=_start, shutdown=_shut),
            spec=DistributedLockSpec(name="seed"),
            owner="replica-a",
        )

        assert step.mutates_shared_state is True
        assert step.singleton_guarded is True

        ctx = _ctx_for(lock)
        await step.startup(ctx)
        await step.shutdown(ctx)

        assert ran == ["start", "shut"]
        assert lock.releases == 1

    @pytest.mark.asyncio
    async def test_non_leader_skips_startup_and_shutdown(self) -> None:
        ran: list[str] = []

        async def _start(_ctx: Any) -> None:
            ran.append("start")

        lock = _FakeLock(holder="replica-a")
        step = singleton_lifecycle_step(
            LifecycleStep(id="seed", startup=_start),
            spec=DistributedLockSpec(name="seed"),
            owner="replica-b",
        )

        ctx = _ctx_for(lock)
        await step.startup(ctx)
        await step.shutdown(ctx)

        assert ran == []
        assert lock.releases == 0

    @pytest.mark.asyncio
    async def test_lock_released_when_step_fails(self) -> None:
        async def _boom(_ctx: Any) -> None:
            raise RuntimeError("boom")

        lock = _FakeLock()
        step = singleton_lifecycle_step(
            LifecycleStep(id="seed", startup=_boom),
            spec=DistributedLockSpec(name="seed"),
            owner="replica-a",
        )

        with pytest.raises(RuntimeError):
            await step.startup(_ctx_for(lock))

        assert lock.releases == 1
