"""Distributed-lock fencing capabilities: the spec's requirements fail closed at resolve."""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import TracerProvider

from forze.application.contracts.dlock import (
    AcquiredLock,
    DistributedLockCapabilities,
    DistributedLockCommandDepKey,
    DistributedLockSpec,
)
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _FencingLock:
    """Reports fencing capability (is FencingAware)."""

    def capabilities(self) -> DistributedLockCapabilities:
        return DistributedLockCapabilities(fencing_tokens=True)

    async def acquire(self, key: str, owner: str) -> AcquiredLock:
        return AcquiredLock(key=key, owner=owner, token=1)

    async def release(self, key: str, owner: str) -> bool:
        return True

    async def reset(self, key: str, owner: str) -> bool:
        return True


class _BestEffortLock:
    """No capabilities() — not FencingAware; acquire returns token=None."""

    async def acquire(self, key: str, owner: str) -> AcquiredLock:
        return AcquiredLock(key=key, owner=owner, token=None)

    async def release(self, key: str, owner: str) -> bool:
        return True

    async def reset(self, key: str, owner: str) -> bool:
        return True


class _FencingDisabledLock:
    """FencingAware but reports fencing_tokens=False (e.g. a backend with tokens off)."""

    def capabilities(self) -> DistributedLockCapabilities:
        return DistributedLockCapabilities(fencing_tokens=False)

    async def acquire(self, key: str, owner: str) -> AcquiredLock:
        return AcquiredLock(key=key, owner=owner, token=None)

    async def release(self, key: str, owner: str) -> bool:
        return True

    async def reset(self, key: str, owner: str) -> bool:
        return True


class _AdvancingLock(_FencingLock):
    """Reports fence advance and implements it."""

    def capabilities(self) -> DistributedLockCapabilities:
        return DistributedLockCapabilities(fencing_tokens=True, fence_advance=True)

    async def advance_fence(self, key: str, *, past: int) -> int:
        return past


class _ClaimsAdvanceWithoutMethodLock(_FencingLock):
    """Reports fence advance but has no ``advance_fence`` — a backend that lies."""

    def capabilities(self) -> DistributedLockCapabilities:
        return DistributedLockCapabilities(fencing_tokens=True, fence_advance=True)


def _ctx(port: object):
    return context_from_deps(
        Deps.plain({DistributedLockCommandDepKey: lambda _c, _s: port})
    )


def test_requires_fencing_resolves_against_a_fencing_backend() -> None:
    ctx = _ctx(_FencingLock())
    spec = DistributedLockSpec(name="lock", requires_fencing_token=True)
    assert ctx.dlock.command(spec) is not None


def test_requires_fencing_fails_closed_against_best_effort_backend() -> None:
    ctx = _ctx(_BestEffortLock())
    spec = DistributedLockSpec(name="lock", requires_fencing_token=True)

    with pytest.raises(CoreException, match="fencing") as ei:
        ctx.dlock.command(spec)
    assert ei.value.code == "dlock.fencing_unsupported"


def test_requires_fencing_fails_closed_when_capability_reports_disabled() -> None:
    # FencingAware but fencing_tokens=False must also be rejected, not just non-FencingAware.
    ctx = _ctx(_FencingDisabledLock())
    spec = DistributedLockSpec(name="lock", requires_fencing_token=True)

    with pytest.raises(CoreException, match="fencing") as ei:
        ctx.dlock.command(spec)
    assert ei.value.code == "dlock.fencing_unsupported"


def test_no_requirement_allows_a_best_effort_backend() -> None:
    ctx = _ctx(_BestEffortLock())
    spec = DistributedLockSpec(name="lock")  # requires_fencing_token defaults False

    assert ctx.dlock.command(spec) is not None


def test_a_read_only_operation_gets_a_lock_that_refuses_on_use() -> None:
    ctx = _ctx(_FencingLock())
    spec = DistributedLockSpec(name="lock", requires_fencing_token=True)

    with ctx.inv_ctx.bind_read_only():
        port = ctx.dlock.command(spec)

        with pytest.raises(CoreException, match="read-only"):
            _ = port.acquire

    # It names its port, not the context it holds (which carries the whole dependency graph).
    assert "inv_ctx" not in repr(port) and len(repr(port)) < 500


def test_a_read_only_operation_is_refused_a_lock_that_cannot_fence() -> None:
    # What a read-only operation holds is the real port wherever it is used to lock, so the
    # fencing requirement is judged on the backend all the same.
    ctx = _ctx(_BestEffortLock())
    spec = DistributedLockSpec(name="lock", requires_fencing_token=True)

    with ctx.inv_ctx.bind_read_only(), pytest.raises(CoreException, match="fencing") as ei:
        ctx.dlock.command(spec)

    assert ei.value.code == "dlock.fencing_unsupported"


def test_requires_fence_advance_resolves_against_an_advancing_backend() -> None:
    ctx = _ctx(_AdvancingLock())
    spec = DistributedLockSpec(name="lock", requires_fence_advance=True)
    assert ctx.dlock.command(spec) is not None


@pytest.mark.parametrize(
    "backend",
    [_FencingLock(), _BestEffortLock(), _ClaimsAdvanceWithoutMethodLock()],
    ids=["flag-false", "not-fencing-aware", "flag-without-method"],
)
def test_requires_fence_advance_fails_closed(backend: object) -> None:
    ctx = _ctx(backend)
    spec = DistributedLockSpec(name="lock", requires_fence_advance=True)

    with pytest.raises(CoreException, match="fence advance") as ei:
        ctx.dlock.command(spec)
    assert ei.value.code == "dlock.fence_advance_unsupported"


def test_a_read_only_operation_is_refused_a_lock_that_cannot_advance() -> None:
    ctx = _ctx(_FencingLock())
    spec = DistributedLockSpec(name="lock", requires_fence_advance=True)

    with ctx.inv_ctx.bind_read_only(), pytest.raises(CoreException, match="fence advance") as ei:
        ctx.dlock.command(spec)

    assert ei.value.code == "dlock.fence_advance_unsupported"


@pytest.mark.parametrize(
    "flags",
    [{"requires_fencing_token": True}, {"requires_fence_advance": True}],
    ids=["fencing", "fence-advance"],
)
def test_a_traced_backend_still_meets_its_requirements(flags: dict[str, bool]) -> None:
    # OTel port spans wrap every resolved port in a proxy that forwards methods through
    # __getattr__; the capability check must see through it, not refuse a capable backend.
    registry = DepsRegistry.from_modules(MockDepsModule()).with_otel_port_spans(
        tracer=TracerProvider().get_tracer("test")
    )
    ctx = ExecutionContext(deps=registry.freeze().resolve())

    assert ctx.dlock.command(DistributedLockSpec(name="lock", **flags)) is not None
