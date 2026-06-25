"""Distributed-lock fencing capability: spec.requires_fencing_token fails closed at resolve."""

from __future__ import annotations

import pytest

from forze.application.contracts.dlock import (
    AcquiredLock,
    DistributedLockCapabilities,
    DistributedLockCommandDepKey,
    DistributedLockSpec,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
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

    with pytest.raises(CoreException, match="fencing"):
        ctx.dlock.command(spec)


def test_no_requirement_allows_a_best_effort_backend() -> None:
    ctx = _ctx(_BestEffortLock())
    spec = DistributedLockSpec(name="lock")  # requires_fencing_token defaults False

    assert ctx.dlock.command(spec) is not None
