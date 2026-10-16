"""The in-memory revocation states and principal epochs against the shared batteries."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    PrincipalEpochDepKey,
    RevocationStatesDepKey,
    RevocationStatesSpec,
)
from forze_mock import MockDepsModule
from forze_mock.adapters.revocation import MockPrincipalEpochStore, MockRevocationStates
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules
from tests.support.revocation_states_conformance import (
    EPOCH_BATTERY,
    STATES_BATTERY,
    EpochCheck,
    EpochHarness,
    StatesCheck,
    StatesHarness,
)

# ----------------------- #

pytestmark = pytest.mark.asyncio


@attrs.define
class _Clock:
    now: float = 1_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def states_harness() -> StatesHarness:
    clock = _Clock()

    async def wait(delta: timedelta) -> None:
        clock.now += delta.total_seconds()

    return StatesHarness(
        backend="mock",
        states=MockRevocationStates(state=MockState(), namespace="battery", clock=clock),
        wait=wait,
    )


@pytest.fixture(params=["journal", "strict"])
def epoch_harness(request: pytest.FixtureRequest) -> EpochHarness:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, transactions=request.param))

    return EpochHarness(
        backend=f"mock ({request.param})",
        epochs=MockPrincipalEpochStore(state=state, in_transaction=lambda: ctx.tx_ctx.depth() > 0),
        # Either transaction manager reverts a raise on rollback, as a database would.
        transaction=lambda: ctx.tx_ctx.scope("default"),
        locks=False,
    )


@pytest.mark.conformance(plane="revocation_states", engine="mock")
@pytest.mark.parametrize("check", STATES_BATTERY, ids=lambda check: check.__name__)
async def test_revocation_states_battery(check: StatesCheck, states_harness: StatesHarness) -> None:
    await check(states_harness)


@pytest.mark.conformance(plane="principal_epoch", engine="mock")
@pytest.mark.parametrize("check", EPOCH_BATTERY, ids=lambda check: check.__name__)
async def test_principal_epoch_battery(check: EpochCheck, epoch_harness: EpochHarness) -> None:
    await check(epoch_harness)


async def test_the_module_wires_both_stores() -> None:
    ctx = context_from_modules(MockDepsModule())
    spec = RevocationStatesSpec(name="authn-revocations")

    states = ctx.deps.resolve_configurable(ctx, RevocationStatesDepKey, spec, route=spec.name)
    epochs = ctx.deps.provide(PrincipalEpochDepKey)(ctx)

    assert isinstance(states, MockRevocationStates)
    assert isinstance(epochs, MockPrincipalEpochStore)

    principal = uuid4()

    async with ctx.tx_ctx.scope("default"):
        assert await epochs.advance(principal) == 1

    assert await epochs.current(principal) == 1
