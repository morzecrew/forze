"""Mock counterparty-rotated credential store — the battery, plus mock-only wiring.

# covers: RotatingCredentialStorePort.get
# covers: RotatingCredentialStorePort.refresh
# covers: RotatingCredentialStorePort.put
# covers: RotatingCredentialStorePort.burn
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.secrets import (
    ExchangedCredential,
    RotatingCredentialsDepKey,
    SecretRef,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockRotatingCredentialStore, MockState
from tests.support.rotating_credentials import (
    EXCHANGE_TIMEOUT,
    REF,
    ROTATING_STORE_BATTERY,
    Check,
    FakeCounterparty,
    RotatingStoreHarness,
    TenantCell,
)

# ----------------------- #


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> RotatingStoreHarness:
    counterparty = FakeCounterparty()
    tenant = TenantCell()
    store = MockRotatingCredentialStore(
        state=MockState(),
        exchanger=counterparty,
        exchange_timeout=EXCHANGE_TIMEOUT,
        tenant_provider=tenant,
    )

    @contextlib.asynccontextmanager
    async def break_persist() -> AsyncIterator[None]:
        # The mock cannot lose a process, so its faithful equivalent is a write that
        # raises: the exchange has completed and the replacement cannot be stored.
        def boom(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("mock persist broken")

        monkeypatch.setattr(MockRotatingCredentialStore, "_persist", boom)

        try:
            yield

        finally:
            monkeypatch.undo()

    return RotatingStoreHarness(
        store=store,
        counterparty=counterparty,
        tenant=tenant,
        break_persist=break_persist,
    )


# ....................... #


@pytest.mark.parametrize("check", ROTATING_STORE_BATTERY, ids=lambda check: check.__name__)
async def test_rotating_store_battery(check: Check, harness: RotatingStoreHarness) -> None:
    await check(harness)


# ....................... #


class TestMockWiring:
    async def test_store_is_registered_only_when_an_exchanger_is_given(self) -> None:
        """No default exchanger exists, so the key must stay unregistered rather than
        resolving to a store that can never rotate anything."""

        bare = context_from_modules(MockDepsModule(state=MockState()))

        with pytest.raises(CoreException):
            bare.deps.provide(RotatingCredentialsDepKey)

        wired = context_from_modules(
            MockDepsModule(state=MockState(), rotating_credentials=FakeCounterparty())
        )

        assert isinstance(
            wired.deps.provide(RotatingCredentialsDepKey), MockRotatingCredentialStore
        )

    async def test_documents_live_in_the_shared_state(self) -> None:
        """The store writes into the same ``MockState`` the rest of the plane shares, so a
        test can seed or inspect a grant directly."""

        state = MockState()
        store = MockRotatingCredentialStore(state=state, exchanger=FakeCounterparty())

        await store.put(REF, ExchangedCredential(access_token="a", refresh_token="r"))

        documents = state.identity["rotating_credentials"]

        assert list(documents) == [f"|{REF.path}"]
        assert documents[f"|{REF.path}"]["refresh_token"] == "r"

    def test_unbounded_exchange_is_refused(self) -> None:
        with pytest.raises(CoreException, match="Exchange timeout must be positive"):
            MockRotatingCredentialStore(
                state=MockState(),
                exchanger=FakeCounterparty(),
                exchange_timeout=timedelta(0),
            )

    async def test_a_second_ref_is_not_serialized_behind_the_first(self) -> None:
        """Per-credential locking must not degenerate into one global lock — a slow
        exchange for one grant cannot stall an unrelated one."""

        counterparty = FakeCounterparty(delay=0.05)
        store = MockRotatingCredentialStore(state=MockState(), exchanger=counterparty)
        other = SecretRef("oauth/other")

        for ref in (REF, other):
            await store.put(
                ref,
                ExchangedCredential(access_token="a", refresh_token=f"r-{ref.path}"),
            )

        versions = [(await store.get(ref)).version for ref in (REF, other)]
        first, second = await asyncio.gather(
            store.refresh(REF, observed=versions[0]),
            store.refresh(other, observed=versions[1]),
        )

        # Both rotated, and each presented its own token.
        assert first.access_token != second.access_token
        assert set(counterparty.presented) == {f"r-{REF.path}", f"r-{other.path}"}
