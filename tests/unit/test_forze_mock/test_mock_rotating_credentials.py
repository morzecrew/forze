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

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.secrets import (
    ExchangedCredential,
    RotatingCredentialsAdminDepKey,
    RotatingCredentialsDepKey,
    SecretRef,
)
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.testing import context_from_modules
from forze_mock import (
    MockDepsModule,
    MockKeyManagement,
    MockRotatingCredentialsAdmin,
    MockRotatingCredentialStore,
    MockState,
)
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
    state = MockState()
    # A real keyring over the mock KMS — the battery's crypto legs must exercise genuine
    # envelope sealing, not a stub that would make the AAD assertions vacuous.
    store = MockRotatingCredentialStore(
        state=state,
        exchanger=counterparty,
        exchange_timeout=EXCHANGE_TIMEOUT,
        tenant_provider=tenant,
        cipher=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk-rotating")),
        ),
    )

    def _document(ref: SecretRef) -> dict[str, Any]:
        key = f"{'' if tenant.tenant_id is None else tenant.tenant_id}|{ref.path}"

        return state.identity["rotating_credentials"][key]

    async def stored_payload(ref: SecretRef) -> JsonDict:
        return dict(_document(ref)["payload"])

    async def write_stored_payload(ref: SecretRef, payload: JsonDict) -> None:
        _document(ref)["payload"] = dict(payload)

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
        admin=MockRotatingCredentialsAdmin(state=state, tenant_provider=tenant),
        break_persist=break_persist,
        stored_payload=stored_payload,
        write_stored_payload=write_stored_payload,
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

        # Factories now, not singletons: the store resolves per scope so it can carry the
        # scope's tenant provider — resolve_simple invokes the factory, provide would
        # return it raw.
        store = wired.deps.resolve_simple(wired, RotatingCredentialsDepKey)
        assert isinstance(store, MockRotatingCredentialStore)
        assert store.tenant_provider is not None, "the wiring must thread the ambient tenant"

        # The control-plane scan rides the same opt-in.
        admin = wired.deps.resolve_simple(wired, RotatingCredentialsAdminDepKey)
        assert isinstance(admin, MockRotatingCredentialsAdmin)
        assert admin.tenant_provider is not None

    async def test_documents_live_in_the_shared_state(self) -> None:
        """The store writes into the same ``MockState`` the rest of the plane shares, so a
        test can seed or inspect a grant directly.

        The document shape mirrors the Postgres row — the credential in a nested ``payload``
        that sealing replaces wholesale, ``expires_at`` a readable sibling — so the shared
        battery's at-rest assertions mean the same thing on both stores.
        """

        state = MockState()
        store = MockRotatingCredentialStore(state=state, exchanger=FakeCounterparty())

        await store.put(REF, ExchangedCredential(access_token="a", refresh_token="r"))

        documents = state.identity["rotating_credentials"]

        assert list(documents) == [f"|{REF.path}"]

        # No cipher wired, so the payload is stored in the clear.
        assert documents[f"|{REF.path}"]["payload"]["refresh_token"] == "r"

    def test_unbounded_exchange_is_refused(self) -> None:
        with pytest.raises(CoreException, match="Exchange timeout must be positive"):
            MockRotatingCredentialStore(
                state=MockState(),
                exchanger=FakeCounterparty(),
                exchange_timeout=timedelta(0),
            )

    async def test_cancellation_while_sealing_leaves_no_replayable_token(self) -> None:
        """The window between a consumed token and its stored replacement.

        The store awaits the keyring in there, so a cancellation can land *after* the
        counterparty burned the token and *before* anything was written — and
        ``CancelledError`` slips past an ``except Exception`` guarding the write. Blocking
        inside the cipher makes that window deterministic instead of a race.
        """

        reached_sealing = asyncio.Event()
        inner = Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk-rotating")),
        )

        class _BlockingCipher:
            def __init__(self) -> None:
                self.block = False

            async def encrypt(self, data: bytes, **kwargs: Any) -> bytes:
                if self.block:
                    reached_sealing.set()
                    await asyncio.Event().wait()  # held until cancelled

                return await inner.encrypt(data, **kwargs)

            async def decrypt(self, blob: bytes, **kwargs: Any) -> bytes:
                return await inner.decrypt(blob, **kwargs)

        cipher = _BlockingCipher()
        counterparty = FakeCounterparty()
        store = MockRotatingCredentialStore(
            state=MockState(),
            exchanger=counterparty,
            cipher=cipher,  # type: ignore[arg-type]
        )

        await store.put(
            REF, ExchangedCredential(access_token="access-seed", refresh_token="refresh-seed")
        )
        before = await store.get(REF)

        cipher.block = True
        rotating = asyncio.ensure_future(store.refresh(REF, observed=before.version))
        await reached_sealing.wait()  # the token is spent; nothing is stored yet
        rotating.cancel()

        with pytest.raises(asyncio.CancelledError):
            await rotating

        assert counterparty.presented == ["refresh-seed"]

        cipher.block = False

        with pytest.raises(CoreException) as poisoned:
            await store.get(REF)

        assert poisoned.value.code == "credential_burnt"

        # And the spent token is never offered again.
        with pytest.raises(CoreException):
            await store.refresh(REF, observed=before.version)

        assert counterparty.presented == ["refresh-seed"]
        assert not counterparty.family_revoked

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


class TestTenantAwareWiring:
    """`routes["rotating_credentials"].tenant_aware` mirrors the Postgres config flag.

    Without it the oracle could not model a deployment that *requires* tagged tenancy:
    `PostgresRotatingCredentialsConfig(tenant_aware=True)` fails closed on an unbound
    tenant, and code written for that deployment needs the same refusal from the mock —
    an unbound call silently landing in the global partition is the permissive direction
    that hides real bugs.
    """

    async def test_tenant_aware_route_fails_closed_without_a_bound_tenant(self) -> None:
        from forze_mock.execution.configs import MockRouteConfig

        ctx = context_from_modules(
            MockDepsModule(
                state=MockState(),
                rotating_credentials=FakeCounterparty(),
                routes={"rotating_credentials": MockRouteConfig(tenant_aware=True)},
            )
        )
        store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
        admin = ctx.deps.resolve_simple(ctx, RotatingCredentialsAdminDepKey)

        with pytest.raises(CoreException) as no_tenant_store:
            await store.get(SecretRef("oauth/acme"))

        assert no_tenant_store.value.code == "tenant_required"

        from datetime import UTC, datetime

        with pytest.raises(CoreException) as no_tenant_scan:
            await admin.due_for_refresh(idle_since=datetime.now(UTC), limit=10)

        assert no_tenant_scan.value.code == "tenant_required"

    async def test_a_bound_tenant_satisfies_the_same_route(self) -> None:
        """The control: the refusal above is the missing tenant, not broken wiring."""

        from uuid import uuid4

        from forze.application.contracts.secrets import ExchangedCredential
        from forze.application.contracts.tenancy import TenantIdentity
        from forze_mock.execution.configs import MockRouteConfig

        ctx = context_from_modules(
            MockDepsModule(
                state=MockState(),
                rotating_credentials=FakeCounterparty(),
                routes={"rotating_credentials": MockRouteConfig(tenant_aware=True)},
            )
        )

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
            stored = await store.put(
                SecretRef("oauth/acme"),
                ExchangedCredential(access_token="a", refresh_token="r"),
            )

            assert (await store.get(SecretRef("oauth/acme"))).version == stored.version
