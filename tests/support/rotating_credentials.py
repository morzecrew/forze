"""Conformance battery for counterparty-rotated credential stores.

Every :class:`RotatingCredentialStorePort` implementation runs this same battery against
its real storage. The properties it checks are not adapter details — they are the whole
reason the contract exists, so an adapter that stores documents correctly while getting
the *ordering* wrong is not an implementation of it:

1. **persist before use** — a caller never observes a credential that is not durable, and
   a persist that fails after a successful exchange says so unmistakably;
2. **single-flight** — concurrent refreshes produce exactly one exchange, because a second
   exchange with a burned token can revoke the whole grant family;
3. **reuse never reaches the counterparty** — a caller holding a stale version cannot
   trigger an exchange;
4. **the burn notice is terminal and typed**, cleared only by re-authorization;
5. **tenant isolation** — one tenant's grant is unreachable from another's.

There is no live third party to differential against, so :class:`FakeCounterparty` *is*
the specification of provider behaviour: it burns each presented token and revokes the
family on reuse, exactly as the OAuth security best practice prescribes. That makes the
fake load-bearing, so the battery asserts the fake's own semantics too — otherwise every
"the family was not revoked" assertion would pass vacuously.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import final
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    ExchangedCredential,
    RotatingCredentialStorePort,
    SecretRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import utcnow

# ----------------------- #

REF = SecretRef("oauth/acme")
"""The credential every check operates on."""

ABSENT_REF = SecretRef("oauth/never-authorized")
"""A ref no check ever authorizes."""

SEED_ACCESS = "access-seed"
SEED_REFRESH = "refresh-seed"

EXCHANGE_TIMEOUT = timedelta(milliseconds=300)
"""Bound the harness builds its store with — short enough to assert the timeout path."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class FakeCounterparty(CredentialExchangerPort):
    """A provider that rotates refresh tokens and punishes reuse, like the real ones.

    Programmable through the three public knobs; everything else is fixed behaviour that
    the battery relies on and asserts.
    """

    delay: float = 0.0
    """Seconds the exchange takes. Widens the race window for the single-flight check."""

    fail_permanently: bool = False
    """Answer as ``invalid_grant`` — the counterparty has rejected the grant for good."""

    fail_transiently: bool = False
    """Answer with a non-grant failure (timeout, 5xx). Must never burn the credential."""

    presented: list[str] = attrs.field(factory=list, init=False)
    """Refresh tokens handed to :meth:`exchange`, in order. The exchange counter."""

    burned: set[str] = attrs.field(factory=set, init=False)
    """Tokens already spent. Presenting one again is reuse."""

    family_revoked: bool = attrs.field(default=False, init=False)
    """Set when reuse is detected — the grant family is gone, as the BCP prescribes."""

    _generation: int = attrs.field(default=0, init=False)

    # ....................... #

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> ExchangedCredential:
        self.presented.append(refresh_token)

        if self.delay > 0:
            await asyncio.sleep(self.delay)

        if self.family_revoked:
            raise exc.precondition("Grant family was revoked.", code=INVALID_GRANT_CODE)

        if refresh_token in self.burned:
            # Reuse detection. This is why single-flight is a safety property: the second
            # caller does not merely waste a round trip, it destroys the grant.
            self.family_revoked = True

            raise exc.precondition(
                "Refresh-token reuse detected; grant family revoked.",
                code=INVALID_GRANT_CODE,
            )

        if self.fail_transiently:
            raise RuntimeError("counterparty temporarily unavailable")

        if self.fail_permanently:
            raise exc.precondition("invalid_grant", code=INVALID_GRANT_CODE)

        self.burned.add(refresh_token)
        self._generation += 1

        return ExchangedCredential(
            access_token=f"access-{self._generation}",
            refresh_token=f"refresh-{self._generation}",
            expires_at=utcnow() + timedelta(hours=1),
            metadata=dict(metadata),
        )


# ....................... #


@final
@attrs.define(slots=True)
class TenantCell:
    """Mutable ambient tenant, so one store can be asked about two tenants."""

    tenant_id: UUID | None = None

    def __call__(self) -> TenantIdentity | None:
        return None if self.tenant_id is None else TenantIdentity(tenant_id=self.tenant_id)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RotatingStoreHarness:
    """One adapter under test, plus the two seams the battery cannot supply itself."""

    store: RotatingCredentialStorePort
    counterparty: FakeCounterparty
    tenant: TenantCell

    break_persist: Callable[[], AbstractAsyncContextManager[None]]
    """Make the durable write fail for the duration of the scope.

    Supplied per adapter because a faithful failure is storage-specific: the mock breaks
    its write, the Postgres store gets a trigger that raises inside the real transaction.
    """

    # ....................... #

    async def seed(self, ref: SecretRef = REF) -> None:
        """Authorize a grant the way a re-authorization flow would."""

        await self.store.put(
            ref,
            ExchangedCredential(
                access_token=SEED_ACCESS,
                refresh_token=SEED_REFRESH,
                metadata={"host": "acme.example"},
            ),
        )


Check = Callable[[RotatingStoreHarness], Awaitable[None]]


# ....................... #


async def check_counterparty_burns_reused_tokens(h: RotatingStoreHarness) -> None:
    """The oracle itself: without reuse detection every single-flight proof is vacuous."""

    first = await h.counterparty.exchange(REF, refresh_token="t0", metadata={})
    assert first.refresh_token != "t0"
    assert not h.counterparty.family_revoked

    with pytest.raises(CoreException) as reuse:
        await h.counterparty.exchange(REF, refresh_token="t0", metadata={})

    assert reuse.value.code == INVALID_GRANT_CODE
    assert h.counterparty.family_revoked, "reuse must revoke the family, not just fail"

    # And the family stays dead — a revoked grant cannot be exchanged back to life.
    with pytest.raises(CoreException) as after:
        await h.counterparty.exchange(REF, refresh_token=first.refresh_token, metadata={})

    assert after.value.code == INVALID_GRANT_CODE


# ....................... #


async def check_put_then_get_round_trip(h: RotatingStoreHarness) -> None:
    stored = await h.store.put(
        REF,
        ExchangedCredential(
            access_token=SEED_ACCESS,
            refresh_token=SEED_REFRESH,
            metadata={"host": "acme.example"},
        ),
    )
    read = await h.store.get(REF)

    assert read.access_token == SEED_ACCESS
    assert read.version == stored.version
    assert read.metadata["host"] == "acme.example"

    # The caller-facing view has no refresh token to leak or replay — structurally, not
    # by convention.
    assert not hasattr(read, "refresh_token")

    # Authorizing a grant never contacts the counterparty.
    assert h.counterparty.presented == []


# ....................... #


async def check_get_missing_fails_closed(h: RotatingStoreHarness) -> None:
    with pytest.raises(CoreException) as missing:
        await h.store.get(ABSENT_REF)

    assert missing.value.kind is ExceptionKind.NOT_FOUND


# ....................... #


async def check_refresh_exchanges_and_persists(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)

    fresh = await h.store.refresh(REF, observed=before.version)

    assert fresh.access_token != before.access_token
    assert fresh.version != before.version
    assert h.counterparty.presented == [SEED_REFRESH], "must present the STORED token"

    # Durable before the call returned: a fresh read observes the replacement, and it
    # carries the metadata forward.
    again = await h.store.get(REF)
    assert again.access_token == fresh.access_token
    assert again.version == fresh.version
    assert again.metadata["host"] == "acme.example"

    # The next rotation presents the *new* token, so the chain advances rather than
    # replaying — replaying would revoke the family.
    onward = await h.store.refresh(REF, observed=again.version)
    assert h.counterparty.presented[-1] != SEED_REFRESH
    assert onward.access_token != fresh.access_token
    assert not h.counterparty.family_revoked


# ....................... #


async def check_concurrent_refresh_is_single_flight(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.delay = 0.05  # widen the window so the racers genuinely overlap

    results = await asyncio.gather(
        *(h.store.refresh(REF, observed=before.version) for _ in range(5))
    )

    assert len(h.counterparty.presented) == 1, "one exchange, or the grant family dies"
    assert not h.counterparty.family_revoked

    # Every racer returns the same document — the losers converge on the winner rather
    # than failing or exchanging again.
    assert len({credential.access_token for credential in results}) == 1
    assert len({credential.version for credential in results}) == 1
    assert results[0].version != before.version


# ....................... #


async def check_stale_observed_never_exchanges(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)
    fresh = await h.store.refresh(REF, observed=before.version)
    exchanges = len(h.counterparty.presented)

    # A caller that slept through the rotation and still holds the old version must not
    # be able to present a token the counterparty already burned.
    converged = await h.store.refresh(REF, observed=before.version)

    assert len(h.counterparty.presented) == exchanges
    assert converged.version == fresh.version
    assert converged.access_token == fresh.access_token
    assert not h.counterparty.family_revoked


# ....................... #


async def check_invalid_grant_burns_terminally(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.fail_permanently = True

    with pytest.raises(CoreException) as burnt:
        await h.store.refresh(REF, observed=before.version)

    assert burnt.value.code == BURNT_CREDENTIAL_CODE

    exchanges = len(h.counterparty.presented)

    # Terminal: reads refuse, and further refreshes refuse *without* hammering the
    # provider with a credential it has already rejected.
    with pytest.raises(CoreException) as on_read:
        await h.store.get(REF)

    assert on_read.value.code == BURNT_CREDENTIAL_CODE

    with pytest.raises(CoreException) as on_retry:
        await h.store.refresh(REF, observed=before.version)

    assert on_retry.value.code == BURNT_CREDENTIAL_CODE
    assert len(h.counterparty.presented) == exchanges


# ....................... #


async def check_transient_failure_preserves_the_credential(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.fail_transiently = True

    # A transient failure must surface as itself — mapping it to a burn would destroy a
    # working grant over a network blip.
    with pytest.raises(RuntimeError):
        await h.store.refresh(REF, observed=before.version)

    h.counterparty.fail_transiently = False
    intact = await h.store.get(REF)

    assert intact.access_token == before.access_token
    assert intact.version == before.version

    # Still rotatable: nothing was consumed or marked.
    recovered = await h.store.refresh(REF, observed=intact.version)
    assert recovered.access_token != before.access_token


# ....................... #


async def check_exchange_timeout_is_transient(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.delay = EXCHANGE_TIMEOUT.total_seconds() * 10

    with pytest.raises(CoreException) as timed_out:
        await h.store.refresh(REF, observed=before.version)

    assert timed_out.value.code == CREDENTIAL_EXCHANGE_TIMEOUT_CODE

    # The store never learned whether the counterparty processed the request, so the
    # stored credential is left exactly as it was.
    h.counterparty.delay = 0.0
    intact = await h.store.get(REF)

    assert intact.access_token == before.access_token
    assert intact.version == before.version


# ....................... #


async def check_persist_loss_is_loud_and_leaves_no_phantom(h: RotatingStoreHarness) -> None:
    await h.seed()
    before = await h.store.get(REF)

    async with h.break_persist():
        with pytest.raises(CoreException) as lost:
            await h.store.refresh(REF, observed=before.version)

    # The exchange DID happen — that is precisely why this is fatal rather than
    # retryable, and why it needs its own code instead of a storage error.
    assert lost.value.code == CREDENTIAL_PERSIST_LOST_CODE
    assert h.counterparty.presented == [SEED_REFRESH]

    # Persist before use: the caller never observed the replacement, and the store did
    # not half-apply it either. The stored grant is the (now dead) old one — recorded
    # honestly rather than replaced by a credential nobody can prove is durable.
    stale = await h.store.get(REF)
    assert stale.access_token == before.access_token
    assert stale.version == before.version


# ....................... #


async def check_burn_then_put_restores(h: RotatingStoreHarness) -> None:
    await h.seed()
    await h.store.burn(REF, reason="revoked in the provider console")

    with pytest.raises(CoreException) as burnt:
        await h.store.get(REF)

    assert burnt.value.code == BURNT_CREDENTIAL_CODE

    # Re-authorization is the only way back, and it must work — otherwise a burn would
    # brick the credential permanently.
    restored = await h.store.put(
        REF,
        ExchangedCredential(access_token="access-reauth", refresh_token="refresh-reauth"),
    )
    live = await h.store.get(REF)

    assert live.access_token == "access-reauth"
    assert live.version == restored.version

    rotated = await h.store.refresh(REF, observed=live.version)
    assert rotated.access_token != "access-reauth"


# ....................... #


async def check_burn_of_an_absent_grant_sticks(h: RotatingStoreHarness) -> None:
    """Learning a grant is dead must record it even when we never stored one.

    Otherwise the notice evaporates and a later read reports a plain "not found", losing
    the one fact an operator needs: this needs re-authorization, not investigation.
    """

    await h.store.burn(ABSENT_REF, reason="authorization never completed")

    with pytest.raises(CoreException) as burnt:
        await h.store.get(ABSENT_REF)

    assert burnt.value.code == BURNT_CREDENTIAL_CODE


# ....................... #


async def check_tenants_are_isolated(h: RotatingStoreHarness) -> None:
    first, second = uuid4(), uuid4()

    h.tenant.tenant_id = first
    await h.store.put(
        REF, ExchangedCredential(access_token="access-first", refresh_token="refresh-first")
    )

    h.tenant.tenant_id = second
    await h.store.put(
        REF, ExchangedCredential(access_token="access-second", refresh_token="refresh-second")
    )

    # The same ref under a different tenant is a different credential, not a shared one.
    h.tenant.tenant_id = first
    mine = await h.store.get(REF)
    assert mine.access_token == "access-first"

    # Rotating one tenant presents only that tenant's token and leaves the other alone.
    await h.store.refresh(REF, observed=mine.version)
    assert h.counterparty.presented == ["refresh-first"]

    h.tenant.tenant_id = second
    theirs = await h.store.get(REF)
    assert theirs.access_token == "access-second"

    h.tenant.tenant_id = None


# ....................... #


ROTATING_STORE_BATTERY: tuple[Check, ...] = (
    check_counterparty_burns_reused_tokens,
    check_put_then_get_round_trip,
    check_get_missing_fails_closed,
    check_refresh_exchanges_and_persists,
    check_concurrent_refresh_is_single_flight,
    check_stale_observed_never_exchanges,
    check_invalid_grant_burns_terminally,
    check_transient_failure_preserves_the_credential,
    check_exchange_timeout_is_transient,
    check_persist_loss_is_loud_and_leaves_no_phantom,
    check_burn_then_put_restores,
    check_burn_of_an_absent_grant_sticks,
    check_tenants_are_isolated,
)
"""Every check, in the order a reader should meet them. An adapter runs all of them."""
