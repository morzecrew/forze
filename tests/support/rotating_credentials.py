"""Conformance battery for counterparty-rotated credential stores.

Every :class:`RotatingCredentialStorePort` implementation runs this same battery against
its real storage. The properties it checks are not adapter details — they are the whole
reason the contract exists, so an adapter that stores documents correctly while getting
the *ordering* wrong is not an implementation of it:

1. **persist before use** — a caller never observes a credential that is not durable, and a
   persist that fails after a successful exchange says so unmistakably. Its converse matters
   just as much: a token that has been *presented* is spent-or-unknown, so any ending that
   loses the outcome must leave the grant unusable rather than restoring a row that still
   looks live;
2. **single-flight** — concurrent refreshes produce exactly one exchange, because a second
   exchange with a burned token can revoke the whole grant family;
3. **reuse never reaches the counterparty** — a caller holding a stale version cannot
   trigger an exchange;
4. **the burn notice is terminal and typed**, cleared only by re-authorization;
5. **tenant isolation** — one tenant's grant is unreachable from another's;
6. **sealed at rest** — the tokens are not readable on disk, the AAD binds each credential to
   its ``(tenant, ref)`` so a lifted row fails authentication rather than decrypting into the
   wrong grant, and a legacy plaintext row still reads so enabling encryption needs no
   migration;
7. **the idleness scan tells the truth** — ``due_for_refresh`` surfaces exactly the grants
   whose last exchange predates the cutoff (oldest first, bounded, tenant-scoped, burnt ones
   flagged rather than hidden), an exchange resets the clock, and refreshing with the
   *scanned* version converges instead of double-exchanging when live traffic got there
   first. This is the plane RFC-style proactive refresh stands on: a scan that lies about
   dueness silently loses idle grants.

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
from datetime import datetime, timedelta
from typing import final
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.crypto import is_encrypted_payload
from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    ExchangedCredential,
    RotatingCredentialsAdminPort,
    RotatingCredentialStorePort,
    SecretRef,
    SecretVersion,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict, utcnow

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

    admin: RotatingCredentialsAdminPort
    """The control-plane scan over the same storage the store writes.

    A separate port on purpose (the data plane must not gain list powers), but the battery
    holds both because its checks are exactly about their agreement: what the store
    persists is what the scan must surface."""

    break_persist: Callable[[], AbstractAsyncContextManager[None]]
    """Make the durable write fail for the duration of the scope.

    Supplied per adapter because a faithful failure is storage-specific: the mock breaks
    its write, the Postgres store gets a trigger that raises inside the real transaction.
    """

    stored_payload: Callable[[SecretRef], Awaitable[JsonDict]]
    """Read the payload exactly as it sits at rest, bypassing the store.

    The only way to assert that a credential is *not* in the clear on disk, and the only way
    to stage the tampering the AAD is supposed to reject.
    """

    write_stored_payload: Callable[[SecretRef, JsonDict], Awaitable[None]]
    """Overwrite the at-rest payload, bypassing the store.

    Stages two things no public method can: a row lifted out of another ref or tenant, and a
    legacy plaintext document written before a keyring was ever wired.
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


async def check_an_ambiguous_timeout_leaves_no_replayable_token(
    h: RotatingStoreHarness,
) -> None:
    """A timeout is transient for the network and terminal for the credential.

    The token was presented; the store simply never learned whether the counterparty
    consumed it. Leaving the row live would let the next worker present the same token, and
    if it *was* consumed that is reuse — which revokes the grant family. So the row is
    marked unusable and the caller is told to re-authorize.
    """

    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.delay = EXCHANGE_TIMEOUT.total_seconds() * 10

    with pytest.raises(CoreException) as timed_out:
        await h.store.refresh(REF, observed=before.version)

    assert timed_out.value.code == CREDENTIAL_EXCHANGE_TIMEOUT_CODE

    h.counterparty.delay = 0.0
    presented = list(h.counterparty.presented)

    # The grant no longer looks live, so nobody replays the token it was holding.
    with pytest.raises(CoreException) as poisoned:
        await h.store.get(REF)

    assert poisoned.value.code == BURNT_CREDENTIAL_CODE

    with pytest.raises(CoreException):
        await h.store.refresh(REF, observed=before.version)

    assert h.counterparty.presented == presented, "the spent token must not be presented again"
    assert not h.counterparty.family_revoked

    # Re-authorization is the way back, as for any other burnt grant.
    await h.store.put(
        REF, ExchangedCredential(access_token="access-reauth", refresh_token="refresh-reauth")
    )
    assert (await h.store.get(REF)).access_token == "access-reauth"


# ....................... #


async def _settle(h: RotatingStoreHarness, stale: SecretVersion) -> None:
    """Wait until an abandoned rotation has recorded an outcome.

    A store that keeps a presented-token section running past the caller's cancellation
    finishes a moment later, so the assertion has to wait for the state to stop moving —
    either the grant is unusable, or it advanced past the version the caller held.
    """

    for _ in range(100):
        try:
            if (await h.store.get(REF)).version != stale:
                return

        except CoreException as e:
            if e.code == BURNT_CREDENTIAL_CODE:
                return

            raise

        await asyncio.sleep(0.02)

    raise AssertionError("the cancelled rotation never recorded an outcome")


# ....................... #


async def check_a_cancelled_exchange_leaves_no_replayable_token(
    h: RotatingStoreHarness,
) -> None:
    """Cancellation is the third way to lose the outcome, and the easiest one to miss.

    ``CancelledError`` is not an ``Exception``, so a handler that guards the other two
    endings does nothing here — and a shutdown landing mid-exchange is the ordinary way it
    happens. The token is in the counterparty's hands either way, so the grant must end up
    just as unusable as after a timeout.
    """

    await h.seed()
    before = await h.store.get(REF)
    h.counterparty.delay = 5.0

    rotating = asyncio.ensure_future(h.store.refresh(REF, observed=before.version))
    await asyncio.sleep(0.05)  # let it reach the counterparty
    rotating.cancel()

    with pytest.raises(asyncio.CancelledError):
        await rotating

    assert h.counterparty.presented == [SEED_REFRESH]

    # The outcome is asserted, not the mechanism. A store may mark the grant unusable, or
    # (if it declines to abandon a section that has already presented the token) carry the
    # rotation through to a new credential. Both satisfy the invariant; a row left live at
    # the version the caller passed in does not.
    await _settle(h, before.version)

    presented = list(h.counterparty.presented)
    h.counterparty.delay = 0.0

    with pytest.raises(CoreException):
        await h.store.refresh(REF, observed=before.version)

    assert h.counterparty.presented == presented, "the spent token must not be presented again"
    assert not h.counterparty.family_revoked


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

    # Persist before use: the caller never observed the replacement, and the store did not
    # half-apply it either. Nor did it leave the old row looking live — that token is spent
    # at the counterparty, so a worker still holding `before.version` would otherwise replay
    # it straight into reuse detection.
    with pytest.raises(CoreException) as poisoned:
        await h.store.get(REF)

    assert poisoned.value.code == BURNT_CREDENTIAL_CODE

    with pytest.raises(CoreException):
        await h.store.refresh(REF, observed=before.version)

    assert h.counterparty.presented == [SEED_REFRESH], "the spent token must not be presented again"
    assert not h.counterparty.family_revoked


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


async def check_credentials_are_sealed_at_rest(h: RotatingStoreHarness) -> None:
    """Every row here is a replayable credential, so the tokens must not be readable on disk.

    Asserting the *absence* of the plaintext is what makes this meaningful — a test that only
    checked the round-trip would pass just as well against a store that sealed nothing.
    """

    await h.seed()
    at_rest = await h.stored_payload(REF)

    assert is_encrypted_payload(at_rest), "the stored payload is not an envelope"
    assert SEED_REFRESH not in str(at_rest), "the refresh token is readable at rest"
    assert SEED_ACCESS not in str(at_rest), "the access token is readable at rest"

    # And it still opens: sealing that cannot round-trip is just data loss.
    opened = await h.store.get(REF)
    assert opened.access_token == SEED_ACCESS
    assert opened.metadata["host"] == "acme.example"

    # A rotation re-seals rather than silently dropping to plaintext.
    rotated = await h.store.refresh(REF, observed=opened.version)
    resealed = await h.stored_payload(REF)

    assert is_encrypted_payload(resealed)
    assert rotated.access_token not in str(resealed)


# ....................... #


async def check_a_row_lifted_to_another_ref_fails_authentication(
    h: RotatingStoreHarness,
) -> None:
    """The AAD binds a credential to its ref, so a copied row cannot be opened elsewhere.

    Worth having quite apart from confidentiality: without the binding, anyone able to write
    the table could promote one tenant's grant into another ref and have it decrypt cleanly.
    """

    other = SecretRef("oauth/other")
    await h.seed()
    await h.seed(other)

    # Lift the first grant's sealed bytes into the second ref's row.
    await h.write_stored_payload(other, await h.stored_payload(REF))

    with pytest.raises(CoreException) as rejected:
        await h.store.get(other)

    # The AEAD refuses it; what matters is that it is refused, not decrypted.
    assert rejected.value.code != BURNT_CREDENTIAL_CODE

    # The untouched ref still reads, so the failure is the binding and not a broken keyring.
    assert (await h.store.get(REF)).access_token == SEED_ACCESS


# ....................... #


async def check_a_row_lifted_to_another_tenant_fails_authentication(
    h: RotatingStoreHarness,
) -> None:
    """The same binding on the tenant axis — the one that would leak across customers."""

    first, second = uuid4(), uuid4()

    h.tenant.tenant_id = first
    await h.seed()
    sealed = await h.stored_payload(REF)

    h.tenant.tenant_id = second
    await h.seed()
    await h.write_stored_payload(REF, sealed)

    with pytest.raises(CoreException) as rejected:
        await h.store.get(REF)

    assert rejected.value.code != BURNT_CREDENTIAL_CODE

    h.tenant.tenant_id = first
    assert (await h.store.get(REF)).access_token == SEED_ACCESS

    h.tenant.tenant_id = None


# ....................... #


async def check_legacy_plaintext_rows_still_read(h: RotatingStoreHarness) -> None:
    """Enabling encryption must not need a migration or a flag day.

    A row written before a keyring was wired is plaintext; it has to keep reading, and seal on
    its next write. Without the pass-through, turning encryption on would brick every existing
    grant at once.
    """

    await h.seed()
    await h.write_stored_payload(
        REF,
        {
            "access_token": "legacy-access",
            "refresh_token": "legacy-refresh",
            "metadata": {"host": "legacy.example"},
        },
    )

    legacy = await h.store.get(REF)

    assert legacy.access_token == "legacy-access"
    assert legacy.metadata["host"] == "legacy.example"

    # The next write seals it, so a plaintext table converts as its grants rotate.
    rotated = await h.store.refresh(REF, observed=legacy.version)

    assert h.counterparty.presented == ["legacy-refresh"], "it used the legacy token"
    assert is_encrypted_payload(await h.stored_payload(REF))
    assert (await h.store.get(REF)).access_token == rotated.access_token


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


# ....................... #


FAR_FUTURE_CUTOFF = timedelta(hours=1)
"""Cutoff offset that makes every existing grant due — dueness is relative to the stamp,
so the battery ages grants by moving the cutoff, never by sleeping or freezing clocks."""


def _cutoff(offset: timedelta) -> datetime:
    return utcnow() + offset


async def check_the_scan_surfaces_idle_grants_and_exchange_resets_the_clock(
    h: RotatingStoreHarness,
) -> None:
    """Proof 1: an idle grant is due, and exchanging it makes it not due.

    The clock reset is the property the whole sweep stands on: without it a refreshed grant
    would stay due forever and every pass would exchange it again — each exchange a burn.
    """

    await h.seed()

    due = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)

    assert [d.ref.path for d in due] == [REF.path]
    assert due[0].burnt_reason is None
    stamped_at = due[0].last_exchanged_at

    # A cutoff at the stamp itself is strict: the grant is NOT due (guards the boundary).
    assert await h.admin.due_for_refresh(idle_since=stamped_at, limit=10) == []

    refreshed = await h.store.refresh(REF, observed=due[0].version)

    # The exchange moved the stamp forward: at a cutoff that previously included the
    # grant, it is no longer due — and the scan agrees the stamp advanced.
    just_after_seed = stamped_at + timedelta(microseconds=1)
    assert await h.admin.due_for_refresh(idle_since=just_after_seed, limit=10) == []

    again = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)
    assert [d.ref.path for d in again] == [REF.path]
    assert again[0].last_exchanged_at > stamped_at
    assert again[0].version == refreshed.version


async def check_a_sweep_with_a_scanned_version_converges_after_live_traffic(
    h: RotatingStoreHarness,
) -> None:
    """Proof 2: scan → live refresh → sweep refresh = exactly one exchange.

    The sweep passes the version it *scanned*; when live traffic exchanged in between,
    that version is stale and the store must return the winner's document without calling
    the counterparty — the single-flight property, re-entered through the scan's output.
    """

    await h.seed()
    scanned = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)

    # Live traffic gets there first.
    live = await h.store.refresh(REF, observed=scanned[0].version)
    exchanges_after_live = len(h.counterparty.presented)

    # The sweep arrives with the scanned (now stale) version: converges, no second exchange.
    swept = await h.store.refresh(REF, observed=scanned[0].version)

    assert len(h.counterparty.presented) == exchanges_after_live
    assert swept.version == live.version
    assert swept.access_token == live.access_token
    assert not h.counterparty.family_revoked


async def check_the_scan_reports_burnt_grants_instead_of_hiding_them(
    h: RotatingStoreHarness,
) -> None:
    """Proof 3: a burnt grant is queryable output, and refreshing it never exchanges."""

    await h.seed()
    await h.store.burn(REF, reason="provider revoked via webhook")

    due = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)

    assert [d.ref.path for d in due] == [REF.path]
    assert due[0].burnt
    assert due[0].burnt_reason == "provider revoked via webhook"

    presented_before = len(h.counterparty.presented)

    with pytest.raises(CoreException) as burnt:
        await h.store.refresh(REF, observed=due[0].version)

    assert burnt.value.code == BURNT_CREDENTIAL_CODE
    assert len(h.counterparty.presented) == presented_before, "a burnt grant must never be presented"


async def check_a_burn_notice_for_an_unknown_ref_reaches_the_scan(
    h: RotatingStoreHarness,
) -> None:
    """A grant that was never stored but is known dead still shows up in the sweep's view.

    ``burn`` on an absent ref writes a placeholder precisely so "needs re-authorization"
    is recorded; a scan that skipped placeholder rows would silently drop those grants
    from the operator's queryable list — the exact alert-someone-missed failure the
    reporting exists to prevent.
    """

    await h.store.burn(ABSENT_REF, reason="authorization was revoked before first use")

    due = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)

    assert [d.ref.path for d in due] == [ABSENT_REF.path]
    assert due[0].burnt
    assert due[0].burnt_reason == "authorization was revoked before first use"


async def check_the_scan_is_bounded_and_oldest_first(h: RotatingStoreHarness) -> None:
    """Proof 5: ``limit`` caps a pass, and the most endangered grant comes first.

    Oldest-first is what turns a bounded pass into a safe one — the grant closest to its
    provider's deadline is served before the cap cuts the batch.
    """

    first = SecretRef("oauth/oldest")
    second = SecretRef("oauth/middle")
    third = SecretRef("oauth/newest")

    for ref in (first, second, third):
        await h.seed(ref)

    everything = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)

    assert [d.ref.path for d in everything] == [first.path, second.path, third.path]

    capped = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=2)

    assert [d.ref.path for d in capped] == [first.path, second.path]

    with pytest.raises(CoreException) as refused:
        await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=0)

    assert refused.value.kind == ExceptionKind.PRECONDITION


async def check_the_scan_is_tenant_scoped(h: RotatingStoreHarness) -> None:
    """Proof 6: a sweep never surfaces another tenant's refs.

    The isolation axis of the data plane, re-proven on the control plane — a scan that
    leaked refs across tenants would hand one tenant's sweep the addresses of another's
    grants, and the refresh runs it enqueues would then operate cross-tenant.
    """

    tenant_a, tenant_b = uuid4(), uuid4()

    h.tenant.tenant_id = tenant_a
    await h.seed()

    h.tenant.tenant_id = tenant_b
    await h.seed(SecretRef("oauth/other-tenant"))

    b_due = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)
    assert [d.ref.path for d in b_due] == ["oauth/other-tenant"]

    h.tenant.tenant_id = tenant_a
    a_due = await h.admin.due_for_refresh(idle_since=_cutoff(FAR_FUTURE_CUTOFF), limit=10)
    assert [d.ref.path for d in a_due] == [REF.path]


ROTATING_STORE_BATTERY: tuple[Check, ...] = (
    check_counterparty_burns_reused_tokens,
    check_put_then_get_round_trip,
    check_get_missing_fails_closed,
    check_refresh_exchanges_and_persists,
    check_concurrent_refresh_is_single_flight,
    check_stale_observed_never_exchanges,
    check_invalid_grant_burns_terminally,
    check_transient_failure_preserves_the_credential,
    check_an_ambiguous_timeout_leaves_no_replayable_token,
    check_a_cancelled_exchange_leaves_no_replayable_token,
    check_persist_loss_is_loud_and_leaves_no_phantom,
    check_burn_then_put_restores,
    check_burn_of_an_absent_grant_sticks,
    check_tenants_are_isolated,
    check_credentials_are_sealed_at_rest,
    check_a_row_lifted_to_another_ref_fails_authentication,
    check_a_row_lifted_to_another_tenant_fails_authentication,
    check_legacy_plaintext_rows_still_read,
    check_the_scan_surfaces_idle_grants_and_exchange_resets_the_clock,
    check_a_sweep_with_a_scanned_version_converges_after_live_traffic,
    check_the_scan_reports_burnt_grants_instead_of_hiding_them,
    check_a_burn_notice_for_an_unknown_ref_reaches_the_scan,
    check_the_scan_is_bounded_and_oldest_first,
    check_the_scan_is_tenant_scoped,
)
"""Every check, in the order a reader should meet them. An adapter runs all of them."""
