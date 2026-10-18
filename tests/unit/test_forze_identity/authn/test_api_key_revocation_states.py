"""Verifying API keys from cached revocation states, on the identity plane's real adapters.

A key's state is the only proof the key exists, so a live one carries a MAC under the server key
and a not-after on the store's clock. Each threat below is someone able to write the store: a
forged state, a victim's state copied onto another digest, a live state replayed over
``revoked``, and a state signed under a key since rotated out. Every one must read as a miss.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
import structlog
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.authn import (
    REVOKED_STATE,
    ApiKeyCredentials,
    AuthnIdentity,
    IssuedApiKey,
    RevocationStatesPort,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source, utcnow
from forze_identity.authn import AuthnKernelConfig, instrument_revocation_states
from forze_identity.authn.services import (
    ApiKeyConfig,
    ApiKeyRevocation,
    ApiKeyService,
    Hs256Signer,
    RevocationStates,
    StateMacKeys,
)
from forze_identity.authn.services.revocation_states import _api_key_message
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_kits.aggregates.authn import (
    AuthnIssueApiKeyRequestDTO,
    AuthnKernelOp,
    build_authn_registry,
)
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry
from forze_kits.aggregates.tenancy.dto import TenantSwitchRequestDTO
from tests.unit.test_forze_identity.authn.test_revocation_states import (
    NS,
    ROUTE,
    SPEC,
    STATES,
    TTL,
    TX,
    _Clock,
    _Stack,
    _stack,
)

pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

# ----------------------- #

MAC = StateMacKeys(active=b"m" * 32)
OLD_MAC = b"o" * 32
DIGESTS = ApiKeyService(pepper=b"a" * 32, config=ApiKeyConfig())
TTL_MS = TTL // timedelta(milliseconds=1)
FOREVER_MS = 2**62


def _key_stack(**kwargs: Any) -> _Stack:
    """A stack whose store clock reads the wall clock's time, so key expiries line up with it."""

    kwargs.setdefault("key_mac", MAC)
    return _stack(clock=_Clock(now=time.time()), **kwargs)


async def _issue(stack: _Stack, principal: UUID, *, actor: UUID | None = None) -> IssuedApiKey:
    return await stack.ctx.authn.api_key_lifecycle(SPEC).issue_api_key(
        AuthnIdentity(principal_id=principal),
        actor_principal_id=actor,
    )


async def _verify(stack: _Stack, key: ApiKeyCredentials) -> UUID | str:
    """The principal the key authenticates as, or ``refused``, from a request of its own."""

    async def request() -> UUID | str:
        try:
            result = await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

        except CoreException:
            return "refused"

        assert result.issuer_tenant_hint is None  # a key names no tenant, cached or not
        return result.identity.principal_id

    return await asyncio.create_task(request())


def _digest(key: ApiKeyCredentials) -> str:
    return DIGESTS.calculate_key_digest(key.key)


def _stored(stack: _Stack, digest: str) -> tuple[str, int] | None:
    """The raw state and its expiry, as the store holds them."""

    stored = (stack.states.inner.namespace, "", f"k:{digest}")

    return stack.states.inner.state.revocation_states.get(stored)


def _plant(stack: _Stack, digest: str, value: str) -> None:
    """Write *value* as anyone able to write the store could: over what is there, never expiring."""

    stored = (stack.states.inner.namespace, "", f"k:{digest}")
    stack.states.inner.state.revocation_states[stored] = (value, FOREVER_MS)


def _message(digest: str, body: str, *, namespace: str = STATES, route: str = STATES) -> bytes:
    """What a state's MAC covers, built by the server's own function for the unit stack."""

    return _api_key_message(route, f"{namespace}:k:{digest}", digest, body)


def _signed(
    key: bytes,
    digest: str,
    principal: UUID,
    *,
    not_after_ms: int,
    expires_ms: int | None = None,
) -> str:
    """A live state for *digest*, signed under *key* as the server signs one."""

    expires = "-" if expires_ms is None else str(expires_ms)
    body = f"live|{principal}|-|{expires}|{not_after_ms}"

    return f"{body}|{StateMacKeys(active=key).sign(_message(digest, body))}"


def _now_ms(stack: _Stack) -> int:
    return int(stack.clock.now * 1000)


# ....................... #


async def test_a_key_is_admitted_from_its_state_with_no_row_read() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key

    assert await _verify(stack, key) == principal
    assert stack.reads == ["api_key.find"]

    stack.reads.clear()
    stack.states.calls.clear()

    assert await _verify(stack, key) == principal
    assert stack.reads == []
    assert stack.states.calls == ["read_api_key"]

    config = _config(stack)
    assert (config.counters.get("miss", "api_key"), config.counters.get("hit", "api_key")) == (1, 1)


def _config(stack: _Stack) -> RevocationStates:
    """The kernel's one value, which every verifier and writer counts on."""

    verifier = stack.ctx.authn.authn(SPEC).api_key_verifier  # type: ignore[attr-defined]
    revocation: ApiKeyRevocation = verifier.revocation

    return revocation.config


async def test_a_delegation_key_keeps_its_actor_from_the_state() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    agent = (
        await stack.ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="service"),
        )
    ).id
    key = (await _issue(stack, principal, actor=agent)).key
    await _verify(stack, key)
    stack.reads.clear()

    async def actor() -> UUID | None:
        result = await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)
        return result.identity.actor.principal_id if result.identity.actor else None

    assert await asyncio.create_task(actor()) == agent
    assert stack.reads == []


@pytest.mark.parametrize("writer", ["owner", "owner in a transaction", "admin", "many", "rotation"])
async def test_each_key_writer_writes_revoked_and_the_key_is_refused_with_no_read(
    writer: str,
) -> None:
    stack = _key_stack()
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    key, key_id = issued.key, str(issued.key_id)
    lifecycle = stack.ctx.authn.api_key_lifecycle(SPEC)
    owner = AuthnIdentity(principal_id=principal)
    assert await _verify(stack, key) == principal

    if writer == "owner":
        await lifecycle.revoke_api_key(owner, key_id)

    elif writer == "owner in a transaction":
        async with stack.ctx.tx_ctx.scope(TX):
            await lifecycle.revoke_api_key(owner, key_id)
            # Written once the revocation commits, never before.
            assert (await stack.states.inner.read_api_key(_digest(key))).state != REVOKED_STATE

    elif writer == "admin":
        await lifecycle.revoke_principal_api_key(key_id)

    elif writer == "many":
        await lifecycle.revoke_many_api_keys(owner, [key_id])

    else:
        await lifecycle.refresh_api_key(key)

    assert (await stack.states.inner.read_api_key(_digest(key))).state == REVOKED_STATE

    stack.reads.clear()
    assert await _verify(stack, key) == "refused"
    assert stack.reads == []


async def test_a_principal_deactivation_writes_revoked_for_every_key() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    keys = [(await _issue(stack, principal)).key for _ in range(2)]

    for key in keys:
        await _verify(stack, key)

    async with stack.ctx.tx_ctx.scope(TX):
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    for key in keys:
        assert (await stack.states.inner.read_api_key(_digest(key))).state == REVOKED_STATE


async def test_a_deactivated_principal_s_key_is_refused_through_the_eligibility_read() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    await _verify(stack, key)

    # The authz registry deactivates the principal and touches no key: its state stays live.
    await stack.ctx.authz.principal_registry(AuthzSpec(name=ROUTE)).deactivate_principal(principal)
    stack.reads.clear()

    assert await _verify(stack, key) == "refused"
    assert stack.reads == []


async def test_a_revocation_whose_write_fails_is_logged_and_refused_within_ttl() -> None:
    stack = _key_stack(write_attempts=2)
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    await _verify(stack, issued.key)

    stack.states.writes_down = True
    stack.states.calls.clear()

    with structlog.testing.capture_logs() as logs:
        await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
            AuthnIdentity(principal_id=principal),
            str(issued.key_id),
        )

    stack.states.writes_down = False
    assert stack.states.calls == ["revoke_api_keys", "revoke_api_keys"]
    failures = [entry for entry in logs if "write-through" in entry["event"]]
    assert [entry["log_level"] for entry in failures] == ["error"]
    assert _digest(issued.key) not in str(failures)  # the key's id is logged, never its digest
    assert _config(stack).counters.get("write_failure", "api_key") == 1

    # The cached live state admits until its not-after: the window.
    assert await _verify(stack, issued.key) == principal

    stack.clock.advance(TTL)
    assert await _verify(stack, issued.key) == "refused"

    # The miss cached the row's revocation, so the next refusal reads nothing.
    stack.reads.clear()
    assert await _verify(stack, issued.key) == "refused"
    assert stack.reads == []


@pytest.mark.parametrize("mac", ["garbage", "under another key"])
async def test_a_forged_state_is_a_miss(mac: str) -> None:
    stack = _key_stack()
    victim = await stack.principal()
    # A key that has no row: whoever forges its state chooses whom it names.
    key = ApiKeyCredentials(key=uuid4().hex)
    digest = _digest(key)
    far = _now_ms(stack) + TTL_MS

    if mac == "garbage":
        _plant(stack, digest, f"live|{victim}|-|-|{far}|{'0' * 64}")

    else:
        _plant(stack, digest, _signed(b"x" * 32, digest, victim, not_after_ms=far))

    assert await _verify(stack, key) == "refused"
    assert stack.reads == ["api_key.find"]


async def test_a_state_copied_onto_another_digest_is_a_miss() -> None:
    stack = _key_stack()
    victim, attacker = await stack.principal(), await stack.principal()
    victim_key = (await _issue(stack, victim)).key
    attacker_key = (await _issue(stack, attacker)).key
    await _verify(stack, victim_key)

    stored = _stored(stack, _digest(victim_key))
    assert stored is not None
    _plant(stack, _digest(attacker_key), stored[0])
    stack.reads.clear()

    assert await _verify(stack, attacker_key) == attacker
    assert stack.reads == ["api_key.find"]


async def test_a_live_state_replayed_over_revoked_is_refused_past_its_not_after() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    digest = _digest(issued.key)
    await _verify(stack, issued.key)

    captured = _stored(stack, digest)
    assert captured is not None
    await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
        AuthnIdentity(principal_id=principal),
        str(issued.key_id),
    )

    # Restored over the revocation with no expiry in the store: its MAC still verifies.
    _plant(stack, digest, captured[0])
    assert await _verify(stack, issued.key) == principal  # inside its not-after: the window

    stack.clock.now = captured[1] / 1000
    stack.reads.clear()

    assert await _verify(stack, issued.key) == "refused"
    assert stack.reads == ["api_key.find"]


@pytest.mark.parametrize("previous", [(), (OLD_MAC,)])
async def test_a_state_signed_under_a_rotated_out_key_is_a_miss(
    previous: tuple[bytes, ...],
) -> None:
    stack = _key_stack(key_mac=StateMacKeys(active=b"n" * 32, previous=previous))
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    digest = _digest(key)
    _plant(stack, digest, _signed(OLD_MAC, digest, principal, not_after_ms=FOREVER_MS - 1))

    assert await _verify(stack, key) == principal
    # Still configured, the old key verifies; dropped, its states are read from the row.
    assert stack.reads == ([] if previous else ["api_key.find"])


async def test_a_lookup_signs_under_the_active_key() -> None:
    rotated = StateMacKeys(active=b"n" * 32, previous=(OLD_MAC,))
    stack = _key_stack(key_mac=rotated)
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    await _verify(stack, key)

    stored = _stored(stack, _digest(key))
    assert stored is not None
    body, _, mac = stored[0].rpartition("|")

    assert StateMacKeys(active=b"n" * 32).verify(_message(_digest(key), body), mac)
    assert not StateMacKeys(active=OLD_MAC).verify(_message(_digest(key), body), mac)


async def test_a_state_whose_not_after_outlives_the_key_s_expiry_is_refused_once_it_expires() -> (
    None
):
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    digest = _digest(key)
    expired = int((utcnow() - timedelta(minutes=1)).timestamp() * 1000)
    _plant(
        stack,
        digest,
        _signed(MAC.active, digest, principal, not_after_ms=FOREVER_MS - 1, expires_ms=expired),
    )

    assert await _verify(stack, key) == "refused"
    assert stack.reads == []  # an expiry never changes, so the state's own refuses


@pytest.mark.parametrize("expires_in", [timedelta(minutes=1), timedelta(hours=1)])
async def test_the_not_after_is_the_earlier_of_ttl_and_the_key_s_expiry(
    expires_in: timedelta,
) -> None:
    stack = _key_stack(api_key=ApiKeyConfig(expires_in=expires_in))
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    assert issued.lifetime is not None and issued.lifetime.expires_at is not None
    expires_ms = int(issued.lifetime.expires_at.timestamp() * 1000)

    await _verify(stack, issued.key)
    stored = _stored(stack, _digest(issued.key))
    assert stored is not None

    expected = min(_now_ms(stack) + TTL_MS, expires_ms)
    assert stored[1] == expected
    assert int(stored[0].split("|")[4]) == expected


async def test_with_the_store_down_a_key_is_checked_from_its_row() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    stack.states.down = True

    assert await _verify(stack, issued.key) == principal

    await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
        AuthnIdentity(principal_id=principal),
        str(issued.key_id),
    )

    assert await _verify(stack, issued.key) == "refused"
    assert "fill_api_key" not in stack.states.calls
    assert _config(stack).counters.get("fallback", "api_key") == 2


async def test_a_lookup_inside_a_transaction_writes_nothing() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key

    async with stack.ctx.tx_ctx.scope(TX):
        assert await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

    assert "fill_api_key" not in stack.states.calls
    assert _stored(stack, _digest(key)) is None


async def test_an_unknown_key_is_refused_and_not_cached() -> None:
    stack = _key_stack()
    key = ApiKeyCredentials(key=uuid4().hex)

    assert await _verify(stack, key) == "refused"
    assert "fill_api_key" not in stack.states.calls


async def test_without_key_mac_keys_keep_their_row_read_and_write_no_state() -> None:
    stack = _key_stack(key_mac=None)
    principal = await stack.principal()
    issued = await _issue(stack, principal)

    for _ in range(2):
        assert await _verify(stack, issued.key) == principal

    assert stack.reads == ["api_key.find"] * 2

    await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
        AuthnIdentity(principal_id=principal),
        str(issued.key_id),
    )

    assert stack.states.calls == []


def test_a_short_mac_key_and_a_helper_without_one_are_refused() -> None:
    with pytest.raises(CoreException):
        StateMacKeys(active=b"m" * 31)

    with pytest.raises(CoreException):
        StateMacKeys(active=b"m" * 32, previous=(b"o" * 31,))

    # A str, or one bytes value where a tuple of them belongs, would fail on every verification.
    with pytest.raises(CoreException):
        StateMacKeys(active="m" * 32)  # type: ignore[arg-type]

    with pytest.raises(CoreException):
        StateMacKeys(active=b"m" * 32, previous=b"o" * 32)  # type: ignore[arg-type]

    with pytest.raises(CoreException):
        ApiKeyRevocation(
            config=RevocationStates(route=STATES, ttl=TTL),
            states=None,  # type: ignore[arg-type]
            accounts=None,  # type: ignore[arg-type]
            in_transaction=lambda: False,
            defer=None,  # type: ignore[arg-type]
        )


def test_a_mac_that_is_not_ascii_does_not_verify() -> None:
    assert not MAC.verify(b"message", "é" * 64)


# ....................... #


async def test_the_counters_are_exported_per_credential() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    token = await stack.login(principal)

    # One miss to warm each, then two hits.
    for _ in range(3):
        await _verify(stack, key)
        await stack.verify(token)

    stack.states.down = True
    await _verify(stack, key)
    await stack.verify(token)
    stack.states.down = False

    stack.states.writes_down = True
    await stack.logout(principal)
    stack.states.writes_down = False

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("test")
    instrument_revocation_states({"default": _config(stack)}, meter=meter)
    data = reader.get_metrics_data()
    assert data is not None

    seen = {
        (metric.name, point.attributes["forze.credential"]): point.value
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        for point in metric.data.data_points
        if point.attributes is not None and point.attributes["forze.revocation_states"] == "default"
    }
    prefix = "forze.authn.revocation_states"

    assert seen == {
        (f"{prefix}.hits", "session"): 2,
        (f"{prefix}.hits", "api_key"): 2,
        (f"{prefix}.misses", "session"): 1,
        (f"{prefix}.misses", "api_key"): 1,
        (f"{prefix}.fallbacks", "session"): 1,
        (f"{prefix}.fallbacks", "api_key"): 1,
        (f"{prefix}.write_failures", "session"): 1,
        (f"{prefix}.write_failures", "api_key"): 0,
        (f"{prefix}.clock_skew", "session"): 0,
        (f"{prefix}.clock_skew", "api_key"): 0,
    }


@pytest.mark.parametrize(
    "value", ["livé|mac", "live|mäc", "signed but unreadable", "revoked|extra"]
)
async def test_a_state_that_proves_nothing_is_a_miss(value: str) -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    digest = _digest(key)

    if value == "signed but unreadable":
        # Signed under the server key, so only its fields are wrong.
        body = f"live|not-a-uuid|-|-|{FOREVER_MS - 1}"
        value = f"{body}|{MAC.sign(_message(digest, body))}"

    _plant(stack, digest, value)

    assert await _verify(stack, key) == principal
    assert stack.reads == ["api_key.find"]


async def test_a_fill_that_fails_admits_from_the_row() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    stack.states.fill_down = True

    with structlog.testing.capture_logs() as logs:
        assert await _verify(stack, key) == principal

    assert [entry["log_level"] for entry in logs if "fill failed" in entry["event"]] == ["warning"]


async def test_deactivating_a_principal_with_no_active_key_writes_no_key_state() -> None:
    stack = _key_stack()
    principal = await stack.principal()

    async with stack.ctx.tx_ctx.scope(TX):
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    assert "revoke_api_keys" not in stack.states.calls


# ....................... #


async def _mint_with_key(stack: _Stack, key: ApiKeyCredentials, what: str) -> Any:
    """One request: authenticate with *key*, then run the kit operation that mints from it."""

    async def request() -> Any:
        result = await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

        with stack.ctx.inv_ctx.bind_identity(authn=result.identity):
            if what == "switch":
                return await run_operation(
                    build_tenancy_registry(SPEC).freeze(),
                    NS.key(TenancyKernelOp.SWITCH_TENANT),
                    TenantSwitchRequestDTO(id=uuid4()),
                    stack.ctx,
                )

            return await run_operation(
                build_authn_registry(SPEC).freeze(),
                NS.key(AuthnKernelOp.ISSUE_API_KEY),
                AuthnIssueApiKeyRequestDTO(label="minted"),
                stack.ctx,
            )

    return await asyncio.create_task(request())


@pytest.mark.parametrize("what", ["api_key", "switch"])
async def test_a_key_admitted_inside_the_window_mints_nothing(what: str) -> None:
    stack = _key_stack()
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    await _verify(stack, issued.key)

    stack.states.writes_down = True
    await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
        AuthnIdentity(principal_id=principal),
        str(issued.key_id),
    )
    stack.states.writes_down = False

    # The cached state still admits it, as the window allows; minting from it must not.
    assert await _verify(stack, issued.key) == principal

    with pytest.raises(CoreException) as caught:
        await _mint_with_key(stack, issued.key, what)

    assert caught.value.code == "api_key_revoked"


@pytest.mark.parametrize("what", ["api_key", "switch"])
async def test_a_live_key_still_mints(what: str) -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    await _verify(stack, key)

    assert await _mint_with_key(stack, key, what) is not None


async def test_a_store_clock_that_lags_cannot_keep_a_state_past_ttl() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    digest = _digest(key)
    stale = int(time.time() * 1000) - 2 * TTL_MS
    _plant(stack, digest, _signed(MAC.active, digest, principal, not_after_ms=stale))

    # A store whose clock reads before the not-after, as a replayed reply could.
    stack.clock.now = (stale - 1_000) / 1000

    assert await _verify(stack, key) == principal
    assert stack.reads == ["api_key.find"]


async def test_revoking_a_revoked_key_again_repairs_a_lost_write() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    owner = AuthnIdentity(principal_id=principal)
    lifecycle = stack.ctx.authn.api_key_lifecycle(SPEC)
    await _verify(stack, issued.key)

    stack.states.writes_down = True
    await lifecycle.revoke_api_key(owner, str(issued.key_id))
    stack.states.writes_down = False
    assert (await stack.states.inner.read_api_key(_digest(issued.key))).state != REVOKED_STATE

    await lifecycle.revoke_api_key(owner, str(issued.key_id))

    assert (await stack.states.inner.read_api_key(_digest(issued.key))).state == REVOKED_STATE


# ....................... #


@pytest.mark.parametrize("apart", ["namespace", "route"])
async def test_a_state_copied_from_another_deployment_is_a_miss(apart: str) -> None:
    """Two deployments sharing the pepper and the MAC key, but not a database or a namespace."""

    one = _key_stack()
    other = _key_stack(**{"namespace" if apart == "namespace" else "states_route": "other"})
    principal = await one.principal()
    # The same principal exists in both, as in a clone.
    await other.ctx.doc.command(policy_principal_spec).create(
        CreatePolicyPrincipalCmd(kind="user"),
        id=principal,
    )
    key = (await _issue(one, principal)).key
    await _verify(one, key)
    stored = _stored(one, _digest(key))
    assert stored is not None

    _plant(other, _digest(key), stored[0])
    other.reads.clear()

    # The key exists only in the first: the copied state proves nothing, and its row is missing.
    assert await _verify(other, key) == "refused"
    assert other.reads == ["api_key.find"]


def test_a_mac_key_that_reuses_another_authn_secret_is_refused() -> None:
    pepper = b"a" * 32

    for key_mac in (
        StateMacKeys(active=pepper),
        StateMacKeys(active=b"m" * 32, previous=(pepper,)),
    ):
        with pytest.raises(CoreException) as caught:
            AuthnKernelConfig(
                api_key_pepper=pepper,
                revocation_states=RevocationStates(route=STATES, ttl=TTL, key_mac=key_mac),
            )

        assert caught.value.code == "authn_state_mac_key_reused"

    # An HMAC secret handed over inside a signer is a secret too.
    for signers in (
        {"access_token_signer": Hs256Signer(secret=pepper)},
        {"access_token_verifiers": (Hs256Signer(secret=b"s" * 32), Hs256Signer(secret=pepper))},
    ):
        with pytest.raises(CoreException) as caught:
            AuthnKernelConfig(
                **signers,  # type: ignore[arg-type]
                revocation_states=RevocationStates(
                    route=STATES,
                    ttl=TTL,
                    key_mac=StateMacKeys(active=pepper),
                ),
            )

        assert caught.value.code == "authn_state_mac_key_reused"

    for secret in ("access_token_secret", "refresh_token_pepper", "reset_token_pepper"):
        with pytest.raises(CoreException):
            AuthnKernelConfig(
                **{secret: pepper},  # type: ignore[arg-type]
                revocation_states=RevocationStates(
                    route=STATES,
                    ttl=TTL,
                    key_mac=StateMacKeys(active=pepper),
                ),
            )


def test_a_reused_secret_still_makes_the_digest_no_state_mac() -> None:
    """With no domain separation, a key presented as a crafted message would have the server
    compute that message's state MAC as the key's digest, a name sent to Redis."""

    pepper = b"a" * 32
    message = b"whatever message a presenter wants signed"
    digest_as_mac = ApiKeyService(pepper=pepper, config=ApiKeyConfig()).calculate_key_digest(
        message.decode(),
    )

    assert not StateMacKeys(active=pepper).verify(message, digest_as_mac)


@pytest.mark.parametrize("what", ["session", "api_key"])
async def test_a_key_admitted_inside_the_window_mints_nothing_for_another_principal(
    what: str,
) -> None:
    stack = _key_stack()
    caller, other = await stack.principal(), await stack.principal()
    issued = await _issue(stack, caller)
    await _verify(stack, issued.key)

    stack.states.writes_down = True
    await stack.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
        AuthnIdentity(principal_id=caller),
        str(issued.key_id),
    )
    stack.states.writes_down = False

    async def request() -> Any:
        # Admitted from the stale state, then a credential minted for someone else.
        await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(issued.key)

        async with stack.ctx.tx_ctx.scope(TX):
            if what == "session":
                return await stack.lifecycle.issue_tokens(AuthnIdentity(principal_id=other))

            return await stack.ctx.authn.api_key_lifecycle(SPEC).issue_api_key(
                AuthnIdentity(principal_id=other),
            )

    with pytest.raises(CoreException) as caught:
        await asyncio.create_task(request())

    assert caught.value.code == "api_key_revoked"


async def test_a_key_that_expires_before_the_mint_mints_nothing() -> None:
    stack = _key_stack(api_key=ApiKeyConfig(expires_in=timedelta(hours=1)))
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    await _verify(stack, issued.key)

    async def request() -> Any:
        await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(issued.key)

        # The key expires between its verification and the issue the request makes from it.
        with bind_time_source(FrozenTimeSource(instant=utcnow() + timedelta(hours=2))):
            return await stack.ctx.authn.api_key_lifecycle(SPEC).issue_api_key(
                AuthnIdentity(principal_id=principal),
            )

    with pytest.raises(CoreException) as caught:
        await asyncio.create_task(request())

    assert caught.value.code == "api_key_revoked"


@pytest.mark.parametrize("skew_ttls", [-3, -1, 1, 3])
async def test_clock_skew_never_refuses_a_key_and_past_two_ttl_is_counted(skew_ttls: int) -> None:
    """The store's clock is the application's plus *skew_ttls* times ``ttl``."""

    stack = _stack(clock=_Clock(now=time.time() + skew_ttls * TTL.total_seconds()), key_mac=MAC)
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key

    for _ in range(3):
        assert await _verify(stack, key) == principal
        stack.clock.advance(timedelta(seconds=1))

    # Behind by two ttl or more, every state is past the application's cap when written.
    skewed = _config(stack).counters.get("clock_skew", "api_key")
    assert (skewed > 0) == (skew_ttls <= -2)


async def test_keys_and_sessions_need_no_api_key_calls_without_key_mac() -> None:
    """An application's own states port, written before the API-key calls, keeps working."""

    stack = _stack(key_mac=None, states_port=lambda states: _SessionStatesOnly(inner=states))
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    lifecycle = stack.ctx.authn.api_key_lifecycle(SPEC)
    token = await stack.login(principal)

    assert await _verify(stack, issued.key) == principal
    assert await stack.verify(token) == "admitted"

    await lifecycle.refresh_api_key(issued.key)
    await lifecycle.revoke_principal_api_key(str((await _issue(stack, principal)).key_id))

    async with stack.ctx.tx_ctx.scope(TX):
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    assert await stack.verify(token) == "session_revoked"


async def test_an_expired_key_is_refused_and_not_cached() -> None:
    stack = _key_stack(api_key=ApiKeyConfig(expires_in=timedelta(minutes=1)))
    principal = await stack.principal()
    issued = await _issue(stack, principal)
    stack.clock.advance(timedelta(minutes=2))

    async def request() -> UUID | str:
        with bind_time_source(FrozenTimeSource(instant=utcnow() + timedelta(minutes=2))):
            return await _verify(stack, issued.key)

    assert await asyncio.create_task(request()) == "refused"
    assert "fill_api_key" not in stack.states.calls


async def test_a_deactivation_retried_after_a_failed_write_writes_revoked() -> None:
    stack = _key_stack()
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    await _verify(stack, key)

    stack.states.writes_down = True

    async with stack.ctx.tx_ctx.scope(TX):
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    stack.states.writes_down = False

    # The principal's eligibility refuses the key meanwhile; the retry repairs its state.
    assert await _verify(stack, key) == "refused"
    assert (await stack.states.inner.read_api_key(_digest(key))).state != REVOKED_STATE

    async with stack.ctx.tx_ctx.scope(TX):
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    assert (await stack.states.inner.read_api_key(_digest(key))).state == REVOKED_STATE


async def test_the_mock_store_s_own_clock_serves_hits() -> None:
    """The mock's clock models Redis's ``TIME``: the application's clock must agree with it."""

    stack = _stack(key_mac=MAC, store_clock=False)
    principal = await stack.principal()
    key = (await _issue(stack, principal)).key
    await _verify(stack, key)
    stack.reads.clear()

    assert await _verify(stack, key) == principal
    assert stack.reads == []
    assert _config(stack).counters.get("clock_skew", "api_key") == 0


@attrs.define(eq=False)
class _SessionStatesOnly:
    """An application's states port written against the session calls alone."""

    inner: Any

    async def read(self, principal_id: UUID, key: str) -> Any:
        return await self.inner.read(principal_id, key)

    async def fill(self, principal_id: UUID, states: Any, *, expire_at_ms: int) -> None:
        await self.inner.fill(principal_id, states, expire_at_ms=expire_at_ms)

    async def revoke(self, principal_id: UUID, keys: Any, *, ttl: timedelta) -> None:
        await self.inner.revoke(principal_id, keys, ttl=ttl)

    async def now_ms(self, principal_id: UUID) -> int:
        return int(await self.inner.now_ms(principal_id))

    async def raise_epoch(self, principal_id: UUID, epoch: int, *, expire_at_ms: int) -> None:
        await self.inner.raise_epoch(principal_id, epoch, expire_at_ms=expire_at_ms)


def test_a_session_states_port_satisfies_the_states_contract_without_the_key_calls() -> None:
    assert isinstance(_SessionStatesOnly(inner=None), RevocationStatesPort)


def test_key_mac_with_a_states_port_without_the_key_calls_is_refused() -> None:
    stack = _stack(key_mac=MAC, states_port=lambda states: _SessionStatesOnly(inner=states))

    with pytest.raises(CoreException) as caught:
        stack.ctx.authn.authn(SPEC)

    assert caught.value.code == "authn_api_key_states_unsupported"
