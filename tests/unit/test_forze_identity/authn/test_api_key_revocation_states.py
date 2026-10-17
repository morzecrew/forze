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

import pytest
import structlog
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.authn import (
    REVOKED_STATE,
    ApiKeyCredentials,
    AuthnIdentity,
    IssuedApiKey,
)
from forze.application.contracts.authz import AuthzSpec
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_identity.authn import instrument_revocation_states
from forze_identity.authn.services import (
    ApiKeyConfig,
    ApiKeyRevocation,
    ApiKeyService,
    RevocationStates,
    StateMacKeys,
)
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from tests.unit.test_forze_identity.authn.test_revocation_states import (
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

    return stack.states.inner.state.revocation_states.get((STATES, "", f"k:{digest}"))


def _plant(stack: _Stack, digest: str, value: str) -> None:
    """Write *value* as anyone able to write the store could: over what is there, never expiring."""

    stack.states.inner.state.revocation_states[(STATES, "", f"k:{digest}")] = (value, FOREVER_MS)


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
    message = b"\n".join((b"forze.authn.api-key-state.v1", f"k:{digest}".encode(), body.encode()))

    return f"{body}|{StateMacKeys(active=key).sign(message)}"


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
    message = b"\n".join(
        (b"forze.authn.api-key-state.v1", f"k:{_digest(key)}".encode(), body.encode()),
    )

    assert StateMacKeys(active=b"n" * 32).verify(message, mac)
    assert not StateMacKeys(active=OLD_MAC).verify(message, mac)


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

    with pytest.raises(CoreException):
        ApiKeyRevocation(
            config=RevocationStates(route=STATES, ttl=TTL),
            states=None,  # type: ignore[arg-type]
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

    for _ in range(2):
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
        (f"{prefix}.hits", "session"): 1,
        (f"{prefix}.hits", "api_key"): 1,
        (f"{prefix}.misses", "session"): 1,
        (f"{prefix}.misses", "api_key"): 1,
        (f"{prefix}.fallbacks", "session"): 1,
        (f"{prefix}.fallbacks", "api_key"): 1,
        (f"{prefix}.write_failures", "session"): 1,
        (f"{prefix}.write_failures", "api_key"): 0,
    }
