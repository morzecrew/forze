"""Counterparty-rotated credentials recipe — single-flight rotation + burn notice (mock)."""

from __future__ import annotations

import asyncio

import pytest

from examples.recipes.rotating_credentials.app import (
    GRANT_REF,
    DemoOAuthProvider,
    authorize,
    build_context,
    call_api,
)
from forze.application.contracts.secrets import RotatingCredentialsDepKey
from forze.base.exceptions import CoreException


async def test_concurrent_callers_rotate_once_and_keep_the_grant_alive() -> None:
    provider = DemoOAuthProvider()
    ctx, _, _runner, _sweeper = build_context(provider)
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    assert await call_api(store, provider) == "ok"
    assert provider.exchanges == 0  # a live token needs no rotation

    # Five workers all find the token spent at the same moment.
    provider.expire_access_token()
    results = await asyncio.gather(*(call_api(store, provider) for _ in range(5)))

    assert results == ["ok"] * 5
    assert provider.exchanges == 1, "a second exchange would present a burned token"
    assert not provider.grant_revoked

    # The rotation is durable, not just in flight: a later call needs no further exchange.
    assert await call_api(store, provider) == "ok"
    assert provider.exchanges == 1


async def test_a_revoked_grant_burns_and_re_authorization_restores_it() -> None:
    provider = DemoOAuthProvider()
    ctx, _, _runner, _sweeper = build_context(provider)
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    provider.revoke()
    provider.expire_access_token()

    with pytest.raises(CoreException) as burnt:
        await call_api(store, provider)

    assert burnt.value.code == "credential_burnt"

    # Terminal until a human re-consents: further calls refuse without contacting the
    # provider again.
    attempted = provider.exchanges

    with pytest.raises(CoreException):
        await call_api(store, provider)

    assert provider.exchanges == attempted

    await authorize(store, provider)
    assert await call_api(store, provider) == "ok"


async def test_the_stored_grant_carries_provider_metadata_forward() -> None:
    """An account-specific endpoint must survive every rotation, or the next exchange
    cannot be addressed."""

    provider = DemoOAuthProvider()
    ctx, _, _runner, _sweeper = build_context(provider)
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    provider.expire_access_token()
    await call_api(store, provider)

    assert (await store.get(GRANT_REF)).metadata["host"] == "crm.example"


async def test_the_sweep_keeps_an_idle_grant_alive_without_callers() -> None:
    """The example's proactive half: no API call happens, yet the grant is exchanged.

    The discriminating assertion is the provider's exchange counter moving while nothing
    calls ``call_api`` — the on-demand path alone can never do that, which is the entire
    reason the sweeper exists.
    """

    import asyncio
    from datetime import timedelta

    from examples.recipes.rotating_credentials.app import sweep_act

    provider = DemoOAuthProvider()
    ctx, _, runner, sweeper = build_context(provider, refresh_if_idle_for=timedelta(milliseconds=5))
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    exchanges_after_authorize = provider.exchanges
    before = await store.get(GRANT_REF)

    await asyncio.sleep(0.02)  # the idle quarter, compressed
    await sweep_act(ctx, runner, sweeper, provider)

    assert provider.exchanges == exchanges_after_authorize + 1
    after = await store.get(GRANT_REF)
    assert after.version != before.version
    assert provider.accepts(after.access_token)


async def test_the_sweep_reports_a_burnt_grant_instead_of_retrying_it() -> None:
    """The other sweep outcome: a revoked grant surfaces as needing re-consent."""

    import asyncio
    from datetime import timedelta

    provider = DemoOAuthProvider()
    ctx, _, runner, sweeper = build_context(provider, refresh_if_idle_for=timedelta(milliseconds=5))
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    await store.burn(GRANT_REF, reason="admin revoked access upstream")
    exchanges_before = provider.exchanges

    await asyncio.sleep(0.02)
    record = await sweeper.sweep_now(ctx)
    while await runner.recover(ctx, limit=10):
        pass

    assert record.output_json is not None
    assert record.output_json["needs_reauthorization"] == [GRANT_REF.path]
    assert provider.exchanges == exchanges_before, "a burnt grant must never be presented"
