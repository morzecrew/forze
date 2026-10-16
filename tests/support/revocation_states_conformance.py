"""Shared conformance batteries for the session fast path's two stores.

``RevocationStatesPort`` caches revocation states, and its safety rests on three write rules no
signature can carry: a lookup's ``fill`` never overwrites and never outlives the instant it was
given; a revocation overwrites; an epoch only rises. Redis keeps them in Lua on the server's
clock, the mock in memory on an injected one, so reading either proves nothing about the other.

``PrincipalEpochPort`` keeps the epoch in the database: it reads ``0`` for a principal with no
row, rises by one per raise, rolls back with its transaction, refuses its locking calls outside
one, and on a backend that locks makes a raise and an issue wait for each other.

Used by:

- ``tests/unit/test_forze_mock/test_mock_revocation_conformance.py`` (the oracle)
- ``tests/integration/test_forze_redis/test_redis_revocation_states_conformance.py``
- ``tests/integration/test_forze_postgres/test_pg_principal_epoch_conformance.py``
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    REVOKED_STATE,
    PrincipalEpochPort,
    RevocationStatesPort,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_LONG_MS = 60_000
_SHORT = timedelta(milliseconds=200)
_PAST_SHORT = timedelta(milliseconds=500)

_BLOCKED_FOR = 0.3
"""How long a statement must stay blocked to count as waiting on the other's lock."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class StatesHarness:
    """One backend's seam for the states battery."""

    backend: str
    """Label used in assertion messages."""

    states: RevocationStatesPort
    """The port under test; each check uses fresh principals."""

    wait: Callable[[timedelta], Awaitable[None]]
    """Let at least this much of the backend's time pass."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class EpochHarness:
    """One backend's seam for the epoch battery."""

    backend: str
    """Label used in assertion messages."""

    epochs: PrincipalEpochPort
    """The port under test; each check uses fresh principals."""

    transaction: Callable[[], AbstractAsyncContextManager[Any]]
    """Open a transaction on the connection the store writes through; each task its own."""

    locks: bool
    """Whether a raise and an issue exclude each other. ``False`` declares a backend that takes
    no lock (the mock), whose lock check is then skipped visibly rather than quietly."""


class _Rollback(RuntimeError):
    """Raised inside a transaction to abort it."""


StatesCheck = Callable[[StatesHarness], Any]
EpochCheck = Callable[[EpochHarness], Any]

# ....................... #


async def check_an_empty_read_finds_nothing_at_the_store_time(h: StatesHarness) -> None:
    principal = uuid4()
    before = await h.states.now_ms(principal)
    read = await h.states.read(principal, f"s:{uuid4()}")

    assert (read.epoch, read.state) == (None, None), h.backend
    assert read.now_ms >= before, h.backend


async def check_fill_sets_only_absent_keys(h: StatesHarness) -> None:
    principal, key = uuid4(), f"f:{uuid4()}"
    expire_at = await h.states.now_ms(principal) + _LONG_MS

    await h.states.fill(principal, {EPOCH_STATE_KEY: "3", key: "live|-"}, expire_at_ms=expire_at)
    await h.states.fill(
        principal, {EPOCH_STATE_KEY: "1", key: REVOKED_STATE}, expire_at_ms=expire_at
    )

    read = await h.states.read(principal, key)

    assert (read.epoch, read.state) == ("3", "live|-"), h.backend


async def check_fill_never_outlives_its_instant(h: StatesHarness) -> None:
    principal, key = uuid4(), f"s:{uuid4()}"
    now = await h.states.now_ms(principal)

    # Landed after its instant: nothing is written.
    await h.states.fill(principal, {key: "live|-"}, expire_at_ms=now - 1)
    assert (await h.states.read(principal, key)).state is None, h.backend

    await h.states.fill(
        principal,
        {key: "live|-"},
        expire_at_ms=now + _SHORT // timedelta(milliseconds=1),
    )
    assert (await h.states.read(principal, key)).state == "live|-", h.backend

    await h.wait(_PAST_SHORT)
    assert (await h.states.read(principal, key)).state is None, h.backend


async def check_revoke_overwrites_and_expires(h: StatesHarness) -> None:
    principal, session, family = uuid4(), f"s:{uuid4()}", f"f:{uuid4()}"
    expire_at = await h.states.now_ms(principal) + _LONG_MS

    await h.states.fill(principal, {session: "live|-", family: "live|-"}, expire_at_ms=expire_at)
    await h.states.revoke(principal, [session, family], ttl=_SHORT)

    assert (await h.states.read(principal, session)).state == REVOKED_STATE, h.backend
    assert (await h.states.read(principal, family)).state == REVOKED_STATE, h.backend

    # A lookup cannot replace a revocation that is still present.
    await h.states.fill(principal, {session: "live|-"}, expire_at_ms=expire_at)
    assert (await h.states.read(principal, session)).state == REVOKED_STATE, h.backend

    await h.wait(_PAST_SHORT)
    assert (await h.states.read(principal, session)).state is None, h.backend


async def check_raise_epoch_keeps_the_larger_and_expires(h: StatesHarness) -> None:
    principal, key = uuid4(), f"s:{uuid4()}"
    now = await h.states.now_ms(principal)

    await h.states.fill(principal, {EPOCH_STATE_KEY: "2"}, expire_at_ms=now + _LONG_MS)
    await h.states.raise_epoch(principal, 5, expire_at_ms=now + _LONG_MS)
    assert (await h.states.read(principal, key)).epoch == "5", h.backend

    await h.states.raise_epoch(principal, 4, expire_at_ms=now + _LONG_MS)
    assert (await h.states.read(principal, key)).epoch == "5", h.backend

    # Landed after its instant: nothing is written, and the cached epoch stays.
    await h.states.raise_epoch(principal, 9, expire_at_ms=now - 1)
    assert (await h.states.read(principal, key)).epoch == "5", h.backend

    # The raise sets the expiry, even when it keeps the cached value.
    now = await h.states.now_ms(principal)
    await h.states.raise_epoch(principal, 1, expire_at_ms=now + _SHORT // timedelta(milliseconds=1))
    await h.wait(_PAST_SHORT)
    assert (await h.states.read(principal, key)).epoch is None, h.backend


async def check_raise_epoch_replaces_an_unreadable_value(h: StatesHarness) -> None:
    principal = uuid4()
    expire_at = await h.states.now_ms(principal) + _LONG_MS

    await h.states.fill(principal, {EPOCH_STATE_KEY: "garbage"}, expire_at_ms=expire_at)
    await h.states.raise_epoch(principal, 1, expire_at_ms=expire_at)

    assert (await h.states.read(principal, f"s:{uuid4()}")).epoch == "1", h.backend


async def check_principals_never_share_states(h: StatesHarness) -> None:
    one, two, key = uuid4(), uuid4(), f"f:{uuid4()}"
    expire_at = await h.states.now_ms(one) + _LONG_MS

    await h.states.fill(one, {EPOCH_STATE_KEY: "7", key: "live|-"}, expire_at_ms=expire_at)

    read = await h.states.read(two, key)

    assert (read.epoch, read.state) == (None, None), h.backend


async def check_the_store_clock_moves_forward(h: StatesHarness) -> None:
    principal = uuid4()
    first = await h.states.now_ms(principal)
    await h.wait(_SHORT)
    second = await h.states.now_ms(principal)

    assert second - first >= _SHORT // timedelta(milliseconds=1) - 5, h.backend


STATES_BATTERY: tuple[StatesCheck, ...] = (
    check_an_empty_read_finds_nothing_at_the_store_time,
    check_fill_sets_only_absent_keys,
    check_fill_never_outlives_its_instant,
    check_revoke_overwrites_and_expires,
    check_raise_epoch_keeps_the_larger_and_expires,
    check_raise_epoch_replaces_an_unreadable_value,
    check_principals_never_share_states,
    check_the_store_clock_moves_forward,
)

# ....................... #


async def check_an_unknown_principal_reads_zero(h: EpochHarness) -> None:
    principal = uuid4()

    assert await h.epochs.current(principal) == 0, h.backend

    async with h.transaction():
        assert await h.epochs.hold(principal) == 0, h.backend

    assert await h.epochs.current(principal) == 0, h.backend


async def check_each_raise_adds_one(h: EpochHarness) -> None:
    principal = uuid4()

    async with h.transaction():
        assert await h.epochs.advance(principal) == 1, h.backend
        assert await h.epochs.advance(principal) == 2, h.backend

    async with h.transaction():
        assert await h.epochs.hold(principal) == 2, h.backend

    assert await h.epochs.current(principal) == 2, h.backend


async def check_a_raise_rolls_back_with_its_transaction(h: EpochHarness) -> None:
    principal = uuid4()

    with pytest.raises(_Rollback):
        async with h.transaction():
            await h.epochs.advance(principal)
            raise _Rollback

    assert await h.epochs.current(principal) == 0, h.backend

    async with h.transaction():
        await h.epochs.advance(principal)

    with pytest.raises(_Rollback):
        async with h.transaction():
            await h.epochs.advance(principal)
            raise _Rollback

    assert await h.epochs.current(principal) == 1, h.backend


async def check_locking_calls_refuse_outside_a_transaction(h: EpochHarness) -> None:
    principal = uuid4()

    with pytest.raises(CoreException):
        await h.epochs.hold(principal)

    with pytest.raises(CoreException):
        await h.epochs.advance(principal)

    assert await h.epochs.current(principal) == 0, h.backend


async def _first_waits_for_second(
    h: EpochHarness,
    first: Callable[[], Awaitable[int]],
    second: Callable[[], Awaitable[int]],
) -> int:
    """Hold *first* open in a transaction, run *second* in another, and return its result.

    Fails unless *second* stays blocked while *first* is open.
    """

    entered, release = asyncio.Event(), asyncio.Event()

    async def _hold_first() -> None:
        async with h.transaction():
            await first()
            entered.set()
            await release.wait()

    async def _run_second() -> int:
        async with h.transaction():
            return await second()

    holder = asyncio.create_task(_hold_first())
    await asyncio.wait_for(entered.wait(), timeout=5)
    waiter = asyncio.create_task(_run_second())

    try:
        done, _ = await asyncio.wait({waiter}, timeout=_BLOCKED_FOR)
        assert not done, f"{h.backend}: the second call did not wait for the first"

    finally:
        release.set()
        await asyncio.wait_for(holder, timeout=5)

    return await asyncio.wait_for(waiter, timeout=5)


async def check_a_raise_and_an_issue_wait_for_each_other(h: EpochHarness) -> None:
    if not h.locks:
        pytest.skip(f"{h.backend} takes no lock")

    for existing in (False, True):
        principal = uuid4()

        if existing:
            async with h.transaction():
                await h.epochs.hold(principal)

        # The issue reads the epoch the raise committed.
        read = await _first_waits_for_second(
            h,
            lambda p=principal: h.epochs.advance(p),
            lambda p=principal: h.epochs.hold(p),
        )
        assert read == 1, (h.backend, existing)

        # And a raise waits for an issue that holds the epoch.
        raised = await _first_waits_for_second(
            h,
            lambda p=principal: h.epochs.hold(p),
            lambda p=principal: h.epochs.advance(p),
        )
        assert raised == 2, (h.backend, existing)


EPOCH_BATTERY: tuple[EpochCheck, ...] = (
    check_an_unknown_principal_reads_zero,
    check_each_raise_adds_one,
    check_a_raise_rolls_back_with_its_transaction,
    check_locking_calls_refuse_outside_a_transaction,
    check_a_raise_and_an_issue_wait_for_each_other,
)
