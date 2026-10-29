"""Shared ``TxLockPort`` conformance battery.

The port promises a lock the transaction holds until it ends: another transaction taking the key
waits until commit or rollback, and never earlier. Postgres keeps that promise with
``pg_advisory_xact_lock``; the in-memory store with the per-owner write locks its transactions
hold. A mock lock released at the statement, or held past a rollback, would make every test
written against it prove the wrong thing, so the same checks run against both.

Used by:

- ``tests/unit/test_forze_mock/test_mock_tx_lock_conformance.py`` (the oracle)
- ``tests/integration/test_forze_postgres/test_pg_tx_lock_conformance.py``
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.transaction import TX_LOCK_OUTSIDE_TRANSACTION_CODE
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

WAIT = 0.3
"""How long a contender is given to take a key it must not get, before it counts as waiting."""


class _Rollback(RuntimeError):
    """Raised inside a transaction to abort it, and swallowed by the check that raises it."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class TxLockHarness:
    """One backend's seam for the transaction-scoped lock battery."""

    first: ExecutionContext
    """One session."""

    second: ExecutionContext
    """Another session over the same store: a different connection where there are any."""

    route: str
    """The transaction route both contexts bind a lock port for."""

    backend: str
    """Label used in assertion messages."""


Check = Callable[[TxLockHarness], Any]
"""One battery check."""


def _key() -> int:
    # Distinct per check, so checks sharing one database never contend with each other.
    return uuid4().int >> 66


async def _holder(
    ctx: ExecutionContext,
    route: str,
    key: int,
    *,
    held: asyncio.Event,
    release: asyncio.Event,
    rollback: bool = False,
) -> None:
    try:
        async with ctx.tx_ctx.scope(route):
            await ctx.tx_locks(route).hold(key)
            held.set()
            await release.wait()

            if rollback:
                raise _Rollback

    except _Rollback:
        pass


async def _waits_until_released(h: TxLockHarness, *, rollback: bool) -> None:
    key = _key()
    held, release, taken = asyncio.Event(), asyncio.Event(), asyncio.Event()
    holder = asyncio.create_task(
        _holder(h.first, h.route, key, held=held, release=release, rollback=rollback)
    )
    await asyncio.wait_for(held.wait(), timeout=5)

    async def contender() -> None:
        async with h.second.tx_ctx.scope(h.route):
            await h.second.tx_locks(h.route).hold(key)
            taken.set()

    task = asyncio.create_task(contender())

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(taken.wait(), timeout=WAIT)

    release.set()
    await asyncio.wait_for(asyncio.gather(holder, task), timeout=5)

    assert taken.is_set(), h.backend


# ....................... #


async def check_a_held_key_waits_until_the_holder_commits(h: TxLockHarness) -> None:
    """The promise: a second transaction gets the key only once the first has committed."""

    await _waits_until_released(h, rollback=False)


async def check_a_rollback_releases_the_key(h: TxLockHarness) -> None:
    """A transaction that rolls back gives its keys back as one that commits does."""

    await _waits_until_released(h, rollback=True)


async def check_another_key_does_not_wait(h: TxLockHarness) -> None:
    """The leg a single global lock would fail: different keys proceed at once."""

    held, release = asyncio.Event(), asyncio.Event()
    holder = asyncio.create_task(_holder(h.first, h.route, _key(), held=held, release=release))
    await asyncio.wait_for(held.wait(), timeout=5)

    async with h.second.tx_ctx.scope(h.route):
        await asyncio.wait_for(h.second.tx_locks(h.route).hold(_key()), timeout=WAIT * 3)

    release.set()
    await asyncio.wait_for(holder, timeout=5)


async def check_a_key_already_held_is_taken_again_without_waiting(h: TxLockHarness) -> None:
    """Re-entrant within one transaction, in any order and any number of calls."""

    first, second = _key(), _key()

    async with h.first.tx_ctx.scope(h.route):
        await h.first.tx_locks(h.route).hold(first, second)
        await asyncio.wait_for(h.first.tx_locks(h.route).hold(second, first), timeout=WAIT * 3)


async def check_outside_a_transaction_it_is_refused(h: TxLockHarness) -> None:
    """A lock that would end with its own statement is refused, not silently taken."""

    with pytest.raises(CoreException) as caught:
        await h.first.tx_locks(h.route).hold(_key())

    assert caught.value.kind is ExceptionKind.PRECONDITION, h.backend
    assert caught.value.code == TX_LOCK_OUTSIDE_TRANSACTION_CODE, h.backend


async def check_opposite_orders_refuse_one_side_as_concurrency(h: TxLockHarness) -> None:
    """Two transactions taking two keys in opposite orders: one is refused, one finishes.

    Retryable ``concurrency`` on both backends, so a retry policy treats the cycle alike.
    """

    a, b = _key(), _key()
    first_held, second_held = asyncio.Event(), asyncio.Event()

    async def take(
        ctx: ExecutionContext, keys: tuple[int, int], mine: asyncio.Event, theirs: asyncio.Event
    ) -> None:
        async with ctx.tx_ctx.scope(h.route):
            await ctx.tx_locks(h.route).hold(keys[0])
            mine.set()
            await theirs.wait()
            await ctx.tx_locks(h.route).hold(keys[1])

    results = await asyncio.wait_for(
        asyncio.gather(
            take(h.first, (a, b), first_held, second_held),
            take(h.second, (b, a), second_held, first_held),
            return_exceptions=True,
        ),
        timeout=15,
    )

    refused = [r for r in results if isinstance(r, CoreException)]

    assert len(refused) == 1, (h.backend, results)
    assert refused[0].kind is ExceptionKind.CONCURRENCY, h.backend
    assert results.count(None) == 1, (h.backend, results)


TX_LOCK_BATTERY: tuple[Check, ...] = (
    check_a_held_key_waits_until_the_holder_commits,
    check_a_rollback_releases_the_key,
    check_another_key_does_not_wait,
    check_a_key_already_held_is_taken_again_without_waiting,
    check_outside_a_transaction_it_is_refused,
    check_opposite_orders_refuse_one_side_as_concurrency,
)
