"""Shared ``IdempotencyPort`` conformance battery: every promise, on every store.

The port's docstrings make eight testable promises, and all three stores honour them — but
each store was verified against a *different subset*, so the plane had no statement that
they agree. The gaps were not symmetric: the payload-hash and in-progress refusals were
pinned on Postgres and Redis but not the oracle, ``fail`` was pinned on the oracle and
Postgres but not Redis, and the "a claim you do not own is left alone" promise was pinned
on the oracle only. This battery is that statement.

The unowned-claim promise is the one worth the most: ``fail`` releases a pending claim so a
legitimate retry can re-execute, and if it released a claim belonging to a *different*
payload it would hand a duplicate request permission to run. That is the failure this port
exists to prevent, so it is asserted on all three rather than on the store that happened to
have a test.

What each check pins:

1. A fresh claim returns ``None`` — nothing stored yet, the caller should execute.
2. A completed operation replays its record instead of re-executing.
3. The same key with a different payload is refused (the idempotency-key safety property).
4. A second claim while the first is in flight is refused.
5. ``key=None`` skips idempotency entirely — no claim, no record, always re-executes.
6. ``fail`` releases the caller's own pending claim, so a retry can execute.
7. ``fail`` leaves a claim for a *different* payload hash untouched.
8. ``fail`` leaves a *completed* record untouched — it releases claims, not results.
9. ``commit`` without a matching pending claim is refused rather than writing a record.
10. A lapsed claim stops blocking its key — a crashed operation does not hold it forever.
11. A lapsed record re-executes instead of replaying, which is what makes the TTL a window.

Checks 3 and 4 are the control for 7: they are what make "the claim is still there" after
an unowned ``fail`` observable at all.

Checks 10 and 11 cover the dedup **window**, which the port's own docstring promises ("a
duplicate within the record's TTL replays … one that arrives after the TTL re-executes")
and which nothing here asserted: every store tested expiry against a different subset (the
oracle seven ways, Postgres once, Redis not at all), so the plane had no statement that
they agree about it either.

**What is deliberately not asserted, and why.** An operation that outlives its own claim
may find the key reclaimed — by a *duplicate of the same request*, which carries the same
``op``, key and ``payload_hash``. Nothing in this port's signature distinguishes the two
callers, so no store can refuse the late one: verified against all four, including the
Redis compare-and-set, whose byte-exact fence matches because the reclaimer wrote
byte-identical claim metadata. Fencing it needs an ownership handle the contract does not
carry, so a check for it would fail everywhere and prove only that the promise is
unmakeable. What each store does when the claim lapsed and *nobody* reclaimed it follows
from its expiry mechanism rather than a decision — Redis and the oracle refuse (the claim
is simply gone), Postgres and Mongo complete the record — and both outcomes are safe, so
asserting either would freeze an accident into a contract.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest

from forze.application.contracts.idempotency import IdempotencyPort, IdempotencyRecord
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

OP = "battery_op"
"""Operation name every check uses; isolation comes from the per-check key instead."""

HASH_A = "hash-aaaa"
HASH_B = "hash-bbbb"
"""Two payload hashes for the same key — the axis checks 3 and 7 turn on."""

RESULT_A = b'{"outcome":"first"}'
RESULT_B = b'{"outcome":"second"}'


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencyHarness:
    """One store's seam for the battery."""

    store: IdempotencyPort
    """The store under test."""

    backend: str
    """Label used in assertion messages, so a failure names the store that disagreed."""

    key: Callable[[], str]
    """Mint a key unused by any other check.

    A factory rather than a fixed key because Postgres and Redis keep state across the
    checks in one session, so a shared key would make the battery order-dependent — and an
    order-dependent conformance suite is one that passes for the wrong reason.
    """

    store_with_ttl: Callable[[timedelta], IdempotencyPort]
    """Mint a second store over the same backing state with a different dedup window.

    The TTL checks need a claim to lapse while the test watches. Rather than wait out
    :attr:`store`'s window (an hour, so the other checks never race the clock), they mint a
    short-window store and sleep past it — the same rows, a different `IdempotencySpec.ttl`.
    """

    min_ttl: timedelta = timedelta(milliseconds=50)
    """The shortest dedup window this store accepts, and so how long a TTL check waits.

    Per-store because Redis keeps its claim under a native key TTL and refuses anything
    below one second; the others compare a stored timestamp and take milliseconds. A single
    shared value would make every leg pay the slowest store's floor.
    """


Check = Callable[[IdempotencyHarness], Any]
"""One battery check."""


# ....................... #


def _record(result: bytes = RESULT_A) -> IdempotencyRecord:
    return IdempotencyRecord(result=result)


async def check_a_fresh_claim_returns_none(h: IdempotencyHarness) -> None:
    """Nothing stored for this key yet: the caller is told to execute."""

    assert await h.store.begin(OP, h.key(), HASH_A) is None, h.backend


async def check_a_completed_operation_replays_its_record(h: IdempotencyHarness) -> None:
    """After ``commit``, a duplicate gets the stored result instead of re-executing."""

    key = h.key()

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend
    await h.store.commit(OP, key, HASH_A, _record())

    replayed = await h.store.begin(OP, key, HASH_A)

    assert replayed is not None, h.backend
    assert replayed.result == RESULT_A, h.backend


async def check_a_payload_hash_mismatch_is_refused(h: IdempotencyHarness) -> None:
    """One key, two payloads: the second is refused rather than served the first's result.

    Reusing an idempotency key for different arguments is a client bug, and answering it
    with the earlier result would silently return someone the wrong outcome.
    """

    key = h.key()
    await h.store.begin(OP, key, HASH_A)
    await h.store.commit(OP, key, HASH_A, _record())

    with pytest.raises(CoreException) as ei:
        await h.store.begin(OP, key, HASH_B)

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend


async def check_an_in_progress_duplicate_is_refused(h: IdempotencyHarness) -> None:
    """A second claim while the first is still running is refused, not queued."""

    key = h.key()

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend

    with pytest.raises(CoreException) as ei:
        await h.store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend


async def check_a_null_key_skips_idempotency(h: IdempotencyHarness) -> None:
    """No key means no idempotency: nothing is claimed, nothing is stored, all calls run."""

    assert await h.store.begin(OP, None, HASH_A) is None, h.backend

    # Neither of these may store anything reachable by a later call...
    await h.store.commit(OP, None, HASH_A, _record())
    await h.store.fail(OP, None, HASH_A)

    # ...so a second pass still reports "not seen" rather than replaying or conflicting.
    assert await h.store.begin(OP, None, HASH_A) is None, h.backend


async def check_fail_releases_the_claim_for_retry(h: IdempotencyHarness) -> None:
    """Releasing a failed operation's claim lets a legitimate retry execute at once.

    Without it the retry would wait out the claim's TTL, which is sized for the redelivery
    horizon rather than for a human pressing the button again.
    """

    key = h.key()
    await h.store.begin(OP, key, HASH_A)
    await h.store.fail(OP, key, HASH_A)

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend


async def check_fail_ignores_a_claim_it_does_not_own(h: IdempotencyHarness) -> None:
    """A release for a *different* payload leaves the live claim in place.

    The dangerous direction: if ``fail`` dropped any claim under the key, a retry carrying
    different arguments could clear the in-flight operation's claim and let a duplicate
    run alongside it. The surviving refusal below is what proves the claim is still held.
    """

    key = h.key()

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend

    await h.store.fail(OP, key, HASH_B)

    with pytest.raises(CoreException) as ei:
        await h.store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend


async def check_fail_leaves_a_completed_record_intact(h: IdempotencyHarness) -> None:
    """``fail`` releases pending claims, never results: a stored outcome survives it.

    A late failure signal arriving after the operation already committed must not erase the
    record, or the next duplicate would re-execute an operation that already happened.
    """

    key = h.key()
    await h.store.begin(OP, key, HASH_A)
    await h.store.commit(OP, key, HASH_A, _record(RESULT_B))

    await h.store.fail(OP, key, HASH_A)

    replayed = await h.store.begin(OP, key, HASH_A)

    assert replayed is not None, h.backend
    assert replayed.result == RESULT_B, h.backend


async def check_commit_without_a_matching_claim_is_refused(h: IdempotencyHarness) -> None:
    """Committing a result nobody claimed is a conflict, not a silent write.

    The claim may have expired and been re-taken by another writer; writing over it would
    replace that writer's operation with this one's result.
    """

    with pytest.raises(CoreException) as ei:
        await h.store.commit(OP, h.key(), HASH_A, _record())

    assert ei.value.kind == ExceptionKind.CONFLICT, h.backend


# ....................... #


async def _lapsed_claim(h: IdempotencyHarness, key: str, payload_hash: str = HASH_A) -> Any:
    """Take a claim through a short-window store and wait until it has lapsed.

    Returns that store, because the checks below need to keep speaking as the operation
    whose claim just expired — that caller is the one whose late ``commit`` and ``fail``
    the promises are about.
    """

    short = h.store_with_ttl(h.min_ttl)

    assert await short.begin(OP, key, payload_hash) is None, h.backend
    await asyncio.sleep(h.min_ttl.total_seconds() * 1.5 + 0.05)

    return short


async def check_a_lapsed_claim_is_reclaimable(h: IdempotencyHarness) -> None:
    """Past its window a claim stops blocking the key, so a crashed operation that never
    released does not hold its key until someone intervenes."""

    key = h.key()
    await _lapsed_claim(h, key)

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend


async def check_a_lapsed_record_re_executes(h: IdempotencyHarness) -> None:
    """The dedup window is a window: past it the stored result is not replayed.

    This is the promise ``IdempotencySpec.ttl`` exists to size — a duplicate arriving later
    than the window runs again, which is why the TTL must cover the redelivery horizon.
    """

    key = h.key()
    short = h.store_with_ttl(h.min_ttl)

    assert await short.begin(OP, key, HASH_A) is None, h.backend
    await short.commit(OP, key, HASH_A, _record())
    await asyncio.sleep(h.min_ttl.total_seconds() * 1.5 + 0.05)

    assert await h.store.begin(OP, key, HASH_A) is None, h.backend


# ....................... #

IDEMPOTENCY_BATTERY: tuple[Check, ...] = (
    check_a_fresh_claim_returns_none,
    check_a_completed_operation_replays_its_record,
    check_a_payload_hash_mismatch_is_refused,
    check_an_in_progress_duplicate_is_refused,
    check_a_null_key_skips_idempotency,
    check_fail_releases_the_claim_for_retry,
    check_fail_ignores_a_claim_it_does_not_own,
    check_fail_leaves_a_completed_record_intact,
    check_commit_without_a_matching_claim_is_refused,
    check_a_lapsed_claim_is_reclaimable,
    check_a_lapsed_record_re_executes,
)
