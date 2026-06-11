"""Mock idempotency TTL parity with the Redis adapter.

Both pending claims and done records expire after the adapter ``ttl``
(refreshed on commit); expired entries are treated as absent on access.
Time is controlled through the TimeSource seam (``bind_time_source`` +
``FrozenTimeSource``) so expiry is asserted without sleeping.
"""

from datetime import UTC, datetime, timedelta

import pytest

from forze.application.contracts.idempotency import IdempotencyRecord
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_mock.adapters import MockIdempotencyAdapter, MockState

# ----------------------- #

_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
_TTL = timedelta(seconds=30)


def _adapter(st: MockState | None = None) -> MockIdempotencyAdapter:
    return MockIdempotencyAdapter(
        state=st or MockState(),
        namespace="idem",
        ttl=_TTL,
    )


# ----------------------- #


async def test_pending_claim_expires_after_ttl_and_begin_succeeds_again() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None  # claim taken

        # Unexpired pending claim still blocks (crashed-flow simulation).
        clock.instant = _T0 + _TTL - timedelta(seconds=1)
        with pytest.raises(CoreException):
            await idem.begin("op", "k", "h")

        # Idempotency window elapsed: the stale claim no longer blocks.
        clock.instant = _T0 + _TTL + timedelta(seconds=1)
        assert await idem.begin("op", "k", "h") is None


async def test_done_record_expires_after_ttl_and_reexecutes() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)
    record = IdempotencyRecord(result=b"ok")

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None
        await idem.commit("op", "k", "h", record)
        assert await idem.begin("op", "k", "h") == record  # dedup while live

        # Done record expired: re-execution path opens (begin claims afresh).
        clock.instant = _T0 + _TTL + timedelta(seconds=1)
        assert await idem.begin("op", "k", "h") is None


async def test_commit_refreshes_ttl_from_commit_time() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)
    record = IdempotencyRecord(result=b"ok")

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None

        # Commit near the end of the pending window refreshes the expiry.
        clock.instant = _T0 + _TTL - timedelta(seconds=1)
        await idem.commit("op", "k", "h", record)

        # Past the original claim expiry but within the refreshed window.
        clock.instant = _T0 + _TTL + timedelta(seconds=10)
        assert await idem.begin("op", "k", "h") == record


async def test_commit_after_claim_expired_raises_conflict() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None

        clock.instant = _T0 + _TTL + timedelta(seconds=1)
        with pytest.raises(CoreException):
            await idem.commit("op", "k", "h", IdempotencyRecord(result=b"late"))


async def test_unexpired_entries_behave_as_before() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)
    record = IdempotencyRecord(result=b"ok")

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None
        await idem.commit("op", "k", "h", record)

        clock.instant = _T0 + timedelta(seconds=10)
        assert await idem.begin("op", "k", "h") == record
        with pytest.raises(CoreException):
            await idem.begin("op", "k", "other-hash")


async def test_fail_releases_pending_claim_before_ttl() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        assert await idem.begin("op", "k", "h") is None
        await idem.fail("op", "k", "h")

        # Same instant — no TTL needed: the claim was explicitly released.
        assert await idem.begin("op", "k", "h") is None


async def test_fail_leaves_done_records_and_foreign_claims_untouched() -> None:
    idem = _adapter()
    clock = FrozenTimeSource(instant=_T0)
    record = IdempotencyRecord(result=b"ok")

    with bind_time_source(clock):
        # A done record survives fail().
        assert await idem.begin("op", "done", "h") is None
        await idem.commit("op", "done", "h", record)
        await idem.fail("op", "done", "h")
        assert await idem.begin("op", "done", "h") == record

        # A pending claim for a different payload hash survives fail().
        assert await idem.begin("op", "k", "h") is None
        await idem.fail("op", "k", "other-hash")
        with pytest.raises(CoreException):
            await idem.begin("op", "k", "h")


def test_non_positive_ttl_is_rejected() -> None:
    with pytest.raises(CoreException):
        MockIdempotencyAdapter(
            state=MockState(),
            namespace="idem",
            ttl=timedelta(seconds=0),
        )
