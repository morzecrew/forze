"""Durable ``MockState`` — what survives a restart, what must not, and what refuses.

The interesting failures here are not "the data came back wrong". They are the ones where the
data comes back *perfectly* and means nothing in the process reading it: a lock whose expiry
is a point on a monotonic clock that no longer exists, a transaction id nothing will ever end,
an id sequence that restarts at 1 and hands out keys the restored rows already hold. So every
round-trip check has a twin asserting the opposite direction, and the classification is
checked against ``MockState`` itself rather than against a list someone maintained.

# covers: MockStatePersistence, mock_state_lifecycle_step
"""

from __future__ import annotations

import asyncio
import os
import pickle
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import AuthnEvent, AuthnEventKind
from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.lifecycle import LifecyclePlan
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockRoutedStateRegistry
from forze_mock.adapters.counter import MockCounterAdapter
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.adapters.idempotency import MockIdempotencyAdapter
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.persistence import (
    DROP_FIELDS,
    PERSIST_FIELDS,
    RESET_FIELDS,
    SNAPSHOT_MAGIC,
    MockStatePersistence,
    _classified,
    mock_state_lifecycle_step,
)
from forze_mock.state import MockState
from tests.support.counter_conformance import COUNTER_BATTERY, CounterHarness
from tests.support.execution_context import context_from_deps
from tests.support.idempotency_conformance import IDEMPOTENCY_BATTERY, IdempotencyHarness
from tests.support.storage_conformance import STORAGE_BATTERY, StorageHarness

# ----------------------- #


def _persistence(tmp_path: Path, **overrides: Any) -> MockStatePersistence:
    return MockStatePersistence(path=tmp_path / "mvp.state", **overrides)


# ....................... #


async def _populated_by_the_batteries(state: MockState) -> None:
    """Fill *state* by running the shared conformance batteries against it.

    Deliberately not a hand-built state. A state written by hand reaches the substores its
    author remembered, which is the same set of substores they classified — the two blind
    spots line up exactly, and the round-trip check passes over the gap. The batteries write
    what the planes actually write.
    """

    run = uuid4().hex[:8]

    counters = CounterHarness(
        counter=MockCounterAdapter(state=state, namespace="conformance"),
        suffix=lambda name: f"{name}-{run}",
        # Supplied so the tenant-partition leg runs rather than skipping: partitioned keys
        # are a shape of the counters store, and a shape that never reached the file would
        # not have been noticed by a round-trip over unpartitioned ones.
        for_tenant=lambda tenant: MockCounterAdapter(
            state=state,
            namespace="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        ),
    )

    for check in COUNTER_BATTERY:
        await check(counters)

    storage_port = MockStorageAdapter(state=state, bucket="files")
    storage = StorageHarness(
        cmd=storage_port,
        query=storage_port,
        key=lambda name: f"{name}-{run}",
        for_bucket=lambda name: (
            MockStorageAdapter(state=state, bucket=name),
            MockStorageAdapter(state=state, bucket=name),
        ),
    )

    for check in STORAGE_BATTERY:
        await check(storage)

    idempotency = IdempotencyHarness(
        backend="mock",
        key=lambda: f"battery-{uuid4().hex[:12]}",
        store_for=lambda ttl, owner: MockIdempotencyAdapter(
            state=state,
            namespace="idem",
            ttl=ttl,
            owner_provider=lambda: owner,
        ),
    )

    for check in IDEMPOTENCY_BATTERY:
        await check(idempotency)


# ....................... #


def _dlock(state: MockState) -> MockDistributedLockAdapter:
    """The lock plane over *state*, on the one route these tests use."""

    return MockDistributedLockAdapter(
        spec=DistributedLockSpec(name="locks"),
        state=state,
        namespace="locks",
    )


# ....................... #


def _payload(path: Path) -> dict[str, Any]:
    """The snapshot's field mapping, read past the three header lines."""

    _magic, _, rest = path.read_bytes().partition(b"\n")
    _version, _, rest = rest.partition(b"\n")
    _fingerprint, _, body = rest.partition(b"\n")

    return pickle.loads(body)


# ....................... #


def _saved(state: MockState, persistence: MockStatePersistence) -> None:
    persistence.acquire()

    try:
        persistence.save(state)

    finally:
        persistence.release()


# ....................... #


def _loaded(persistence: MockStatePersistence, state: MockState | None = None) -> MockState:
    restored = state if state is not None else MockState()
    persistence.acquire()

    try:
        persistence.load(restored)

    finally:
        persistence.release()

    return restored


# ....................... #


class TestClassification:
    def test_every_field_lands_in_exactly_one_bucket(self) -> None:
        """The check that catches the next substore someone adds — which is the failure mode
        this design has, since a field nobody classified would otherwise be persisted by
        default and a process-local value would come back in a process it cannot mean
        anything in."""

        names = {field.name for field in attrs.fields(MockState)}

        assert names == PERSIST_FIELDS | RESET_FIELDS | DROP_FIELDS
        assert not PERSIST_FIELDS & RESET_FIELDS
        assert not PERSIST_FIELDS & DROP_FIELDS
        assert not RESET_FIELDS & DROP_FIELDS

    # ....................... #

    def test_an_unclassified_field_refuses_at_import(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dropping a name from a bucket stands in for adding a field to ``MockState``: both
        leave a field no bucket claims, and the whole classification is only honest if that
        stops the import rather than being skipped quietly."""

        monkeypatch.setattr(
            "forze_mock.persistence.PERSIST_FIELDS",
            PERSIST_FIELDS - {"documents"},
        )

        with pytest.raises(CoreException, match="documents"):
            _classified()

    # ....................... #

    def test_a_classified_field_that_no_longer_exists_refuses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other direction, and the one a rename takes: the field set still adds up, so
        the first check passes while the snapshot silently loses a store."""

        monkeypatch.setattr(
            "forze_mock.persistence.PERSIST_FIELDS",
            PERSIST_FIELDS | {"documents_renamed_at_some_point"},
        )

        with pytest.raises(CoreException, match="documents_renamed_at_some_point"):
            _classified()


# ....................... #


class TestRoundTrip:
    async def test_a_state_the_batteries_populated_comes_back_field_for_field(
        self, tmp_path: Path
    ) -> None:
        """Field-by-field equality across every persisted store, over a state three planes
        wrote rather than one the author typed."""

        state = MockState()
        await _populated_by_the_batteries(state)

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        restored = _loaded(persistence)

        for name in sorted(PERSIST_FIELDS):
            assert getattr(restored, name) == getattr(state, name), name

        # And the batteries did reach past the fields a hand-built state would have had.
        assert state.counters
        assert state.storage_bytes
        assert state.idempotency

    # ....................... #

    def test_the_restore_refills_the_containers_the_adapters_hold(self, tmp_path: Path) -> None:
        """An adapter is built when the deps module is, which is before any lifecycle hook
        runs — so it already holds a reference to the substore it was handed. A restore that
        rebound the container would leave every adapter reading an object nothing writes."""

        state = MockState()
        state.documents["orders"] = {"o-1": {"id": "o-1", "total": 12}}

        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        fresh = MockState()
        held = fresh.documents  # what an adapter constructed before startup would be holding

        _loaded(persistence, fresh)

        assert fresh.documents is held
        assert held["orders"]["o-1"]["total"] == 12

    # ....................... #

    def test_a_missing_file_is_a_fresh_start(self, tmp_path: Path) -> None:
        """Nothing to restore is the first run, not an error."""

        persistence = _persistence(tmp_path)
        persistence.acquire()

        try:
            assert persistence.load(MockState()) is False

        finally:
            persistence.release()

    # ....................... #

    def test_the_dropped_fields_never_reach_the_file(self, tmp_path: Path) -> None:
        """Test observability, and — for ``authn_events`` — the only field holding domain
        objects, so dropping it is also what leaves the file with no class coupling at all."""

        state = MockState()
        state.tx_read_only_calls.append(True)
        state.storage_presigns.append({"bucket": "files", "key": "k", "method": "GET"})
        state.authn_events.append(
            AuthnEvent(
                kind=AuthnEventKind.LOGIN_SUCCEEDED,
                route="default",
                occurred_at=datetime.now(UTC),
            )
        )

        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        assert set(_payload(persistence.path)) == PERSIST_FIELDS
        assert not DROP_FIELDS & set(_payload(persistence.path))

        restored = _loaded(persistence)

        assert restored.authn_events == []
        assert restored.tx_read_only_calls == []
        assert restored.storage_presigns == []


# ....................... #


class TestResetFields:
    async def test_a_held_lock_does_not_come_back_held(self, tmp_path: Path) -> None:
        """The expiry is a ``monotonic()`` reading, and a monotonic clock has no meaning
        across processes: restored, the lock expires at an arbitrary point or never. The
        honest post-restart answer is that nobody holds anything."""

        state = MockState()
        acquired = await _dlock(state).acquire("job", owner="the-process-that-died")

        assert acquired is not None
        assert state.dlocks["locks"]["job"][0] == "the-process-that-died"

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        restored = _loaded(persistence)

        assert restored.dlocks == {}

        # The other direction, which is the one that matters: a fresh process can take it.
        assert await _dlock(restored).acquire("job", owner="the-process-that-restarted")

    # ....................... #

    def test_in_flight_transactions_do_not_come_back(self, tmp_path: Path) -> None:
        """Restoring them resurrects transactions that no longer exist and that nothing will
        ever end, and the commit log's pruning is only meaningful against that active set."""

        state = MockState()
        state.mvcc_active.extend([3, 4])
        state.mvcc_commit_log.append((3, {"orders": frozenset({"o-1"})}))
        state.mvcc_version = 9

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        restored = _loaded(persistence)

        assert restored.mvcc_active == []
        assert restored.mvcc_commit_log == []
        assert restored.mvcc_version == 0

    # ....................... #

    def test_a_reset_clears_what_the_loading_state_already_held(self, tmp_path: Path) -> None:
        """A reset happens on the restore, not merely by starting from a fresh object — so a
        state that was used before loading does not carry its own stale holders across."""

        state = MockState()
        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        reused = MockState()
        reused.dlocks["default"] = {"job": ("someone", 12345.0)}
        reused.mvcc_active.append(7)

        _loaded(persistence, reused)

        assert reused.dlocks == {}
        assert reused.mvcc_active == []

    # ....................... #

    def test_the_live_primitives_are_the_state_s_own(self, tmp_path: Path) -> None:
        """Machinery, not data: the loading process built its own lock table, and swapping it
        under a waiter would hand the next caller a different lock."""

        state = MockState()
        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        fresh = MockState()
        lock = fresh.lock
        stripes = fresh.rotating_credential_locks

        _loaded(persistence, fresh)

        assert fresh.lock is lock
        assert fresh.rotating_credential_locks is stripes


# ....................... #


class TestMonotonicCounters:
    def test_the_id_sequence_does_not_restart(self, tmp_path: Path) -> None:
        """``_MockState__seq`` is private and looks like an internal a "skip what starts with
        an underscore" rule would drop. Dropped, ids restart at 1 and collide with the rows
        that were just restored."""

        state = MockState()
        issued = [state.next_id() for _ in range(5)]

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        restored = _loaded(persistence)

        assert restored.next_id() not in issued
        assert int(restored.next_id().rsplit("-", 1)[1]) > 5

    # ....................... #

    async def test_fencing_tokens_do_not_regress(self, tmp_path: Path) -> None:
        """A fence that repeats is the exact failure fencing exists to prevent: a stale holder
        writes with a token the store has already seen and is not detected."""

        state = MockState()
        first = await _dlock(state).acquire("job", owner="first")

        assert first is not None
        await _dlock(state).release("job", owner="first")

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        restored = _loaded(persistence)

        after = await _dlock(restored).acquire("job", owner="second")

        assert after is not None
        assert after.token is not None
        assert first.token is not None
        assert after.token > first.token


# ....................... #


class TestFormat:
    def test_a_file_that_is_not_a_snapshot_is_refused(self, tmp_path: Path) -> None:
        """Something else already lives at the configured path — read as a snapshot it would
        be an unpickle of a stranger's bytes."""

        persistence = _persistence(tmp_path)
        persistence.path.write_bytes(b"just some other file\nwith lines\n")

        with pytest.raises(CoreException, match="not a mock state snapshot"):
            _loaded(persistence)

    # ....................... #

    def test_a_snapshot_of_a_different_field_set_is_refused(self, tmp_path: Path) -> None:
        """The fingerprint is what stands between a file written before a store was added and
        a restore into a state whose shape has moved. It refuses; it never migrates."""

        state = MockState()
        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        magic, _, rest = persistence.path.read_bytes().partition(b"\n")
        version, _, rest = rest.partition(b"\n")
        _fingerprint, _, body = rest.partition(b"\n")
        persistence.path.write_bytes(b"\n".join((magic, version, b"0000000000000000", body)))

        with pytest.raises(CoreException, match="different build"):
            _loaded(persistence)

    # ....................... #

    def test_a_truncated_payload_is_refused_not_half_restored(self, tmp_path: Path) -> None:
        """A partial read is a refusal, not a state carrying whichever stores made it in."""

        state = MockState()
        state.documents["orders"] = {"o-1": {"id": "o-1"}}

        persistence = _persistence(tmp_path)
        _saved(state, persistence)

        raw = persistence.path.read_bytes()
        persistence.path.write_bytes(raw[: len(raw) // 2])

        restored = MockState()

        with pytest.raises(CoreException, match="cannot read"):
            _loaded(persistence, restored)

        assert restored.documents == {}


# ....................... #


class TestAtomicity:
    def test_an_interrupted_write_leaves_the_previous_snapshot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Interrupted between the temp file and the rename. The old snapshot is what a
        restart finds, whole and loadable — never a torn file whose second half is missing."""

        state = MockState()
        state.documents["orders"] = {"o-1": {"id": "o-1", "total": 12}}

        persistence = _persistence(tmp_path)
        _saved(state, persistence)
        intact = persistence.path.read_bytes()

        state.documents["orders"]["o-2"] = {"id": "o-2", "total": 30}

        def _interrupted(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("the process died here")

        monkeypatch.setattr(os, "replace", _interrupted)

        with pytest.raises(OSError, match="died here"):
            _saved(state, persistence)

        monkeypatch.undo()

        assert persistence.path.read_bytes() == intact
        assert _loaded(persistence).documents == {"orders": {"o-1": {"id": "o-1", "total": 12}}}

        # And the half-written file is not left behind to accumulate one per failed flush.
        assert [entry.name for entry in tmp_path.iterdir()] == ["mvp.state", "mvp.state.lock"]


# ....................... #


_HOLD = """
import sys, time
from pathlib import Path
from forze_mock.persistence import MockStatePersistence

p = MockStatePersistence(path=Path(sys.argv[1]))
p.acquire()
print("held", flush=True)
time.sleep(60)
"""

_TRY = """
import sys
from pathlib import Path
from forze.base.exceptions import CoreException
from forze_mock.persistence import MockStatePersistence

try:
    MockStatePersistence(path=Path(sys.argv[1])).acquire()
except CoreException as refusal:
    print(refusal.code, refusal.details["holder"], flush=True)
    sys.exit(3)

print("acquired", flush=True)
"""


def _holder(path: Path) -> subprocess.Popen[str]:
    """A real second process holding the lock, up and confirmed before the test proceeds."""

    process = subprocess.Popen(
        [sys.executable, "-c", _HOLD, str(path)],
        stdout=subprocess.PIPE,
        text=True,
    )

    if process.stdout is None or process.stdout.readline().strip() != "held":  # pragma: no cover
        process.kill()
        pytest.fail("the holder process never took the lock")

    return process


class TestSingleWriter:
    def test_a_second_process_is_refused_and_told_who_holds_it(self, tmp_path: Path) -> None:
        """Not coordinated — refused. Two processes flushing the same path would each write
        their whole state over the other's on the way out, and the survivor is whichever
        exited last."""

        persistence = _persistence(tmp_path)
        holder = _holder(persistence.path)

        try:
            with pytest.raises(CoreException, match="already owned by pid") as refusal:
                persistence.acquire()

            assert refusal.value.details is not None
            assert refusal.value.details["holder"] == str(holder.pid)

        finally:
            holder.kill()
            holder.wait(timeout=10)

    # ....................... #

    def test_the_lock_still_guards_the_path_after_a_flush(self, tmp_path: Path) -> None:
        """The reason the lock is a sibling file rather than the snapshot itself: a flush
        replaces the snapshot with :func:`os.replace`, which swaps the inode. A lock held on
        the old one stops guarding the path the moment the first flush lands."""

        persistence = _persistence(tmp_path)
        persistence.acquire()

        try:
            persistence.save(MockState())
            persistence.save(MockState())

            attempt = subprocess.run(
                [sys.executable, "-c", _TRY, str(persistence.path)],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )

            assert attempt.returncode == 3, attempt.stdout
            assert attempt.stdout.split() == ["mock_state_snapshot_locked", str(os.getpid())]

        finally:
            persistence.release()

    # ....................... #

    def test_a_holder_that_died_leaves_nothing_wedged(self, tmp_path: Path) -> None:
        """The whole point of the lock is the case where the previous process did not exit
        cleanly, so the interesting question is whether that case wedges the path. The OS
        releases an ``flock`` on process death — verified rather than assumed."""

        persistence = _persistence(tmp_path)
        holder = _holder(persistence.path)
        holder.kill()
        holder.wait(timeout=10)

        persistence.acquire()
        persistence.release()

    # ....................... #

    def test_release_is_idempotent(self, tmp_path: Path) -> None:
        """Shutdown releases in a ``finally``; a second release must not raise out of a
        lifecycle hook and abort the teardown of every remaining step."""

        persistence = _persistence(tmp_path)
        persistence.acquire()
        persistence.release()
        persistence.release()


# ....................... #


class TestLifecycle:
    async def test_the_step_loads_at_startup_and_writes_at_shutdown(self, tmp_path: Path) -> None:
        """The whole feature, end to end: two runs of the same wiring, one restart between
        them, and the orders are still there."""

        first = MockState()
        module = MockDepsModule(state=first)
        ctx = context_from_deps(module())
        persistence = _persistence(tmp_path)
        plan = LifecyclePlan.from_steps(
            mock_state_lifecycle_step(state=first, persistence=persistence)
        ).freeze()

        await plan.startup(ctx)
        first.documents["orders"] = {"o-1": {"id": "o-1", "total": 12}}
        await plan.shutdown(ctx)

        second = MockState()
        restarted = context_from_deps(MockDepsModule(state=second)())
        again = LifecyclePlan.from_steps(
            mock_state_lifecycle_step(state=second, persistence=_persistence(tmp_path))
        ).freeze()

        await again.startup(restarted)

        try:
            assert second.documents == {"orders": {"o-1": {"id": "o-1", "total": 12}}}

        finally:
            await again.shutdown(restarted)

    # ....................... #

    def test_the_step_is_long_running_only_when_it_flushes(self, tmp_path: Path) -> None:
        """A periodic flush is a background task, and a step that holds one cannot be hosted
        by a function that freezes between invocations. Shutdown-only holds no task, so
        marking it would refuse a ``SERVERLESS`` deployment for nothing."""

        state = MockState()

        assert (
            mock_state_lifecycle_step(
                state=state, persistence=_persistence(tmp_path)
            ).requires_long_running
            is False
        )
        assert (
            mock_state_lifecycle_step(
                state=state,
                persistence=_persistence(tmp_path, flush_every=timedelta(seconds=30)),
            ).requires_long_running
            is True
        )

    # ....................... #

    def test_the_step_does_not_claim_to_mutate_shared_infrastructure(self, tmp_path: Path) -> None:
        """The file is process-local and the lock in front of it means a second replica
        refuses rather than races, so there is nothing for a fleet to stampede."""

        step = mock_state_lifecycle_step(state=MockState(), persistence=_persistence(tmp_path))

        assert step.mutates_shared_state is False

    # ....................... #

    async def test_a_periodic_flush_writes_before_shutdown(self, tmp_path: Path) -> None:
        """ "Shutdown only" means a ``kill -9`` costs the whole session rather than the last
        few minutes. The interval is what buys that back, so it has to actually tick."""

        state = MockState()
        ctx = context_from_deps(MockDepsModule(state=state)())
        persistence = _persistence(tmp_path, flush_every=timedelta(milliseconds=20))
        plan = LifecyclePlan.from_steps(
            mock_state_lifecycle_step(state=state, persistence=persistence)
        ).freeze()

        await plan.startup(ctx)

        try:
            state.documents["orders"] = {"o-1": {"id": "o-1"}}

            for _ in range(1000):
                if persistence.path.exists():
                    break

                await asyncio.sleep(0.01)

            else:  # pragma: no cover - the loop is what the test is waiting on
                pytest.fail("the flush loop never wrote a snapshot")

            assert _payload(persistence.path)["documents"] == {"orders": {"o-1": {"id": "o-1"}}}

        finally:
            await plan.shutdown(ctx)

    # ....................... #

    async def test_a_routed_state_registry_is_refused(self, tmp_path: Path) -> None:
        """Persisting the unrouted state while the adapters read per-tenant ones would write a
        file that is almost empty and lose every tenant's data without saying so. Refused at
        startup rather than at wiring, because that is the first moment the two are both
        visible — and it fires without the user having to hand the step the registry."""

        state = MockState()
        registry = MockRoutedStateRegistry(max_entries=4)
        ctx = context_from_deps(MockDepsModule(state=state, routed_state=registry)())
        plan = LifecyclePlan.from_steps(
            mock_state_lifecycle_step(state=state, persistence=_persistence(tmp_path))
        ).freeze()

        with pytest.raises(CoreException, match="routed"):
            await plan.startup(ctx)

    # ....................... #

    async def test_a_refused_startup_leaves_no_lock_behind(self, tmp_path: Path) -> None:
        """A startup that took the lock and then failed would wedge the path against its own
        next attempt — which is the shape of a bug that only appears on the second run."""

        state = MockState()
        persistence = _persistence(tmp_path)
        persistence.path.write_bytes(b"not a snapshot\n")

        ctx = context_from_deps(MockDepsModule(state=state)())
        plan = LifecyclePlan.from_steps(
            mock_state_lifecycle_step(state=state, persistence=persistence)
        ).freeze()

        with pytest.raises(CoreException):
            await plan.startup(ctx)

        persistence.acquire()
        persistence.release()


# ....................... #


class TestUnwiredIsUntouched:
    def test_nothing_is_written_unless_the_step_is_wired(self, tmp_path: Path) -> None:
        """Off by default, and that is what keeps the mock's other job — being the
        deterministic store a simulation runs against — exactly as it was."""

        state = MockState()
        MockDepsModule(state=state)()
        state.documents["orders"] = {"o-1": {"id": "o-1"}}

        assert list(tmp_path.iterdir()) == []
        assert SNAPSHOT_MAGIC == b"forze-mock-state"
