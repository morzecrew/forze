"""Opt-in durability for :class:`~forze_mock.state.MockState` — the MVP that survives a restart.

``forze_mock`` is the zero-infrastructure backend and it forgets everything on exit. That is
correct for tests, where fresh state per case is the point, and useless for *running* an
application: an MVP restarts and its orders are gone. The answers were ``docker compose up``
or losing the data.

This adds the third answer, to the backend that already exists rather than to a new one. The
state's data fields are written to one file at shutdown and read back at startup, wired
through an ordinary :class:`~forze.application.contracts.execution.LifecycleStep`. No adapter,
port, capability or semantic of the mock changes, and nothing joins any conformance battery —
which is the whole reason this is affordable where a SQLite backend is not.

**It is not a database, and the ways it is not are load-bearing:**

- **No crash durability.** ``kill -9``, an OOM kill or a power cut discards everything since
  the last flush. This is a snapshot, not a write-ahead log.
- **One process.** The file has a single owner, enforced by an advisory lock and a refusal.
- **Everything is in RAM.** The working set is bounded by memory, not by the file.
- **The format is disposable.** It is pickle of ``MockState``'s own fields; a refactor
  invalidates existing files, and the version check refuses rather than migrating.

The upgrade path is Postgres. This exists for the window before that is worth standing up.

**The field classification is the design.** Everything else here is plumbing. Three explicit
buckets — persist, reset, drop — that must together account for every ``attrs`` field on
:class:`~forze_mock.state.MockState`, checked at import. Two of the three exist precisely
because a rule would get them wrong: a monotonic clock stamp and an in-flight transaction id
survive a pickle round-trip perfectly and mean nothing in the process that reads them back.
"""

from __future__ import annotations

import asyncio
import copy
import fcntl
import hashlib
import os
import pickle  # nosec B403
import tempfile
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.background.loop import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import exc
from forze.base.logging import get_logger
from forze_mock.execution.keys import MockRoutedStateDepKey
from forze_mock.state import MockState, _emptied_in_place, _fresh_default

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

log = get_logger(__name__)

SNAPSHOT_MAGIC: Final = b"forze-mock-state"
"""First line of the file. Distinguishes a snapshot from anything else at the path."""

SNAPSHOT_VERSION: Final = 1
"""Bumped when the header or the payload's shape changes. A mismatch refuses; see
:meth:`MockStatePersistence.read`."""

# ....................... #

PERSIST_FIELDS: Final = frozenset(
    {
        "documents",
        "counters",
        "cache_kv",
        "cache_pointers",
        "cache_bodies",
        "idempotency",
        "inbox",
        "storage",
        "storage_bytes",
        "storage_buckets",
        "storage_multipart",
        "storage_sse",
        "queues",
        "queue_pending",
        "pubsub_logs",
        "streams",
        "stream_ack",
        "commit_stream_partitions",
        "commit_stream_offsets",
        "analytics_query_hits",
        "analytics_ingest_log",
        "outbox_rows",
        "hlc_checkpoint",
        "search_snapshots",
        "search_snapshot_chunks",
        "graph_vertices",
        "graph_edges",
        "durable_workflows",
        "durable_schedules",
        "durable_events",
        "durable_step_memo",
        "durable_runs",
        "durable_run_schedules",
        "identity",
        # Both of these look like internals a "skip what's private / skip what's a counter"
        # rule would drop, and dropping either corrupts silently rather than loudly.
        "_MockState__seq",  # ids restart at 1 and collide with the rows just restored
        "dlock_fences",  # fencing tokens regress, which is the failure fencing prevents
    }
)
"""The application's data — written out, and read back as it was."""

_LIVE_FIELDS: Final = frozenset(
    {
        "_MockState__lock",
        "_MockState__tx_serializer",
        "rotating_credential_locks",
    }
)
"""The state's machinery rather than its data: a re-entrant lock, the strict-transaction
serializer, and the rotating-credential stripe table.

A new process builds its own, so a restore leaves them alone entirely — rebuilding a lock
table mid-flight would hand the next caller a different lock than the one a waiter is on."""

RESET_FIELDS: Final = _LIVE_FIELDS | frozenset(
    {
        # Data whose values are process-local, and whose types say nothing about that.
        "dlocks",  # holder expiries come from monotonic(), meaningless in a new process
        "mvcc_active",  # in-flight transaction ids that nothing will ever end
        "mvcc_commit_log",  # snapshot bookkeeping, only meaningful against the active set
        "mvcc_version",
    }
)
"""Restored to a fresh value rather than to the saved one.

A restart means every lock holder and every in-flight transaction is gone; the honest
post-restart state is *none held, none active*. Restoring them instead produces a state that
round-trips perfectly and deadlocks: a lock whose expiry is an arbitrary point on a clock that
no longer exists, held by a process that does not."""

DROP_FIELDS: Final = frozenset(
    {
        "tx_read_only_calls",
        "storage_presigns",
        "authn_events",
    }
)
"""Never written. Test observability by their own declaration — a running application does not
need them across a restart.

``authn_events`` is the one that had to be decided rather than read off the field: it holds
:class:`~forze.application.contracts.authn.AuthnEvent` instances, so persisting it would pin a
disposable file to a contract type. Dropping it does not make the file class-free — a document
namespace is a ``JournalingStore`` once anything has written to it, and whatever an application
puts in a mock store is pickled along with it — but it keeps forze's own contract classes out of
the format, which is the part this side controls."""


# ....................... #


def _classified() -> dict[str, attrs.Attribute[Any]]:
    """Every :class:`~forze_mock.state.MockState` field, by name, once the buckets agree.

    Runs at import, so a substore added to the state without a disposition fails on the
    import rather than in a snapshot six weeks later that quietly lacks it. Both directions
    are checked: an unclassified field is the case this exists for, and a classified name
    that no longer exists is how a rename slips past the first check.
    """

    fields = {field.name: field for field in attrs.fields(MockState)}
    classified = PERSIST_FIELDS | RESET_FIELDS | DROP_FIELDS

    if unclassified := set(fields) - classified:
        raise exc.configuration(
            "MockState fields have no persistence disposition: "
            f"{', '.join(sorted(unclassified))}. Add each to PERSIST_FIELDS, RESET_FIELDS or "
            "DROP_FIELDS in forze_mock.persistence — persisting a store by default is how a "
            "process-local value comes back in a process it cannot mean anything in.",
            code="mock_state_field_unclassified",
        )

    if stale := classified - set(fields):
        raise exc.configuration(
            f"Persistence classifies fields MockState no longer has: {', '.join(sorted(stale))}",
            code="mock_state_field_unknown",
        )

    return fields


_FIELDS: Final = _classified()

_FINGERPRINT: Final = hashlib.blake2b(
    "\n".join(sorted(PERSIST_FIELDS)).encode("utf-8"),
    digest_size=8,
).hexdigest()
"""Digest of the persisted field set. A file written before a field was added or removed is
refused by this rather than restored into a state whose shape has moved."""

_BUILD_HEADER: Final = (str(SNAPSHOT_VERSION).encode("utf-8"), _FINGERPRINT.encode("utf-8"))
"""The two header lines a snapshot this build wrote carries, written and compared from here so
the two halves cannot drift into disagreeing about the same file."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockStatePersistence:
    """A snapshot file for one :class:`~forze_mock.state.MockState`.

    Wire it with :func:`mock_state_lifecycle_step`. The hooks take the lock with
    :meth:`acquire`, restore through :meth:`read` and :meth:`install`, write through
    :meth:`capture` and :meth:`write`, and give the path back with :meth:`release`; the two
    pairs are split because only one half of each may leave the event loop — see
    :meth:`capture`. :meth:`load` and :meth:`save` compose them for a caller with no loop to
    keep responsive. Nothing here runs unless it is wired, so an unconfigured mock behaves
    exactly as it did before.
    """

    path: Path
    """Where the snapshot lives. Its parent directory is created if missing."""

    flush_every: timedelta | None = None
    """Interval between periodic flushes, or ``None`` for shutdown-only.

    Off by default: a periodic flush needs a background task, which makes the lifecycle step
    ``requires_long_running`` and so refuses assembly under a ``SERVERLESS`` profile. Set it
    once ``kill -9`` costing the whole session stops being acceptable — which for most people
    is right after the first time it happens."""

    __lock_fd: int | None = attrs.field(default=None, init=False, repr=False)
    """Descriptor of the held lock file, or ``None`` before :meth:`acquire`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.path.name:
            raise exc.configuration(
                f"Mock state persistence needs a file to write and {self.path} names no file. "
                "Give the path a filename — the lock lives beside it under the same name, so "
                "there is nothing for it to be a sibling of.",
                code="mock_state_snapshot_path_nameless",
                details={"path": str(self.path)},
            )

        if self.flush_every is not None and self.flush_every.total_seconds() <= 0:
            raise exc.configuration(
                "Mock state flush_every must be positive. A non-positive interval turns the "
                "flush loop into a spin that rewrites the whole snapshot as fast as the disk "
                "allows, which looks from the outside like the feature working. Leave it unset "
                "for shutdown-only.",
                code="mock_state_flush_interval_not_positive",
                details={"flush_every": str(self.flush_every)},
            )

    # ....................... #

    @property
    def lock_path(self) -> Path:
        """The advisory lock's own file, a sibling of the snapshot.

        Deliberately not the snapshot itself. A flush replaces the snapshot with
        :func:`os.replace`, which swaps the inode — a lock held on the old one stops guarding
        the path the moment the first flush lands, and the next process to start opens a
        different file and locks it successfully. This one is created once and never replaced.
        """

        return self.path.with_name(f"{self.path.name}.lock")

    # ....................... #

    def acquire(self) -> None:
        """Take the single-writer lock, or refuse naming whoever holds it.

        Held for the life of the process. ``flock`` is released by the OS when a process dies,
        so a previous run that crashed leaves nothing to clean up — which matters, because the
        case this guards is exactly the one where the previous process did not exit cleanly.
        """

        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)

        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

        except OSError:
            # flock names no holder, so the holder writes itself in below and we read it back.
            holder = os.read(descriptor, 32).decode("utf-8", "replace").strip() or "unknown"
            os.close(descriptor)

            raise exc.configuration(
                f"Mock state snapshot {self.path} is already owned by pid {holder}. One process "
                "owns a snapshot; a second would overwrite the first's data on the way out.",
                code="mock_state_snapshot_locked",
                details={"path": str(self.path), "holder": holder},
            ) from None

        os.ftruncate(descriptor, 0)
        os.write(descriptor, str(os.getpid()).encode("utf-8"))

        self.__lock_fd = descriptor

    # ....................... #

    def release(self) -> None:
        """Release the single-writer lock. Idempotent, and never raises.

        Every caller is on a cleanup path — the shutdown hook's ``finally``, the startup
        hook's failure branch — where an exception raised from here would *replace* the
        outcome it trails: the write error an operator needs to see, or the refusal that
        explains why startup stopped. Closing the descriptor is what releases the lock, so
        there is nothing to unlock separately.
        """

        descriptor, self.__lock_fd = self.__lock_fd, None

        if descriptor is None:
            return

        try:
            os.close(descriptor)

        except OSError:
            log.warning(
                "could not release the mock state snapshot lock",
                path=str(self.lock_path),
                exc_info=True,
            )

    # ....................... #

    def read(self) -> dict[str, Any] | None:
        """The snapshot's fields, or ``None`` when there is no snapshot to read.

        Touches nothing shared — a file and a private mapping — so this half is what runs
        off the event loop.

        :raises CoreException: the file exists and is not a snapshot this build can read.
        """

        try:
            raw = self.path.read_bytes()

        except FileNotFoundError:
            return None

        except OSError as error:
            # A directory at the path, a permission the process does not have. Named here
            # because the alternative is a bare errno surfacing from a lifecycle hook, which
            # says nothing about which configured path produced it.
            raise exc.configuration(
                f"Mock state snapshot {self.path} cannot be read: {error}.",
                code="mock_state_snapshot_unopenable",
                details={"path": str(self.path)},
            ) from error

        return self._decode(raw)

    # ....................... #

    def install(self, state: MockState, payload: Mapping[str, Any]) -> None:
        """Put a snapshot read by :meth:`read` onto *state*.

        Runs wherever the state's other writers run — see :meth:`capture` for why that is
        not a detail.
        """

        with state.lock:
            for name in PERSIST_FIELDS:
                self._set_field(state, name, payload[name])

            # Not merely "left alone": a reset happens on the *restore*, so a state that was
            # used before loading does not carry its own stale holders past the boundary.
            for name in RESET_FIELDS - _LIVE_FIELDS:
                self._set_field(state, name, _fresh_default(state, _FIELDS[name].default))

        log.info("restored mock state from a snapshot", path=str(self.path))

    # ....................... #

    def load(self, state: MockState) -> bool:
        """Restore *state* from the snapshot, if one is there.

        :returns: ``True`` when a snapshot was read, ``False`` for a fresh start.
        :raises CoreException: the file exists and is not a snapshot this build can read.
        """

        payload = self.read()

        if payload is None:
            return False

        self.install(state, payload)

        return True

    # ....................... #

    def capture(self, state: MockState) -> dict[str, Any]:
        """Deep-copy the persisted fields, under the state's lock.

        **This half cannot be moved off the event loop**, and the reason is not performance.
        The mock's stores are read and written by adapters that hold ``state.lock`` for the
        access itself and not for the surrounding work — a document read iterates a live view
        without it, on the grounds that nothing else on the loop can interleave. That
        reasoning is sound for the loop and false for a worker thread, so a copy taken in one
        would race writers this class has no business locking against. Copying here and
        serializing elsewhere keeps the guarantee the rest of the mock already relies on.
        """

        with state.lock:
            return copy.deepcopy({name: getattr(state, name) for name in PERSIST_FIELDS})

    # ....................... #

    def write(self, payload: Mapping[str, Any]) -> None:
        """Serialize a captured *payload* and land it, atomically.

        The counterpart of :meth:`capture`: *payload* is private to the caller, so this is
        the half that is safe — and worth — running off the event loop.
        """

        self._write(
            b"\n".join(
                (
                    SNAPSHOT_MAGIC,
                    *_BUILD_HEADER,
                    pickle.dumps(dict(payload), protocol=pickle.HIGHEST_PROTOCOL),
                )
            )
        )

    # ....................... #

    def save(self, state: MockState) -> None:
        """Capture *state* and write it — the two halves, for a caller with no event loop
        to keep responsive."""

        self.write(self.capture(state))

    # ....................... #

    def _set_field(self, state: MockState, name: str, value: Any) -> None:
        """Put *value* on *state*, refilling the existing container where there is one.

        Identity is the point, and it is the same reason :meth:`MockState.clear` works this
        way: the adapters were built when the deps module was, which is before any lifecycle
        hook runs, so each one already holds a reference to the substore it was handed.
        Rebinding here would leave every one of them writing into an object nothing reads.
        """

        if not _emptied_in_place(getattr(state, name, None), value):
            setattr(state, name, value)

    # ....................... #

    def _decode(self, raw: bytes) -> dict[str, Any]:
        """Parse the header, then the payload — in that order, so a stale file is refused
        before its pickle is executed."""

        magic, _, rest = raw.partition(b"\n")
        version, _, rest = rest.partition(b"\n")
        fingerprint, _, body = rest.partition(b"\n")

        if magic != SNAPSHOT_MAGIC:
            raise exc.configuration(
                f"{self.path} is not a mock state snapshot.",
                code="mock_state_snapshot_unrecognized",
                details={"path": str(self.path)},
            )

        if (version, fingerprint) != _BUILD_HEADER:
            raise exc.configuration(
                f"{self.path} was written by a different build of MockState and is not "
                "migrated — development data is disposable by design. Delete it to start fresh.",
                code="mock_state_snapshot_incompatible",
                details={"path": str(self.path)},
            )

        try:
            # The file is written by the application itself, at a path the application
            # chose. Someone who can write there can write the application, so an
            # untrusted snapshot is not a threat this can be defended from here.
            payload: dict[str, Any] = pickle.loads(body)  # nosec B301

            if not isinstance(payload, dict) or set(payload) != PERSIST_FIELDS:
                raise TypeError("the payload is not this build's field mapping")

        except Exception as error:
            raise exc.configuration(
                f"{self.path} is a mock state snapshot this build cannot read: {error}. "
                "Delete it to start fresh.",
                code="mock_state_snapshot_unreadable",
                details={"path": str(self.path)},
            ) from error

        return payload

    # ....................... #

    def _write(self, blob: bytes) -> None:
        """Land *blob* at :attr:`path` whole, or not at all.

        Same directory, so the rename is within one filesystem and therefore atomic: a crash
        during a flush leaves the previous snapshot or the new one, never a half of either.
        """

        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, staged = tempfile.mkstemp(dir=self.path.parent, prefix=f".{self.path.name}.")

        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())

            os.replace(staged, self.path)

        except BaseException:
            Path(staged).unlink(missing_ok=True)

            raise


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _MockStateStartupHook(LifecycleHook):
    """Take the lock, restore the snapshot, and start the flush loop if there is one."""

    state: MockState
    persistence: MockStatePersistence

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="mock_state_flush")),
        init=False,
    )

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the flush loop between flushes. Idempotent."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if ctx.deps.exists(MockRoutedStateDepKey):
            raise exc.configuration(
                "Mock state persistence is configured alongside a routed (per-tenant) state "
                "registry, which would snapshot the unrouted state and silently lose every "
                "tenant's data. Persist a single state, or drop the persistence — a routed "
                "MVP is past what a snapshot file is for.",
                code="mock_state_persistence_routed",
            )

        self.persistence.acquire()

        try:
            payload = await asyncio.to_thread(self.persistence.read)

            if payload is not None:
                self.persistence.install(self.state, payload)

        except BaseException:
            # A startup that took the lock and then failed would wedge the path against its
            # own next attempt, which is a bug that only shows up on the second run.
            self.persistence.release()

            raise

        if self.persistence.flush_every is None or self.control.running:
            return

        self.control.arm()
        self.control.task = asyncio.create_task(self._flush(), name=self.control.loop_name)
        ctx.drainables.register(self)

    # ....................... #

    async def _flush(self) -> None:
        """Flush on an interval until asked to stop; one failed flush does not end the loop."""

        delay = (self.persistence.flush_every or timedelta()).total_seconds()

        while True:
            if await self.control.sleep_or_stop(delay):
                return

            try:
                await asyncio.to_thread(
                    self.persistence.write,
                    self.persistence.capture(self.state),
                )

            # A cancellation still ends the loop: `CancelledError` is a `BaseException`, so
            # the clause below cannot catch it and does not need a guard in front of it.
            except Exception:
                log.exception("periodic mock state flush failed", path=str(self.persistence.path))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _MockStateShutdownHook(LifecycleHook):
    """Stop the flush loop, write the final snapshot, release the lock."""

    state: MockState
    persistence: MockStatePersistence
    startup: _MockStateStartupHook

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)

        try:
            await asyncio.to_thread(
                self.persistence.write,
                self.persistence.capture(self.state),
            )

        finally:
            # A process that cannot write its snapshot still has no business holding the
            # path. `release` is quiet by contract, so it cannot replace the write's error.
            self.persistence.release()


# ....................... #


def mock_state_lifecycle_step(
    name: str = "mock_state_persistence",
    *,
    state: MockState,
    persistence: MockStatePersistence,
) -> LifecycleStep:
    """Load *state* from *persistence* at startup and write it back at shutdown.

    ``mutates_shared_state`` stays ``False``: the file is process-local, and the lock in front
    of it means a second replica refuses rather than races.

    :param name: The step's id.
    :param state: The state the deps module was built with — the same object, not a copy.
    :param persistence: Where the snapshot lives, and how often it is written.
    """

    startup = _MockStateStartupHook(state=state, persistence=persistence)

    return LifecycleStep(
        id=name,
        startup=startup,
        shutdown=_MockStateShutdownHook(
            state=state,
            persistence=persistence,
            startup=startup,
        ),
        requires_long_running=persistence.flush_every is not None,
    )
