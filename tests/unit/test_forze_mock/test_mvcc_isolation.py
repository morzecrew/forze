"""Mock MVCC isolation — snapshot & serializable over the document store (WS5).

The journal manager honors an operation's declared :class:`IsolationLevel` via a buffered
overlay: a snapshot/serializable transaction reads an as-of-begin snapshot (+ its own buffered
writes) and validates at commit against concurrently-committed write-sets. Two demonstrations
over *real* document ports under the simulation loop:

* **write skew** — two transactions each read both rows then write a different one;
  serializable rejects the second committer (read-write conflict), read-committed/snapshot let
  it through (snapshot permits write-skew by design — the writes don't overlap);
* **lost update** — two transactions read then write the *same* row; snapshot surfaces the
  conflict (write-write, first-committer-wins), read-committed silently loses one.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule
from forze_mock.adapters._mvcc import (  # pyright: ignore[reportPrivateUsage]
    _TOMBSTONE,
    IsolatedStoreView,
    MvccTx,
)
from forze_mock.state import MockState

# ----------------------- #
# Domain — flags (write-skew) and a counter (lost-update), through the document port.


class Flag(Document):
    on: bool = True


class FlagCreate(CreateDocumentCmd):
    on: bool = True


class FlagUpdate(BaseDTO):
    on: bool | None = None


class FlagRead(ReadDocument):
    on: bool


FLAG_SPEC = DocumentSpec(
    name="flags",
    read=FlagRead,
    write=DocumentWriteTypes(domain=Flag, create_cmd=FlagCreate, update_cmd=FlagUpdate),
)


class Counter(Document):
    value: int = 0


class CounterCreate(CreateDocumentCmd):
    value: int = 0


class CounterUpdate(BaseDTO):
    value: int | None = None


class CounterRead(ReadDocument):
    value: int


COUNTER_SPEC = DocumentSpec(
    name="counters",
    read=CounterRead,
    write=DocumentWriteTypes(
        domain=Counter, create_cmd=CounterCreate, update_cmd=CounterUpdate
    ),
)


class TargetCmd(BaseModel):
    target: UUID


# ....................... #
# Handlers — ordinary forze code, no isolation awareness.


@attrs.define(slots=True, kw_only=True)
class _CreateFlag(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        flag = await self.ctx.document.command(FLAG_SPEC).create(FlagCreate())
        return flag.id


@attrs.define(slots=True, kw_only=True)
class _TurnOff(Handler[TargetCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: TargetCmd) -> None:
        # Read every flag (the read-set covers both), and only turn one off while at least
        # two are on — so one stays on. Two concurrent turn-offs both observe "two on".
        page = await self.ctx.document.query(FLAG_SPEC).find_many()
        on_count = sum(1 for hit in page.hits if hit.on)

        if on_count >= 2:
            flag = await self.ctx.document.query(FLAG_SPEC).get(args.target)
            await self.ctx.document.command(FLAG_SPEC).update(
                args.target, flag.rev, FlagUpdate(on=False)
            )


@attrs.define(slots=True, kw_only=True)
class _CreateCounter(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        counter = await self.ctx.document.command(COUNTER_SPEC).create(CounterCreate())
        return counter.id


@attrs.define(slots=True, kw_only=True)
class _Increment(Handler[TargetCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: TargetCmd) -> None:
        counter = await self.ctx.document.query(COUNTER_SPEC).get(args.target)
        # Unconditional write (rev=None): no optimistic-concurrency guard, so a lost update is
        # only prevented by the isolation level, not by the row revision.
        await self.ctx.document.command(COUNTER_SPEC).update(
            args.target, None, CounterUpdate(value=counter.value + 1)
        )


def _registry(isolation: IsolationLevel) -> OperationRegistry:
    plan = (
        OperationPlan()
        .bind_tx()
        .set_route("mock")
        .set_isolation(isolation)
        .finish(deep=False)
    )
    handlers = {
        "create_flag": lambda ctx: _CreateFlag(ctx=ctx),
        "turn_off": lambda ctx: _TurnOff(ctx=ctx),
        "create_counter": lambda ctx: _CreateCounter(ctx=ctx),
        "increment": lambda ctx: _Increment(ctx=ctx),
    }
    return OperationRegistry(
        handlers=handlers,
        plans=dict.fromkeys(handlers, plan),
        descriptors={
            "create_flag": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "turn_off": OperationDescriptor(
                input_type=TargetCmd, output_type=None, description="x"
            ),
            "create_counter": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "increment": OperationDescriptor(
                input_type=TargetCmd, output_type=None, description="x"
            ),
        },
    ).freeze()


# ....................... #


def _run_write_skew(isolation: IsolationLevel) -> tuple[int, list[str]]:
    registry = _registry(isolation)
    out: dict[str, object] = {}

    async def scenario() -> None:
        deps = DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
        ctx = ExecutionContext(deps=deps)

        f1 = await run_operation(registry, "create_flag", None, ctx)
        f2 = await run_operation(registry, "create_flag", None, ctx)

        errors: list[str] = []

        async def turn(target: UUID) -> None:
            try:
                await run_operation(registry, "turn_off", TargetCmd(target=target), ctx)
            except CoreException as error:
                errors.append(error.code or "")

        await asyncio.gather(turn(f1), turn(f2))

        page = await ctx.document.query(FLAG_SPEC).find_many()
        out["on"] = sum(1 for hit in page.hits if hit.on)
        out["errors"] = errors

    run_simulation(scenario, seed=0, schedule_seed=0)
    return int(out["on"]), list(out["errors"])  # type: ignore[arg-type]


def test_serializable_prevents_write_skew() -> None:
    on_count, errors = _run_write_skew(IsolationLevel.SERIALIZABLE)
    assert on_count == 1  # one flag stays on — the invariant holds
    assert "serialization_failure" in errors  # the losing transaction was aborted


def test_read_committed_allows_write_skew() -> None:
    on_count, errors = _run_write_skew(IsolationLevel.READ_COMMITTED)
    assert on_count == 0  # both turned off — write skew
    assert errors == []  # read-committed never aborts for isolation


def test_snapshot_allows_write_skew() -> None:
    # Snapshot isolation permits write-skew by design: the two writes touch different rows,
    # so there is no write-write conflict to reject.
    on_count, errors = _run_write_skew(IsolationLevel.SNAPSHOT)
    assert on_count == 0
    assert errors == []


def _run_lost_update(isolation: IsolationLevel) -> list[str]:
    registry = _registry(isolation)
    out: dict[str, object] = {}

    async def scenario() -> None:
        deps = DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
        ctx = ExecutionContext(deps=deps)

        counter = await run_operation(registry, "create_counter", None, ctx)
        errors: list[str] = []

        async def inc() -> None:
            try:
                await run_operation(
                    registry, "increment", TargetCmd(target=counter), ctx
                )
            except CoreException as error:
                errors.append(error.code or "")

        await asyncio.gather(inc(), inc())
        out["errors"] = errors

    run_simulation(scenario, seed=0, schedule_seed=0)
    return list(out["errors"])  # type: ignore[arg-type]


def test_snapshot_surfaces_lost_update_as_write_write_conflict() -> None:
    # Two unconditional increments of the same row: snapshot's first-committer-wins aborts the
    # loser, so the conflict is surfaced rather than silently losing a write.
    errors = _run_lost_update(IsolationLevel.SNAPSHOT)
    assert "serialization_failure" in errors


def test_read_committed_loses_update_silently() -> None:
    # Read-committed never aborts: a concurrent lost update goes unsignalled.
    errors = _run_lost_update(IsolationLevel.READ_COMMITTED)
    assert errors == []


# ....................... #
# Unit coverage for the overlay primitives.


def test_view_reads_overlay_then_snapshot_and_records_reads() -> None:
    reads: set[object] = set()
    view = IsolatedStoreView(
        snapshot={"a": 1, "b": 2}, overlay={"b": 20, "c": 30}, reads=reads
    )

    assert view["a"] == 1  # from snapshot
    assert view["b"] == 20  # overlay shadows snapshot
    assert view["c"] == 30  # overlay-only
    assert "a" in reads and "b" in reads and "c" in reads


def test_view_tombstone_hides_a_deleted_key() -> None:
    view = IsolatedStoreView(snapshot={"a": 1}, overlay={}, reads=set())
    del view["a"]

    assert view.overlay["a"] is _TOMBSTONE
    assert "a" not in view
    assert view.get("a", "default") == "default"
    assert list(view.values()) == []


def test_view_merged_iteration_methods() -> None:
    reads: set[object] = set()
    view = IsolatedStoreView(
        snapshot={"a": 1, "b": 2}, overlay={"b": 20, "c": 30}, reads=reads
    )

    assert dict(view.items()) == {"a": 1, "b": 20, "c": 30}
    assert set(view.keys()) == {"a", "b", "c"}
    assert sorted(view) == ["a", "b", "c"]  # __iter__
    assert len(view) == 3
    assert reads == {"a", "b", "c"}  # every scan records all visible keys


def test_view_delete_missing_key_raises() -> None:
    view = IsolatedStoreView(snapshot={}, overlay={}, reads=set())

    try:
        del view["nope"]
        raise AssertionError("expected KeyError")
    except KeyError:
        pass


def test_commit_applies_tombstones_and_creates_new_namespace() -> None:
    state = MockState()
    state.documents["flags"] = {"keep": {"id": "keep"}, "drop": {"id": "drop"}}

    tx = MvccTx.begin(state, serializable=False)
    # Delete an existing row and write into a brand-new namespace.
    tx.overlays.setdefault("flags", {})["drop"] = _TOMBSTONE
    tx.overlays.setdefault("audit", {})["a1"] = {"id": "a1"}

    tx.commit(state)

    assert "drop" not in state.documents["flags"]
    assert "keep" in state.documents["flags"]
    assert state.documents["audit"] == {"a1": {"id": "a1"}}


def test_mvcc_validate_and_commit_conflicts() -> None:
    state = MockState()
    state.documents["flags"] = {"x": {"id": "x"}, "y": {"id": "y"}}

    # A serializable transaction that read {x} and a concurrent commit that wrote {x}.
    tx = MvccTx.begin(state, serializable=True)
    tx.reads.setdefault("flags", set()).add("x")
    # Simulate a concurrent commit after begin.
    state.mvcc_version += 1
    state.mvcc_commit_log.append((state.mvcc_version, {"flags": frozenset({"x"})}))

    try:
        tx.validate(state)
        raise AssertionError("expected a serialization failure")
    except CoreException as error:
        assert error.code == "serialization_failure"

    # A snapshot transaction (read-write not checked) with a disjoint write-set is clean.
    snap = MvccTx.begin(state, serializable=False)
    snap.reads.setdefault("flags", set()).add("x")
    snap.overlays.setdefault("flags", {})["y"] = {"id": "y", "v": 1}
    snap.validate(state)  # no write-write overlap → no raise
    snap.commit(state)
    assert state.documents["flags"]["y"] == {"id": "y", "v": 1}


def test_serializable_scan_conflicts_with_a_concurrent_insert_phantom() -> None:
    state = MockState()
    state.documents["slots"] = {}

    # A serializable transaction scans the namespace (finds it empty), then a concurrent
    # transaction inserts a NEW key. The inserted key was never in the scanner's read-set, so
    # key-level tracking alone would miss it — namespace-scan tracking catches the phantom.
    scanner = MvccTx.begin(state, serializable=True)
    list(scanner.view("slots", state.documents["slots"]).keys())  # scan for absence

    state.mvcc_version += 1
    state.mvcc_commit_log.append((state.mvcc_version, {"slots": frozenset({"new"})}))

    try:
        scanner.validate(state)
        raise AssertionError("expected a phantom serialization failure")
    except CoreException as error:
        assert error.code == "serialization_failure"

    # Snapshot isolation does not track the scan → the same situation is allowed.
    snap = MvccTx.begin(state, serializable=False)
    list(snap.view("slots", state.documents["slots"]).keys())
    state.mvcc_version += 1
    state.mvcc_commit_log.append((state.mvcc_version, {"slots": frozenset({"new2"})}))
    snap.validate(state)  # no raise
