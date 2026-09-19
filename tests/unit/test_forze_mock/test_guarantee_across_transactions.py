"""A declared uniqueness holds across two concurrent transactions, not only within one.

The enforcement battery drives the guarantee through one caller at a time, which proves the
check fires and says nothing about the case a transaction exists for. Inside a transaction the
check runs against that transaction's own view — buffered writes over an as-of-begin snapshot —
and that view, by construction, does not contain a concurrent transaction's uncommitted rows.
Two writers inserting different ids that carry the same guaranteed tuple therefore both pass
their check, and both commit.

A real store does not behave that way. A unique index is not snapshot-scoped: the second
inserter blocks on the first and fails once it commits. So the property here is read off the
rows the workload left behind rather than off the calls, at every isolation level — a guarantee
that held only under `serializable` would be a guarantee no deployment could rely on.

The ungoverned contrast is what makes the rest mean anything: the same workload against the same
spec minus the guarantee must leave two rows, or the writers never raced and the green runs above
attest nothing.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.guarantees import UniqueTogether
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters._mvcc import MvccTx, _mvcc_tx  # pyright: ignore[reportPrivateUsage]

# ----------------------- #


class _Fact(Document):
    root_id: str


class _FactRead(ReadDocument):
    root_id: str


class _FactCreate(CreateDocumentCmd):
    root_id: str


class _FactUpdate(BaseDTO):
    root_id: str | None = None


GOVERNED = DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate](
    name="facts",
    read=_FactRead,
    write=DocumentWriteTypes(domain=_Fact, create_cmd=_FactCreate, update_cmd=_FactUpdate),
    guarantees=(UniqueTogether(fields=("root_id",)),),
)

UNGOVERNED = DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate](
    name="facts",
    read=_FactRead,
    write=DocumentWriteTypes(domain=_Fact, create_cmd=_FactCreate, update_cmd=_FactUpdate),
)


class _Make(BaseModel):
    root_id: str


@attrs.define(slots=True, kw_only=True)
class _Writer(Handler[_Make, None]):
    """Insert a row for one root, with no coordination of any kind.

    A fresh id every time, so the duplicate-id check at commit cannot be what refuses it — the
    only thing that can is the guarantee.
    """

    ctx: ExecutionContext
    spec: DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate]

    async def __call__(self, args: _Make) -> None:
        await self.ctx.document.command(self.spec).create(_FactCreate(root_id=args.root_id))


def _registry(
    spec: DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate],
    isolation: IsolationLevel,
) -> OperationRegistry:
    plan = OperationPlan().bind_tx().set_route("mock").set_isolation(isolation).finish(deep=False)
    handlers = {"write": lambda ctx: _Writer(ctx=ctx, spec=spec)}

    return OperationRegistry(
        handlers=handlers,
        plans=dict.fromkeys(handlers, plan),
        descriptors={
            "write": OperationDescriptor(input_type=_Make, output_type=None, description="x"),
        },
    ).freeze()


def _race(
    spec: DocumentSpec[_FactRead, _Fact, _FactCreate, _FactUpdate],
    isolation: IsolationLevel,
) -> tuple[int, list[str]]:
    registry = _registry(spec, isolation)
    out: dict[str, object] = {}

    async def scenario() -> None:
        deps = DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
        ctx = ExecutionContext(deps=deps)
        errors: list[str] = []

        async def write() -> None:
            try:
                await run_operation(registry, "write", _Make(root_id="one"), ctx)

            except CoreException as error:
                errors.append(error.code or "")

        await asyncio.gather(write(), write())

        page = await ctx.document.query(spec).find_many()
        out["rows"] = sum(1 for hit in page.hits if hit.root_id == "one")
        out["errors"] = errors

    run_simulation(scenario, seed=0, schedule_seed=0)

    return int(out["rows"]), list(out["errors"])  # type: ignore[arg-type]


# ....................... #

_LEVELS = [
    IsolationLevel.READ_COMMITTED,
    IsolationLevel.SNAPSHOT,
    IsolationLevel.SERIALIZABLE,
]


class TestTwoTransactionsCannotBothLandTheSameTuple:
    @pytest.mark.parametrize("isolation", _LEVELS)
    def test_one_row_survives(self, isolation: IsolationLevel) -> None:
        rows, errors = _race(GOVERNED, isolation)

        assert rows == 1, f"the guarantee did not survive {isolation} (errors: {errors})"

    @pytest.mark.parametrize("isolation", _LEVELS)
    def test_the_loser_is_refused_as_a_conflict(self, isolation: IsolationLevel) -> None:
        # The kind matters as much as the count: a unique violation is `conflict` on every real
        # backend and at every isolation level, never a serialization failure. A caller that
        # retries a serialization failure and reports a conflict would do the wrong thing with
        # either if the two were swapped here.
        _rows, errors = _race(GOVERNED, isolation)

        assert errors == ["core.conflict"], errors

    @pytest.mark.parametrize("isolation", _LEVELS)
    def test_without_the_guarantee_both_land(self, isolation: IsolationLevel) -> None:
        # The contrast. If this ever passed, the two writers were not concurrent and the legs
        # above would be green whatever the enforcement did.
        rows, errors = _race(UNGOVERNED, isolation)

        assert rows == 2, f"the writers did not race under {isolation}"
        assert errors == []


# ....................... #


class TestOnlyTheFinalValueOfAKeyIsRechecked:
    """A transaction is judged on what it publishes, not on what it passed through.

    A key written and then rewritten inside one transaction lands once, holding its final value.
    Re-checking every intermediate value against the committed store rejects a transaction over
    a tuple it no longer carries — a spurious conflict, and one that gets worse the more work a
    transaction does before committing.
    """

    def test_a_row_moved_off_a_tuple_does_not_conflict_with_it(self) -> None:
        mock_state = MockState()
        deps = DepsRegistry.from_modules(MockDepsModule(state=mock_state)).freeze().resolve()
        command = ExecutionContext(deps=deps).document.command(GOVERNED)
        tx = MvccTx.begin(mock_state, serializable=False, read_committed=False)
        token = _mvcc_tx.set(tx)

        async def write() -> None:
            row = await command.create(_FactCreate(root_id="x"))
            await command.update(row.id, row.rev, _FactUpdate(root_id="z"))

        try:
            asyncio.run(write())

        finally:
            _mvcc_tx.reset(token)

        # A concurrent committer publishes the tuple this transaction wrote and then left.
        mock_state.documents.setdefault("facts", {})[uuid4()] = {
            "id": str(uuid4()),
            "rev": 1,
            "root_id": "x",
        }

        try:
            tx.validate(mock_state)

        finally:
            tx.finish(mock_state)

    def test_a_row_deleted_within_the_transaction_is_not_rechecked(self) -> None:
        # Nothing lands for the key, so there is nothing to conflict with. Checking it anyway
        # rejects a transaction over a tuple it does not publish — the same spurious conflict as
        # a rewritten key, reached through the delete path instead.
        mock_state = MockState()
        deps = DepsRegistry.from_modules(MockDepsModule(state=mock_state)).freeze().resolve()
        command = ExecutionContext(deps=deps).document.command(GOVERNED)
        tx = MvccTx.begin(mock_state, serializable=False, read_committed=False)
        token = _mvcc_tx.set(tx)

        async def write_then_drop() -> None:
            row = await command.create(_FactCreate(root_id="x"))
            await command.kill(row.id)

        try:
            asyncio.run(write_then_drop())

        finally:
            _mvcc_tx.reset(token)

        mock_state.documents.setdefault("facts", {})[uuid4()] = {
            "id": str(uuid4()),
            "rev": 1,
            "root_id": "x",
        }

        try:
            tx.validate(mock_state)

        finally:
            tx.finish(mock_state)

    def test_a_row_deleted_and_its_tuple_reused_in_one_transaction(self) -> None:
        """Delete a row and write another carrying its tuple, in one transaction.

        The commit-time check judges the state the commit will produce, which means laying the
        overlay over the committed rows — and a hard delete has to *remove* one there, not merely
        fail to add it. Without that the deleted row is still in the merged view and the
        replacement reads as a duplicate of something that is on its way out.
        """

        mock_state = MockState()
        deps = DepsRegistry.from_modules(MockDepsModule(state=mock_state)).freeze().resolve()
        command = ExecutionContext(deps=deps).document.command(GOVERNED)

        existing = asyncio.run(command.create(_FactCreate(root_id="x")))

        tx = MvccTx.begin(mock_state, serializable=False, read_committed=False)
        token = _mvcc_tx.set(tx)

        async def replace() -> None:
            await command.kill(existing.id)
            await command.create(_FactCreate(root_id="x"))

        try:
            asyncio.run(replace())

        finally:
            _mvcc_tx.reset(token)

        try:
            tx.validate(mock_state)

        finally:
            tx.finish(mock_state)

    def test_a_row_left_on_the_tuple_still_conflicts(self) -> None:
        # The contrast: reading the final overlay is not a way of skipping the check.
        mock_state = MockState()
        deps = DepsRegistry.from_modules(MockDepsModule(state=mock_state)).freeze().resolve()
        command = ExecutionContext(deps=deps).document.command(GOVERNED)
        tx = MvccTx.begin(mock_state, serializable=False, read_committed=False)
        token = _mvcc_tx.set(tx)

        try:
            asyncio.run(command.create(_FactCreate(root_id="x")))

        finally:
            _mvcc_tx.reset(token)

        mock_state.documents.setdefault("facts", {})[uuid4()] = {
            "id": str(uuid4()),
            "rev": 1,
            "root_id": "x",
        }

        try:
            with pytest.raises(CoreException) as caught:
                tx.validate(mock_state)

            assert caught.value.code == "core.conflict"

        finally:
            tx.finish(mock_state)
