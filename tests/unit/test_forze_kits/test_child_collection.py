"""``ChildCollectionSync`` reconciles a parent's children against an incoming list (RFC 0050).

Each partition on its own, the delete-before-create order against a declared uniqueness, both
delete paths, the transaction, a reorder's writes, values typed differently on the two models,
stale revisions, and tenancy — all on the in-memory store, which enforces ``UniqueTogether`` as
a real index does.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any
from unittest.mock import patch
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import Field, StringConstraints

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import UniqueTogether
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules, unguarded
from forze_kits.aggregates import ChildCollectionChanges, ChildCollectionSync
from forze_kits.domain.soft_deletion import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #


class Line(Document):
    order_id: UUID
    sku: str
    unit: str | None = None
    quantity: int = 1
    position: int = 0


class CreateLine(CreateDocumentCmd):
    order_id: UUID
    sku: str
    unit: str | None = None
    quantity: int = 1
    position: int = 0


class UpdateLine(BaseDTO):
    unit: str | None = None
    quantity: int | None = None
    position: int | None = None


class ReadLine(ReadDocument):
    order_id: UUID
    sku: str
    unit: str | None = None
    quantity: int = 1
    position: int = 0


class SoftLine(DocWithSoftDeletion):
    order_id: UUID
    sku: str
    quantity: int = 1


class CreateSoftLine(CreateDocumentCmd):
    order_id: UUID
    sku: str
    quantity: int = 1


class UpdateSoftLine(UpdateCmdWithSoftDeletion):
    quantity: int | None = None


class ReadSoftLine(ReadDocument):
    order_id: UUID
    sku: str
    quantity: int = 1
    is_deleted: bool = False


def _spec(
    *guarantees: UniqueTogether, hard_delete: bool = True, history: bool = False
) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec(
        name="lines",
        read=ReadLine,
        write=DocumentWriteTypes(domain=Line, create_cmd=CreateLine, update_cmd=UpdateLine),
        guarantees=guarantees,
        hard_delete=hard_delete,
        history_enabled=history,
    )


def _sync(
    spec: DocumentSpec[Any, Any, Any, Any] | None = None,
    *,
    key: tuple[str, ...] = ("sku",),
    compare: frozenset[str] = frozenset({"quantity", "position"}),
) -> ChildCollectionSync[Any, Any]:
    return ChildCollectionSync(
        child_spec=spec or _spec(), parent_fk="order_id", key_fields=key, compare_fields=compare
    )


async def _seed(ctx: ExecutionContext, spec: Any, *lines: CreateLine) -> list[ReadLine]:
    async with ctx.tx_ctx.scope("mock"):
        return list(await ctx.document.command(spec).create_many(list(lines)))


async def _reconcile(
    ctx: ExecutionContext, sync: ChildCollectionSync[Any, Any], parent: UUID, *lines: Any
) -> ChildCollectionChanges[Any]:
    async with ctx.tx_ctx.scope("mock"):
        return await sync.reconcile(ctx, parent, list(lines))


async def _children(ctx: ExecutionContext, spec: Any, parent: UUID) -> dict[str, Any]:
    page = await ctx.document.query(spec).find_many(filters={"$values": {"order_id": parent}})

    return {row.sku: row for row in page.hits}


def _refusal(ei: pytest.ExceptionInfo[CoreException]) -> tuple[ExceptionKind, str]:
    return ei.value.kind, ei.value.code


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


# ....................... #


class TestTheDeclaration:
    def test_a_key_is_required(self) -> None:
        with pytest.raises(CoreException) as ei:
            _sync(key=())

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize(
        ("key", "compare", "named"),
        [
            (("skew",), frozenset(), "skew"),
            (("sku",), frozenset({"price"}), "price"),
        ],
    )
    def test_an_unknown_field_is_refused(
        self, key: tuple[str, ...], compare: frozenset[str], named: str
    ) -> None:
        with pytest.raises(CoreException, match=named) as ei:
            _sync(key=key, compare=compare)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_compare_field_the_update_cannot_write_is_refused(self) -> None:
        with pytest.raises(CoreException, match="update command") as ei:
            _sync(compare=frozenset({"sku"}))

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_spec_that_forbids_hard_deletes_and_has_no_flag_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _sync(_spec(hard_delete=False))

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "hard_delete_forbidden")

    def test_a_soft_delete_flag_the_update_cannot_write_is_refused(self) -> None:
        spec = DocumentSpec(
            name="soft_lines",
            read=ReadSoftLine,
            write=DocumentWriteTypes(
                domain=SoftLine, create_cmd=CreateSoftLine, update_cmd=UpdateLine
            ),
        )

        with pytest.raises(CoreException) as ei:
            ChildCollectionSync(child_spec=spec, parent_fk="order_id", key_fields=("sku",))

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_soft_delete_unwired")


# ....................... #


class TestThePartitions:
    async def test_an_added_child_is_created(self, ctx: ExecutionContext) -> None:
        order = uuid4()

        changes = await _reconcile(ctx, _sync(), order, CreateLine(order_id=order, sku="a"))

        assert [row.sku for row in changes.created] == ["a"]
        assert (changes.removed, changes.updated) == ([], [])
        assert set(await _children(ctx, _spec(), order)) == {"a"}

    async def test_a_dropped_child_is_killed(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        a, _ = await _seed(
            ctx, _spec(), CreateLine(order_id=order, sku="a"), CreateLine(order_id=order, sku="b")
        )

        changes = await _reconcile(ctx, _sync(), order, CreateLine(order_id=order, sku="b"))

        assert [row.id for row in changes.removed] == [a.id]
        assert set(await _children(ctx, _spec(), order)) == {"b"}

    async def test_a_changed_child_is_updated_in_place(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        (a,) = await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a", quantity=1))

        changes = await _reconcile(
            ctx, _sync(), order, CreateLine(order_id=order, sku="a", quantity=5)
        )

        ((before, after),) = changes.updated
        assert (before.id, before.quantity, before.rev) == (a.id, 1, a.rev)
        assert (after.id, after.quantity, after.rev) == (a.id, 5, a.rev + 1)
        assert changes.created == []

    async def test_an_unchanged_child_is_not_written(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        (a,) = await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a", quantity=3))
        port = type(unguarded(ctx.document.command(_spec())))

        # Not even as a no-op patch: a "diff" that sends every kept child is a rewrite.
        with patch.object(port, "update_many", side_effect=AssertionError("written")):
            changes = await _reconcile(
                ctx, _sync(), order, CreateLine(order_id=order, sku="a", quantity=3)
            )

        stored = (await _children(ctx, _spec(), order))["a"]
        assert changes == ChildCollectionChanges(removed=[], created=[], updated=[])
        assert (stored.rev, stored.last_update_at) == (a.rev, a.last_update_at)

    async def test_a_field_outside_compare_fields_is_not_a_change(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()
        await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a", quantity=3))

        changes = await _reconcile(
            ctx,
            _sync(compare=frozenset({"position"})),
            order,
            CreateLine(order_id=order, sku="a", quantity=9),
        )

        assert changes.updated == []
        assert (await _children(ctx, _spec(), order))["a"].quantity == 3

    async def test_an_empty_list_removes_every_child(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        await _seed(
            ctx, _spec(), CreateLine(order_id=order, sku="a"), CreateLine(order_id=order, sku="b")
        )

        changes = await _reconcile(ctx, _sync(), order)

        assert len(changes.removed) == 2
        assert await _children(ctx, _spec(), order) == {}

    async def test_another_parents_children_are_left_alone(self, ctx: ExecutionContext) -> None:
        mine, theirs = uuid4(), uuid4()
        await _seed(ctx, _spec(), CreateLine(order_id=theirs, sku="a"))

        await _reconcile(ctx, _sync(), mine, CreateLine(order_id=mine, sku="b"))

        assert set(await _children(ctx, _spec(), theirs)) == {"a"}

    async def test_a_composite_key_with_a_null_part_matches(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        sync = _sync(key=("sku", "unit"))
        await _seed(
            ctx,
            _spec(),
            CreateLine(order_id=order, sku="a", unit=None, quantity=1),
            CreateLine(order_id=order, sku="a", unit="box", quantity=1),
        )

        changes = await _reconcile(
            ctx,
            sync,
            order,
            CreateLine(order_id=order, sku="a", unit=None, quantity=2),
            CreateLine(order_id=order, sku="a", unit="box", quantity=1),
        )

        ((before, after),) = changes.updated
        assert (before.unit, after.quantity) == (None, 2)
        assert (changes.created, changes.removed) == ([], [])


# ....................... #


class TestTheOrder:
    """Decision 5: a new child may take a unique value a removed child held."""

    CODE_PER_ORDER = UniqueTogether(fields=("order_id", "position"))

    async def test_the_store_refuses_the_duplicate_while_the_old_row_exists(
        self, ctx: ExecutionContext
    ) -> None:
        # The control: without the delete first, the create collides.
        spec = _spec(self.CODE_PER_ORDER)
        order = uuid4()
        await _seed(ctx, spec, CreateLine(order_id=order, sku="a", position=0))

        with pytest.raises(CoreException) as ei:
            await _seed(ctx, spec, CreateLine(order_id=order, sku="b", position=0))

        assert ei.value.kind is ExceptionKind.CONFLICT

    async def test_a_removed_childs_unique_value_goes_to_a_new_one(
        self, ctx: ExecutionContext
    ) -> None:
        spec = _spec(self.CODE_PER_ORDER)
        order = uuid4()
        await _seed(ctx, spec, CreateLine(order_id=order, sku="a", position=0))

        changes = await _reconcile(
            ctx, _sync(spec), order, CreateLine(order_id=order, sku="b", position=0)
        )

        assert ([r.sku for r in changes.removed], [r.sku for r in changes.created]) == (
            ["a"],
            ["b"],
        )
        assert {k: v.position for k, v in (await _children(ctx, spec, order)).items()} == {"b": 0}


# ....................... #


class TestSoftDeletion:
    SPEC = DocumentSpec(
        name="soft_lines",
        read=ReadSoftLine,
        write=DocumentWriteTypes(
            domain=SoftLine, create_cmd=CreateSoftLine, update_cmd=UpdateSoftLine
        ),
        guarantees=(
            UniqueTogether(fields=("order_id", "sku"), where={"$values": {"is_deleted": False}}),
        ),
        hard_delete=False,
    )

    def _sync(self) -> ChildCollectionSync[Any, Any]:
        return ChildCollectionSync(
            child_spec=self.SPEC,
            parent_fk="order_id",
            key_fields=("sku",),
            compare_fields=frozenset({"quantity"}),
        )

    async def test_a_dropped_child_is_flagged_not_erased(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        (a,) = await _seed(ctx, self.SPEC, CreateSoftLine(order_id=order, sku="a"))

        changes = await _reconcile(ctx, self._sync(), order)

        stored = await ctx.document.query(self.SPEC).get(a.id)
        assert [row.id for row in changes.removed] == [a.id]
        assert (stored.is_deleted, stored.rev) == (True, a.rev + 1)

    async def test_a_deleted_child_is_not_current_and_its_key_can_return(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()
        (a,) = await _seed(ctx, self.SPEC, CreateSoftLine(order_id=order, sku="a"))
        await _reconcile(ctx, self._sync(), order)

        changes = await _reconcile(
            ctx, self._sync(), order, CreateSoftLine(order_id=order, sku="a", quantity=2)
        )

        (again,) = changes.created
        assert again.id != a.id
        assert (changes.removed, changes.updated) == ([], [])


# ....................... #


class TestTheTransaction:
    async def test_it_refuses_to_run_outside_one(self, ctx: ExecutionContext) -> None:
        order = uuid4()

        with pytest.raises(CoreException) as ei:
            await _sync().reconcile(ctx, order, [CreateLine(order_id=order, sku="a")])

        assert _refusal(ei) == (ExceptionKind.PRECONDITION, "child_sync_outside_transaction")
        assert await _children(ctx, _spec(), order) == {}

    async def test_a_failed_update_batch_commits_no_create(self, ctx: ExecutionContext) -> None:
        spec = _spec(UniqueTogether(fields=("order_id", "position")))
        order = uuid4()
        await _seed(
            ctx,
            spec,
            CreateLine(order_id=order, sku="a", position=0),
            CreateLine(order_id=order, sku="b", position=1),
        )

        # "a" moves onto "b"'s position, which "b" keeps: the update batch is refused.
        with pytest.raises(CoreException) as ei:
            await _reconcile(
                ctx,
                _sync(spec),
                order,
                CreateLine(order_id=order, sku="a", position=1),
                CreateLine(order_id=order, sku="b", position=1),
                CreateLine(order_id=order, sku="c", position=2),
            )

        assert ei.value.kind is ExceptionKind.CONFLICT
        assert {k: v.position for k, v in (await _children(ctx, spec, order)).items()} == {
            "a": 0,
            "b": 1,
        }


# ....................... #


class TestAReorder:
    """A reorder is an update of every child whose position changed — never free."""

    SPEC = _spec(UniqueTogether(fields=("order_id", "position"), holds="commit"))

    async def _four(self, ctx: ExecutionContext, order: UUID) -> None:
        await _seed(
            ctx,
            self.SPEC,
            *(CreateLine(order_id=order, sku=s, position=i) for i, s in enumerate("abcd")),
        )

    @staticmethod
    def _lines(order: UUID, skus: str) -> list[CreateLine]:
        return [CreateLine(order_id=order, sku=s, position=i) for i, s in enumerate(skus)]

    async def test_moving_the_last_child_first_writes_every_child(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()
        await self._four(ctx, order)

        changes = await _reconcile(ctx, _sync(self.SPEC), order, *self._lines(order, "dabc"))

        assert {after.sku: after.position for _, after in changes.updated} == {
            "d": 0,
            "a": 1,
            "b": 2,
            "c": 3,
        }

    async def test_swapping_two_children_writes_only_those_two(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        await self._four(ctx, order)

        changes = await _reconcile(ctx, _sync(self.SPEC), order, *self._lines(order, "abdc"))

        assert {after.sku: after.position for _, after in changes.updated} == {"d": 2, "c": 3}


# ....................... #


class TestAmbiguity:
    async def test_two_incoming_children_with_one_key_are_refused(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()

        with pytest.raises(CoreException) as ei:
            await _reconcile(
                ctx,
                _sync(),
                order,
                CreateLine(order_id=order, sku="a"),
                CreateLine(order_id=order, sku="a", quantity=2),
            )

        assert _refusal(ei) == (ExceptionKind.VALIDATION, "child_key_duplicate")

    async def test_one_command_listed_twice_is_refused(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        line = CreateLine(order_id=order, sku="a")

        with pytest.raises(CoreException) as ei:
            await _reconcile(ctx, _sync(), order, line, line)

        assert _refusal(ei) == (ExceptionKind.VALIDATION, "child_key_duplicate")

    async def test_a_child_of_another_parent_is_refused(self, ctx: ExecutionContext) -> None:
        with pytest.raises(CoreException) as ei:
            await _reconcile(ctx, _sync(), uuid4(), CreateLine(order_id=uuid4(), sku="a"))

        assert _refusal(ei) == (ExceptionKind.VALIDATION, "child_parent_mismatch")

    async def test_two_stored_children_with_one_key_write_nothing(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()
        await _seed(
            ctx,
            _spec(),
            CreateLine(order_id=order, sku="a", position=0),
            CreateLine(order_id=order, sku="a", position=1),
        )

        with pytest.raises(CoreException) as ei:
            await _reconcile(ctx, _sync(), order, CreateLine(order_id=order, sku="b"))

        assert _refusal(ei) == (ExceptionKind.CONFLICT, "child_key_ambiguous")
        assert set(await _children(ctx, _spec(), order)) == {"a"}


# ....................... #


class _TrimmedLine(Document):
    order_id: UUID
    sku: str
    note: Annotated[str, StringConstraints(to_lower=True)] = ""


class _CreateNoted(CreateDocumentCmd):
    order_id: UUID
    sku: str
    note: str = ""


class _UpdateNoted(BaseDTO):
    note: str | None = None


class _ReadNoted(ReadDocument):
    order_id: UUID
    sku: str
    note: str = ""


class TestAPatchTheStoreAlreadyHolds:
    async def test_it_is_not_reported_as_a_change(self, ctx: ExecutionContext) -> None:
        # The domain lowers the note, so "X" differs from the stored "x" by ``==`` and the
        # patch is sent, but the store finds nothing to change. A stale revision merged by a
        # store keeping history lands here too.
        spec = DocumentSpec(
            name="noted_lines",
            read=_ReadNoted,
            write=DocumentWriteTypes(
                domain=_TrimmedLine, create_cmd=_CreateNoted, update_cmd=_UpdateNoted
            ),
        )
        sync = ChildCollectionSync(
            child_spec=spec,
            parent_fk="order_id",
            key_fields=("sku",),
            compare_fields=frozenset({"note"}),
        )
        order = uuid4()
        (a,) = await _seed(ctx, spec, _CreateNoted(order_id=order, sku="a", note="x"))

        changes = await _reconcile(
            ctx, sync, order, _CreateNoted(order_id=order, sku="a", note="X")
        )

        assert changes.updated == []
        assert (await ctx.document.query(spec).get(a.id)).rev == a.rev


# ....................... #


class Grade(Enum):
    A = "a"
    B = "b"


class Priced(Document):
    order_id: UUID
    grade: Grade
    price: Decimal
    due: datetime


class CreatePriced(CreateDocumentCmd):
    order_id: UUID
    grade: str
    price: float
    due: datetime


class UpdatePriced(BaseDTO):
    price: Decimal | None = None
    due: datetime | None = None


class ReadPriced(ReadDocument):
    order_id: UUID
    grade: Grade
    price: Decimal
    due: datetime


class TestValuesTypedDifferently:
    """The create command's float, string and naive datetime meet the stored types."""

    SPEC = DocumentSpec(
        name="priced_lines",
        read=ReadPriced,
        write=DocumentWriteTypes(domain=Priced, create_cmd=CreatePriced, update_cmd=UpdatePriced),
    )
    SYNC = ChildCollectionSync(
        child_spec=SPEC,
        parent_fk="order_id",
        key_fields=("grade",),
        compare_fields=frozenset({"price", "due"}),
    )
    DUE = datetime(2026, 1, 1, 9, 30)

    async def test_equal_values_are_neither_recreated_nor_rewritten(
        self, ctx: ExecutionContext
    ) -> None:
        # Stored: an enum, a decimal and an aware instant. Sent: a string, a float, and the same
        # instant as a naive UTC datetime.
        order = uuid4()
        due = self.DUE.replace(tzinfo=UTC)
        (a,) = await _seed(
            ctx, self.SPEC, CreatePriced(order_id=order, grade="a", price=1.1, due=due)
        )
        assert (a.grade, a.price, a.due) == (Grade.A, Decimal("1.1"), due)

        port = type(unguarded(ctx.document.command(self.SPEC)))
        sent = CreatePriced(order_id=order, grade="a", price=1.1, due=self.DUE)

        with patch.object(port, "update_many", side_effect=AssertionError("written")):
            changes = await _reconcile(ctx, self.SYNC, order, sent)

        assert changes == ChildCollectionChanges(removed=[], created=[], updated=[])

    async def test_a_changed_value_is_an_update(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        await _seed(
            ctx, self.SPEC, CreatePriced(order_id=order, grade="a", price=1.1, due=self.DUE)
        )

        changes = await _reconcile(
            ctx, self.SYNC, order, CreatePriced(order_id=order, grade="a", price=2.5, due=self.DUE)
        )

        ((_, after),) = changes.updated
        assert after.price == Decimal("2.5")

    async def test_a_value_the_read_model_cannot_hold_is_refused(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()

        with pytest.raises(CoreException) as ei:
            await _reconcile(
                ctx,
                self.SYNC,
                order,
                CreatePriced(order_id=order, grade="z", price=1, due=self.DUE),
            )

        assert _refusal(ei) == (ExceptionKind.VALIDATION, "child_value_unreadable")

    async def test_a_new_childs_compare_value_is_read_before_any_write(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()

        with pytest.raises(CoreException) as ei:
            await _reconcile(
                ctx,
                self.SYNC,
                order,
                CreatePriced(order_id=order, grade="a", price=float("inf"), due=self.DUE),
            )

        assert _refusal(ei) == (ExceptionKind.VALIDATION, "child_value_unreadable")


# ....................... #


class TestStaleRevisions:
    """A child another writer changed after the read is refused, never overwritten or deleted."""

    @staticmethod
    async def _changed_after_read(
        ctx: ExecutionContext, spec: Any, order: UUID, row: Any, dto: BaseDTO
    ) -> Sequence[Any]:
        page = await ctx.document.query(spec).find_many(filters={"$values": {"order_id": order}})

        async with ctx.tx_ctx.scope("mock"):
            await ctx.document.command(spec).update(row.id, row.rev, dto)

        return page.hits

    @staticmethod
    def _reading(ctx: ExecutionContext, spec: Any, stale: Sequence[Any]) -> Any:
        async def stream(*_: Any, **__: Any) -> AsyncIterator[Sequence[Any]]:
            yield stale

        return patch.object(type(ctx.document.query(spec)), "find_stream", stream)

    @pytest.mark.parametrize("history", [False, True])
    async def test_an_update(self, ctx: ExecutionContext, history: bool) -> None:
        spec = _spec(history=history)
        order = uuid4()
        (a,) = await _seed(ctx, spec, CreateLine(order_id=order, sku="a", quantity=1))
        stale = await self._changed_after_read(ctx, spec, order, a, UpdateLine(position=4))

        with self._reading(ctx, spec, stale), pytest.raises(CoreException) as ei:
            await _reconcile(
                ctx, _sync(spec), order, CreateLine(order_id=order, sku="a", quantity=5)
            )

        assert _refusal(ei) == (ExceptionKind.PRECONDITION, "revision_mismatch")
        stored = (await _children(ctx, spec, order))["a"]
        assert (stored.quantity, stored.position) == (1, 4)

    @pytest.mark.parametrize("history", [False, True])
    async def test_a_hard_delete(self, ctx: ExecutionContext, history: bool) -> None:
        spec = _spec(history=history)
        order = uuid4()
        a, _ = await _seed(
            ctx, spec, CreateLine(order_id=order, sku="a"), CreateLine(order_id=order, sku="b")
        )
        stale = await self._changed_after_read(ctx, spec, order, a, UpdateLine(quantity=2))

        with self._reading(ctx, spec, stale), pytest.raises(CoreException) as ei:
            await _reconcile(ctx, _sync(spec), order, CreateLine(order_id=order, sku="b"))

        assert _refusal(ei) == (ExceptionKind.PRECONDITION, "revision_mismatch")
        assert set(await _children(ctx, spec, order)) == {"a", "b"}

    @pytest.mark.parametrize("history", [False, True])
    async def test_a_soft_delete(self, ctx: ExecutionContext, history: bool) -> None:
        spec = attrs.evolve(TestSoftDeletion.SPEC, history_enabled=history)
        sync = ChildCollectionSync(child_spec=spec, parent_fk="order_id", key_fields=("sku",))
        order = uuid4()
        (a,) = await _seed(ctx, spec, CreateSoftLine(order_id=order, sku="a"))
        stale = await self._changed_after_read(ctx, spec, order, a, UpdateSoftLine(quantity=2))

        with self._reading(ctx, spec, stale), pytest.raises(CoreException) as ei:
            await _reconcile(ctx, sync, order)

        assert _refusal(ei) == (ExceptionKind.PRECONDITION, "revision_mismatch")
        assert (await ctx.document.query(spec).get(a.id)).is_deleted is False


@pytest.mark.parametrize("history", [False, True])
async def test_a_soft_delete_another_writer_already_made_is_refused(
    ctx: ExecutionContext, history: bool
) -> None:
    # Strict, the write at the revision read is refused, so ``removed`` never lists a child
    # this call did not delete.
    spec = attrs.evolve(TestSoftDeletion.SPEC, history_enabled=history)
    sync = ChildCollectionSync(child_spec=spec, parent_fk="order_id", key_fields=("sku",))
    order = uuid4()
    (a,) = await _seed(ctx, spec, CreateSoftLine(order_id=order, sku="a"))
    stale = await TestStaleRevisions._changed_after_read(
        ctx, spec, order, a, UpdateSoftLine(is_deleted=True)
    )

    with TestStaleRevisions._reading(ctx, spec, stale), pytest.raises(CoreException) as ei:
        await _reconcile(ctx, sync, order)

    assert _refusal(ei) == (ExceptionKind.PRECONDITION, "revision_mismatch")
    assert (await ctx.document.query(spec).get(a.id)).rev == a.rev + 1


# ....................... #


class TestTheRead:
    async def test_every_child_is_read_though_a_list_read_would_stop_short(
        self, ctx: ExecutionContext
    ) -> None:
        # A port whose ``find_many`` stops at two rows: the other children must still be seen,
        # or they would be created a second time.
        order = uuid4()
        lines = [CreateLine(order_id=order, sku=f"s{i}", position=i) for i in range(5)]
        await _seed(ctx, _spec(), *lines)
        query = type(ctx.document.query(_spec()))
        find_many = query.find_many

        async def two_rows(self: Any, *args: Any, **kwargs: Any) -> Any:
            page = await find_many(self, *args, **kwargs)
            page.hits[2:] = []

            return page

        with patch.object(query, "find_many", two_rows):
            changes = await _reconcile(ctx, _sync(), order, *lines)

        assert changes == ChildCollectionChanges(removed=[], created=[], updated=[])

    async def test_updates_pair_by_id_when_the_port_reorders_its_results(
        self, ctx: ExecutionContext
    ) -> None:
        order = uuid4()
        await _seed(
            ctx,
            _spec(),
            CreateLine(order_id=order, sku="a", quantity=1),
            CreateLine(order_id=order, sku="b", quantity=1),
        )
        port = type(unguarded(ctx.document.command(_spec())))
        update_many = port.update_many

        async def reversed_results(self: Any, *args: Any, **kwargs: Any) -> Any:
            return list(reversed(await update_many(self, *args, **kwargs)))

        with patch.object(port, "update_many", reversed_results):
            changes = await _reconcile(
                ctx,
                _sync(),
                order,
                CreateLine(order_id=order, sku="a", quantity=2),
                CreateLine(order_id=order, sku="b", quantity=3),
            )

        assert {(b.sku, a.sku, a.quantity) for b, a in changes.updated} == {
            ("a", "a", 2),
            ("b", "b", 3),
        }

    async def test_a_compare_field_cleared_to_none_is_written(self, ctx: ExecutionContext) -> None:
        order = uuid4()
        await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a", unit="box"))

        changes = await _reconcile(
            ctx,
            _sync(compare=frozenset({"unit"})),
            order,
            CreateLine(order_id=order, sku="a", unit=None),
        )

        ((before, after),) = changes.updated
        assert (before.unit, after.unit) == ("box", None)
        assert (await _children(ctx, _spec(), order))["a"].unit is None


# ....................... #


class TestTenancy:
    async def test_another_tenants_children_of_the_same_parent_are_not_seen(self) -> None:
        ctx = context_from_modules(
            MockDepsModule(routes={"lines": MockRouteConfig(tenant_aware=True)})
        )
        first, second = TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())
        order = uuid4()

        with ctx.inv_ctx.bind_identity(tenant=first):
            await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a"))

        with ctx.inv_ctx.bind_identity(tenant=second):
            changes = await _reconcile(ctx, _sync(), order, CreateLine(order_id=order, sku="b"))

        assert (changes.removed, [row.sku for row in changes.created]) == ([], ["b"])

        with ctx.inv_ctx.bind_identity(tenant=first):
            assert set(await _children(ctx, _spec(), order)) == {"a"}


# ....................... #


class TestTheTransactionsConnection:
    async def test_a_child_port_outside_the_transaction_is_refused(
        self, ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # As a port on another backend than the transaction's reports itself.
        order = uuid4()
        await _seed(ctx, _spec(), CreateLine(order_id=order, sku="a"))
        port = type(unguarded(ctx.document.command(_spec())))
        monkeypatch.setattr(port, "is_transactionally_enlisted", lambda _: False, raising=False)

        with pytest.raises(CoreException) as ei:
            await _reconcile(ctx, _sync(), order, CreateLine(order_id=order, sku="b"))

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "core.tx.not_enlisted")
        assert set(await _children(ctx, _spec(), order)) == {"a"}


# ....................... #


class Box(BaseDTO):
    width: int = 0


class _Tagged(Document):
    order_id: UUID
    sku: str
    tags: dict[str, int] = Field(default_factory=dict)
    box: Box | None = None
    loose: Any = None
    labels: list[str] = Field(default_factory=list)


class _CreateTagged(CreateDocumentCmd):
    order_id: UUID
    sku: str
    tags: dict[str, int] = Field(default_factory=dict)
    box: Box | None = None
    loose: Any = None
    labels: list[str] = Field(default_factory=list)


class _UpdateTagged(BaseDTO):
    tags: dict[str, int] | None = None
    box: Box | None = None
    loose: Any = None
    labels: list[str] | None = None


class _ReadTagged(ReadDocument):
    order_id: UUID
    sku: str
    tags: dict[str, int] = Field(default_factory=dict)
    box: Box | None = None
    loose: Any = None
    labels: list[str] = Field(default_factory=list)


class TestFieldsAnUpdateMerges:
    """A compare field an update merges into would keep a key the incoming value drops."""

    SPEC = DocumentSpec(
        name="tagged_lines",
        read=_ReadTagged,
        write=DocumentWriteTypes(
            domain=_Tagged, create_cmd=_CreateTagged, update_cmd=_UpdateTagged
        ),
    )

    @pytest.mark.parametrize("field", ["tags", "box", "loose"])
    def test_a_mapping_a_model_or_any_is_refused(self, field: str) -> None:
        with pytest.raises(CoreException, match=field) as ei:
            ChildCollectionSync(
                child_spec=self.SPEC,
                parent_fk="order_id",
                key_fields=("sku",),
                compare_fields=frozenset({field}),
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_compare_field_merged")

    def test_a_key_field_whose_type_may_not_hash_is_refused(self) -> None:
        with pytest.raises(CoreException, match="labels") as ei:
            ChildCollectionSync(
                child_spec=self.SPEC, parent_fk="order_id", key_fields=("sku", "labels")
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_key_unhashable")

    def test_a_list_is_replaced_and_may_be_compared(self) -> None:
        ChildCollectionSync(
            child_spec=self.SPEC,
            parent_fk="order_id",
            key_fields=("sku",),
            compare_fields=frozenset({"labels"}),
        )


# ....................... #


class _Coded(Document):
    order_id: UUID
    sku: str
    code: str = ""
    quantity: int = 1


class _CreateCoded(CreateDocumentCmd):
    order_id: UUID
    sku: str
    code: str = ""
    quantity: int = 1


class _UpdateCoded(BaseDTO):
    quantity: int | None = None


class _UpdateCodedStrictly(BaseDTO):
    quantity: int | None = None
    reason: str


class _UpdateCodedRequiringQuantity(BaseDTO):
    quantity: int
    code: str | None = None


class _ReadCoded(ReadDocument):
    order_id: UUID
    sku: str
    code: str = ""
    quantity: int = 1


class _Window(BaseDTO):
    day: int


def _coded(**overrides: Any) -> DocumentSpec[Any, Any, Any, Any]:
    fields: dict[str, Any] = {
        "name": "coded_lines",
        "read": _ReadCoded,
        "write": DocumentWriteTypes(
            domain=_Coded, create_cmd=_CreateCoded, update_cmd=_UpdateCoded
        ),
    }

    return DocumentSpec(**(fields | overrides))


class TestSpecsItCannotServe:
    def test_a_field_the_write_omits_is_refused_as_a_key(self) -> None:
        # Omitted, it reads back as its default, so two children would share one key.
        with pytest.raises(CoreException, match="code") as ei:
            ChildCollectionSync(
                child_spec=_coded(write_omit_fields=frozenset({"code"})),
                parent_fk="order_id",
                key_fields=("sku", "code"),
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_field_not_stored")

    def test_a_field_the_read_does_not_store_is_refused_as_compared(self) -> None:
        with pytest.raises(CoreException, match="quantity") as ei:
            ChildCollectionSync(
                child_spec=_coded(lenient_read_fields=frozenset({"quantity"})),
                parent_fk="order_id",
                key_fields=("sku",),
                compare_fields=frozenset({"quantity"}),
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_field_not_stored")

    def test_a_parametrized_spec_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            ChildCollectionSync(
                child_spec=_coded(query_params=_Window), parent_fk="order_id", key_fields=("sku",)
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_spec_parametrized")

    def test_an_update_command_requiring_an_unpatched_field_is_refused(self) -> None:
        spec = _coded(
            write=DocumentWriteTypes(
                domain=_Coded, create_cmd=_CreateCoded, update_cmd=_UpdateCodedStrictly
            )
        )

        with pytest.raises(CoreException, match="reason") as ei:
            ChildCollectionSync(
                child_spec=spec,
                parent_fk="order_id",
                key_fields=("sku",),
                compare_fields=frozenset({"quantity"}),
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_update_requires_fields")

    def test_an_update_command_requiring_a_compare_field_is_refused(self) -> None:
        # A patch carries only the fields that changed: changing ``code`` alone omits quantity.
        spec = _coded(
            write=DocumentWriteTypes(
                domain=_Coded, create_cmd=_CreateCoded, update_cmd=_UpdateCodedRequiringQuantity
            )
        )

        with pytest.raises(CoreException) as ei:
            ChildCollectionSync(
                child_spec=spec,
                parent_fk="order_id",
                key_fields=("sku",),
                compare_fields=frozenset({"quantity", "code"}),
            )

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_update_requires_fields")

    def test_a_key_of_a_tuple_of_lists_is_refused(self) -> None:
        class _Listed(_ReadCoded):
            sku: tuple[list[int], ...]  # type: ignore[assignment]

        spec = _coded(read=_Listed)

        with pytest.raises(CoreException) as ei:
            ChildCollectionSync(child_spec=spec, parent_fk="order_id", key_fields=("sku",))

        assert _refusal(ei) == (ExceptionKind.CONFIGURATION, "child_key_unhashable")

    def test_an_update_command_never_built_may_require_anything(self) -> None:
        # Hard deletes and no compare fields: no patch is ever made.
        spec = _coded(
            write=DocumentWriteTypes(
                domain=_Coded, create_cmd=_CreateCoded, update_cmd=_UpdateCodedStrictly
            )
        )

        ChildCollectionSync(child_spec=spec, parent_fk="order_id", key_fields=("sku",))
