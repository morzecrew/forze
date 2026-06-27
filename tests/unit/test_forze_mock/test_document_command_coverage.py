"""Coverage for :mod:`forze_mock.adapters._document_command` write branches."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.document import KeyedCreate
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from uuid import UUID

from forze.application.execution import DomainEventRegistry
from forze.domain.models import (
    AggregateRoot,
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
    event_emitter,
)
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockDocumentAdapter
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #


class Thing(Document):
    name: str = "x"


class ThingCreate(CreateDocumentCmd):
    name: str = "x"


class ThingUpdate(BaseDTO):
    name: str | None = None


class ThingRead(ReadDocument):
    name: str


SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(
        domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate
    ),
)


# Soft-delete capable model.
class Soft(DocWithSoftDeletion):
    name: str = "x"


class SoftCreate(CreateDocumentCmd):
    name: str = "x"


class SoftUpdate(BaseDTO):
    name: str | None = None


class SoftRead(ReadDocument):
    name: str
    is_deleted: bool = False


SOFT_SPEC = DocumentSpec(
    name="softs",
    read=SoftRead,
    write=DocumentWriteTypes(
        domain=Soft, create_cmd=SoftCreate, update_cmd=SoftUpdate
    ),
)


# ....................... #


def _ctx(state: MockState, *, strict_tx: bool = False) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state, strict_tx=strict_tx))


# Aggregate model emitting a domain event on status transition.
class Order(Document, AggregateRoot):
    status: str = "pending"

    @event_emitter(fields={"status"})
    def _on_confirm(before, after, diff) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status == "confirmed" and before.status != "confirmed":
            return OrderConfirmed(aggregate_id=after.id)
        return None


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


class OrderCreate(CreateDocumentCmd):
    status: str = "pending"


class OrderUpdate(BaseDTO):
    status: str | None = None


class OrderRead(ReadDocument):
    status: str


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
    ),
)


def _docs(state: MockState, ns: str = "things") -> dict:
    return state.documents.get(ns, {})


def _plain_adapter(
    state: MockState,
) -> MockDocumentAdapter[ThingRead, Thing, ThingCreate, ThingUpdate]:
    return MockDocumentAdapter(
        spec=SPEC,
        state=state,
        namespace="things",
        read_model=ThingRead,
        domain_model=Thing,
    )


NO_UPDATE_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate),
)


def _no_update_adapter(
    state: MockState,
) -> MockDocumentAdapter[ThingRead, Thing, ThingCreate, ThingUpdate]:
    return MockDocumentAdapter(
        spec=NO_UPDATE_SPEC,
        state=state,
        namespace="things",
        read_model=ThingRead,
        domain_model=Thing,
    )


def _soft_adapter(
    state: MockState,
) -> MockDocumentAdapter[SoftRead, Soft, SoftCreate, SoftUpdate]:
    return MockDocumentAdapter(
        spec=SOFT_SPEC,
        state=state,
        namespace="softs",
        read_model=SoftRead,
        domain_model=Soft,
    )


# ----------------------- #


class TestDomainEventDispatch:
    async def test_aggregate_update_dispatches_event(self) -> None:
        seen: list[DomainEvent] = []
        registry = DomainEventRegistry()

        def factory(_ctx):  # type: ignore[no-untyped-def]
            async def handler(event: OrderConfirmed) -> None:
                seen.append(event)

            return handler

        registry.register(OrderConfirmed, factory)
        ctx = context_from_deps(MockDepsModule(domain_events=registry)())
        cmd = ctx.document.command(ORDER_SPEC)

        created = await cmd.create(OrderCreate())
        await cmd.update(created.id, created.rev, OrderUpdate(status="confirmed"))
        assert len(seen) == 1 and seen[0].aggregate_id == created.id

    async def test_aggregate_event_without_dispatcher_raises(self) -> None:
        adapter = MockDocumentAdapter(
            spec=ORDER_SPEC,
            state=MockState(),
            namespace="orders",
            read_model=OrderRead,
            domain_model=Order,
            dispatcher_provider=lambda: None,
        )
        created = await adapter.create(OrderCreate())
        with pytest.raises(CoreException) as ei:
            await adapter.update(created.id, created.rev, OrderUpdate(status="confirmed"))
        assert ei.value.kind is ExceptionKind.CONFIGURATION


class TestReadOnlyTxRejectsWrites:
    async def test_create_in_read_only_tx_raises(self) -> None:
        state = MockState()
        ctx = _ctx(state, strict_tx=True)
        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock", read_only=True):
                await ctx.document.command(SPEC).create(ThingCreate(name="a"))
        assert ei.value.code == "read_only_tx"


# ....................... #


class TestCreate:
    async def test_duplicate_id_conflict(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        pk = uuid4()
        await adapter.create(ThingCreate(name="a"), id=pk)
        with pytest.raises(CoreException) as ei:
            await adapter.create(ThingCreate(name="b"), id=pk)
        assert ei.value.kind is ExceptionKind.CONFLICT

    async def test_return_new_false(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        out = await adapter.create(ThingCreate(name="a"), return_new=False)
        assert out is None
        assert len(_docs(state)) == 1

    async def test_create_injects_tenant_id_when_tenant_aware(self) -> None:
        state = MockState()
        tid = uuid4()
        from forze.application.contracts.tenancy import TenantIdentity

        adapter = MockDocumentAdapter(
            spec=SPEC,
            state=state,
            namespace="things",
            read_model=ThingRead,
            domain_model=Thing,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        )
        created = await adapter.create(ThingCreate(name="a"))
        store = adapter._store()  # type: ignore[reportPrivateUsage]
        assert store[created.id]["tenant_id"] == str(tid)


# ....................... #


class TestCreateMany:
    async def test_empty_return_new(self) -> None:
        adapter = _plain_adapter(MockState())
        assert await adapter.create_many([]) == []

    async def test_empty_return_new_false(self) -> None:
        adapter = _plain_adapter(MockState())
        assert await adapter.create_many([], return_new=False) is None

    async def test_return_new_false_path(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        out = await adapter.create_many(
            [ThingCreate(name="a"), ThingCreate(name="b")], return_new=False
        )
        assert out is None
        assert len(_docs(state)) == 2

    async def test_return_new_true_path(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        out = await adapter.create_many([ThingCreate(name="a"), ThingCreate(name="b")])
        assert [r.name for r in out] == ["a", "b"]


# ....................... #


class TestEnsure:
    async def test_ensure_existing_returns_current(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        pk = uuid4()
        await adapter.ensure(pk, ThingCreate(name="first"))
        again = await adapter.ensure(pk, ThingCreate(name="second"))
        assert again.name == "first"  # existing not overwritten
        assert len(_docs(state)) == 1

    async def test_ensure_return_new_false(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        pk = uuid4()
        out = await adapter.ensure(pk, ThingCreate(name="a"), return_new=False)
        assert out is None
        assert pk in _docs(state)

    async def test_ensure_many_empty(self) -> None:
        adapter = _plain_adapter(MockState())
        assert await adapter.ensure_many([]) == []
        assert await adapter.ensure_many([], return_new=False) is None

    async def test_ensure_many_duplicate_ids_raise(self) -> None:
        adapter = _plain_adapter(MockState())
        pk = uuid4()
        with pytest.raises(CoreException) as ei:
            await adapter.ensure_many(
                [
                    KeyedCreate(id=pk, payload=ThingCreate(name="a")),
                    KeyedCreate(id=pk, payload=ThingCreate(name="b")),
                ]
            )
        assert "distinct id" in str(ei.value)

    async def test_ensure_many_return_new_false(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        out = await adapter.ensure_many(
            [KeyedCreate(id=uuid4(), payload=ThingCreate(name="a"))],
            return_new=False,
        )
        assert out is None
        assert len(_docs(state)) == 1

    async def test_ensure_many_return_new_true(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        out = await adapter.ensure_many(
            [KeyedCreate(id=uuid4(), payload=ThingCreate(name="a"))]
        )
        assert [r.name for r in out] == ["a"]


# ....................... #


class TestUpsert:
    async def test_upsert_update_path_existing_doc(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        pk = uuid4()
        await adapter.create(ThingCreate(name="orig"), id=pk)
        out = await adapter.upsert(pk, ThingCreate(name="x"), ThingUpdate(name="new"))
        assert out.name == "new"
        assert len(_docs(state)) == 1

    async def test_upsert_create_path(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        pk = uuid4()
        out = await adapter.upsert(pk, ThingCreate(name="c"), ThingUpdate(name="u"))
        assert out.name == "c"


# ....................... #


class TestPatchCodecAndCheckRev:
    async def test_update_with_no_update_codec_uses_domain(self) -> None:
        # write enabled but no update codec configured -> _patch_codec falls back
        # to the domain codec (document.py lines 150-156).
        state = MockState()
        adapter = _no_update_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        out = await adapter.update(created.id, created.rev, ThingUpdate(name="b"))
        assert out.name == "b"

    def test_patch_codec_read_only_spec_uses_read_codec(self) -> None:
        # A read-only spec (no write) -> _patch_codec returns the read codec
        # (document.py line 157).
        ro_spec = DocumentSpec(name="things", read=ThingRead)
        adapter = MockDocumentAdapter(
            spec=ro_spec,
            state=MockState(),
            namespace="things",
            read_model=ThingRead,
        )
        codec = adapter._patch_codec()  # type: ignore[reportPrivateUsage]
        assert codec is adapter._read_codec()  # type: ignore[reportPrivateUsage]

    async def test_supports_soft_delete_false_when_no_domain_model(self) -> None:
        # domain_model=None -> _supports_soft_delete returns False (line 798).
        ro_spec = DocumentSpec(name="things", read=ThingRead)
        adapter = MockDocumentAdapter(
            spec=ro_spec,
            state=MockState(),
            namespace="things",
            read_model=ThingRead,
        )
        with pytest.raises(CoreException, match="Soft deletion is not supported"):
            await adapter.delete(uuid4(), 0)

    def test_check_rev_none_is_noop(self) -> None:
        # ``expected_rev is None`` short-circuits (document.py line 176); callers
        # always pass an int, so exercise the defensive branch directly.
        adapter = _plain_adapter(MockState())
        adapter._check_rev(5, None)  # type: ignore[reportPrivateUsage]  # no raise


class TestUpdate:
    async def test_rev_conflict_raises(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        # A stale-rev write raises the same revision_mismatch precondition the real adapters do.
        with pytest.raises(CoreException) as excinfo:
            await adapter.update(created.id, created.rev + 99, ThingUpdate(name="b"))
        assert excinfo.value.code == "revision_mismatch"

    async def test_no_diff_keeps_rev_and_returns_doc(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        # Update with the same value: empty diff, rev unchanged.
        out = await adapter.update(created.id, created.rev, ThingUpdate(name="a"))
        assert out.rev == created.rev

    async def test_return_diff_with_change(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        out, diff = await adapter.update(
            created.id, created.rev, ThingUpdate(name="b"), return_diff=True
        )
        assert out.name == "b"
        assert diff.get("name") == "b"
        assert diff.get("rev") == created.rev + 1

    async def test_return_diff_no_change_returns_empty(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        out, diff = await adapter.update(
            created.id, created.rev, ThingUpdate(name="a"), return_diff=True
        )
        assert diff == {}

    async def test_return_new_false_with_diff(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        diff = await adapter.update(
            created.id, created.rev, ThingUpdate(name="b"),
            return_new=False, return_diff=True,
        )
        assert diff.get("name") == "b"

    async def test_return_new_false_no_diff(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        out = await adapter.update(
            created.id, created.rev, ThingUpdate(name="a"), return_new=False
        )
        assert out is None


# ....................... #


class TestUpdateMatching:
    async def test_empty_patch_returns_empty(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        await adapter.create(ThingCreate(name="a"))
        # An all-None update DTO encodes to an empty patch.
        assert await adapter.update_matching({"$values": {}}, ThingUpdate()) == []
        assert (
            await adapter.update_matching({"$values": {}}, ThingUpdate(), return_new=False)
            == 0
        )

    async def test_matched_but_no_change_is_skipped(self) -> None:
        # The filter matches a row, but the patch is a no-op for it: the
        # ``if not diff: continue`` path keeps it untouched and uncounted.
        state = MockState()
        adapter = _plain_adapter(state)
        await adapter.create(ThingCreate(name="a"))
        n = await adapter.update_matching(
            {"$values": {"name": "a"}}, ThingUpdate(name="a"), return_new=False
        )
        assert n == 0

    async def test_unsupported_update_raises(self) -> None:
        state = MockState()
        adapter = _no_update_adapter(state)
        with pytest.raises(CoreException, match="Update command type is not supported"):
            await adapter.update_matching({"$values": {}}, ThingUpdate(name="x"))

    async def test_strict_unsupported_update_raises(self) -> None:
        state = MockState()
        adapter = _no_update_adapter(state)
        with pytest.raises(CoreException, match="Update command type is not supported"):
            await adapter.update_matching_strict({"$values": {}}, ThingUpdate(name="x"))

    async def test_zero_match_filter(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        await adapter.create(ThingCreate(name="a"))
        out = await adapter.update_matching(
            {"$values": {"name": "nope"}}, ThingUpdate(name="z")
        )
        assert out == []

    async def test_match_updates_return_new_false(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        await adapter.create(ThingCreate(name="a"))
        await adapter.create(ThingCreate(name="a"))
        n = await adapter.update_matching(
            {"$values": {"name": "a"}}, ThingUpdate(name="b"), return_new=False
        )
        assert n == 2

    async def test_match_updates_return_new(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        await adapter.create(ThingCreate(name="a"))
        out = await adapter.update_matching(
            {"$values": {"name": "a"}}, ThingUpdate(name="b")
        )
        assert [r.name for r in out] == ["b"]


# ....................... #


class TestTouch:
    async def test_touch_bumps_rev(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        out = await adapter.touch(created.id)
        assert out.rev == created.rev + 1

    async def test_touch_return_new_false(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        assert await adapter.touch(created.id, return_new=False) is None


# ....................... #


class TestDeleteRestoreUnsupported:
    async def test_delete_unsupported_model_raises(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        with pytest.raises(CoreException, match="Soft deletion is not supported"):
            await adapter.delete(created.id, created.rev)

    async def test_restore_unsupported_model_raises(self) -> None:
        state = MockState()
        adapter = _plain_adapter(state)
        created = await adapter.create(ThingCreate(name="a"))
        with pytest.raises(CoreException, match="Soft deletion is not supported"):
            await adapter.restore(created.id, created.rev)

    async def test_delete_many_unsupported_raises(self) -> None:
        adapter = _plain_adapter(MockState())
        with pytest.raises(CoreException, match="Soft deletion is not supported"):
            await adapter.delete_many([(uuid4(), 0)])

    async def test_restore_many_unsupported_raises(self) -> None:
        adapter = _plain_adapter(MockState())
        with pytest.raises(CoreException, match="Soft deletion is not supported"):
            await adapter.restore_many([(uuid4(), 0)])


# ....................... #


class TestSoftDeleteRestore:
    async def test_delete_then_idempotent_delete(self) -> None:
        state = MockState()
        adapter = _soft_adapter(state)
        created = await adapter.create(SoftCreate(name="a"))

        deleted = await adapter.delete(created.id, created.rev)
        assert deleted.is_deleted is True

        # Already-deleted: re-encode without a rev bump.
        again = await adapter.delete(deleted.id, deleted.rev)
        assert again.is_deleted is True
        assert again.rev == deleted.rev

    async def test_delete_return_new_false(self) -> None:
        state = MockState()
        adapter = _soft_adapter(state)
        created = await adapter.create(SoftCreate(name="a"))
        assert await adapter.delete(created.id, created.rev, return_new=False) is None

    async def test_restore_then_idempotent_restore(self) -> None:
        state = MockState()
        adapter = _soft_adapter(state)
        created = await adapter.create(SoftCreate(name="a"))
        deleted = await adapter.delete(created.id, created.rev)

        restored = await adapter.restore(deleted.id, deleted.rev)
        assert restored.is_deleted is False

        # Already-active: re-encode without a rev bump.
        again = await adapter.restore(restored.id, restored.rev)
        assert again.is_deleted is False
        assert again.rev == restored.rev

    async def test_restore_return_new_false(self) -> None:
        state = MockState()
        adapter = _soft_adapter(state)
        created = await adapter.create(SoftCreate(name="a"))
        deleted = await adapter.delete(created.id, created.rev)
        assert (
            await adapter.restore(deleted.id, deleted.rev, return_new=False) is None
        )
