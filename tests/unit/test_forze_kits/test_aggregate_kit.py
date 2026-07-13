"""`AggregateKit` composes the four primitives (P1-P4) behind one typed declaration (mock).

The composition proof is that a kit declaring soft-delete + search + invariants + outbox freezes a
registry whose write ops carry *all four* concerns without a transaction-scope/merge conflict. The
rest exercises each concern through the kit (so the wiring is really attached) plus the escape hatch.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.contracts.outbox import (
    OutboxDestination,
    OutboxSpec,
)
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import CreateDocumentCmd, DomainEvent, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentIdDTO, DocumentUpdateDTO
from forze_kits.aggregates.document.dto import DocumentIdRevDTO, ListRequestDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.aggregates.storage import StorageFacade, StorageKernelOp
from forze_kits.integrations.outbox import EmitMapping, OutboxEmit, RelayBinding
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule, MockStateDepKey

# ----------------------- #

_TX = "mock"


class WidgetCreated(DomainEvent):
    aggregate_id: UUID


class WidgetPayload(BaseModel):
    widget_id: str


class Widget(DocWithSoftDeletion):
    group: str
    qty: int = 0


class WidgetCreate(CreateDocumentCmd):
    group: str
    qty: int = 0


class WidgetUpdate(UpdateCmdWithSoftDeletion):
    qty: int | None = None


class WidgetRead(ReadDocument):
    group: str
    qty: int = 0
    is_deleted: bool = False


WIDGET_SPEC = DocumentSpec(
    name="widgets",
    read=WidgetRead,
    write=DocumentWriteTypes(
        domain=Widget, create_cmd=WidgetCreate, update_cmd=WidgetUpdate
    ),
)
# soft_delete + search: the kit requires `is_deleted` filterable on the index (facetable
# provisions it via ensure_index) so its search read ops can exclude soft-deleted rows.
WIDGET_INDEX = SearchSpec(
    name="widgets_index",
    model_type=WidgetRead,
    fields=["group"],
    facetable_fields={"is_deleted"},
)
QUEUE = QueueSpec(name="widget-events", codec=PydanticModelCodec(WidgetPayload))
OUTBOX = OutboxSpec(
    name="widget-events",
    codec=PydanticModelCodec(WidgetPayload),
    destination=OutboxDestination.queue(route="widget-events", channel="widget-events"),
)

# A group's total qty must stay within a cap (a cross-record law).
GROUP_CAP = SystemInvariant(
    name="widget_group_cap",
    read_set=ReadSet(spec=WIDGET_SPEC, scope_keys=("group",)),
    aggregate=SumOf("qty"),
    holds=lambda total: total <= 10,
)


def _outbox() -> OutboxEmit:
    return OutboxEmit(
        spec=OUTBOX,
        emits=(
            EmitMapping(
                event=WidgetCreated,
                event_type="widget.created",
                to_payload=lambda e: WidgetPayload(widget_id=str(e.aggregate_id)),
            ),
        ),
        relay=RelayBinding(queue_spec=QUEUE),
    )


def _full_kit() -> AggregateKit[WidgetRead, Widget, WidgetCreate, WidgetUpdate]:
    return AggregateKit(
        spec=WIDGET_SPEC,
        soft_delete=True,
        search=WIDGET_INDEX,
        invariants=(GROUP_CAP,),
        outbox=_outbox(),
    )


def _key(op) -> str:
    return WIDGET_SPEC.default_namespace.key(op)


async def _create(reg, ctx, group: str, qty: int):
    return await run_operation(
        reg, _key(DocumentKernelOp.CREATE), WidgetCreate(group=group, qty=qty), ctx
    )


# ....................... #


class TestComposition:
    def test_all_four_concerns_freeze_without_conflict(self) -> None:
        reg = _full_kit().registry(tx_route=_TX)
        keys = (
            reg.handlers
        )  # freezing all four concerns onto the write ops raised no conflict

        # document write + read ops, soft-delete ops, and the external search query ops all present.
        assert _key(DocumentKernelOp.CREATE) in keys
        assert _key(SoftDeletionKernelOp.DELETE) in keys
        assert _key(SoftDeletionKernelOp.RESTORE) in keys
        assert WIDGET_INDEX.default_namespace.key("typed") in keys

    def test_outbox_bridge_and_relay_are_emitted_separately(self) -> None:
        kit = _full_kit()

        # The staging bridge lands on the domain-event registry (a deps concern)...
        events = kit.domain_events()
        assert events.factories_for(WidgetCreated(aggregate_id=uuid4()))

        # ...and the relay lands on the lifecycle steps (a runtime concern) — never fused.
        assert len(kit.lifecycle_steps()) == 1

    def test_no_outbox_emits_no_bridge_or_relay(self) -> None:
        kit = AggregateKit(spec=WIDGET_SPEC)
        assert not kit.domain_events().factories_for(
            WidgetCreated(aggregate_id=uuid4())
        )
        assert kit.lifecycle_steps() == ()


# ....................... #


class TestConcernsWiredThroughKit:
    async def test_invariant_rolls_back_an_over_cap_write(self) -> None:
        runtime = build_runtime(
            MockDepsModule(domain_events=_full_kit().domain_events())
        )
        reg = _full_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(reg, ctx, "A", 5)  # within cap

            with pytest.raises(CoreException) as ei:
                await _create(reg, ctx, "A", 20)  # would push group A to 25 > 10
            assert ei.value.kind is ExceptionKind.DOMAIN

    async def test_soft_delete_excludes_from_list_and_syncs_search(self) -> None:
        runtime = build_runtime(
            MockDepsModule(domain_events=_full_kit().domain_events())
        )
        reg = _full_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            widget = await _create(ctx=ctx, reg=reg, group="A", qty=1)

            # search sync (P2): the external index bucket tracked the create.
            index = ctx.deps.provide(MockStateDepKey).documents.get("widgets_index", {})
            assert widget.id in index

            # soft-delete (P3): delete then LIST excludes it.
            deleted = await run_operation(
                reg,
                _key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=widget.id, rev=widget.rev),
                ctx,
            )
            listed = await run_operation(
                reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx
            )
            assert listed.count == 0

            # ...and the external index drops it too — no ghost that would 404 on read.
            assert widget.id not in index

            # restore re-adds it to the index.
            await run_operation(
                reg,
                _key(SoftDeletionKernelOp.RESTORE),
                DocumentIdRevDTO(id=widget.id, rev=deleted.rev),
                ctx,
            )
            assert widget.id in index

    async def test_generic_update_that_soft_deletes_drops_the_index_entry(self) -> None:
        runtime = build_runtime(
            MockDepsModule(domain_events=_full_kit().domain_events())
        )
        reg = _full_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            widget = await _create(ctx=ctx, reg=reg, group="A", qty=1)

            index = ctx.deps.provide(MockStateDepKey).documents.get("widgets_index", {})
            assert widget.id in index

            # Soft-deleting through the *generic* UPDATE op (not the DELETE op) must also
            # remove the row from the index — no ghost that search returns and GET 404s.
            await run_operation(
                reg,
                _key(DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(
                    id=widget.id, rev=widget.rev, dto=WidgetUpdate(is_deleted=True)
                ),
                ctx,
            )
            assert widget.id not in index

    async def test_facade_runs_create_end_to_end(self) -> None:
        kit = _full_kit()
        runtime = build_runtime(MockDepsModule(domain_events=kit.domain_events()))
        widgets = kit.facade(runtime, tx_route=_TX)

        async with runtime.scope():
            created = await widgets().create(WidgetCreate(group="A", qty=1))
            assert created.group == "A"


# ....................... #


class TestSearchReadExclusion:
    """Kit search reads must never return a soft-deleted ghost, even one still indexed."""

    async def test_ghost_in_the_index_is_not_returnable(self) -> None:
        from forze_kits.aggregates.search import SearchKernelOp, SearchRequestDTO

        runtime = build_runtime(
            MockDepsModule(domain_events=_full_kit().domain_events())
        )
        reg = _full_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            live = await _create(reg, ctx, "A", 1)
            ghost = await _create(reg, ctx, "A", 1)

            index = ctx.deps.provide(MockStateDepKey).documents["widgets_index"]
            ghost_doc = dict(index[ghost.id])

            # Soft-delete the second row, then re-inject its flagged document — the state
            # a bulk backfill (or an in-flight upsert) can leave in the index.
            await run_operation(
                reg,
                _key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=ghost.id, rev=ghost.rev),
                ctx,
            )
            index[ghost.id] = {**ghost_doc, "is_deleted": True}

            page = await run_operation(
                reg,
                WIDGET_INDEX.default_namespace.key(SearchKernelOp.TYPED),
                SearchRequestDTO(),
                ctx,
            )

            assert [hit.id for hit in page.hits] == [live.id]

    async def test_caller_filters_are_conjoined_not_replaced(self) -> None:
        from forze_kits.aggregates.search import SearchKernelOp, SearchRequestDTO

        runtime = build_runtime(
            MockDepsModule(domain_events=_full_kit().domain_events())
        )
        reg = _full_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            in_group = await _create(reg, ctx, "A", 1)
            await _create(reg, ctx, "B", 1)

            page = await run_operation(
                reg,
                WIDGET_INDEX.default_namespace.key(SearchKernelOp.TYPED),
                SearchRequestDTO(filters={"$values": {"group": "A"}}),
                ctx,
            )

            assert [hit.id for hit in page.hits] == [in_group.id]

    def test_soft_delete_with_unfilterable_index_fails_closed(self) -> None:
        bare = SearchSpec(name="widgets_bare", model_type=WidgetRead, fields=["group"])

        with pytest.raises(CoreException) as ei:
            AggregateKit(spec=WIDGET_SPEC, soft_delete=True, search=bare)
        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


@attrs.define(frozen=True, kw_only=True, slots=True)
class _StubGet(Handler[DocumentIdDTO, str]):
    async def __call__(self, args: DocumentIdDTO) -> str:  # noqa: ARG002
        return "stubbed"


class TestEscapeHatch:
    async def test_handlers_override_a_generated_op(self) -> None:
        kit = AggregateKit(
            spec=WIDGET_SPEC,
            handlers={DocumentKernelOp.GET: lambda ctx: _StubGet()},  # noqa: ARG005
        )
        runtime = build_runtime(MockDepsModule())
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            result = await run_operation(
                reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=uuid4()), ctx
            )
            assert result == "stubbed"  # the override replaced the generated GET

    def test_extra_ops_merge_into_the_registry(self) -> None:
        extra = OperationRegistry(handlers={"widgets.report": lambda ctx: _StubGet()})  # noqa: ARG005
        kit = AggregateKit(spec=WIDGET_SPEC, extra_ops=extra)

        assert "widgets.report" in kit.registry(tx_route=_TX).handlers


# ....................... #


class TestBackendRequirements:
    def test_full_kit_lists_every_wired_route(self) -> None:
        req = _full_kit().backend_requirements(tx_route=_TX)

        assert req.document_route == "widgets"
        assert req.search_route == "widgets_index"
        assert req.outbox_route == "widget-events"
        assert req.tx_route == _TX
        assert req.crypto_required is False

    def test_minimal_kit_has_no_optional_routes(self) -> None:
        req = AggregateKit(spec=WIDGET_SPEC).backend_requirements()

        assert req.document_route == "widgets"
        assert req.search_route is None
        assert req.outbox_route is None
        assert req.crypto_required is False
        assert req.tx_route == "default"

    def test_encrypted_spec_requires_a_keyring(self) -> None:
        from forze.application.contracts.crypto import FieldEncryption

        encrypted = attrs.evolve(
            WIDGET_SPEC, encryption=FieldEncryption(encrypted=frozenset({"group"}))
        )
        assert (
            AggregateKit(spec=encrypted).backend_requirements().crypto_required is True
        )


# ....................... #

_BLOBS = StorageSpec(name="widgets_blobs")


class TestStorageSlice:
    def test_blob_ops_compose_alongside_document_ops(self) -> None:
        keys = (
            AggregateKit(spec=WIDGET_SPEC, storage=_BLOBS)
            .registry(tx_route=_TX)
            .handlers
        )

        # the blob surface, under the storage spec's own namespace...
        assert _BLOBS.default_namespace.key(StorageKernelOp.UPLOAD) in keys
        assert _BLOBS.default_namespace.key(StorageKernelOp.DOWNLOAD) in keys
        # ...and the document surface is intact.
        assert _key(DocumentKernelOp.CREATE) in keys

    def test_name_collision_with_the_document_is_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            AggregateKit(spec=WIDGET_SPEC, storage=StorageSpec(name="widgets"))
        assert ei.value.kind is ExceptionKind.CONFIGURATION  # list/delete would collide

    def test_backend_requirements_reports_the_storage_route(self) -> None:
        req = AggregateKit(spec=WIDGET_SPEC, storage=_BLOBS).backend_requirements()
        assert req.storage_route == "widgets_blobs"

    def test_storage_facade_needs_a_storage_spec(self) -> None:
        runtime = build_runtime(MockDepsModule())
        with pytest.raises(CoreException):
            AggregateKit(spec=WIDGET_SPEC).storage_facade(runtime)

    async def test_storage_facade_yields_a_storage_facade(self) -> None:
        runtime = build_runtime(MockDepsModule())
        factory = AggregateKit(spec=WIDGET_SPEC, storage=_BLOBS).storage_facade(
            runtime, tx_route=_TX
        )

        async with runtime.scope():
            assert isinstance(factory(), StorageFacade)
