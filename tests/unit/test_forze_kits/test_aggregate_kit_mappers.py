"""`AggregateKit(mappers=..., dtos=...)` maps the inbound DTOs the author declares.

The mappers are the base the kit's own arms compose on: soft deletion adds its exclusion after the
author's list mapper, and versioning strips lineage before the author's update mapper and seeds a
first version from the author's create mapper. An arm that assigned its own mapper instead would
drop the author's silently, so each composition case drives the composed registry.
"""

from __future__ import annotations

from typing import Any, Final

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentDTOs, DocumentMappers
from forze_kits.aggregates.document.dto import DocumentIdRevDTO, DocumentUpdateDTO, ListRequestDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.domain.soft_deletion import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_mock import MockDepsModule

from .test_versioned_kit import (
    POLICY,
    READINGS,
    Reading,
    ReadingCreate,
    ReadingRead,
    ReadingUpdate,
)

# ----------------------- #

_TX: Final = "mock"


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


class WidgetIn(BaseDTO):
    """An inbound create DTO that is not the domain command."""

    label: str


WIDGETS: Final = DocumentSpec(
    name="widgets",
    read=WidgetRead,
    write=DocumentWriteTypes(domain=Widget, create_cmd=WidgetCreate, update_cmd=WidgetUpdate),
)


def _key(spec: DocumentSpec[Any, Any, Any, Any], op: object) -> str:
    return spec.default_namespace.key(op)  # type: ignore[arg-type]


def _upper_group(ctx: Any) -> Any:
    async def _map(source: WidgetCreate) -> WidgetCreate:
        return source.model_copy(update={"group": source.group.upper()})

    return _map


def _double_qty(ctx: Any) -> Any:
    async def _map(source: Any) -> Any:
        return source.model_copy(update={"qty": source.qty * 2})

    return _map


def _group_a_only(ctx: Any) -> Any:
    async def _map(source: ListRequestDTO) -> ListRequestDTO:
        mine = {"$values": {"group": "a"}}
        filters = mine if source.filters is None else {"$and": [mine, source.filters]}
        return source.model_copy(update={"filters": filters})

    return _map


# ....................... #


class TestTheMappers:
    async def test_create_and_update_run_the_authors_mappers(self) -> None:
        kit = AggregateKit(
            spec=WIDGETS, mappers=DocumentMappers(create=_upper_group, update=_double_qty)
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.CREATE), WidgetCreate(group="a"), ctx
            )
            updated = await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=3)),
                ctx,
            )

        assert made.group == "A"
        assert updated.data.qty == 6

    async def test_soft_delete_keeps_the_authors_list_and_update_mappers(self) -> None:
        kit = AggregateKit(
            spec=WIDGETS,
            soft_delete=True,
            mappers=DocumentMappers(list=_group_a_only, update=_double_qty),
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            create = _key(WIDGETS, DocumentKernelOp.CREATE)
            kept = await run_operation(reg, create, WidgetCreate(group="a"), ctx)
            gone = await run_operation(reg, create, WidgetCreate(group="a"), ctx)
            await run_operation(reg, create, WidgetCreate(group="b"), ctx)
            await run_operation(
                reg,
                _key(WIDGETS, SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=gone.id, rev=gone.rev),
                ctx,
            )
            updated = await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=kept.id, rev=kept.rev, dto=WidgetUpdate(qty=2)),
                ctx,
            )
            page = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.LIST), ListRequestDTO(), ctx
            )

        # Both restrictions: the author's (group a) and the kit's (not deleted).
        assert [hit.id for hit in page.hits] == [kept.id]
        assert updated.data.qty == 4

    async def test_versioning_keeps_the_authors_create_and_update_mappers(self) -> None:
        def _meter_upper(ctx: Any) -> Any:
            async def _map(source: ReadingCreate) -> ReadingCreate:
                return source.model_copy(update={"meter": source.meter.upper()})

            return _map

        seen: list[ReadingUpdate] = []

        def _recording(ctx: Any) -> Any:
            async def _map(source: ReadingUpdate) -> ReadingUpdate:
                seen.append(source)
                return source

            return _map

        kit: AggregateKit[ReadingRead, Reading, ReadingCreate, ReadingUpdate] = AggregateKit(
            spec=READINGS,
            versioned=POLICY,
            mappers=DocumentMappers(create=_meter_upper, update=_recording),
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m1"), ctx
            )
            await run_operation(
                reg,
                _key(READINGS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=ReadingUpdate(is_current=False)),
                ctx,
            )

        assert made.meter == "M1"
        # Still seeded as the first version of its own fact.
        assert (made.version, made.root_id) == (1, made.id)
        # The author's update mapper ran, after the kit stripped the lineage field it reserves.
        [mapped] = seen
        assert "is_current" not in mapped.model_fields_set


class TestTheDTOs:
    async def test_an_inbound_dto_other_than_the_command_is_mapped_and_advertised(self) -> None:
        def _from_label(ctx: Any) -> Any:
            async def _map(source: WidgetIn) -> WidgetCreate:
                return WidgetCreate(group=source.label)

            return _map

        kit = AggregateKit(
            spec=WIDGETS,
            dtos=DocumentDTOs(read=WidgetRead, create=WidgetIn, update=WidgetUpdate),
            mappers=DocumentMappers(create=_from_label),
        )
        reg = kit.registry(tx_route=_TX)
        create = _key(WIDGETS, DocumentKernelOp.CREATE)

        descriptor = reg.catalog()[create].descriptor
        assert descriptor is not None and descriptor.input_type is WidgetIn

        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            made = await run_operation(reg, create, WidgetIn(label="z"), runtime.get_context())

        assert made.group == "z"

    def test_a_read_dto_other_than_the_specs_is_refused(self) -> None:
        class OtherRead(ReadDocument):
            group: str

        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=WIDGETS, dtos=DocumentDTOs(read=OtherRead))

        assert caught.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


def _depth_recorder(seen: list[int]) -> Any:
    """A handler factory recording the transaction depth it runs at."""

    def _factory(ctx: Any) -> Any:
        async def _handler(args: Any) -> None:
            seen.append(ctx.tx_ctx.depth())

        return _handler

    return _factory


class TestTransactionalWrites:
    @pytest.mark.parametrize(
        "op",
        [
            DocumentKernelOp.CREATE,
            DocumentKernelOp.UPDATE,
            DocumentKernelOp.KILL,
            SoftDeletionKernelOp.DELETE,
            SoftDeletionKernelOp.RESTORE,
        ],
    )
    async def test_each_write_runs_in_a_transaction(self, op: str) -> None:
        # The handler is replaced, the plan is not: whatever runs the op runs inside the tx.
        seen: list[int] = []
        kit = AggregateKit(
            spec=WIDGETS,
            soft_delete=True,
            transactional_writes=True,
            handlers={op: _depth_recorder(seen)},
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            await run_operation(reg, _key(WIDGETS, op), None, runtime.get_context())

        assert seen == [1]

    async def test_a_generated_create_runs_its_mapper_inside_the_transaction(self) -> None:
        seen: list[int] = []

        def _recording(ctx: Any) -> Any:
            async def _map(source: WidgetCreate) -> WidgetCreate:
                seen.append(ctx.tx_ctx.depth())
                return source

            return _map

        for transactional in (False, True):
            kit = AggregateKit(
                spec=WIDGETS,
                transactional_writes=transactional,
                mappers=DocumentMappers(create=_recording),
            )
            reg = kit.registry(tx_route=_TX)
            runtime = build_runtime(MockDepsModule())

            async with runtime.scope():
                await run_operation(
                    reg,
                    _key(WIDGETS, DocumentKernelOp.CREATE),
                    WidgetCreate(group="a"),
                    runtime.get_context(),
                )

        # Off by default; on, the mapper (a number-id counter, say) shares the write's tx.
        assert seen == [0, 1]

    async def test_it_composes_with_an_arm_that_binds_the_same_write(self) -> None:
        from forze.application.contracts.audit import AuditSpec
        from forze.application.hooks.audit import Audited
        from forze_kits.integrations.audit import AuditDepsModule

        kit = AggregateKit(
            spec=WIDGETS,
            transactional_writes=True,
            audit={DocumentKernelOp.CREATE: Audited(spec=AuditSpec(action="widget.create"))},
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime([MockDepsModule(), AuditDepsModule(tx_route=_TX)])

        async with runtime.scope():
            made = await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.CREATE),
                WidgetCreate(group="a"),
                runtime.get_context(),
            )

        assert made.group == "a"


# ....................... #


class TestUpdateReturnsTheRecord:
    async def test_the_document_factory_returns_and_advertises_the_read_model(self) -> None:
        from forze_kits.aggregates.document import build_document_registry

        reg = build_document_registry(WIDGETS, update_returns="record").freeze()
        update = _key(WIDGETS, DocumentKernelOp.UPDATE)

        descriptor = reg.catalog()[update].descriptor
        assert descriptor is not None and descriptor.output_type is WidgetRead

        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.CREATE), WidgetCreate(group="a"), ctx
            )
            updated = await run_operation(
                reg,
                update,
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=3)),
                ctx,
            )

        assert isinstance(updated, WidgetRead)
        assert (updated.qty, updated.rev) == (3, made.rev + 1)

    def test_an_unknown_mode_is_refused(self) -> None:
        from forze_kits.aggregates.document import build_document_registry

        with pytest.raises(CoreException) as caught:
            build_document_registry(WIDGETS, update_returns="diff")  # type: ignore[arg-type]

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    async def test_search_sync_indexes_the_returned_record(self) -> None:
        from forze.application.contracts.search import SearchSpec
        from forze_mock import MockStateDepKey

        index_spec = SearchSpec(
            name="widgets_index",
            model_type=WidgetRead,
            fields=["group"],
        )
        kit = AggregateKit(spec=WIDGETS, search=index_spec, update_returns="record")
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.CREATE), WidgetCreate(group="a"), ctx
            )
            await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=7)),
                ctx,
            )
            index = ctx.deps.provide(MockStateDepKey).documents["widgets_index"]

            assert index[made.id]["qty"] == 7

    async def test_an_invariant_scopes_by_the_returned_record(self) -> None:
        from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant

        cap = SystemInvariant(
            name="widget_group_cap",
            read_set=ReadSet(spec=WIDGETS, scope_keys=("group",)),
            aggregate=SumOf("qty"),
            holds=lambda total: total <= 10,
        )
        kit = AggregateKit(spec=WIDGETS, invariants=(cap,), update_returns="record")
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.CREATE), WidgetCreate(group="a"), ctx
            )

            with pytest.raises(CoreException):
                await run_operation(
                    reg,
                    _key(WIDGETS, DocumentKernelOp.UPDATE),
                    DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=20)),
                    ctx,
                )
