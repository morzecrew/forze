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
