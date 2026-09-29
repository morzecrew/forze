"""How an `AggregateKit` writes: the author's mappers and DTOs, its transaction, its update result.

The mappers are the base the kit's own arms compose on: soft deletion adds its exclusion after the
author's list mapper, and versioning strips lineage before the author's update mapper and seeds a
first version from the author's create mapper. An arm that assigned its own mapper instead would
drop the author's silently, so each composition case drives the composed registry. The
transaction legs observe the depth an operation runs at, not only that a binding registered.
"""

from __future__ import annotations

from typing import Any, Final
from uuid import UUID

import pytest
from pydantic import ConfigDict, field_validator
from pydantic.alias_generators import to_camel

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
from forze_kits.aggregates.versioned import ONE_CURRENT_VERSION, ONE_SUCCESSOR
from forze_kits.domain.soft_deletion import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_kits.domain.versioned import (
    CreateCmdWithVersioningFields,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
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
            updated = await run_operation(
                reg,
                _key(READINGS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=ReadingUpdate(is_current=False)),
                ctx,
            )

        assert made.meter == "M1"
        # Still seeded as the first version of its own fact.
        assert (made.version, made.root_id) == (1, made.id)
        # The author's update mapper ran, and the lineage field it passed on was stripped after.
        assert len(seen) == 1
        assert updated.data.is_current is True


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

        seen: dict[str, int] = {}

        def _depth(label: str) -> Any:
            def _factory(ctx: Any) -> Any:
                async def _map(source: Any) -> Any:
                    seen[label] = ctx.tx_ctx.depth()
                    return source

                return _map

            return _factory

        # Audit binds CREATE itself; UPDATE is bound by transactional_writes alone.
        kit = AggregateKit(
            spec=WIDGETS,
            transactional_writes=True,
            mappers=DocumentMappers(create=_depth("create"), update=_depth("update")),
            audit={DocumentKernelOp.CREATE: Audited(spec=AuditSpec(action="widget.create"))},
        )
        reg = kit.registry(tx_route=_TX)
        runtime = build_runtime([MockDepsModule(), AuditDepsModule(tx_route=_TX)])

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(WIDGETS, DocumentKernelOp.CREATE), WidgetCreate(group="a"), ctx
            )
            await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=1)),
                ctx,
            )

        # One scope each: the two bindings on CREATE merged rather than nesting.
        assert seen == {"create": 1, "update": 1}


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
            updated = await run_operation(
                reg,
                _key(WIDGETS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=WidgetUpdate(qty=7)),
                ctx,
            )
            index = ctx.deps.provide(MockStateDepKey).documents["widgets_index"]

            assert index[made.id]["qty"] == 7

        assert isinstance(updated, WidgetRead)

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


# ....................... #


class Meter(DocWithVersioning):
    unit_kwh: int = 0
    label: str | None = None


class MeterCreate(CreateCmdWithVersioningFields):
    unit_kwh: int = 0
    label: str | None = None


class MeterUpdate(UpdateCmdWithVersioning):
    """A camelCase boundary command with a validator that is not idempotent."""

    model_config = ConfigDict(alias_generator=to_camel, frozen=True)

    unit_kwh: int | None = None
    label: str | None = None

    @field_validator("label")
    @classmethod
    def _prefixed(cls, value: str | None) -> str | None:
        return None if value is None else f"x-{value}"


class MeterRead(ReadDocument):
    unit_kwh: int = 0
    label: str | None = None
    root_id: UUID
    version: int
    supersedes_id: UUID | None = None
    is_current: bool = True
    superseded_at: object = None


METERS: Final = DocumentSpec(
    name="meters",
    read=MeterRead,
    write=DocumentWriteTypes(domain=Meter, create_cmd=MeterCreate, update_cmd=MeterUpdate),
    guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
)


class ReadingFix(BaseDTO):
    """An inbound correction in watt-hours; the command stores kilowatt-hours."""

    reading_wh: int | None = None


class ReadingPatchIn(BaseDTO):
    """An inbound patch carrying a field the update command does not have."""

    kwh: int | None = None
    note: str | None = None


def _wh_to_kwh(ctx: Any) -> Any:
    async def _map(source: ReadingFix) -> ReadingUpdate:
        if source.reading_wh is None:
            return ReadingUpdate()

        return ReadingUpdate(kwh=source.reading_wh // 1000)

    return _map


def _kwh_times_ten(ctx: Any) -> Any:
    async def _map(source: ReadingUpdate) -> ReadingUpdate:
        return source.model_copy(update={"kwh": (source.kwh or 0) * 10})

    return _map


def _versioned(**kit: Any) -> Any:
    return AggregateKit(spec=READINGS, versioned=POLICY, **kit).registry(tx_route=_TX)


class TestVersionedUpdateMapping:
    async def test_a_correction_maps_through_the_authors_update_mapper(self) -> None:
        from forze_kits.aggregates.versioned import CorrectDocumentDTO, VersionedKernelOp

        reg = _versioned(
            dtos=DocumentDTOs(read=ReadingRead, create=ReadingCreate, update=ReadingFix),
            mappers=DocumentMappers(update=_wh_to_kwh),
        )
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m", kwh=1), ctx
            )
            corrected = await run_operation(
                reg,
                _key(READINGS, VersionedKernelOp.CORRECT),
                CorrectDocumentDTO(
                    id=made.id, expected_version=1, dto=ReadingFix(reading_wh=9000), reason="wh"
                ),
                ctx,
            )

        assert (corrected.version, corrected.kwh, corrected.meter) == (2, 9, "m")

    async def test_a_custom_update_dto_without_a_mapper_is_mapped_to_the_command(self) -> None:
        from forze_kits.aggregates.versioned import CorrectDocumentDTO, VersionedKernelOp

        reg = _versioned(
            dtos=DocumentDTOs(read=ReadingRead, create=ReadingCreate, update=ReadingPatchIn),
        )
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m"), ctx
            )
            # The inbound-only field is dropped by the DTO-to-command mapping, not sent to the
            # store; nothing the command carries changed, which a version allows.
            await run_operation(
                reg,
                _key(READINGS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=ReadingPatchIn(note="x")),
                ctx,
            )
            corrected = await run_operation(
                reg,
                _key(READINGS, VersionedKernelOp.CORRECT),
                CorrectDocumentDTO(
                    id=made.id, expected_version=1, dto=ReadingPatchIn(kwh=5), reason="fix"
                ),
                ctx,
            )

        assert (corrected.version, corrected.kwh) == (2, 5)

    async def test_lineage_an_authors_mapper_outputs_is_stripped(self) -> None:
        from datetime import UTC, datetime

        class ArchiveIn(BaseDTO):
            archived: bool = False

        def _retiring(ctx: Any) -> Any:
            async def _map(source: ArchiveIn) -> ReadingUpdate:
                return ReadingUpdate(is_current=False, superseded_at=datetime.now(UTC))

            return _map

        reg = _versioned(
            dtos=DocumentDTOs(read=ReadingRead, create=ReadingCreate, update=ArchiveIn),
            mappers=DocumentMappers(update=_retiring),
        )
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m"), ctx
            )
            updated = await run_operation(
                reg,
                _key(READINGS, DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(id=made.id, rev=made.rev, dto=ArchiveIn(archived=True)),
                ctx,
            )

        # An ordinary update cannot retire the only version of a fact, whoever built the patch.
        assert updated.data.is_current is True


    async def test_an_aliased_command_keeps_its_fields_and_validates_once(self) -> None:
        from forze_kits.aggregates.versioned import CorrectDocumentDTO, VersionedKernelOp

        reg = AggregateKit(spec=METERS, versioned=POLICY).registry(tx_route=_TX)
        runtime = build_runtime(MockDepsModule())
        key = METERS.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(reg, key(DocumentKernelOp.CREATE), MeterCreate(), ctx)
            # What a camelCase boundary parses; the validator has already run once.
            patch = MeterUpdate.model_validate({"unitKwh": 7, "label": "a", "isCurrent": False})
            corrected = await run_operation(
                reg,
                key(VersionedKernelOp.CORRECT),
                CorrectDocumentDTO(id=made.id, expected_version=1, dto=patch, reason="fix"),
                ctx,
            )

            # An ordinary update of the same asserted field is refused — so it reached the
            # command rather than being dropped with the lineage field beside it.
            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    key(DocumentKernelOp.UPDATE),
                    DocumentUpdateDTO(
                        id=corrected.id,
                        rev=corrected.rev,
                        dto=MeterUpdate.model_validate({"unitKwh": 8, "isCurrent": False}),
                    ),
                    ctx,
                )

        assert (corrected.version, corrected.unit_kwh, corrected.label) == (2, 7, "x-a")
        assert caught.value.kind is ExceptionKind.DOMAIN

    async def test_a_stripped_lineage_field_takes_its_default_and_reads_as_unset(self) -> None:
        from datetime import UTC, datetime

        from forze_kits.aggregates.versioned.wiring import (
            _without_lineage,  # pyright: ignore[reportPrivateUsage]
        )

        def _retiring(ctx: Any) -> Any:
            async def _map(source: Any) -> ReadingUpdate:
                return ReadingUpdate(kwh=3, is_current=False, superseded_at=datetime.now(UTC))

            return _map

        cmd = await _without_lineage(_retiring)(None)(None)

        assert (cmd.kwh, cmd.is_current, cmd.superseded_at) == (3, None, None)
        assert cmd.model_fields_set == {"kwh"}

    async def test_stripping_lineage_keeps_each_value_on_its_own_field(self) -> None:
        # One field's alias is another field's name: rebuilding by name would hand the first
        # field the second one's value.
        from pydantic import Field as PydanticField

        from forze_kits.aggregates.versioned.wiring import (
            _without_lineage,  # pyright: ignore[reportPrivateUsage]
        )

        class _Crossed(BaseDTO):
            a: int = PydanticField(0, alias="b_alias")
            b_alias: int = PydanticField(7, alias="zz")
            is_current: bool | None = None

        def _retiring(ctx: Any) -> Any:
            async def _map(source: Any) -> _Crossed:
                return _Crossed.model_validate({"b_alias": 3, "zz": 9, "is_current": False})

            return _map

        cmd = await _without_lineage(_retiring)(None)(None)

        assert (cmd.a, cmd.b_alias, cmd.is_current) == (3, 9, None)
        assert cmd.model_fields_set == {"a", "b_alias"}

    async def test_wiring_used_directly_maps_update_and_correct_alike(self) -> None:
        from forze_kits.aggregates.document import build_document_registry
        from forze_kits.aggregates.versioned import (
            CorrectDocumentDTO,
            VersionedKernelOp,
            versioned_wiring,
        )

        dtos = DocumentDTOs(read=ReadingRead, create=ReadingCreate, update=ReadingFix)
        wiring = versioned_wiring(READINGS, POLICY, dtos=dtos)

        # A mapper handed to mappers() that the wiring's ops never see is refused, not dropped.
        with pytest.raises(CoreException) as caught:
            wiring.mappers(DocumentMappers(update=_wh_to_kwh))

        assert caught.value.kind is ExceptionKind.CONFIGURATION

        wiring = versioned_wiring(READINGS, POLICY, dtos=dtos, update_mapper=_wh_to_kwh)
        reg = wiring.bind(build_document_registry(READINGS, dtos, wiring.mappers())).freeze()
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m", kwh=1), ctx
            )
            corrected = await run_operation(
                reg,
                _key(READINGS, VersionedKernelOp.CORRECT),
                CorrectDocumentDTO(
                    id=made.id, expected_version=1, dto=ReadingFix(reading_wh=9000), reason="r"
                ),
                ctx,
            )

        assert corrected.kwh == 9


# ....................... #


class TestTheDeclarationIsWhole:
    def test_an_empty_dto_slot_disables_its_operation(self) -> None:
        # The document factory's idiom for "this aggregate has no update": the kit keeps it.
        kit = AggregateKit(spec=WIDGETS, dtos=DocumentDTOs(read=WidgetRead, create=WidgetCreate))
        keys = kit.registry(tx_route=_TX).catalog()

        assert _key(WIDGETS, DocumentKernelOp.CREATE) in keys
        assert _key(WIDGETS, DocumentKernelOp.UPDATE) not in keys

    @pytest.mark.parametrize(
        ("dtos", "option"),
        [
            (
                DocumentDTOs(read=WidgetRead, update=WidgetUpdate),
                {"mappers": DocumentMappers(create=_upper_group)},
            ),
            (
                DocumentDTOs(read=WidgetRead, create=WidgetCreate),
                {"mappers": DocumentMappers(update=_double_qty)},
            ),
            (DocumentDTOs(read=WidgetRead, create=WidgetCreate), {"update_returns": "record"}),
        ],
        ids=["create-mapper", "update-mapper", "record"],
    )
    def test_an_option_for_a_disabled_operation_is_refused(
        self, dtos: DocumentDTOs[Any, Any, Any], option: dict[str, Any]
    ) -> None:
        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=WIDGETS, dtos=dtos, **option)

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    async def test_a_versioned_kit_without_update_still_corrects_through_the_mapper(self) -> None:
        # UPDATE disabled, CORRECT kept: the update mapper still has an operation to serve.
        from forze_kits.aggregates.versioned import CorrectDocumentDTO, VersionedKernelOp

        reg = _versioned(
            dtos=DocumentDTOs(read=ReadingRead, create=ReadingCreate),
            mappers=DocumentMappers(update=_kwh_times_ten),
        )
        assert _key(READINGS, DocumentKernelOp.UPDATE) not in reg.catalog()

        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            made = await run_operation(
                reg, _key(READINGS, DocumentKernelOp.CREATE), ReadingCreate(meter="m"), ctx
            )
            corrected = await run_operation(
                reg,
                _key(READINGS, VersionedKernelOp.CORRECT),
                CorrectDocumentDTO(
                    id=made.id, expected_version=1, dto=ReadingUpdate(kwh=2), reason="fix"
                ),
                ctx,
            )

        assert corrected.kwh == 20

    @pytest.mark.parametrize(
        "option",
        [
            {"mappers": DocumentMappers(create=_upper_group)},
            {"mappers": DocumentMappers(update=_double_qty)},
            {"dtos": DocumentDTOs(read=WidgetRead, create=WidgetCreate)},
            {"dtos": DocumentDTOs(read=WidgetRead, update=WidgetUpdate)},
            {"update_returns": "record"},
            {"transactional_writes": True},
        ],
        ids=["create-mapper", "update-mapper", "create-dto", "update-dto", "record", "tx"],
    )
    def test_a_write_option_on_a_read_only_spec_is_refused(self, option: dict[str, Any]) -> None:
        read_only = DocumentSpec(name="widgets", read=WidgetRead)

        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=read_only, **option)

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize(
        "option",
        [
            {"mappers": DocumentMappers(update=_double_qty)},
            {"dtos": DocumentDTOs(read=WidgetRead, update=WidgetUpdate)},
            {"update_returns": "record"},
        ],
        ids=["update-mapper", "update-dto", "record"],
    )
    def test_an_update_option_without_an_update_command_is_refused(
        self, option: dict[str, Any]
    ) -> None:
        create_only = DocumentSpec(
            name="widgets",
            read=WidgetRead,
            write=DocumentWriteTypes(domain=Widget, create_cmd=WidgetCreate),
        )

        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=create_only, **option)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
