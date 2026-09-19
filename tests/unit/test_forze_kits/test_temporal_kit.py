"""An effective-dated aggregate answers what is in force, and refuses what it cannot keep.

Each leg drives the **composed registry** rather than a handler in isolation: the declaration's
claim is that one field on the kit produces the fields, the two reads, the guarantee and the
refusals together, and a handler that works while its guarantee is unwired would pass a unit
test and fail a deployment.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import NonOverlapping
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document.dto import DocumentIdRevDTO, DocumentUpdateDTO, ListRequestDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.aggregates.temporal import (
    EffectiveOnDTO,
    TemporalKernelOp,
    TemporalPolicy,
    TimelineDTO,
    temporal_facade,
)
from forze_kits.aggregates.versioned import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    CorrectDocumentDTO,
    VersionedKernelOp,
    VersionedPolicy,
)
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze_kits.domain.temporal import CreateCmdWithTemporalFields, DocWithTemporal
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mock import MockDepsModule

pytestmark = [pytest.mark.asyncio]

# ----------------------- #

_TX = "mock"

POLICY = TemporalPolicy(key=("employee_id",), bounds="[]")
NO_OVERLAP = NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]")


class Contract(DocWithTemporal):
    employee_id: str
    hours: int


class HalfOpenContract(DocWithTemporal):
    temporal_bounds = "[)"

    employee_id: str
    hours: int


class ContractRead(ReadDocument):
    employee_id: str
    hours: int
    valid_from: date
    valid_to: date | None = None


class ContractCreate(CreateCmdWithTemporalFields):
    employee_id: str
    hours: int


class ContractUpdate(BaseDTO):
    hours: int | None = None
    valid_to: date | None = None


CONTRACTS = DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
    name="contracts",
    read=ContractRead,
    write=DocumentWriteTypes(domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate),
    guarantees=(NO_OVERLAP,),
)


class DeletableContract(DocWithTemporal, SoftDeletionMixin):
    employee_id: str
    hours: int


class DeletableRead(ReadDocument):
    employee_id: str
    hours: int
    valid_from: date
    valid_to: date | None = None
    is_deleted: bool = False


class DeletableCreate(CreateCmdWithTemporalFields):
    employee_id: str
    hours: int


DELETABLE = DocumentSpec[DeletableRead, DeletableContract, DeletableCreate, ContractUpdate](
    name="contracts",
    read=DeletableRead,
    write=DocumentWriteTypes(
        domain=DeletableContract, create_cmd=DeletableCreate, update_cmd=ContractUpdate
    ),
    guarantees=(NO_OVERLAP,),
)


def _kit() -> AggregateKit[ContractRead, Contract, ContractCreate, ContractUpdate]:
    return AggregateKit(spec=CONTRACTS, temporal=POLICY)


def _half_open_kit() -> AggregateKit[Any, Any, Any, Any]:
    """A kit whose convention excludes the end day, where a same-day period covers nothing."""

    return AggregateKit(
        spec=DocumentSpec[ContractRead, HalfOpenContract, ContractCreate, ContractUpdate](
            name="contracts",
            read=ContractRead,
            write=DocumentWriteTypes(
                domain=HalfOpenContract,
                create_cmd=ContractCreate,
                update_cmd=ContractUpdate,
            ),
            guarantees=(
                NonOverlapping(
                    key=("employee_id",), period=("valid_from", "valid_to"), bounds="[)"
                ),
            ),
        ),
        temporal=TemporalPolicy(key=("employee_id",), bounds="[)"),
    )


def _key(op: object) -> str:
    return CONTRACTS.default_namespace.key(op)  # type: ignore[arg-type]


async def _create(reg, ctx, *, employee: str, hours: int, start: date, end: date | None):
    return await run_operation(
        reg,
        _key(DocumentKernelOp.CREATE),
        ContractCreate(employee_id=employee, hours=hours, valid_from=start, valid_to=end),
        ctx,
    )


# ....................... #


class TestWhatIsInForceOnADay:
    async def test_the_row_covering_the_day_is_returned(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )
            await _create(reg, ctx, employee="e1", hours=20, start=date(2026, 4, 1), end=None)

            row = await run_operation(
                reg,
                _key(TemporalKernelOp.EFFECTIVE_ON),
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 15)),
                ctx,
            )

        assert row.hours == 40

    async def test_the_declared_bounds_decide_the_last_day(self) -> None:
        # `[]` puts the end day in force, which is what "valid through 31 March" means to the
        # human who typed it. The convention is declared precisely so this is not a guess.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )

            row = await run_operation(
                reg,
                _key(TemporalKernelOp.EFFECTIVE_ON),
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 3, 31)),
                ctx,
            )

        assert row.hours == 40

    async def test_a_day_before_the_first_period_is_not_found(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    _key(TemporalKernelOp.EFFECTIVE_ON),
                    EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2025, 12, 31)),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.NOT_FOUND

    async def test_an_open_ended_row_is_in_force_indefinitely(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(reg, ctx, employee="e1", hours=35, start=date(2026, 1, 1), end=None)

            row = await run_operation(
                reg,
                _key(TemporalKernelOp.EFFECTIVE_ON),
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2099, 1, 1)),
                ctx,
            )

        assert row.hours == 35

    async def test_another_key_is_never_answered_with(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=None)

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    _key(TemporalKernelOp.EFFECTIVE_ON),
                    EffectiveOnDTO(key={"employee_id": "e2"}, on=date(2026, 6, 1)),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.NOT_FOUND


# ....................... #


class TestTheTimeline:
    async def test_a_window_returns_the_rows_it_meets_in_order(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )
            await _create(
                reg, ctx, employee="e1", hours=30, start=date(2026, 4, 1), end=date(2026, 6, 30)
            )
            await _create(reg, ctx, employee="e1", hours=20, start=date(2026, 7, 1), end=None)

            page = await run_operation(
                reg,
                _key(TemporalKernelOp.TIMELINE),
                TimelineDTO(
                    key={"employee_id": "e1"}, start=date(2026, 3, 1), end=date(2026, 5, 1)
                ),
                ctx,
            )

        assert [row.hours for row in page.hits] == [40, 30]

    async def test_a_window_meeting_nothing_is_empty(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )

            page = await run_operation(
                reg,
                _key(TemporalKernelOp.TIMELINE),
                TimelineDTO(
                    key={"employee_id": "e1"}, start=date(2027, 1, 1), end=date(2027, 2, 1)
                ),
                ctx,
            )

        assert list(page.hits) == []

    async def test_an_open_window_reaches_the_open_row(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(reg, ctx, employee="e1", hours=20, start=date(2026, 7, 1), end=None)

            page = await run_operation(
                reg,
                _key(TemporalKernelOp.TIMELINE),
                TimelineDTO(key={"employee_id": "e1"}, start=date(2099, 1, 1), end=None),
                ctx,
            )

        assert [row.hours for row in page.hits] == [20]

    async def test_the_timeline_is_a_page(self) -> None:
        # The bound the design did not name: a timeline is unbounded by nature, and an
        # unbounded read meets the store's implicit cap and truncates without saying so.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()

            for month in range(1, 7):
                await _create(
                    reg,
                    ctx,
                    employee="e1",
                    hours=month,
                    start=date(2026, month, 1),
                    end=date(2026, month, 28),
                )

            page = await run_operation(
                reg,
                _key(TemporalKernelOp.TIMELINE),
                TimelineDTO(
                    key={"employee_id": "e1"},
                    start=date(2026, 1, 1),
                    end=date(2026, 12, 31),
                    size=2,
                ),
                ctx,
            )

        assert [row.hours for row in page.hits] == [1, 2]


# ....................... #


class TestTheStoreKeepsTheRuleTheReadsAssume:
    async def test_an_overlapping_row_is_refused(self) -> None:
        # The declaration's whole point: "the row in force on a day" is a well-formed question
        # only because the store refuses to hold two of them.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )

            with pytest.raises(CoreException) as caught:
                await _create(
                    reg,
                    ctx,
                    employee="e1",
                    hours=20,
                    start=date(2026, 3, 1),
                    end=date(2026, 5, 1),
                )

        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_a_row_under_another_key_is_accepted(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )
            row = await _create(
                reg, ctx, employee="e2", hours=20, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )

        assert row.employee_id == "e2"

    async def test_a_kit_without_the_guarantee_refuses_to_build(self) -> None:
        bare = DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
            name="contracts",
            read=ContractRead,
            write=DocumentWriteTypes(
                domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
            ),
        )

        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=bare, temporal=POLICY).registry(tx_route=_TX)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "NonOverlapping" in caught.value.summary

    async def test_a_guarantee_with_other_bounds_does_not_satisfy_the_policy(self) -> None:
        # The convention has to be the same value in both places, or the reads answer one
        # question and the store keeps a different one.
        mismatched = DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
            name="contracts",
            read=ContractRead,
            write=DocumentWriteTypes(
                domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
            ),
            guarantees=(
                NonOverlapping(
                    key=("employee_id",), period=("valid_from", "valid_to"), bounds="[)"
                ),
            ),
        )

        with pytest.raises(CoreException):
            AggregateKit(spec=mismatched, temporal=POLICY).registry(tx_route=_TX)


# ....................... #


class TestAPeriodInForceOnNoDay:
    async def test_a_create_of_an_empty_period_is_refused(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _half_open_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    _key(DocumentKernelOp.CREATE),
                    ContractCreate(
                        employee_id="e1",
                        hours=40,
                        valid_from=date(2026, 1, 1),
                        valid_to=date(2026, 1, 1),
                    ),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.DOMAIN

    async def test_the_same_dates_are_fine_when_the_end_is_in_force(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 1, 1)
            )

        assert row.valid_to == date(2026, 1, 1)

    async def test_an_update_cannot_shrink_a_row_into_one(self) -> None:
        # The other write path. A row edited into a period covering no day is as unreadable as
        # one created that way.
        runtime = build_runtime(MockDepsModule())
        reg = _half_open_kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await run_operation(
                reg,
                _key(DocumentKernelOp.CREATE),
                ContractCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 6, 1),
                ),
                ctx,
            )

            with pytest.raises(CoreException):
                await run_operation(
                    reg,
                    _key(DocumentKernelOp.UPDATE),
                    DocumentUpdateDTO(
                        id=row.id, rev=row.rev, dto=ContractUpdate(valid_to=date(2026, 1, 1))
                    ),
                    ctx,
                )


# ....................... #


class TestTheDatedReadsSeeWhatTheOtherArmsHide:
    """A composed aggregate must not answer "in force" with a row it hides everywhere else.

    Soft deletion and versioning install their exclusions on the mapper slots the *generated*
    reads share. These two reads build their own filter, so they inherit nothing — and a read
    that returns a soft-deleted contract, or a version that has since been corrected, is worse
    than one that does not exist: every other read in the aggregate disagrees with it.
    """

    async def test_a_soft_deleted_row_is_not_in_force(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=DELETABLE, soft_delete=True, temporal=POLICY).registry(tx_route=_TX)
        key = DELETABLE.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await run_operation(
                reg,
                key(DocumentKernelOp.CREATE),
                DeletableCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                ),
                ctx,
            )
            await run_operation(
                reg,
                key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=row.id, rev=row.rev),
                ctx,
            )

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    key(TemporalKernelOp.EFFECTIVE_ON),
                    EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 1)),
                    ctx,
                )

            page = await run_operation(
                reg,
                key(TemporalKernelOp.TIMELINE),
                TimelineDTO(
                    key={"employee_id": "e1"},
                    start=date(2026, 1, 1),
                    end=date(2026, 12, 31),
                ),
                ctx,
            )

        assert caught.value.kind is ExceptionKind.NOT_FOUND
        assert list(page.hits) == []

    async def test_the_same_row_is_in_force_before_it_is_deleted(self) -> None:
        # The contrast: the restriction excludes deleted rows, not every row.
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=DELETABLE, soft_delete=True, temporal=POLICY).registry(tx_route=_TX)
        key = DELETABLE.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            await run_operation(
                reg,
                key(DocumentKernelOp.CREATE),
                DeletableCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                ),
                ctx,
            )
            row = await run_operation(
                reg,
                key(TemporalKernelOp.EFFECTIVE_ON),
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 1)),
                ctx,
            )

        assert row.hours == 40


# ....................... #


class TestTheDeclarationRefusesWhatItCannotServe:
    async def test_a_read_model_hiding_the_dates_cannot_carry_the_guarantee(self) -> None:
        # The refusal lives on the spec, not on this kit: a temporal aggregate must declare the
        # non-overlap guarantee, the guarantee names the validity fields, and a spec refuses a
        # guarantee naming a field it does not store. A second check in the kit could only fire
        # for a spec that cannot be built.
        class Hidden(ReadDocument):
            employee_id: str
            hours: int

        with pytest.raises(CoreException) as caught:
            DocumentSpec[Any, Contract, ContractCreate, ContractUpdate](
                name="contracts",
                read=Hidden,
                write=DocumentWriteTypes(
                    domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
                ),
                guarantees=(NO_OVERLAP,),
            )

        assert "valid_from" in caught.value.summary

    async def test_a_key_the_aggregate_does_not_store_cannot_carry_it_either(self) -> None:
        with pytest.raises(CoreException) as caught:
            DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
                name="contracts",
                read=ContractRead,
                write=DocumentWriteTypes(
                    domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
                ),
                guarantees=(
                    NonOverlapping(
                        key=("cost_centre",), period=("valid_from", "valid_to"), bounds="[]"
                    ),
                ),
            )

        assert "cost_centre" in caught.value.summary

    async def test_a_policy_key_the_guarantee_does_not_name_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            AggregateKit(
                spec=CONTRACTS, temporal=TemporalPolicy(key=("cost_centre",), bounds="[]")
            ).registry(tx_route=_TX)

        assert "cost_centre" in caught.value.summary

    async def test_a_model_whose_convention_differs_from_the_policy_is_refused(self) -> None:
        # The convention is stated twice by necessity — the reads need it on the policy, the
        # write path needs it on the model — so the one thing that must not happen is the two
        # disagreeing, and it is refused rather than reconciled.
        spec = DocumentSpec[ContractRead, HalfOpenContract, ContractCreate, ContractUpdate](
            name="contracts",
            read=ContractRead,
            write=DocumentWriteTypes(
                domain=HalfOpenContract,
                create_cmd=ContractCreate,
                update_cmd=ContractUpdate,
            ),
            guarantees=(NO_OVERLAP,),
        )

        with pytest.raises(CoreException) as caught:
            AggregateKit(spec=spec, temporal=POLICY).registry(tx_route=_TX)

        assert "temporal_bounds" in caught.value.summary

    async def test_the_facade_reaches_both_reads(self) -> None:
        runtime = build_runtime(MockDepsModule())
        kit = _kit()
        facade = temporal_facade(runtime, kit.registry(tx_route=_TX), CONTRACTS)

        async with runtime.scope():
            await facade().create(
                ContractCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                )
            )
            row = await facade().effective_on(
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 1))
            )
            page = await facade().timeline(
                TimelineDTO(
                    key={"employee_id": "e1"}, start=date(2026, 1, 1), end=date(2026, 12, 31)
                )
            )

        assert row.hours == 40
        assert len(list(page.hits)) == 1

    async def test_the_generated_list_is_untouched(self) -> None:
        # Effective dating adds questions; it does not change the answers already there.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(
                reg, ctx, employee="e1", hours=40, start=date(2026, 1, 1), end=date(2026, 3, 31)
            )
            await _create(reg, ctx, employee="e1", hours=20, start=date(2026, 4, 1), end=None)

            page = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        assert len(list(page.hits)) == 2


# ....................... #


class TestTheKitsOwnFrontDoor:
    """The accessors most callers use, which the handler-level batteries never touch."""

    async def test_the_plain_facade_carries_the_dated_reads(self) -> None:
        # `facade()` is the surface an author reaches for first; a kit that left the reads in
        # the registry and out of that facade would deliver half the declaration.
        runtime = build_runtime(MockDepsModule())
        facade = _kit().facade(runtime)

        async with runtime.scope():
            await facade().create(
                ContractCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                )
            )
            row = await facade().effective_on(  # type: ignore[attr-defined]
                EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 1))
            )

        assert row.hours == 40

    async def test_validity_facade_is_the_precisely_typed_one(self) -> None:
        runtime = build_runtime(MockDepsModule())
        facade = _kit().validity_facade(runtime)

        async with runtime.scope():
            await facade().create(
                ContractCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                )
            )
            page = await facade().timeline(
                TimelineDTO(
                    key={"employee_id": "e1"}, start=date(2026, 1, 1), end=date(2026, 12, 31)
                )
            )

        assert len(list(page.hits)) == 1

    async def test_validity_facade_without_the_policy_is_refused(self) -> None:
        runtime = build_runtime(MockDepsModule())
        plain = AggregateKit(spec=CONTRACTS)

        with pytest.raises(CoreException) as caught:
            plain.validity_facade(runtime)

        assert caught.value.kind is ExceptionKind.PRECONDITION

    async def test_a_policy_with_no_key_is_refused(self) -> None:
        # Without a key the aggregate asserts that no two rows in the whole relation overlap,
        # which is one timeline for every record it holds.
        with pytest.raises(CoreException) as caught:
            TemporalPolicy(key=())

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    async def test_a_model_without_the_mixin_is_left_alone(self) -> None:
        # A domain model that never declared a convention states nothing to disagree with, so
        # the bounds check has nothing to compare and must not invent a mismatch.
        from forze_kits.aggregates.temporal.wiring import temporal_wiring

        assert temporal_wiring(CONTRACTS, POLICY) is not None


# ....................... #


class BitemporalContract(DocWithTemporal, DocWithVersioning):
    employee_id: str
    hours: int


class BitemporalRead(ReadDocument):
    employee_id: str
    hours: int
    valid_from: date
    valid_to: date | None = None
    root_id: UUID
    version: int = 1
    supersedes_id: UUID | None = None
    is_current: bool = True
    superseded_at: datetime | None = None


class BitemporalCreate(CreateCmdWithTemporalFields, CreateCmdWithVersioningFields):
    employee_id: str
    hours: int


class BitemporalUpdate(UpdateCmdWithVersioning):
    hours: int | None = None


class CorrectionRead(ReadDocument):
    root_id: UUID
    from_id: UUID
    to_id: UUID
    actor_id: UUID | None = None
    reason: str


CORRECTIONS = DocumentSpec(
    name="contract_corrections",
    read=CorrectionRead,
    write=DocumentWriteTypes(domain=CorrectionDoc, create_cmd=CreateCorrectionCmd),
)

BITEMPORAL = DocumentSpec[BitemporalRead, BitemporalContract, BitemporalCreate, BitemporalUpdate](
    name="contracts",
    read=BitemporalRead,
    write=DocumentWriteTypes(
        domain=BitemporalContract,
        create_cmd=BitemporalCreate,
        update_cmd=BitemporalUpdate,
    ),
    guarantees=(NO_OVERLAP, ONE_CURRENT_VERSION, ONE_SUCCESSOR),
)


class TestTheBitemporalCaseIsRefusedForNow:
    """Validity and lineage on one aggregate: refused, loudly, rather than wired and broken.

    The two arms answer different questions and ought to compose — when the fact applied, and
    when we asserted it. They do not yet, and the reason is not a wiring detail: a correction
    inserts a successor carrying its **predecessor's dates**, because it corrects what the row
    says rather than when it applied. Two rows under one key then hold the same period, and the
    non-overlap guarantee refuses the correction.

    What the composition needs is a non-overlap guarantee restricted to current versions — the
    filtered form the uniqueness member already has and this one does not. Until the vocabulary
    grows it, the declaration is refused at build: an aggregate whose every correction fails is
    worse than one that says so before it runs.
    """

    @staticmethod
    def _kit() -> AggregateKit[Any, Any, Any, Any]:
        return AggregateKit(
            spec=BITEMPORAL,
            temporal=POLICY,
            versioned=VersionedPolicy(corrections=CORRECTIONS),
        )

    async def test_declaring_both_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            self._kit()

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "current versions" in caught.value.summary

    async def test_the_refusal_is_what_a_correction_would_have_hit(self) -> None:
        # The defect the refusal stands in for, reproduced through the lineage arm alone: the
        # successor carries the predecessor's period, and the guarantee refuses it.
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(
            spec=BITEMPORAL, versioned=VersionedPolicy(corrections=CORRECTIONS)
        ).registry(tx_route=_TX)
        key = BITEMPORAL.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await run_operation(
                reg,
                key(DocumentKernelOp.CREATE),
                BitemporalCreate(
                    employee_id="e1",
                    hours=40,
                    valid_from=date(2026, 1, 1),
                    valid_to=date(2026, 3, 31),
                ),
                ctx,
            )

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    key(VersionedKernelOp.CORRECT),
                    CorrectDocumentDTO(
                        id=first.id,
                        expected_version=first.version,
                        dto=BitemporalUpdate(hours=35),
                        reason="payroll misread the contract",
                    ),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.CONFLICT
        assert "overlapping periods" in caught.value.summary


# ....................... #


class TestAReadOnlyAggregateCanBeDated:
    async def test_a_spec_with_no_write_side_wires(self) -> None:
        # A view-backed effective-dated aggregate: nothing writes it, so there is no domain
        # model to read a convention off, and the bounds check must not reach for one.
        from forze_kits.aggregates.temporal.wiring import temporal_wiring

        read_only = DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
            name="contracts",
            read=ContractRead,
            guarantees=(NO_OVERLAP,),
        )

        assert temporal_wiring(read_only, POLICY).policy is POLICY
