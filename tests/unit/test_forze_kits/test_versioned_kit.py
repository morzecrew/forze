"""A versioned aggregate corrects instead of overwriting, and reads current unless asked otherwise.

The kit's claim is that one declaration replaces the hand-rolled pattern: lineage fields, a
correction that supersedes in one transaction, a read side that hides superseded rows, and the two
storage guarantees the correctness rests on. Each leg drives the **composed registry** rather than a
handler in isolation, because the wiring is most of what is being claimed — a handler that works
while the read mapper is unattached would pass a unit test and fail a deployment.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import CountAll, ReadSet, SystemInvariant
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document.dto import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    ListRequestDTO,
)
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.aggregates.versioned import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    CorrectDocumentDTO,
    FactAsOfDTO,
    FactIdDTO,
    VersionedKernelOp,
    VersionedPolicy,
)
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mock import MockDepsModule

# ----------------------- #

_TX = "mock"


class Reading(DocWithVersioning):
    meter: str
    kwh: int = 0


class ReadingCreate(CreateCmdWithVersioningFields):
    meter: str
    kwh: int = 0


class ReadingUpdate(UpdateCmdWithVersioning):
    meter: str | None = None
    kwh: int | None = None


class ReadingRead(ReadDocument):
    meter: str
    kwh: int = 0
    root_id: UUID
    version: int
    supersedes_id: UUID | None = None
    is_current: bool = True
    superseded_at: object = None


class CorrectionRead(ReadDocument):
    root_id: UUID
    from_id: UUID
    to_id: UUID
    actor_id: UUID | None = None
    reason: str


READINGS = DocumentSpec(
    name="readings",
    read=ReadingRead,
    write=DocumentWriteTypes(domain=Reading, create_cmd=ReadingCreate, update_cmd=ReadingUpdate),
    guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
)

CORRECTIONS = DocumentSpec(
    name="reading_corrections",
    read=CorrectionRead,
    write=DocumentWriteTypes(domain=CorrectionDoc, create_cmd=CreateCorrectionCmd),
)

POLICY = VersionedPolicy(corrections=CORRECTIONS)


def _kit() -> AggregateKit[ReadingRead, Reading, ReadingCreate, ReadingUpdate]:
    return AggregateKit(spec=READINGS, versioned=POLICY)


def _key(op: object) -> str:
    return READINGS.default_namespace.key(op)  # type: ignore[arg-type]


async def _create(reg, ctx, meter: str, kwh: int) -> ReadingRead:
    return await run_operation(
        reg, _key(DocumentKernelOp.CREATE), ReadingCreate(meter=meter, kwh=kwh), ctx
    )


async def _correct(reg, ctx, row: ReadingRead, *, kwh: int, reason: str = "meter misread"):
    return await run_operation(
        reg,
        _key(VersionedKernelOp.CORRECT),
        CorrectDocumentDTO(
            id=row.id,
            expected_version=row.version,
            dto=ReadingUpdate(kwh=kwh),
            reason=reason,
        ),
        ctx,
    )


# ....................... #


class TestTheFirstVersionSeedsTheFact:
    async def test_a_create_is_version_one_of_a_new_fact(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            row = await _create(reg, runtime.get_context(), "m-1", 100)

        # `root_id` is the fact and `id` is this version of it; on a first insert they are the
        # same value, so a reference to the fact needs no second lookup.
        assert row.root_id == row.id
        assert row.version == 1
        assert row.supersedes_id is None
        assert row.is_current is True

    async def test_a_caller_cannot_declare_its_row_a_version_of_another_fact(self) -> None:
        # The lineage fields are overwritten, not defaulted. Accepting them here would let a
        # create write a row into somebody else's chain without going through `correct`.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)
        stolen = UUID("00000000-0000-0000-0000-0000000000ff")

        async with runtime.scope():
            row = await run_operation(
                reg,
                _key(DocumentKernelOp.CREATE),
                ReadingCreate(meter="m-1", kwh=1, root_id=stolen, version=7),
                runtime.get_context(),
            )

        assert row.root_id == row.id != stolen
        assert row.version == 1


# ....................... #


class TestACorrectionSupersedes:
    async def test_it_inserts_a_successor_and_retires_the_predecessor(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            assert second.version == 2
            assert second.supersedes_id == first.id
            assert second.root_id == first.root_id
            assert second.is_current is True

            # The predecessor is still there, still addressable, and no longer current.
            chain = await run_operation(
                reg, _key(VersionedKernelOp.HISTORY), FactIdDTO(root_id=first.root_id), ctx
            )

        assert [row.version for row in chain.hits] == [1, 2]
        assert [row.is_current for row in chain.hits] == [False, True]
        assert chain.hits[0].superseded_at is not None

    async def test_it_records_who_corrected_it_and_why(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120, reason="meter misread")

            records = await ctx.doc.query(CORRECTIONS).find_many()

        assert len(records.hits) == 1
        record = records.hits[0]
        assert record.root_id == first.root_id
        assert record.from_id == first.id
        assert record.to_id == second.id
        assert record.reason == "meter misread"

    async def test_a_stale_expected_version_is_a_conflict(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)

            # A second caller still holding version 1 corrects the fact it read.
            with pytest.raises(CoreException) as caught:
                await _correct(reg, ctx, first, kwh=140)

        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_correcting_a_superseded_version_is_a_conflict(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)

            # Same row, and this time the caller's version number is right for it — what is
            # wrong is that the row is no longer the fact's current assertion.
            stale = CorrectDocumentDTO(
                id=first.id, expected_version=1, dto=ReadingUpdate(kwh=99), reason="late"
            )

            with pytest.raises(CoreException) as caught:
                await run_operation(reg, _key(VersionedKernelOp.CORRECT), stale, ctx)

        assert caught.value.kind is ExceptionKind.CONFLICT
        assert caught.value.details.get("reason") == "not_current"


# ....................... #


class TestTheReadSideShowsCurrentVersions:
    async def test_list_hides_superseded_versions(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)

            page = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        # One row for the fact, and it is the corrected one — an aggregate that opted into
        # lineage reads like one that never had it.
        assert [row.kwh for row in page.hits] == [120]

    async def test_get_of_a_superseded_version_is_not_found(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=first.id), ctx
                )

        assert caught.value.kind is ExceptionKind.NOT_FOUND

    async def test_get_of_the_current_version_still_works(self) -> None:
        # The contrast: the guard rejects a superseded row, not every row.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            row = await run_operation(
                reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=second.id), ctx
            )

        assert row.kwh == 120

    async def test_a_correction_carries_the_fields_it_did_not_patch(self) -> None:
        # A correction is a new assertion of the whole fact, not a delta: reading the current
        # version must not mean walking the chain and replaying patches.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

        assert second.meter == "m-1"


# ....................... #


class TestAsOfReadsTheVersionThatWasCurrent:
    async def test_it_picks_the_version_current_at_an_instant(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            before = await run_operation(
                reg,
                _key(VersionedKernelOp.AS_OF),
                FactAsOfDTO(root_id=first.root_id, at=first.created_at),
                ctx,
            )
            after = await run_operation(
                reg,
                _key(VersionedKernelOp.AS_OF),
                FactAsOfDTO(root_id=first.root_id, at=second.created_at),
                ctx,
            )

        # The window is half-open, so an instant equal to the correction belongs to the
        # successor and never to both.
        assert before.kwh == 100
        assert after.kwh == 120

    async def test_an_instant_before_the_fact_existed_is_not_found(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    _key(VersionedKernelOp.AS_OF),
                    FactAsOfDTO(root_id=first.root_id, at=first.created_at - timedelta(seconds=1)),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.NOT_FOUND


# ....................... #


class TestTheKitRefusesWithoutItsGuarantees:
    """The kit's correctness rests on declared storage guarantees, not on its own write path.

    Without the first, two corrections of one fact both leave a current row; without the second,
    they both leave a successor and the chain forks — which is the defect in the hand-rolled code
    this kit exists to replace. So a versioned aggregate that could reach a store without them is
    one the kit must refuse to build, and refusing at declaration is the earliest place it can.
    """

    @staticmethod
    def _spec_with(*guarantees: object) -> DocumentSpec:
        return DocumentSpec(
            name="readings",
            read=ReadingRead,
            write=DocumentWriteTypes(
                domain=Reading, create_cmd=ReadingCreate, update_cmd=ReadingUpdate
            ),
            guarantees=guarantees,  # type: ignore[arg-type]
        )

    @pytest.mark.parametrize(
        ("declared", "missing"),
        [
            ((), "unique_together"),
            ((ONE_CURRENT_VERSION,), "unique_together"),
            ((ONE_SUCCESSOR,), "unique_together"),
        ],
    )
    def test_a_spec_missing_either_guarantee_is_refused(
        self,
        declared: tuple[object, ...],
        missing: str,
    ) -> None:
        kit = AggregateKit(spec=self._spec_with(*declared), versioned=POLICY)

        with pytest.raises(CoreException) as caught:
            kit.registry(tx_route=_TX)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert missing in caught.value.details.get("missing", [])

    def test_both_declared_builds(self) -> None:
        # The contrast: the refusal is about the guarantees, not about versioning itself.
        kit = AggregateKit(
            spec=self._spec_with(ONE_CURRENT_VERSION, ONE_SUCCESSOR), versioned=POLICY
        )

        assert kit.registry(tx_route=_TX) is not None


# ....................... #


class TestComposition:
    def test_the_lineage_ops_join_the_document_surface(self) -> None:
        keys = _kit().registry(tx_route=_TX).handlers

        assert _key(DocumentKernelOp.CREATE) in keys
        assert _key(VersionedKernelOp.CORRECT) in keys
        assert _key(VersionedKernelOp.HISTORY) in keys
        assert _key(VersionedKernelOp.AS_OF) in keys

    def test_an_unversioned_kit_has_none_of_them(self) -> None:
        plain = AggregateKit(spec=READINGS)
        keys = plain.registry(tx_route=_TX).handlers

        assert _key(DocumentKernelOp.CREATE) in keys
        assert _key(VersionedKernelOp.CORRECT) not in keys


# ....................... #


ONE_HEAD = SystemInvariant(
    name="single_current_head",
    read_set=ReadSet(
        spec=READINGS,
        scope_keys=("root_id",),
        where={"$values": {"is_current": True}},
    ),
    aggregate=CountAll(),
    holds=lambda n: n <= 1,
)


class TestTheInvariantWatchesTheCorrection:
    """`single_current_head` is the detective control for a path that bypassed the handlers.

    The guarantee is what prevents a fork; the invariant is what notices one, and it has to be
    attached to `correct` and not only to the generated writes — a correction is the operation
    most able to break the law, since it is the only one that writes two rows.
    """

    @staticmethod
    def _isolation(reg: Any, op: Any) -> Any:
        """The isolation floor an op's transaction runs at.

        The signal that a law is *bound*, rather than merely present: preventive enforcement is
        correct only at or above the law's `required_isolation`, so binding one raises the write
        to it. The presence of a plan proves nothing — every operation has one.
        """

        return reg.plans[_key(op)].tx.isolation

    @pytest.mark.parametrize(
        "op", [DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE, VersionedKernelOp.CORRECT]
    )
    def test_the_kit_carries_the_law_without_an_author_declaring_it(self, op: Any) -> None:
        # §1: the kit declares two storage guarantees *and one invariant*. Left to the author it
        # is a control that silently does not exist — nothing about a kit missing it looks
        # different until a path outside the handlers leaves a second current row.
        assert self._isolation(_kit().registry(tx_route=_TX), op) is IsolationLevel.SERIALIZABLE

    @pytest.mark.parametrize("op", [DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE])
    def test_an_unversioned_kit_carries_no_law(self, op: Any) -> None:
        # The contrast, and the reason it is needed: every operation has a plan, so "a plan
        # exists" is not evidence of anything. An unbound write has no isolation floor at all.
        reg = AggregateKit(spec=READINGS).registry(tx_route=_TX)

        assert self._isolation(reg, op) is None

    def test_an_author_law_is_added_to_it_rather_than_replacing_it(self) -> None:
        reg = AggregateKit(spec=READINGS, versioned=POLICY, invariants=(ONE_HEAD,)).registry(
            tx_route=_TX
        )

        assert self._isolation(reg, VersionedKernelOp.CORRECT) is IsolationLevel.SERIALIZABLE

    async def test_a_correction_still_passes_under_the_law(self) -> None:
        # The contrast that keeps the binding from being a way of refusing every correction.
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=READINGS, versioned=POLICY, invariants=(ONE_HEAD,)).registry(
            tx_route=_TX
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

        assert second.version == 2


# ....................... #


class _BothDoc(DocWithVersioning, SoftDeletionMixin):
    meter: str


class _BothCreate(CreateCmdWithVersioningFields):
    meter: str


class _BothUpdate(UpdateCmdWithVersioning, SoftDeletionMixin):
    meter: str | None = None


class _BothRead(ReadDocument):
    meter: str
    root_id: Any = None
    version: int = 1
    supersedes_id: Any = None
    is_current: bool = True
    superseded_at: Any = None
    is_deleted: bool = False


BOTH = DocumentSpec(
    name="both",
    read=_BothRead,
    write=DocumentWriteTypes(
        domain=_BothDoc, create_cmd=_BothCreate, update_cmd=_BothUpdate
    ),
    guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
)


class TestComposedWithSoftDeletion:
    """Both arms guard GET, and only the last one bound actually runs.

    Neither arm's own battery can see this: soft-delete's passes without versioning and
    versioning's passes without soft-delete. The composition is where the guard goes missing, so
    the composition is where it has to be pinned.
    """

    async def test_a_soft_deleted_row_is_still_refused(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=BOTH, soft_delete=True, versioned=POLICY).registry(
            tx_route=_TX
        )
        key = BOTH.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await run_operation(
                reg, key(DocumentKernelOp.CREATE), _BothCreate(meter="m"), ctx
            )
            await run_operation(
                reg,
                key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=row.id, rev=row.rev),
                ctx,
            )

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg, key(DocumentKernelOp.GET), DocumentIdDTO(id=row.id), ctx
                )

        assert caught.value.kind is ExceptionKind.NOT_FOUND

    async def test_a_live_current_row_still_reads(self) -> None:
        # The contrast: two guards on one handler must not refuse everything.
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=BOTH, soft_delete=True, versioned=POLICY).registry(
            tx_route=_TX
        )
        key = BOTH.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await run_operation(
                reg, key(DocumentKernelOp.CREATE), _BothCreate(meter="m"), ctx
            )
            got = await run_operation(
                reg, key(DocumentKernelOp.GET), DocumentIdDTO(id=row.id), ctx
            )

        assert got.meter == "m"
