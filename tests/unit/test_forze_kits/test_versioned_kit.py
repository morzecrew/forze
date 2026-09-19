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
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document.dto import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentUpdateDTO,
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
    build_versioned_registry,
    versioned_facade,
)
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mock import MockDepsModule, MockStateDepKey

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

    async def test_list_hides_a_row_that_is_current_and_deleted(self) -> None:
        # The other half of the shared mapper slot, and the one the composition actually broke:
        # the versioned list mapper replaced soft deletion's, so a deleted row that was still
        # the current version came back in every list.
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
            page = await run_operation(reg, key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        assert page.hits == []

    async def test_list_still_returns_a_live_current_row(self) -> None:
        # The contrast: stacking two restrictions must not exclude everything.
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=BOTH, soft_delete=True, versioned=POLICY).registry(
            tx_route=_TX
        )
        key = BOTH.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            await run_operation(reg, key(DocumentKernelOp.CREATE), _BothCreate(meter="m"), ctx)
            page = await run_operation(reg, key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        assert [r.meter for r in page.hits] == ["m"]

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


# ....................... #


class TestTheRestrictionSurvivesACallersFilter:
    """The list restriction has two branches and a caller's filter takes the other one.

    With no filters the mapper returns the restriction alone; with filters it conjoins. A leg
    that only ever lists unfiltered exercises the first branch and leaves the second — the one a
    real caller hits — unproven.
    """

    async def test_a_filtered_list_still_hides_superseded_versions(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)
            await _create(reg, ctx, "m-2", 500)

            page = await run_operation(
                reg,
                _key(DocumentKernelOp.LIST),
                ListRequestDTO(filters={"$values": {"meter": "m-1"}}),
                ctx,
            )

        assert [row.kwh for row in page.hits] == [120]

    async def test_the_callers_filter_is_still_applied(self) -> None:
        # The contrast: conjoining must not swallow what the caller asked for.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            await _create(reg, ctx, "m-1", 100)
            await _create(reg, ctx, "m-2", 500)

            page = await run_operation(
                reg,
                _key(DocumentKernelOp.LIST),
                ListRequestDTO(filters={"$values": {"meter": "m-2"}}),
                ctx,
            )

        assert [row.meter for row in page.hits] == ["m-2"]


# ....................... #


class TestAWrongVersionOnACurrentRow:
    async def test_it_is_a_conflict_naming_the_version(self) -> None:
        """The version check, reached on a row that *is* current.

        The stale-caller leg cannot prove this one: by the time a caller's version is stale the
        row it read has usually been superseded, so the not-current check fires first and the
        version comparison is never reached. A caller working from a stale cache passes a wrong
        number for a row that is still current, and that is the case this pins.
        """

        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)

            with pytest.raises(CoreException) as caught:
                await run_operation(
                    reg,
                    _key(VersionedKernelOp.CORRECT),
                    CorrectDocumentDTO(
                        id=first.id,
                        expected_version=7,
                        dto=ReadingUpdate(kwh=120),
                        reason="stale cache",
                    ),
                    ctx,
                )

        assert caught.value.kind is ExceptionKind.CONFLICT
        assert caught.value.details.get("version") == 1
        assert caught.value.details.get("expected_version") == 7


# ....................... #


class TestASupersededVersionIsNotWritable:
    async def test_an_update_to_a_superseded_row_is_refused(self) -> None:
        """A corrected fact is history, and history does not get edited in place.

        The mixin's guard is the last line against a path that reached the row directly — a
        repair script, a bulk update, a hand-written handler. Without it the chain says one
        thing and the row says another, with nothing to show which is right.
        """

        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            await _correct(reg, ctx, first, kwh=120)

            with pytest.raises(CoreException) as caught:
                await ctx.doc.command(READINGS).update(
                    pk=first.id, rev=first.rev + 1, dto=ReadingUpdate(kwh=999)
                )

        assert caught.value.kind is ExceptionKind.DOMAIN

    async def test_the_current_version_is_still_writable(self) -> None:
        # The contrast: the guard is about superseded rows, not about updates.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            updated = await ctx.doc.command(READINGS).update(
                pk=first.id, rev=first.rev, dto=ReadingUpdate(kwh=111)
            )

        assert updated.kwh == 111


# ....................... #


class TestTheFacadeAndTheInertCases:
    async def test_the_facade_reaches_the_lineage_ops(self) -> None:
        # The facade is the surface §5.3 promises; a registry that carries the ops while the
        # facade cannot reach them delivers half of it.
        runtime = build_runtime(MockDepsModule())
        kit = _kit()
        facade = versioned_facade(runtime, kit.registry(tx_route=_TX), READINGS)

        async with runtime.scope():
            row = await facade().create(ReadingCreate(meter="m-1", kwh=100))
            corrected = await facade().correct(
                CorrectDocumentDTO(
                    id=row.id,
                    expected_version=row.version,
                    dto=ReadingUpdate(kwh=120),
                    reason="meter misread",
                )
            )
            chain = await facade().history(FactIdDTO(root_id=row.root_id))

        assert corrected.version == 2
        assert [r.version for r in chain.hits] == [1, 2]

    def test_a_spec_without_update_support_gets_no_lineage_ops(self) -> None:
        # Correcting a fact means writing one, so an aggregate that cannot be updated has
        # nothing to correct — the ops are absent rather than present and broken.
        read_only = DocumentSpec(
            name="readings",
            read=ReadingRead,
            write=DocumentWriteTypes(domain=Reading, create_cmd=ReadingCreate),
            guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
        )

        reg = build_versioned_registry(read_only, POLICY)

        assert reg.operation_keys() == frozenset()


# ....................... #


class TestOrdinaryWritesCannotForgeLineage:
    """The generated UPDATE accepts the kit's own update command, lineage fields included.

    That command carries `is_current` because the correction's retire write needs it — and the
    same command is what the boundary accepts, so a caller could retire the only version of a
    fact through an ordinary update: no successor, no correction record, and a fact left with no
    current version at all. Which is the thing the aggregate exists to prevent.
    """

    async def test_an_update_cannot_retire_a_version(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "m-1", 100)

            await run_operation(
                reg,
                _key(DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(
                    id=row.id, rev=row.rev, dto=ReadingUpdate(is_current=False)
                ),
                ctx,
            )

            page = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        # The fact still has its current version: the lineage field was dropped, not honoured.
        assert [r.version for r in page.hits] == [1]

    async def test_the_rest_of_the_patch_still_applies(self) -> None:
        # The contrast: dropping the lineage fields must not drop the update.
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "m-1", 100)

            await run_operation(
                reg,
                _key(DocumentKernelOp.UPDATE),
                DocumentUpdateDTO(
                    id=row.id, rev=row.rev, dto=ReadingUpdate(kwh=7, is_current=False)
                ),
                ctx,
            )

            page = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)

        assert [(r.kwh, r.is_current) for r in page.hits] == [(7, True)]


# ....................... #


class TestTheReadModelMustExposeWhatTheKitReads:
    @staticmethod
    def _spec(read: type, create: type = ReadingCreate) -> DocumentSpec:
        return DocumentSpec(
            name="readings",
            read=read,
            write=DocumentWriteTypes(
                domain=Reading, create_cmd=create, update_cmd=ReadingUpdate
            ),
            guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
        )

    def test_a_read_model_without_the_lineage_fields_is_refused(self) -> None:
        """The three lineage fields no guarantee names, so nothing else would catch them.

        Three of the five are already covered — `root_id`, `supersedes_id` and `is_current`
        appear in the guarantees, and `DocumentSpec` refuses a guarantee naming a field the
        aggregate does not store. `version` and `superseded_at` appear in neither, so this is
        the only check that sees them: without `version` the expected-version comparison has
        nothing to compare, and a stale correction reads as fresh.
        """

        class Partial(ReadDocument):
            meter: str
            kwh: int = 0
            root_id: UUID
            supersedes_id: UUID | None = None
            is_current: bool = True

        with pytest.raises(CoreException, match="does not expose"):
            AggregateKit(spec=self._spec(Partial), versioned=POLICY).registry(tx_route=_TX)

    def test_a_write_omitted_field_cannot_be_counted_lost(self) -> None:
        """A field omitted from writes is on the read model by construction.

        So the carry check needs no special case for it: `write_omit_fields` may only name a
        field the read model declares, which is exactly the set the check compares against. A
        spec naming one it does not declare is refused before the kit ever sees it.
        """

        class OmittedCreate(CreateCmdWithVersioningFields):
            meter: str
            kwh: int = 0
            scratch: str = ""

        with pytest.raises(CoreException, match="not non-computed fields on the read model"):
            DocumentSpec(
                name="readings",
                read=ReadingRead,
                write=DocumentWriteTypes(
                    domain=Reading, create_cmd=OmittedCreate, update_cmd=ReadingUpdate
                ),
                write_omit_fields={"scratch"},
                guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
            )

    def test_a_persisted_field_missing_from_the_read_model_is_refused(self) -> None:
        # A correction builds the successor from the predecessor's *read model*, so a field
        # persisted through the create command but not exposed there would silently fall back to
        # the command's default — the fact would quietly lose what it asserted.
        class HiddenCreate(CreateCmdWithVersioningFields):
            meter: str
            kwh: int = 0
            secret: str = ""

        with pytest.raises(CoreException, match="without exposing"):
            AggregateKit(
                spec=self._spec(ReadingRead, HiddenCreate), versioned=POLICY
            ).registry(tx_route=_TX)

    def test_the_declared_shape_builds(self) -> None:
        assert _kit().registry(tx_route=_TX) is not None

    def test_a_read_only_spec_has_no_create_command_to_check(self) -> None:
        # Nothing is persisted through a create command there, so there is nothing that could be
        # lost when a correction rebuilds the fact — the check has to pass it by rather than
        # read a command that is not declared.
        read_only = DocumentSpec(
            name="readings",
            read=ReadingRead,
            write=None,
            guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
        )

        assert AggregateKit(spec=read_only, versioned=POLICY).registry(tx_route=_TX)


# ....................... #


class TestAsOfSpansTheHandover:
    async def test_no_instant_between_two_versions_is_unaccounted(self) -> None:
        """A correction writes the retirement before the successor exists.

        `superseded_at` is when the retirement was recorded; the successor's `created_at` is a
        moment later. Reading the window from the first leaves the interval between the two
        writes matching no version, so a report asks what a fact said and is told it did not
        exist. The window comes from the chain instead.
        """

        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            # The instant the predecessor was retired — before the successor was written.
            chain = await run_operation(
                reg, _key(VersionedKernelOp.HISTORY), FactIdDTO(root_id=first.root_id), ctx
            )
            retired_at = chain.hits[0].superseded_at

            assert retired_at is not None
            assert retired_at <= second.created_at

            answered = await run_operation(
                reg,
                _key(VersionedKernelOp.AS_OF),
                FactAsOfDTO(root_id=first.root_id, at=retired_at),
                ctx,
            )

        # Whichever side of the handover it falls on, some version answers for it.
        assert answered.version in (1, 2)


# ....................... #


SEARCH = SearchSpec(
    name="readings_index",
    model_type=ReadingRead,
    fields=["meter"],
    facetable_fields={"is_current"},
)


class TestComposedWithSearch:
    """An index carries every version, so both halves of search have to know about lineage.

    The sync binds CREATE, UPDATE and KILL — and a correction is none of them, so without an
    explicit binding the index keeps the superseded version and never learns about its
    replacement. And a query that did not restrict to current versions would answer with facts
    that have since been corrected, because the superseded rows are in the index precisely
    because the sync put them there.
    """

    def test_an_index_that_cannot_filter_current_is_refused(self) -> None:
        blind = SearchSpec(name="blind", model_type=ReadingRead, fields=["meter"])

        with pytest.raises(CoreException, match="must be able to filter"):
            AggregateKit(spec=READINGS, versioned=POLICY, search=blind)

    async def test_search_reads_stack_both_restrictions(self) -> None:
        """With soft deletion composed too, both restrictions have to reach the query.

        The search mappers already carry an exclusion, so the current-version restriction has to
        run after it rather than replace it — the same shared-slot problem the document list
        family has, and invisible unless a search actually runs.
        """

        from forze_kits.aggregates.search import SearchKernelOp, SearchRequestDTO

        both_index = SearchSpec(
            name="both_index",
            model_type=_BothRead,
            fields=["meter"],
            facetable_fields={"is_current", "is_deleted"},
        )
        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(
            spec=BOTH, soft_delete=True, versioned=POLICY, search=both_index
        ).registry(tx_route=_TX)
        key = BOTH.default_namespace.key

        async with runtime.scope():
            ctx = runtime.get_context()
            live = await run_operation(
                reg, key(DocumentKernelOp.CREATE), _BothCreate(meter="live"), ctx
            )
            ghost = await run_operation(
                reg, key(DocumentKernelOp.CREATE), _BothCreate(meter="ghost"), ctx
            )
            await run_operation(
                reg,
                key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=ghost.id, rev=ghost.rev),
                ctx,
            )

            index = ctx.deps.provide(MockStateDepKey).documents.get("both_index", {})
            index[ghost.id] = {**index.get(ghost.id, {}), "is_deleted": True}

            page = await run_operation(
                reg,
                both_index.default_namespace.key(SearchKernelOp.TYPED),
                SearchRequestDTO(),
                ctx,
            )

        assert [hit.id for hit in page.hits] == [live.id]

    async def test_the_superseded_version_leaves_the_index(self) -> None:
        """Indexing only the successor leaves the old entry claiming to be current.

        The predecessor was indexed while it *was* current, so its entry still says so — and the
        read-side restriction cannot filter it out, because it reads that same stale value. A
        search would answer with a fact that has been corrected.
        """

        runtime = build_runtime(MockDepsModule())
        reg = AggregateKit(spec=READINGS, versioned=POLICY, search=SEARCH).registry(
            tx_route=_TX
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            index = ctx.deps.provide(MockStateDepKey).documents.get("readings_index", {})

        assert second.id in index
        assert first.id not in index

    async def test_a_correction_reaches_the_index(self) -> None:
        runtime = build_runtime(MockDepsModule())
        kit = AggregateKit(spec=READINGS, versioned=POLICY, search=SEARCH)
        reg = kit.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            first = await _create(reg, ctx, "m-1", 100)
            second = await _correct(reg, ctx, first, kwh=120)

            index = ctx.deps.provide(MockStateDepKey).documents.get("readings_index", {})

        # The successor is indexed. Without the binding the index holds only the superseded
        # version, and a search answers with a fact that has been corrected.
        assert second.id in index


# ....................... #


class TestALongChainIsStillAnswerable:
    """A chain is bounded only by the number of corrections, and the store caps an open read.

    `find_many`-style reads carry an implicit cap that truncates with a warning the caller never
    sees — so a read that walked the chain would answer from a partial history without knowing
    it had. `as_of` asks for one row and `history` paginates.
    """

    async def test_as_of_answers_from_one_row_not_the_chain(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "m-1", 0)
            first_at = row.created_at

            for step in range(1, 12):
                row = await _correct(reg, ctx, row, kwh=step)

            earliest = await run_operation(
                reg,
                _key(VersionedKernelOp.AS_OF),
                FactAsOfDTO(root_id=row.root_id, at=first_at),
                ctx,
            )
            latest = await run_operation(
                reg,
                _key(VersionedKernelOp.AS_OF),
                FactAsOfDTO(root_id=row.root_id, at=row.created_at),
                ctx,
            )

        # The oldest version is still reachable at the far end of a long chain, and the newest
        # answers for the present.
        assert earliest.version == 1
        assert latest.version == 12

    async def test_history_pages_through_the_chain(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _kit().registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            row = await _create(reg, ctx, "m-1", 0)

            for step in range(1, 6):
                row = await _correct(reg, ctx, row, kwh=step)

            first = await run_operation(
                reg,
                _key(VersionedKernelOp.HISTORY),
                FactIdDTO(root_id=row.root_id, page=1, size=2),
                ctx,
            )
            second = await run_operation(
                reg,
                _key(VersionedKernelOp.HISTORY),
                FactIdDTO(root_id=row.root_id, page=2, size=2),
                ctx,
            )

        assert [r.version for r in first.hits] == [1, 2]
        assert [r.version for r in second.hits] == [3, 4]


# ....................... #


class TestTheSecondAggregateIsAdvertised:
    """A correction writes two aggregates, and only one of them is in the author's own code.

    An application provisions storage from the advertised inventory, so a route the kit wired
    behind the author's back and did not declare is a route that simply does not exist in the
    deployment — discovered the first time somebody corrects a fact, which is exactly the
    failure `spec_contributions` and `backend_requirements` exist to prevent.
    """

    def test_the_corrections_spec_is_contributed(self) -> None:
        names = {str(entry.spec.name) for entry in _kit().spec_contributions().freeze().entries}

        assert str(READINGS.name) in names
        assert str(CORRECTIONS.name) in names

    def test_the_corrections_route_is_reported(self) -> None:
        assert _kit().backend_requirements().corrections_route == CORRECTIONS.name

    def test_it_contributes_alongside_the_other_arms(self) -> None:
        # The corrections spec joins the inventory rather than displacing what else the kit
        # wired — a versioned aggregate with a blob bucket advertises both.
        kit = AggregateKit(
            spec=READINGS,
            versioned=POLICY,
            storage=StorageSpec(name="readings_blobs"),
        )
        names = {str(entry.spec.name) for entry in kit.spec_contributions().freeze().entries}

        assert {str(READINGS.name), str(CORRECTIONS.name), "readings_blobs"} <= names

    def test_an_unversioned_kit_reports_neither(self) -> None:
        # The contrast: the second route rides with versioning, not with every kit.
        plain = AggregateKit(spec=READINGS)

        assert plain.backend_requirements().corrections_route is None
        assert {
            str(entry.spec.name) for entry in plain.spec_contributions().freeze().entries
        } == {str(READINGS.name)}


# ....................... #


class TestTheFacadeReachesWhatTheKitComposed:
    async def test_the_kits_own_facade_carries_the_lineage_operations(self) -> None:
        """`kit.facade()` is the surface most callers use.

        Handing back a plain document facade there leaves `correct`, `history` and `as_of` in
        the registry and unreachable from the kit's own API — the feature configured and then
        hidden behind the accessor that is supposed to expose it.
        """

        runtime = build_runtime(MockDepsModule())
        facade = _kit().facade(runtime, tx_route=_TX)

        async with runtime.scope():
            row = await facade().create(ReadingCreate(meter="m-1", kwh=100))
            corrected = await facade().correct(
                CorrectDocumentDTO(
                    id=row.id,
                    expected_version=row.version,
                    dto=ReadingUpdate(kwh=120),
                    reason="meter misread",
                )
            )

        assert corrected.version == 2

    async def test_the_precisely_typed_accessor_returns_the_same_surface(self) -> None:
        runtime = build_runtime(MockDepsModule())
        facade = _kit().lineage_facade(runtime, tx_route=_TX)

        async with runtime.scope():
            row = await facade().create(ReadingCreate(meter="m-1", kwh=100))
            chain = await facade().history(FactIdDTO(root_id=row.root_id))

        assert [r.version for r in chain.hits] == [1]

    def test_it_refuses_without_a_versioning_policy(self) -> None:
        runtime = build_runtime(MockDepsModule())

        with pytest.raises(CoreException, match="requires a versioning policy"):
            AggregateKit(spec=READINGS).lineage_facade(runtime, tx_route=_TX)
