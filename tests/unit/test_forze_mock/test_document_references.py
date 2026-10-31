"""The in-memory store applies a declared reference when it hard-deletes the referenced row.

Postgres removes, keeps or clears a child row through its foreign key's ``ON DELETE``; the
in-memory store has no foreign keys, so without the declaration a killed parent leaves its
children listed and colliding with their replacements.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import Reference, ReferenceAction, UniqueTogether
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import IsolationLevel
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockDocumentAdapter
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Stage(Document):
    name: str = ""
    is_deleted: bool = False


class _StageRead(ReadDocument):
    name: str = ""
    is_deleted: bool = False


class _StageCreate(CreateDocumentCmd):
    name: str = ""


class _Line(Document):
    stage_id: UUID | None = None
    position: int = 0


class _LineRead(ReadDocument):
    stage_id: UUID | None = None
    position: int = 0


class _LineCreate(CreateDocumentCmd):
    stage_id: UUID | None = None
    position: int = 0


class _Note(Document):
    line_id: UUID


class _NoteRead(ReadDocument):
    line_id: UUID


class _NoteCreate(CreateDocumentCmd):
    line_id: UUID


STAGES = DocumentSpec(
    name="stages",
    read=_StageRead,
    write=DocumentWriteTypes(domain=_Stage, create_cmd=_StageCreate),
)


def _lines(on_delete: ReferenceAction) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec(
        name="lines",
        read=_LineRead,
        write=DocumentWriteTypes(domain=_Line, create_cmd=_LineCreate),
        guarantees=(
            Reference(field="stage_id", spec="stages", on_delete=on_delete),
            UniqueTogether(fields=("stage_id", "position"), skip_null=True),
        ),
    )


def _notes(on_delete: ReferenceAction) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        guarantees=(Reference(field="line_id", spec="lines", on_delete=on_delete),),
    )


async def _ids(ctx: Any, spec: DocumentSpec[Any, Any, Any, Any]) -> set[UUID]:
    return {row.id for row in (await ctx.doc.query(spec).find_many()).hits}


def _is_conflict(error: CoreException) -> bool:
    return error.kind is ExceptionKind.CONFLICT


# ....................... #


class TestCascade:
    async def test_a_killed_parent_takes_its_children_and_only_its_own(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("cascade"))
        doomed, kept = await stages.create(_StageCreate()), await stages.create(_StageCreate())
        await lines.create(_LineCreate(stage_id=doomed.id, position=0))
        survivor = await lines.create(_LineCreate(stage_id=kept.id, position=0))

        await stages.kill(doomed.id)

        assert await _ids(ctx, _lines("cascade")) == {survivor.id}
        # The stale child is gone, so its replacement does not collide with it.
        await stages.ensure(doomed.id, _StageCreate())
        await lines.create(_LineCreate(stage_id=doomed.id, position=0))

    async def test_grandchildren_go_too(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("cascade"))
        notes = ctx.doc.command(_notes("cascade"))
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))
        await notes.create(_NoteCreate(line_id=line.id))

        await stages.kill(stage.id)

        assert await _ids(ctx, _notes("cascade")) == set()

    async def test_a_cycle_of_references_ends(self) -> None:
        class _Node(Document):
            parent_id: UUID | None = None

        class _NodeRead(ReadDocument):
            parent_id: UUID | None = None

        nodes = DocumentSpec(
            name="nodes",
            read=_NodeRead,
            write=DocumentWriteTypes(domain=_Node, create_cmd=_Node),
            guarantees=(Reference(field="parent_id", spec="nodes", on_delete="cascade"),),
        )
        ctx = context_from_modules(MockDepsModule())
        command = ctx.doc.command(nodes)
        a, b = uuid4(), uuid4()
        await command.ensure(a, _Node(id=a, parent_id=b))
        await command.ensure(b, _Node(id=b, parent_id=a))
        other = await command.create(_Node())

        await command.kill(a)

        assert await _ids(ctx, nodes) == {other.id}

    @pytest.mark.parametrize(
        ("transactions", "isolation"),
        [
            ("journal", None),
            ("journal", IsolationLevel.SNAPSHOT),
            ("strict", None),
        ],
    )
    async def test_a_rollback_restores_the_parent_and_its_children(
        self, transactions: Literal["journal", "strict"], isolation: IsolationLevel | None
    ) -> None:
        ctx = context_from_modules(MockDepsModule(transactions=transactions))
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("cascade"))
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))

        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("mock", isolation=isolation):
                await stages.kill(stage.id)
                assert await _ids(ctx, _lines("cascade")) == set()
                raise RuntimeError("abort")

        assert await _ids(ctx, STAGES) == {stage.id}
        assert await _ids(ctx, _lines("cascade")) == {line.id}

    async def test_a_soft_delete_leaves_the_children(self) -> None:
        # ON DELETE fires on a hard delete only; a soft delete is an update.
        state = MockState()
        stages = MockDocumentAdapter(
            spec=STAGES, state=state, namespace="stages", read_model=_StageRead, domain_model=_Stage
        )
        lines = MockDocumentAdapter(
            spec=_lines("cascade"),
            state=state,
            namespace="lines",
            read_model=_LineRead,
            domain_model=_Line,
        )
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))

        await stages.delete(stage.id, stage.rev)

        assert (await lines.get(line.id)).stage_id == stage.id


# ....................... #


class TestRestrict:
    async def test_the_kill_is_refused_while_a_child_remains(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("restrict"))
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))

        with pytest.raises(CoreException) as caught:
            await stages.kill(stage.id)

        # The kind Postgres's ON DELETE RESTRICT violation maps to, so the two stores agree.
        assert _is_conflict(caught.value)
        # Kept for the logs, never shown: the child may belong to another tenant.
        assert caught.value.expose_details is False
        assert caught.value.details == {
            "spec": "lines",
            "fields": ["stage_id"],
            "references": "stages",
        }
        assert await _ids(ctx, STAGES) == {stage.id}

        await lines.kill(line.id)
        await stages.kill(stage.id)

        assert await _ids(ctx, STAGES) == set()

    @pytest.mark.parametrize(
        ("cascading", "restricting"), [("stage_id", "origin_id"), ("origin_id", "stage_id")]
    )
    async def test_a_row_the_cascade_deletes_still_restricts(
        self, cascading: str, restricting: str
    ) -> None:
        # Postgres's answer depends on which key was created first; this one does not depend on
        # the order the references are visited in.
        class _Pair(Document):
            stage_id: UUID | None = None
            origin_id: UUID | None = None

        class _PairRead(ReadDocument):
            stage_id: UUID | None = None
            origin_id: UUID | None = None

        pairs = DocumentSpec(
            name="pairs",
            read=_PairRead,
            write=DocumentWriteTypes(domain=_Pair, create_cmd=_Pair),
            guarantees=(
                Reference(field=cascading, spec="stages", on_delete="cascade"),
                Reference(field=restricting, spec="stages", on_delete="restrict"),
            ),
        )
        ctx = context_from_modules(MockDepsModule())
        stage = await ctx.doc.command(STAGES).create(_StageCreate())
        await ctx.doc.command(pairs).create(_Pair(stage_id=stage.id, origin_id=stage.id))

        with pytest.raises(CoreException) as caught:
            await ctx.doc.command(STAGES).kill(stage.id)

        assert _is_conflict(caught.value)

    async def test_a_restrict_deep_in_a_cascade_refuses_all_of_it(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("cascade"))
        notes = ctx.doc.command(_notes("restrict"))
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))
        await notes.create(_NoteCreate(line_id=line.id))

        with pytest.raises(CoreException) as caught:
            await stages.kill(stage.id)

        assert _is_conflict(caught.value)
        assert await _ids(ctx, _lines("cascade")) == {line.id}

    async def test_kill_many_is_refused_whole(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("restrict"))
        free, held = await stages.create(_StageCreate()), await stages.create(_StageCreate())
        await lines.create(_LineCreate(stage_id=held.id))

        with pytest.raises(CoreException):
            await stages.kill_many([free.id, held.id])

        assert await _ids(ctx, STAGES) == {free.id, held.id}


# ....................... #


class TestSetNull:
    async def test_the_child_stays_with_its_reference_cleared(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(_lines("set_null"))
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))

        await stages.kill(stage.id)

        assert (await ctx.doc.query(_lines("set_null")).get(line.id)).stage_id is None

    async def test_a_snapshot_transaction_clears_it_at_commit(self) -> None:
        # Inside the transaction the cleared row is rechecked when it commits, as a write is.
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(self._strict_lines())
        stage = await stages.create(_StageCreate())
        line = await lines.create(_LineCreate(stage_id=stage.id))

        async with ctx.tx_ctx.scope("mock", isolation=IsolationLevel.SNAPSHOT):
            await stages.kill(stage.id)

        assert (await ctx.doc.query(self._strict_lines()).get(line.id)).stage_id is None

    @staticmethod
    def _strict_lines() -> DocumentSpec[Any, Any, Any, Any]:
        # Nulls compare equal here (NULLS NOT DISTINCT), so two cleared rows collide.
        return DocumentSpec(
            name="lines",
            read=_LineRead,
            write=DocumentWriteTypes(domain=_Line, create_cmd=_LineCreate),
            guarantees=(
                Reference(field="stage_id", spec="stages", on_delete="set_null"),
                UniqueTogether(fields=("stage_id", "position")),
            ),
        )

    async def test_cleared_rows_that_collide_refuse_the_whole_delete(self) -> None:
        # Each row is checked against what the whole cascade leaves, not against the store
        # before it: alone, either cleared row would pass.
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(self._strict_lines())
        first, second = await stages.create(_StageCreate()), await stages.create(_StageCreate())
        await lines.create(_LineCreate(stage_id=first.id, position=1))
        await lines.create(_LineCreate(stage_id=second.id, position=1))

        with pytest.raises(CoreException) as caught:
            await stages.kill_many([first.id, second.id])

        assert _is_conflict(caught.value)
        assert await _ids(ctx, STAGES) == {first.id, second.id}
        rows = (await ctx.doc.query(self._strict_lines()).find_many()).hits
        assert {row.stage_id for row in rows} == {first.id, second.id}

    async def test_a_cleared_row_meeting_a_stored_one_refuses_too(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(self._strict_lines())
        stage = await stages.create(_StageCreate())
        await lines.create(_LineCreate(stage_id=None, position=1))
        await lines.create(_LineCreate(stage_id=stage.id, position=1))

        with pytest.raises(CoreException) as caught:
            await stages.kill(stage.id)

        assert _is_conflict(caught.value)
        assert await _ids(ctx, STAGES) == {stage.id}

    async def test_a_spec_with_no_adapter_in_this_process_is_refused(self) -> None:
        # Restored from a snapshot before its port was resolved: its guarantees are unknown.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        stages, lines = ctx.doc.command(STAGES), ctx.doc.command(self._strict_lines())
        stage = await stages.create(_StageCreate())
        await lines.create(_LineCreate(stage_id=stage.id))
        state.reference_checks.clear()

        with pytest.raises(CoreException, match="cannot be checked"):
            await stages.kill(stage.id)

        assert await _ids(ctx, STAGES) == {stage.id}


# ....................... #


class TestTenancy:
    def _pair(
        self,
        state: MockState,
        bound: dict[str, TenantIdentity | None],
        *,
        stages_scoped: bool = True,
    ) -> tuple[MockDocumentAdapter[Any, Any, Any, Any], MockDocumentAdapter[Any, Any, Any, Any]]:
        stages = MockDocumentAdapter(
            spec=STAGES,
            state=state,
            namespace="stages",
            read_model=_StageRead,
            domain_model=_Stage,
            tenant_aware=stages_scoped,
            tenant_provider=lambda: bound["id"],
        )
        lines = MockDocumentAdapter(
            spec=_lines("cascade"),
            state=state,
            namespace="lines",
            read_model=_LineRead,
            domain_model=_Line,
            tenant_aware=True,
            tenant_provider=lambda: bound["id"],
        )

        return stages, lines

    async def test_a_kill_reaches_only_its_own_tenant_s_rows(self) -> None:
        # The same stage id in two tenants: the delete in one leaves the other's children.
        state = MockState()
        bound: dict[str, TenantIdentity | None] = {"id": TenantIdentity(tenant_id=uuid4())}
        stages, lines = self._pair(state, bound)
        shared = uuid4()
        t1, t2 = bound["id"], TenantIdentity(tenant_id=uuid4())

        await stages.ensure(shared, _StageCreate())
        await lines.create(_LineCreate(stage_id=shared))

        bound["id"] = t2
        await stages.ensure(shared, _StageCreate())
        theirs = await lines.create(_LineCreate(stage_id=shared))

        bound["id"] = t1
        await stages.kill(shared)

        assert (await lines.find_many()).hits == []

        bound["id"] = t2
        assert [row.id for row in (await lines.find_many()).hits] == [theirs.id]

    async def test_an_unscoped_parent_reaches_every_tenant_s_children(self) -> None:
        # A foreign key reaches every tenant's rows, and so do their own children's in turn.
        state = MockState()
        bound: dict[str, TenantIdentity | None] = {"id": None}
        stages, lines = self._pair(state, bound, stages_scoped=False)
        notes = MockDocumentAdapter(
            spec=_notes("cascade"),
            state=state,
            namespace="notes",
            read_model=_NoteRead,
            domain_model=_Note,
            tenant_aware=True,
            tenant_provider=lambda: bound["id"],
        )
        stage, other = await stages.create(_StageCreate()), await stages.create(_StageCreate())
        tenants = [TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())]
        kept: dict[UUID, UUID] = {}

        for tenant in tenants:
            bound["id"] = tenant
            line = await lines.create(_LineCreate(stage_id=stage.id))
            await notes.create(_NoteCreate(line_id=line.id))
            kept[tenant.tenant_id] = (await lines.create(_LineCreate(stage_id=other.id))).id

        bound["id"] = None
        await stages.kill(stage.id)

        for tenant in tenants:
            bound["id"] = tenant
            assert [row.id for row in (await lines.find_many()).hits] == [kept[tenant.tenant_id]]
            assert (await notes.find_many()).hits == []


# ....................... #


class _Revision(Document):
    source_id: int | None = None


class _RevisionRead(ReadDocument):
    source_id: int | None = None


class _Bom(Document):
    stage_id: UUID | None = None
    source_id: int | None = None


class _BomRead(ReadDocument):
    stage_id: UUID | None = None
    source_id: int | None = None


REVISIONS = DocumentSpec(
    name="revisions",
    read=_RevisionRead,
    write=DocumentWriteTypes(domain=_Revision, create_cmd=_Revision),
)


def _boms(
    on_delete: ReferenceAction, set_null_fields: tuple[str, ...] | None = None
) -> DocumentSpec[Any, Any, Any, Any]:
    # FOREIGN KEY (stage_id, source_id) REFERENCES revisions (id, source_id)
    return DocumentSpec(
        name="boms",
        read=_BomRead,
        write=DocumentWriteTypes(domain=_Bom, create_cmd=_Bom),
        guarantees=(
            Reference(
                fields=("stage_id", "source_id"),
                spec="revisions",
                references=("id", "source_id"),
                on_delete=on_delete,
                set_null_fields=set_null_fields,
            ),
        ),
    )


class TestCompositeReference:
    async def _rows(self, ctx: Any, on_delete: ReferenceAction) -> tuple[UUID, UUID, set[UUID]]:
        """A revision, a row naming it by both fields, and rows naming it by one or none."""

        revision = await ctx.doc.command(REVISIONS).create(_Revision(source_id=7))
        boms = ctx.doc.command(_boms(on_delete))
        named = await boms.create(_Bom(stage_id=revision.id, source_id=7))
        others = {
            (await boms.create(row)).id
            for row in (
                _Bom(stage_id=revision.id, source_id=8),  # same stage, another source
                _Bom(stage_id=revision.id, source_id=None),  # a null names nothing
                _Bom(stage_id=None, source_id=7),
                _Bom(stage_id=uuid4(), source_id=7),
            )
        }

        return revision.id, named.id, others

    async def test_a_cascade_takes_the_rows_matching_every_field(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        revision, _, others = await self._rows(ctx, "cascade")

        await ctx.doc.command(REVISIONS).kill(revision)

        assert await _ids(ctx, _boms("cascade")) == others

    async def test_a_restrict_is_refused_by_the_matching_row_alone(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        revision, named, _ = await self._rows(ctx, "restrict")

        with pytest.raises(CoreException) as caught:
            await ctx.doc.command(REVISIONS).kill(revision)

        assert _is_conflict(caught.value)
        assert caught.value.details == {
            "spec": "boms",
            "fields": ["stage_id", "source_id"],
            "references": "revisions",
        }
        assert "through ('stage_id', 'source_id')" in caught.value.summary

        # The rows naming it by one field do not hold it.
        await ctx.doc.command(_boms("restrict")).kill(named)
        await ctx.doc.command(REVISIONS).kill(revision)

    async def test_set_null_clears_every_field_of_the_matching_row(self) -> None:
        # As ON DELETE SET NULL without a column list clears every column of the key.
        ctx = context_from_modules(MockDepsModule())
        revision, named, others = await self._rows(ctx, "set_null")
        before = {row.id: row for row in (await ctx.doc.query(_boms("set_null")).find_many()).hits}

        await ctx.doc.command(REVISIONS).kill(revision)

        after = {row.id: row for row in (await ctx.doc.query(_boms("set_null")).find_many()).hits}
        assert (after[named].stage_id, after[named].source_id) == (None, None)
        assert {pk: after[pk] for pk in others} == {pk: before[pk] for pk in others}

    async def test_set_null_fields_clears_only_those(self) -> None:
        # As ON DELETE SET NULL (stage_id): the source the row belongs to stays.
        ctx = context_from_modules(MockDepsModule())
        boms = _boms("set_null", set_null_fields=("stage_id",))
        revision = await ctx.doc.command(REVISIONS).create(_Revision(source_id=7))
        row = await ctx.doc.command(boms).create(_Bom(stage_id=revision.id, source_id=7))

        await ctx.doc.command(REVISIONS).kill(revision.id)

        cleared = await ctx.doc.query(boms).get(row.id)
        assert (cleared.stage_id, cleared.source_id) == (None, 7)

    @pytest.mark.parametrize(
        ("set_null_fields", "refused"), [(None, True), (("stage_id",), False)]
    )
    async def test_clearing_a_field_a_grandchild_keys_on_is_refused(
        self, set_null_fields: tuple[str, ...] | None, refused: bool
    ) -> None:
        # The note references (bom id, source_id): clearing the bom's source_id would leave it
        # naming a key no bom holds, and Postgres refuses that update (ON UPDATE NO ACTION).
        class _Note(Document):
            bom_id: UUID | None = None
            source_id: int | None = None

        class _NoteRead(ReadDocument):
            bom_id: UUID | None = None
            source_id: int | None = None

        notes = DocumentSpec(
            name="bom_notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_Note),
            guarantees=(
                Reference(
                    fields=("bom_id", "source_id"),
                    spec="boms",
                    references=("id", "source_id"),
                    on_delete="cascade",
                ),
            ),
        )
        ctx = context_from_modules(MockDepsModule())
        boms = _boms("set_null", set_null_fields=set_null_fields)
        revision = await ctx.doc.command(REVISIONS).create(_Revision(source_id=7))
        bom = await ctx.doc.command(boms).create(_Bom(stage_id=revision.id, source_id=7))
        note = await ctx.doc.command(notes).create(_Note(bom_id=bom.id, source_id=7))

        if not refused:
            await ctx.doc.command(REVISIONS).kill(revision.id)

            assert await _ids(ctx, notes) == {note.id}
            return

        with pytest.raises(CoreException) as caught:
            await ctx.doc.command(REVISIONS).kill(revision.id)

        assert _is_conflict(caught.value)
        assert caught.value.expose_details is False
        assert caught.value.details == {
            "spec": "bom_notes",
            "fields": ["bom_id", "source_id"],
            "references": "boms",
        }
        # Refused whole: the revision and the bom are as they were.
        assert await _ids(ctx, REVISIONS) == {revision.id}
        assert (await ctx.doc.query(boms).get(bom.id)).source_id == 7

    async def test_a_parent_holding_a_null_is_named_by_nothing(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        revision = await ctx.doc.command(REVISIONS).create(_Revision(source_id=None))
        row = await ctx.doc.command(_boms("restrict")).create(
            _Bom(stage_id=revision.id, source_id=None)
        )

        await ctx.doc.command(REVISIONS).kill(revision.id)

        assert await _ids(ctx, _boms("restrict")) == {row.id}

    async def test_a_kill_reaches_only_its_own_tenant_s_rows(self) -> None:
        state = MockState()
        bound: dict[str, TenantIdentity | None] = {"id": TenantIdentity(tenant_id=uuid4())}
        revisions, boms = (
            MockDocumentAdapter(
                spec=spec,
                state=state,
                namespace=str(spec.name),
                read_model=read,
                domain_model=domain,
                tenant_aware=True,
                tenant_provider=lambda: bound["id"],
            )
            for spec, read, domain in (
                (REVISIONS, _RevisionRead, _Revision),
                (_boms("cascade"), _BomRead, _Bom),
            )
        )
        shared, t1, t2 = uuid4(), bound["id"], TenantIdentity(tenant_id=uuid4())

        await revisions.ensure(shared, _Revision(source_id=7))
        await boms.create(_Bom(stage_id=shared, source_id=7))

        bound["id"] = t2
        await revisions.ensure(shared, _Revision(source_id=7))
        theirs = await boms.create(_Bom(stage_id=shared, source_id=7))

        bound["id"] = t1
        await revisions.kill(shared)

        assert (await boms.find_many()).hits == []

        bound["id"] = t2
        assert [row.id for row in (await boms.find_many()).hits] == [theirs.id]


# ....................... #


class TestRecord:
    async def test_rows_the_child_adapter_never_wrote_are_reached(self) -> None:
        # Restored from a snapshot, or written by another spec on the namespace: the record is
        # made when the child's adapter is built, before it writes anything.
        state = MockState()
        stages = MockDocumentAdapter(
            spec=STAGES, state=state, namespace="stages", read_model=_StageRead, domain_model=_Stage
        )
        stage = await stages.create(_StageCreate())
        line = _Line(stage_id=stage.id)
        state.documents["lines"] = {line.id: line.model_dump(mode="json")}
        MockDocumentAdapter(
            spec=_lines("cascade"),
            state=state,
            namespace="lines",
            read_model=_LineRead,
            domain_model=_Line,
        )

        await stages.kill(stage.id)

        assert dict(state.documents["lines"]) == {}

    async def test_seeded_children_go_with_their_parent(self) -> None:
        seeding = pytest.importorskip("forze_mock.seeding")
        ctx = context_from_modules(MockDepsModule())
        lines = _lines("cascade")
        plan = seeding.SeedPlan(
            specs=(
                seeding.spec_seed(STAGES, count=2),
                seeding.spec_seed(lines, fixtures=[{"position": n} for n in range(4)]),
            ),
            links={"lines": {"stage_id": "stages"}},
            rng_seed=7,
        )
        created = await seeding.apply_seed(ctx, plan)
        doomed, kept = created["stages"]
        seeded = (await ctx.doc.query(lines).find_many()).hits
        assert doomed in {row.stage_id for row in seeded}  # the seed linked some to it

        await ctx.doc.command(STAGES).kill(doomed)

        rows = (await ctx.doc.query(lines).find_many()).hits
        assert {row.stage_id for row in rows} <= {kept}
