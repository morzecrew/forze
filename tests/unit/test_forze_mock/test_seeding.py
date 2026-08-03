"""Spec-driven seeding for the in-memory mock.

The two claims worth testing are the ones a code read clears wrongly: seeded rows really go
through the write path (so encryption, soft deletion and bookkeeping apply to them), and the
same plan really reproduces byte-for-byte — including in another process, where the hash
seed and any live-object address would otherwise leak in.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.queue import QueueQueryDepKey, QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.seeding import (
    QueueSeed,
    SearchSeed,
    SeedPlan,
    StorageSeed,
    apply_seed,
    infer_links,
    load_fixtures,
    seed_order,
    singularize,
    spec_seed,
)

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[3]

# ....................... #


class _Project(Document):
    name: str = ""


class _ProjectCreate(CreateDocumentCmd):
    name: str = ""


class _ProjectUpdate(BaseDTO):
    name: str | None = None


class _ProjectRead(ReadDocument):
    name: str = ""


class _Task(Document):
    title: str = ""
    project_id: UUID | None = None
    parent_id: UUID | None = None


class _TaskCreate(CreateDocumentCmd):
    title: str = ""
    project_id: UUID | None = None
    parent_id: UUID | None = None


class _TaskUpdate(BaseDTO):
    title: str | None = None


class _TaskRead(ReadDocument):
    title: str = ""
    project_id: UUID | None = None
    parent_id: UUID | None = None


def _projects(name: str = "projects") -> DocumentSpec:
    return DocumentSpec(
        name=name,
        read=_ProjectRead,
        write=DocumentWriteTypes(
            domain=_Project, create_cmd=_ProjectCreate, update_cmd=_ProjectUpdate
        ),
    )


def _tasks(name: str = "tasks") -> DocumentSpec:
    return DocumentSpec(
        name=name,
        read=_TaskRead,
        write=DocumentWriteTypes(domain=_Task, create_cmd=_TaskCreate, update_cmd=_TaskUpdate),
    )


async def _hits(ctx, spec: DocumentSpec) -> list[BaseModel]:
    page = await ctx.doc.query(spec).find_many()

    return list(page.hits)


# ....................... #


class TestSeedsGoThroughTheWritePath:
    @pytest.mark.asyncio
    async def test_rows_carry_the_bookkeeping_the_write_path_produces(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        result = await apply_seed(ctx, SeedPlan(specs=(spec_seed(_projects(), count=3),)))

        rows = await _hits(ctx, _projects())

        assert len(rows) == 3 == result.total
        # rev and timestamps exist because the document command produced them — writing
        # dicts into MockState would have left rows the read path cannot faithfully return.
        assert {row.rev for row in rows} == {1}
        assert all(row.created_at is not None for row in rows)
        assert {row.id for row in rows} == set(result["projects"])

    @pytest.mark.asyncio
    async def test_a_sealed_field_is_encrypted_at_rest_like_any_other_write(self) -> None:
        # Acceptance item 1: seeding a spec with field encryption has to seal, or the seed
        # would be the one write in the app that stores plaintext.
        state = MockState()
        spec = DocumentSpec(
            name="vaults",
            read=_ProjectRead,
            write=DocumentWriteTypes(
                domain=_Project, create_cmd=_ProjectCreate, update_cmd=_ProjectUpdate
            ),
            encryption=FieldEncryption(encrypted=frozenset({"name"})),
        )
        ctx = context_from_modules(MockDepsModule(state=state))

        result = await apply_seed(ctx, SeedPlan(specs=(spec_seed(spec, count=2),)))

        at_rest = {row["name"] for row in state.documents["vaults"].values()}
        seeded = {row["name"] for row in result.rows["vaults"]}

        assert len(at_rest) == 2
        assert at_rest.isdisjoint(seeded), "seeded values were stored in the clear"

        # ...and the read path returns exactly what was seeded, decrypted.
        assert {row.name for row in await _hits(ctx, spec)} == seeded

    @pytest.mark.asyncio
    async def test_a_soft_deletable_spec_seeds_live_rows(self) -> None:
        # Acceptance item 1's other half: the write path is what sets the soft-deletion
        # flag, so seeded rows are live and the delete/restore ops have something to act on.
        class _LiveDoc(DocWithSoftDeletion):
            name: str = ""

        class _LiveRead(ReadDocument):
            name: str = ""
            is_deleted: bool = False

        spec = DocumentSpec(
            name="live",
            read=_LiveRead,
            write=DocumentWriteTypes(
                domain=_LiveDoc,
                create_cmd=_ProjectCreate,
                update_cmd=UpdateCmdWithSoftDeletion,
            ),
        )
        ctx = context_from_modules(MockDepsModule())

        await apply_seed(ctx, SeedPlan(specs=(spec_seed(spec, count=3),)))
        rows = await _hits(ctx, spec)

        assert len(rows) == 3
        assert not any(row.is_deleted for row in rows)

    @pytest.mark.asyncio
    async def test_a_read_only_spec_is_refused_with_a_reason(self) -> None:
        read_only = DocumentSpec(name="articles", read=_ProjectRead, write=None)

        with pytest.raises(CoreException, match="Cannot seed read-only spec 'articles'"):
            spec_seed(read_only, count=1)


class TestReferentialIntegrity:
    @pytest.mark.asyncio
    async def test_a_linked_field_names_a_seeded_document(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        # Plan order is deliberately backwards: dependencies decide the real order.
        plan = SeedPlan(specs=(spec_seed(_tasks(), count=6), spec_seed(_projects(), count=2)))
        result = await apply_seed(ctx, plan)

        tasks = await _hits(ctx, _tasks())

        assert len(tasks) == 6
        assert {task.project_id for task in tasks} <= set(result["projects"])

    def test_the_order_follows_the_graph_not_the_plan(self) -> None:
        seeds = (spec_seed(_tasks(), count=1), spec_seed(_projects(), count=1))
        links = infer_links(seeds)

        # `parent_id` is deliberately *not* inferred: "parent" names no seeded spec, and
        # guessing that it means "tasks" would be magic the author cannot see.
        assert links == {"tasks": {"project_id": "projects"}}
        assert [seed.spec.name for seed in seed_order(seeds, links)] == ["projects", "tasks"]

    def test_an_override_corrects_a_name_the_heuristic_cannot_see(self) -> None:
        # `owner_id` looks like nothing; the plan says it is a project.
        class _OwnedCreate(CreateDocumentCmd):
            owner_id: UUID | None = None

        owned = DocumentSpec(
            name="owned",
            read=_ProjectRead,
            write=DocumentWriteTypes(
                domain=_Project, create_cmd=_OwnedCreate, update_cmd=_ProjectUpdate
            ),
        )
        seeds = (spec_seed(owned, count=1), spec_seed(_projects(), count=1))

        assert infer_links(seeds) == {}
        assert infer_links(seeds, overrides={"owned": {"owner_id": "projects"}}) == {
            "owned": {"owner_id": "projects"}
        }

    def test_a_none_override_opts_a_field_out(self) -> None:
        seeds = (spec_seed(_tasks(), count=1), spec_seed(_projects(), count=1))

        assert infer_links(seeds, overrides={"tasks": {"project_id": None}}) == {}

    @pytest.mark.asyncio
    async def test_a_self_reference_points_at_an_earlier_row_not_at_nothing(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        result = await apply_seed(
            ctx,
            SeedPlan(
                specs=(spec_seed(_tasks(), count=5),),
                links={"tasks": {"parent_id": "tasks"}},
            ),
        )

        tasks = await _hits(ctx, _tasks())
        parents = {task.parent_id for task in tasks} - {None}
        seeded = set(result["tasks"])

        # A self-link must not be a cycle *and* must not dangle: every non-null parent is
        # a task this seed created (the first row has nothing to point at).
        assert parents
        assert parents <= seeded

    def test_a_cycle_between_specs_is_refused_with_the_way_out(self) -> None:
        class _ProjectWithLead(CreateDocumentCmd):
            task_id: UUID | None = None

        cyclic = DocumentSpec(
            name="projects",
            read=_ProjectRead,
            write=DocumentWriteTypes(
                domain=_Project, create_cmd=_ProjectWithLead, update_cmd=_ProjectUpdate
            ),
        )
        seeds = (spec_seed(_tasks(), count=1), spec_seed(cyclic, count=1))

        with pytest.raises(CoreException, match="cycle: projects, tasks"):
            seed_order(seeds, infer_links(seeds))

    @pytest.mark.parametrize(
        ("plural", "expected"),
        [("projects", "project"), ("categories", "category"), ("batches", "batch"), ("x", "x")],
    )
    def test_singularize(self, plural: str, expected: str) -> None:
        assert singularize(plural) == expected


class TestPlausibility:
    @pytest.mark.asyncio
    async def test_fixtures_are_applied_verbatim_and_come_first(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(
            specs=(
                spec_seed(
                    _projects(),
                    count=2,
                    fixtures=({"name": "Apollo"}, {"name": "Gemini"}),
                ),
            )
        )
        result = await apply_seed(ctx, plan)

        rows = await _hits(ctx, _projects())

        assert len(rows) == 4, "fixtures and generated rows are both seeded"
        assert [row["name"] for row in result.rows["projects"][:2]] == ["Apollo", "Gemini"]

    @pytest.mark.asyncio
    async def test_overrides_apply_to_every_row(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(
            specs=(
                spec_seed(
                    _projects(),
                    count=3,
                    fixtures=({"name": "ignored"},),
                    overrides={"name": "forced"},
                ),
            )
        )
        await apply_seed(ctx, plan)

        assert {row.name for row in await _hits(ctx, _projects())} == {"forced"}

    @pytest.mark.asyncio
    async def test_an_unknown_fixture_field_is_named_once_not_buried_in_a_row_error(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(specs=(spec_seed(_projects(), fixtures=({"nmae": "typo"},)),))

        with pytest.raises(CoreException, match="does not accept: nmae"):
            await apply_seed(ctx, plan)

    @pytest.mark.parametrize("suffix", [".json", ".yaml"])
    def test_fixtures_load_from_json_and_yaml(self, tmp_path: Path, suffix: str) -> None:
        rows = [{"name": "Apollo"}, {"name": "Gemini"}]
        source = tmp_path / f"projects{suffix}"
        source.write_text(json.dumps(rows) if suffix == ".json" else "- name: Apollo\n- name: Gemini\n")

        assert load_fixtures(source) == tuple(rows)

    def test_a_fixture_file_that_is_not_a_list_of_rows_is_refused(self, tmp_path: Path) -> None:
        source = tmp_path / "bad.json"
        source.write_text(json.dumps({"name": "not a list"}))

        with pytest.raises(CoreException, match="must hold a list of row mappings"):
            load_fixtures(source)


class TestDeterminism:
    @pytest.mark.asyncio
    async def test_the_same_plan_reproduces_the_same_documents(self) -> None:
        async def run() -> list[dict]:
            ctx = context_from_modules(MockDepsModule())
            await apply_seed(
                ctx,
                SeedPlan(
                    specs=(spec_seed(_tasks(), count=4), spec_seed(_projects(), count=2)),
                    rng_seed=11,
                ),
            )

            return [row.model_dump(mode="json") for row in await _hits(ctx, _tasks())]

        assert await run() == await run()

    @pytest.mark.asyncio
    async def test_a_different_seed_produces_different_data(self) -> None:
        async def run(rng_seed: int) -> list[str]:
            ctx = context_from_modules(MockDepsModule())
            await apply_seed(
                ctx, SeedPlan(specs=(spec_seed(_projects(), count=3),), rng_seed=rng_seed)
            )

            return sorted(row.name for row in await _hits(ctx, _projects()))

        assert await run(1) != await run(2)

    @pytest.mark.asyncio
    async def test_wall_clock_ids_are_not_reproducible_and_say_so(self) -> None:
        # `instant=None` is the documented opt-out; asserting it *stops* reproducing is
        # what keeps the default's value honest.
        async def run() -> set[UUID]:
            ctx = context_from_modules(MockDepsModule())
            result = await apply_seed(
                ctx, SeedPlan(specs=(spec_seed(_projects(), count=2),), instant=None)
            )

            return set(result["projects"])

        assert await run() != await run()

    def test_two_processes_produce_byte_identical_documents(self) -> None:
        # The trap this exists for: a live-object address, `default=str`, or a set iterated
        # under a per-process hash seed all reproduce fine *in* one process and diverge
        # across two. Four runs, four hash seeds.
        # Run as a module, not a path: `python tests/support/x.py` puts that directory on
        # sys.path, where `logging.py` shadows the stdlib and the child dies before seeding.
        snapshots = {
            subprocess.run(
                [sys.executable, "-m", "tests.support.seeding_snapshot"],
                capture_output=True,
                check=True,
                cwd=_REPO_ROOT,
                env={**os.environ, "PYTHONHASHSEED": str(run)},
            ).stdout
            for run in range(4)
        }

        assert len(snapshots) == 1, "the seed is not reproducible across processes"

# ....................... #


class _Indexed(BaseModel):
    id: str = ""
    title: str = ""


class _Msg(BaseModel):
    body: str = ""


_INDEX = SearchSpec(name="notes_index", model_type=_Indexed, fields=["title"])
_BLOBS = StorageSpec(name="attachments")
_QUEUE = QueueSpec(name="jobs", codec=PydanticModelCodec(model_type=_Msg))


class TestEveryPlaneGoesThroughItsOwnWritePath:
    """§9 wants "a new plane the seeder cannot fill" to fail CI, which only means something
    once the seeder fills planes rather than document specs. Each of these asserts through the
    plane's *read* path, so a seeder that wrote into ``MockState`` directly would not pass."""

    @pytest.mark.asyncio
    async def test_search_documents_are_upserted_and_searchable(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(
            ctx,
            SeedPlan(
                search=(SearchSeed(spec=_INDEX, fixtures=({"id": "a", "title": "alpha"},)),),
            ),
        )

        page = await ctx.search.query(_INDEX).search("alpha", pagination={"limit": 10})

        assert [hit.id for hit in page.hits] == ["a"]
        assert result.indexed["notes_index"] == ("a",)

    @pytest.mark.asyncio
    async def test_search_ids_follow_a_seeded_document_spec(self) -> None:
        # An index whose ids name nothing is an index every hit of which 404s when the
        # client fetches the row behind it.
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(
            ctx,
            SeedPlan(
                specs=(spec_seed(_projects(), count=3),),
                search=(SearchSeed(spec=_INDEX, count=3, ids_from="projects"),),
            ),
        )

        assert set(result.indexed["notes_index"]) == {str(doc) for doc in result["projects"]}

    @pytest.mark.asyncio
    async def test_storage_objects_are_uploaded_and_downloadable(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(
            ctx,
            SeedPlan(
                storage=(
                    StorageSeed(
                        spec=_BLOBS,
                        objects=({"filename": "readme.txt", "data": "hello"},),
                        count=2,
                    ),
                ),
            ),
        )

        keys = result.stored["attachments"]
        assert len(keys) == 3

        downloaded = await ctx.storage.query(_BLOBS).download(keys[0])
        assert downloaded.data == b"hello"

    @pytest.mark.asyncio
    async def test_queue_messages_are_enqueued_and_consumable(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(
            ctx,
            SeedPlan(
                queues=(
                    QueueSeed(
                        spec=_QUEUE,
                        channel="jobs",
                        fixtures=({"body": "first"}, {"body": "second"}),
                    ),
                ),
            ),
        )

        assert result.queued == {"jobs/jobs": 2}

        received = await ctx.deps.resolve_configurable(
            ctx, QueueQueryDepKey, _QUEUE, route=_QUEUE.name
        ).receive("jobs", limit=2)

        assert [message.payload.body for message in received] == ["first", "second"]

    @pytest.mark.asyncio
    async def test_a_search_seed_naming_an_unseeded_spec_is_refused(self) -> None:
        with pytest.raises(CoreException, match="take ids from specs that are not seeded"):
            SeedPlan(search=(SearchSeed(spec=_INDEX, count=1, ids_from="ghosts"),))

    @pytest.mark.asyncio
    async def test_every_plane_reproduces_from_one_seed(self) -> None:
        async def run() -> tuple:
            ctx = context_from_modules(MockDepsModule())
            result = await apply_seed(
                ctx,
                SeedPlan(
                    specs=(spec_seed(_projects(), count=2),),
                    search=(SearchSeed(spec=_INDEX, count=2, ids_from="projects"),),
                    storage=(StorageSeed(spec=_BLOBS, count=2),),
                    queues=(QueueSeed(spec=_QUEUE, channel="jobs", count=2),),
                    rng_seed=5,
                ),
            )

            return (
                tuple(str(doc) for doc in result["projects"]),
                result.indexed["notes_index"],
                result.stored["attachments"],
                tuple(sorted(result.queued.items())),
            )

        assert await run() == await run()

    @pytest.mark.asyncio
    async def test_the_plane_totals_are_reported_together(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(
            ctx,
            SeedPlan(
                specs=(spec_seed(_projects(), count=2),),
                search=(SearchSeed(spec=_INDEX, count=1),),
                storage=(StorageSeed(spec=_BLOBS, count=1),),
                queues=(QueueSeed(spec=_QUEUE, channel="jobs", count=3),),
            ),
        )

        assert result.total == 2, "total stays document-only"
        assert result.total_all_planes == 7
