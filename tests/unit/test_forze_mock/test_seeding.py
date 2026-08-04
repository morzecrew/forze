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
from forze.base.primitives import StripedAsyncLocks
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
    seed_plan,
    singularize,
    spec_seed,
)
from forze_mock.seeding.values import split_row_id

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


class _RequiredRefCreate(CreateDocumentCmd):
    project_id: UUID


def _required_ref(name: str = "referrers") -> DocumentSpec:
    """A spec whose reference field cannot be null — so it cannot be rooted."""

    return DocumentSpec(
        name=name,
        read=_ProjectRead,
        write=DocumentWriteTypes(
            domain=_Project, create_cmd=_RequiredRefCreate, update_cmd=_ProjectUpdate
        ),
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
    async def test_the_recorded_rows_are_the_payloads_the_write_path_received(self) -> None:
        # `SeedResult.rows` says "as created", so it has to be the validated create command:
        # the row it came from still carries the reserved `id` key the command never sees,
        # and its reference fields still hold the generated values, from before linking.
        ctx = context_from_modules(MockDepsModule())
        fixed = UUID("11111111-1111-1111-1111-111111111111")
        result = await apply_seed(
            ctx,
            SeedPlan(
                specs=(
                    spec_seed(_tasks(), count=3),
                    spec_seed(_projects(), fixtures=[{"id": str(fixed), "name": "Apollo"}]),
                )
            ),
        )

        project_row = result.rows["projects"][0]

        assert "id" not in project_row, "the reserved fixture key is not a create-command field"
        assert project_row["name"] == "Apollo"
        assert {row["project_id"] for row in result.rows["tasks"]} == {str(fixed)}

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

    @pytest.mark.asyncio
    async def test_a_required_reference_with_nothing_to_point_at_is_refused(self) -> None:
        # The residual hole the nullable case leaves: a non-nullable reference cannot become
        # a root, so letting the generated UUID stand would seed a row whose reference 404s.
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(
            specs=(spec_seed(_required_ref(), count=2), spec_seed(_projects(), count=0)),
        )

        with pytest.raises(CoreException, match="that spec seeds nothing"):
            await apply_seed(ctx, plan)

    @pytest.mark.asyncio
    async def test_a_required_self_reference_is_refused_rather_than_dangling(self) -> None:
        # Same rule, the other population: the *first* row of a self-link has no earlier row.
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(
            specs=(spec_seed(_required_ref(name="projects"), count=2),),
            links={"projects": {"project_id": "projects"}},
        )

        with pytest.raises(CoreException, match="first row of a self-reference"):
            await apply_seed(ctx, plan)

    def test_an_override_targeting_an_unseeded_spec_is_refused(self) -> None:
        seeds = (spec_seed(_tasks(), count=1),)

        with pytest.raises(CoreException, match="targets 'ghosts', which is not seeded"):
            infer_links(seeds, overrides={"tasks": {"project_id": "ghosts"}})

    def test_an_override_keyed_by_an_unseeded_spec_is_refused(self) -> None:
        # A key outside `seeds` is never looked up, so an unchecked one makes a typo
        # indistinguishable from a link the author believes they declared. `SeedPlan` catches
        # it at declaration; `infer_links` is public and has to catch it too.
        seeds = (spec_seed(_tasks(), count=1), spec_seed(_projects(), count=1))

        with pytest.raises(CoreException, match="name specs that are not seeded: taks"):
            infer_links(seeds, overrides={"taks": {"project_id": "projects"}})

    def test_an_override_naming_a_field_the_command_lacks_is_refused(self) -> None:
        # The typo case: an override that matches no field would otherwise be inert, and a
        # link the author believes they declared is worse than one they know is missing.
        seeds = (spec_seed(_tasks(), count=1), spec_seed(_projects(), count=1))

        with pytest.raises(CoreException, match="does not create: projekt_id"):
            infer_links(seeds, overrides={"tasks": {"projekt_id": "projects"}})

    def test_bookkeeping_fields_are_never_read_as_references(self) -> None:
        # A spec literally named `keys` makes `_key` a matching stem, so `last_update_at`
        # and friends are excluded by name rather than by luck of the suffix list.
        class _BookkeepingCreate(CreateDocumentCmd):
            id: UUID | None = None
            rev: int = 0
            created_at: str = ""
            last_update_at: str = ""

        spec = DocumentSpec(
            name="records",
            read=_ProjectRead,
            write=DocumentWriteTypes(
                domain=_Project, create_cmd=_BookkeepingCreate, update_cmd=_ProjectUpdate
            ),
        )

        assert infer_links((spec_seed(spec, count=1), spec_seed(_projects(), count=1))) == {}

    def test_a_plan_refuses_link_overrides_for_specs_it_does_not_seed(self) -> None:
        with pytest.raises(CoreException, match="name specs that are not seeded: ghosts"):
            SeedPlan(specs=(spec_seed(_projects(), count=1),), links={"ghosts": {"x": None}})

    def test_the_convenience_constructors_build_the_same_plan(self) -> None:
        one = seed_plan(spec_seed(_projects(), count=2), rng_seed=9)
        two = SeedPlan(specs=(), rng_seed=9).with_specs(spec_seed(_projects(), count=2))

        assert one.rng_seed == 9
        assert [seed.spec.name for seed in one.specs] == [seed.spec.name for seed in two.specs]

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

    def test_two_specs_reducing_to_one_stem_are_refused_rather_than_ordered(self) -> None:
        # `projects` and `project` share a stem, so `project_id` could mean either — and
        # which one won depended on plan order, linking to a spec the author never named.
        seeds = (spec_seed(_projects("projects"), count=1), spec_seed(_projects("project"), count=1))

        with pytest.raises(CoreException, match="both reduce to 'project'"):
            infer_links(seeds)


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
    async def test_a_non_string_fixture_key_is_still_a_configuration_error(self) -> None:
        # A YAML fixture can carry a non-string key. Sorting it against the string field
        # names raises a TypeError that buries the configuration error the author needs.
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(specs=(spec_seed(_projects(), fixtures=[{7: "seven", "nope": 1}]),))

        with pytest.raises(CoreException, match="does not accept: 7, nope"):
            await apply_seed(ctx, plan)

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
        source.write_text(
            json.dumps(rows) if suffix == ".json" else "- name: Apollo\n- name: Gemini\n"
        )

        assert load_fixtures(source) == tuple(rows)

    def test_a_missing_fixture_file_is_named(self, tmp_path: Path) -> None:
        with pytest.raises(CoreException, match="Fixture file not found"):
            load_fixtures(tmp_path / "absent.json")

    @pytest.mark.asyncio
    async def test_a_fixture_id_may_be_a_uuid_object_or_its_text(self) -> None:
        # Both spellings reach a plan: JSON gives text, a Python-declared plan gives a UUID.
        ctx = context_from_modules(MockDepsModule())
        as_object = UUID("22222222-2222-2222-2222-222222222222")
        as_text = "33333333-3333-3333-3333-333333333333"

        result = await apply_seed(
            ctx,
            SeedPlan(
                specs=(
                    spec_seed(
                        _projects(),
                        fixtures=[{"id": as_object, "name": "a"}, {"id": as_text, "name": "b"}],
                    ),
                )
            ),
        )

        assert result["projects"] == (as_object, UUID(as_text))

    def test_a_fixture_id_that_is_not_a_uuid_is_refused_by_value(self) -> None:
        seed = spec_seed(_projects(), fixtures=[{"id": "not-a-uuid", "name": "a"}])

        with pytest.raises(CoreException, match="Fixture 'id' is not a UUID"):
            split_row_id(seed.fixtures[0])

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

    @pytest.mark.parametrize(
        "seed",
        [
            pytest.param(lambda: spec_seed(_projects(), count=-1), id="documents"),
            pytest.param(lambda: SearchSeed(spec=_INDEX, count=-1), id="search"),
            pytest.param(lambda: StorageSeed(spec=_BLOBS, count=-1), id="storage"),
            pytest.param(lambda: QueueSeed(spec=_QUEUE, channel="jobs", count=-1), id="queues"),
        ],
    )
    def test_a_negative_count_is_refused_on_every_plane(self, seed) -> None:
        # `range(-1)` is empty, so a negative count silently means "generate nothing" — a
        # plan that asked for rows would produce none and say nothing about it.
        with pytest.raises(CoreException, match="must not be negative"):
            seed()

    @pytest.mark.parametrize(
        ("plan", "expected"),
        [
            pytest.param(
                lambda: SeedPlan(search=(SearchSeed(spec=_INDEX), SearchSeed(spec=_INDEX))),
                "Search index seeded more than once: notes_index",
                id="search",
            ),
            pytest.param(
                lambda: SeedPlan(storage=(StorageSeed(spec=_BLOBS), StorageSeed(spec=_BLOBS))),
                "Storage spec seeded more than once: attachments",
                id="storage",
            ),
            pytest.param(
                lambda: SeedPlan(
                    queues=(
                        QueueSeed(spec=_QUEUE, channel="jobs"),
                        QueueSeed(spec=_QUEUE, channel="jobs"),
                    )
                ),
                "Queue channel seeded more than once: jobs/jobs",
                id="queues",
            ),
        ],
    )
    def test_two_seeds_for_one_target_are_refused(self, plan, expected: str) -> None:
        # Every plane's result map is keyed by the target's name, so a second seed does not
        # add to the report — it replaces it, and `total_all_planes` undercounts real rows.
        with pytest.raises(CoreException, match=expected):
            plan()

    def test_two_queue_channels_on_one_spec_are_still_fine(self) -> None:
        plan = SeedPlan(
            queues=(
                QueueSeed(spec=_QUEUE, channel="jobs"),
                QueueSeed(spec=_QUEUE, channel="retries"),
            )
        )

        assert len(plan.queues) == 2

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
    async def test_an_empty_search_seed_touches_the_index_not_at_all(self) -> None:
        # No documents means no upsert — an empty write is not the same as no write, and a
        # plane declared but unfilled should report nothing rather than an empty batch.
        ctx = context_from_modules(MockDepsModule())

        result = await apply_seed(ctx, SeedPlan(search=(SearchSeed(spec=_INDEX),)))

        assert result.indexed == {"notes_index": ()}
        assert result.total_all_planes == 0

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


class TestStateResetIsTotal:
    """`MockState.clear()` is what `POST /_mock/reset` calls, so a store it misses is data
    that survives a reset — and the miss is silent."""

    def test_every_public_field_returns_to_its_declared_default(self) -> None:
        import attrs

        state = MockState()
        state.documents["notes"] = {"a": {"id": "a"}}
        state.storage_buckets.add("bucket")
        state.mvcc_version = 57  # a scalar counter, not a factory-defaulted collection

        state.clear()

        for field in attrs.fields(MockState):
            if field.name.startswith("_") or field.default is attrs.NOTHING:
                continue

            if isinstance(getattr(state, field.name), StripedAsyncLocks):
                continue  # machinery, asserted separately below

            expected = (
                field.default.factory()
                if isinstance(field.default, attrs.Factory)
                else field.default
            )

            assert getattr(state, field.name) == expected, (
                f"{field.name} survived a reset — a store the derivation skips is data that "
                "outlives POST /_mock/reset"
            )

    def test_a_store_someone_is_holding_is_emptied_not_swapped_out(self) -> None:
        # Adapters and open transactions hold these containers directly — and
        # `restore_tx_stores` already resets in place, so a `clear()` that swapped them would
        # leave a rollback writing into a store nothing can read.
        state = MockState()
        held = state.documents
        held_identity = state.identity
        state.documents["notes"] = {"a": {"id": "a"}}
        state.identity["authn"]["key"] = "value"

        state.clear()

        assert state.documents is held, "the container was replaced, not emptied"
        assert held == {}
        # A populated default is refilled, not merely emptied.
        assert state.identity is held_identity
        assert sorted(held_identity) == ["authn", "authz", "secrets", "tenants"]
        assert held_identity["authn"] == {}

    def test_a_takes_self_factory_is_rebuilt_rather_than_crashing_the_reset(self) -> None:
        # `takes_self=True` factories are used across this codebase, so calling every factory
        # bare would turn the day one lands on MockState into a broken `POST /_mock/reset`.
        import attrs

        from forze_mock.state import _fresh_default

        @attrs.define(slots=True)
        class _Holder:
            size: int = 3
            items: list[int] = attrs.field(
                default=attrs.Factory(lambda self: [0] * self.size, takes_self=True)
            )

        holder = _Holder()
        field = attrs.fields(_Holder).items

        assert _fresh_default(holder, field.default) == [0, 0, 0]

    def test_a_field_whose_type_changed_is_replaced_rather_than_emptied(self) -> None:
        from forze_mock.state import _emptied_in_place

        # Nothing to empty in place when the two are not the same container — the caller
        # falls back to assignment rather than half-resetting.
        assert _emptied_in_place(57, 0) is False
        assert _emptied_in_place(None, {}) is False
        assert _emptied_in_place({"a": 1}, {}) is True

    def test_the_machinery_survives_because_callers_may_be_waiting_on_it(self) -> None:
        # Rebuilding a striped lock table mid-flight hands the next caller a *different*
        # lock than the one a waiter holds, which silently breaks single-flight.
        state = MockState()
        lock = state.lock
        stripes = state.rotating_credential_locks

        state.clear()

        assert state.lock is lock
        assert state.rotating_credential_locks is stripes
