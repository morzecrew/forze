"""Coverage for :mod:`forze_mock.adapters.document` query paths + remaining
batch write helpers in :mod:`forze_mock.adapters._document_command`."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
    UpsertItem,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import MockDocumentAdapter, MockState

# ----------------------- #


class Item(DocWithSoftDeletion):
    title: str
    n: int = 0


class ItemCreate(CreateDocumentCmd):
    title: str
    n: int = 0


class ItemUpdate(BaseDTO):
    title: str | None = None
    n: int | None = None


class ItemRead(ReadDocument):
    title: str
    n: int = 0
    is_deleted: bool = False


SPEC = DocumentSpec(
    name="items",
    read=ItemRead,
    write=DocumentWriteTypes(
        domain=Item, create_cmd=ItemCreate, update_cmd=ItemUpdate
    ),
)


def _adapter(
    state: MockState,
    *,
    tenant_aware: bool = False,
    tenant_provider=lambda: None,
) -> MockDocumentAdapter[ItemRead, Item, ItemCreate, ItemUpdate]:
    return MockDocumentAdapter(
        spec=SPEC,
        state=state,
        namespace="items",
        read_model=ItemRead,
        domain_model=Item,
        tenant_aware=tenant_aware,
        tenant_provider=tenant_provider,
    )


# ----------------------- #
# Query side


class TestCrossTenantVisibility:
    async def test_doc_not_visible_to_other_tenant(self) -> None:
        state = MockState()
        t1, t2 = uuid4(), uuid4()
        current = {"id": TenantIdentity(tenant_id=t1)}
        adapter = _adapter(
            state, tenant_aware=True, tenant_provider=lambda: current["id"]
        )

        created = await adapter.create(ItemCreate(title="a"))
        # Same partition (mock partitions by tenant), but switch the bound tenant
        # so the row's tenant_id stamp no longer matches: invisible.
        current["id"] = TenantIdentity(tenant_id=t2)
        with pytest.raises(CoreException, match="not found"):
            await adapter.get(created.id)
        assert await adapter.find({"$values": {"title": "a"}}) is None
        assert await adapter.count() == 0

    async def test_doc_visible_filters_mismatched_and_untagged_rows(self) -> None:
        # ``_store`` partitions by tenant, so the ``_doc_visible`` stamp check is a
        # second layer of defense. Seed rows into the bound tenant's partition with
        # mismatched / missing ``tenant_id`` stamps to exercise that filter directly.
        state = MockState()
        t1, t2 = uuid4(), uuid4()
        adapter = _adapter(
            state,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=t1),
        )
        mine = await adapter.create(ItemCreate(title="mine"))  # stamped t1

        store = adapter._store()  # type: ignore[reportPrivateUsage]
        # A row stamped for another tenant living in t1's partition: not visible.
        foreign = dict(store[mine.id])
        foreign_id = uuid4()
        foreign["tenant_id"] = str(t2)
        foreign["id"] = str(foreign_id)
        store[foreign_id] = foreign
        # A row with no tenant stamp at all: not visible while a tenant is bound.
        untagged = dict(store[mine.id])
        untagged_id = uuid4()
        untagged.pop("tenant_id", None)
        untagged["id"] = str(untagged_id)
        store[untagged_id] = untagged

        visible = await adapter.find_many()
        assert {h.title for h in visible.hits} == {"mine"}
        assert await adapter.count() == 1


class TestGetMany:
    async def test_hits(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))
        out = await adapter.get_many([a.id, b.id])
        assert {r.title for r in out} == {"a", "b"}

    async def test_missing_raises(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        with pytest.raises(CoreException, match="not found"):
            await adapter.get_many([a.id, uuid4()])


class TestSingleResultHelpers:
    async def test_find_project_select_hit_and_miss(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        await adapter.create(ItemCreate(title="a", n=1))

        found = await adapter.find({"$values": {"title": "a"}})
        assert found is not None and found.title == "a"
        assert await adapter.find({"$values": {"title": "z"}}) is None

        proj = await adapter.project({"$values": {"title": "a"}}, ["title"])
        assert proj == {"title": "a"}
        assert await adapter.project({"$values": {"title": "z"}}, ["title"]) is None

        sel = await adapter.select({"$values": {"title": "a"}}, ItemRead)
        assert sel is not None and sel.title == "a"
        assert await adapter.select({"$values": {"title": "z"}}, ItemRead) is None


class TestRowLockParamsAccepted:
    async def test_for_update_modes_are_noops(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        created = await adapter.create(ItemCreate(title="a"))

        assert (await adapter.get(created.id, for_update=True)).title == "a"
        assert (
            await adapter.find({"$values": {"title": "a"}}, for_update="nowait")
        ) is not None
        assert (
            await adapter.project(
                {"$values": {"title": "a"}}, ["title"], for_update="skip_locked"
            )
        ) == {"title": "a"}
        assert (
            await adapter.select({"$values": {"title": "a"}}, ItemRead, for_update=True)
        ) is not None


class TestAggregate:
    async def test_aggregate_many_and_page(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        await adapter.create(ItemCreate(title="a", n=1))
        await adapter.create(ItemCreate(title="a", n=2))
        await adapter.create(ItemCreate(title="b", n=4))

        aggs = {
            "$groups": {"title": "title"},
            "$computed": {"total": {"$sum": "n"}},
        }
        page = await adapter.aggregate_many(aggs)
        by_title = {row["title"]: row["total"] for row in page.hits}
        assert by_title == {"a": 3, "b": 4}

        counted = await adapter.aggregate_page(aggs)
        assert counted.count == 2

    async def test_select_aggregated_many_and_page(self) -> None:
        from pydantic import BaseModel

        class _Row(BaseModel):
            title: str
            total: int

        state = MockState()
        adapter = _adapter(state)
        await adapter.create(ItemCreate(title="a", n=1))
        await adapter.create(ItemCreate(title="a", n=2))

        aggs = {
            "$groups": {"title": "title"},
            "$computed": {"total": {"$sum": "n"}},
        }
        many = await adapter.select_many_aggregated(_Row, aggs)
        assert many.hits[0].total == 3

        page = await adapter.select_page_aggregated(_Row, aggs)
        assert page.hits[0].total == 3
        assert page.count == 1


class TestOffsetPagination:
    async def test_offset_and_limit(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        for t in ["a", "b", "c", "d"]:
            await adapter.create(ItemCreate(title=t))

        page = await adapter.find_many(
            sorts={"title": "asc"}, pagination={"offset": 1, "limit": 2}
        )
        assert [h.title for h in page.hits] == ["b", "c"]

    async def test_select_page_and_find_page(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        await adapter.create(ItemCreate(title="a"))
        await adapter.create(ItemCreate(title="b"))

        fp = await adapter.find_page(sorts={"title": "asc"}, pagination={"limit": 1})
        assert fp.count == 2 and [h.title for h in fp.hits] == ["a"]

        sp = await adapter.select_page(
            ItemRead, sorts={"title": "asc"}, pagination={"limit": 1}
        )
        assert sp.count == 2 and sp.hits[0].title == "a"

        pp = await adapter.project_page(
            ["id", "title"], sorts={"title": "asc"}, pagination={"limit": 1}
        )
        assert pp.count == 2 and pp.hits[0]["title"] == "a"


class TestCursorAfterBefore:
    async def test_after_and_before_cursors(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        for t in ["b", "d", "f", "h"]:
            await adapter.create(ItemCreate(title=t))

        page1 = await adapter.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
        assert [h.title for h in page1.hits] == ["b", "d"]
        assert page1.has_more and page1.next_cursor is not None

        page2 = await adapter.find_cursor(
            sorts={"title": "asc"},
            cursor={"limit": 2, "after": page1.next_cursor},
        )
        assert [h.title for h in page2.hits] == ["f", "h"]

        # Walk back with the "before" cursor.
        back = await adapter.find_cursor(
            sorts={"title": "asc"},
            cursor={"limit": 2, "before": page2.prev_cursor},
        )
        assert [h.title for h in back.hits] == ["b", "d"]

    async def test_project_and_select_cursor(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        for t in ["a", "b"]:
            await adapter.create(ItemCreate(title=t))

        pj = await adapter.project_cursor(
            ["id", "title"], sorts={"title": "asc"}, cursor={"limit": 1}
        )
        assert pj.hits[0]["title"] == "a"

        sc = await adapter.select_cursor(
            ItemRead, sorts={"title": "asc"}, cursor={"limit": 1}
        )
        assert sc.hits[0].title == "a"


class TestStreams:
    async def test_find_project_select_stream_non_multiple(self) -> None:
        # 3 items / chunk 2: the second page is short -> stream ends on the
        # ``not has_more`` branch.
        state = MockState()
        adapter = _adapter(state)
        for t in ["a", "b", "c"]:
            await adapter.create(ItemCreate(title=t))

        seen: list[str] = []
        async for chunk in adapter.find_stream(
            sorts={"title": "asc"}, chunk_size=2
        ):
            seen.extend(h.title for h in chunk)
        assert seen == ["a", "b", "c"]

        proj: list[str] = []
        async for chunk in adapter.project_stream(
            ["id", "title"], sorts={"title": "asc"}, chunk_size=2
        ):
            proj.extend(h["title"] for h in chunk)
        assert proj == ["a", "b", "c"]

        sel: list[str] = []
        async for chunk in adapter.select_stream(
            ItemRead, sorts={"title": "asc"}, chunk_size=2
        ):
            sel.extend(h.title for h in chunk)
        assert sel == ["a", "b", "c"]

    async def test_streams_exact_multiple_ends_on_empty_page(self) -> None:
        # 4 items / chunk 2: pages [2, 2] are full (has_more stays true via the
        # cursor), so the final fetch returns an empty page -> ``not page.hits``
        # break path.
        state = MockState()
        adapter = _adapter(state)
        for t in ["a", "b", "c", "d"]:
            await adapter.create(ItemCreate(title=t))

        find_seen: list[str] = []
        async for chunk in adapter.find_stream(sorts={"title": "asc"}, chunk_size=2):
            find_seen.extend(h.title for h in chunk)
        assert find_seen == ["a", "b", "c", "d"]

        proj_seen: list[str] = []
        async for chunk in adapter.project_stream(
            ["id", "title"], sorts={"title": "asc"}, chunk_size=2
        ):
            proj_seen.extend(h["title"] for h in chunk)
        assert proj_seen == ["a", "b", "c", "d"]

        sel_seen: list[str] = []
        async for chunk in adapter.select_stream(
            ItemRead, sorts={"title": "asc"}, chunk_size=2
        ):
            sel_seen.extend(h.title for h in chunk)
        assert sel_seen == ["a", "b", "c", "d"]

    async def test_empty_streams(self) -> None:
        adapter = _adapter(MockState())
        assert [c async for c in adapter.find_stream()] == []
        assert [c async for c in adapter.project_stream(["id"])] == []
        assert [c async for c in adapter.select_stream(ItemRead)] == []


# ----------------------- #
# Batch write helpers (command mixin)


class TestUpdateMany:
    async def test_empty(self) -> None:
        adapter = _adapter(MockState())
        assert await adapter.update_many([]) == []
        assert await adapter.update_many([], return_new=False) is None

    async def test_duplicate_pks_raise(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        created = await adapter.create(ItemCreate(title="a"))
        with pytest.raises(CoreException, match="unique"):
            await adapter.update_many(
                [
                    KeyedUpdate(id=created.id, rev=created.rev, dto=ItemUpdate(title="x")),
                    KeyedUpdate(id=created.id, rev=created.rev, dto=ItemUpdate(title="y")),
                ]
            )

    async def test_return_new_and_diff_variants(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))

        out = await adapter.update_many(
            [KeyedUpdate(id=a.id, rev=a.rev, dto=ItemUpdate(title="a2"))]
        )
        assert out[0].title == "a2"

        with_diff = await adapter.update_many(
            [KeyedUpdate(id=b.id, rev=b.rev, dto=ItemUpdate(title="b2"))], return_diff=True
        )
        assert with_diff[0][1]["title"] == "b2"

    async def test_return_new_false_variants(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))

        diffs = await adapter.update_many(
            [KeyedUpdate(id=a.id, rev=a.rev, dto=ItemUpdate(title="a2"))],
            return_new=False, return_diff=True,
        )
        assert diffs[0]["title"] == "a2"

        out = await adapter.update_many(
            [KeyedUpdate(id=b.id, rev=b.rev, dto=ItemUpdate(title="b2"))], return_new=False
        )
        assert out is None


class TestUpdateMatchingStrict:
    async def test_chunked_update(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        for i in range(5):
            await adapter.create(ItemCreate(title="a", n=i))

        out = await adapter.update_matching_strict(
            {"$values": {"title": "a"}}, ItemUpdate(title="b"), chunk_size=2
        )
        assert len(out) == 5
        assert all(r.title == "b" for r in out)

    async def test_chunked_update_count(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        for i in range(3):
            await adapter.create(ItemCreate(title="a", n=i))
        n = await adapter.update_matching_strict(
            {"$values": {"title": "a"}}, ItemUpdate(title="b"),
            return_new=False, chunk_size=2,
        )
        assert n == 3

    async def test_invalid_chunk_size(self) -> None:
        adapter = _adapter(MockState())
        with pytest.raises(CoreException, match="chunk_size must be positive"):
            await adapter.update_matching_strict(
                {"$values": {}}, ItemUpdate(title="b"), chunk_size=0
            )

    async def test_no_rows_breaks_immediately(self) -> None:
        adapter = _adapter(MockState())
        assert (
            await adapter.update_matching_strict(
                {"$values": {"title": "none"}}, ItemUpdate(title="b")
            )
            == []
        )


class TestUpsertMany:
    async def test_empty(self) -> None:
        adapter = _adapter(MockState())
        assert await adapter.upsert_many([]) == []
        assert await adapter.upsert_many([], return_new=False) is None

    async def test_duplicate_ids_raise(self) -> None:
        adapter = _adapter(MockState())
        pk = uuid4()
        with pytest.raises(CoreException, match="distinct id"):
            await adapter.upsert_many(
                [
                    UpsertItem(id=pk, create=ItemCreate(title="a"), update=ItemUpdate()),
                    UpsertItem(id=pk, create=ItemCreate(title="b"), update=ItemUpdate()),
                ]
            )

    async def test_return_new_and_false(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        pk1, pk2 = uuid4(), uuid4()
        out = await adapter.upsert_many(
            [UpsertItem(id=pk1, create=ItemCreate(title="a"), update=ItemUpdate())]
        )
        assert out[0].title == "a"
        assert (
            await adapter.upsert_many(
                [UpsertItem(id=pk2, create=ItemCreate(title="b"), update=ItemUpdate())],
                return_new=False,
            )
            is None
        )


class TestTouchMany:
    async def test_empty(self) -> None:
        adapter = _adapter(MockState())
        assert await adapter.touch_many([]) == []
        assert await adapter.touch_many([], return_new=False) is None

    async def test_duplicate_pks_raise(self) -> None:
        adapter = _adapter(MockState())
        pk = uuid4()
        with pytest.raises(CoreException, match="unique"):
            await adapter.touch_many([pk, pk])

    async def test_return_new_and_false(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))
        out = await adapter.touch_many([a.id])
        assert out[0].rev == a.rev + 1
        assert await adapter.touch_many([b.id], return_new=False) is None


class TestKillMany:
    async def test_duplicate_pks_raise(self) -> None:
        adapter = _adapter(MockState())
        pk = uuid4()
        with pytest.raises(CoreException, match="unique"):
            await adapter.kill_many([pk, pk])

    async def test_kill_many_removes(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))
        await adapter.kill_many([a.id, b.id])
        assert await adapter.count() == 0


class TestDeleteRestoreMany:
    async def test_delete_many_empty(self) -> None:
        adapter = _adapter(MockState())
        assert await adapter.delete_many([]) == []
        assert await adapter.delete_many([], return_new=False) is None

    async def test_delete_many_return_new_and_false(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))
        out = await adapter.delete_many([(a.id, a.rev)])
        assert out[0].is_deleted is True
        assert await adapter.delete_many([(b.id, b.rev)], return_new=False) is None

    async def test_restore_many_empty(self) -> None:
        adapter = _adapter(MockState())
        assert await adapter.restore_many([]) == []
        assert await adapter.restore_many([], return_new=False) is None

    async def test_restore_many_return_new_and_false(self) -> None:
        state = MockState()
        adapter = _adapter(state)
        a = await adapter.create(ItemCreate(title="a"))
        b = await adapter.create(ItemCreate(title="b"))
        da = await adapter.delete(a.id, a.rev)
        db = await adapter.delete(b.id, b.rev)
        out = await adapter.restore_many([(da.id, da.rev)])
        assert out[0].is_deleted is False
        assert (
            await adapter.restore_many([(db.id, db.rev)], return_new=False) is None
        )
