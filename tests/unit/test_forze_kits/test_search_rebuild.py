"""`rebuild_search_index` backfills an external index from the document plane it indexes.

The sweep's contract is an *equivalence*, not a fill: a rebuilt index must hold exactly what
an unbroken incremental sync would have produced. So the load-bearing test here is not "did
rows arrive" — it is :class:`TestEquivalenceWithIncrementalSync`, which drives the two paths
over the same rows and compares the resulting index bucket byte for byte. Anything the sweep
learns to do differently from ``bind_search_sync`` fails there, which is the point: a rebuild
that quietly produces a *different* index than the app has been maintaining is the failure
mode worth a test, and it is invisible to a test that only counts rows.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits import document_facade
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import (
    DocumentIdDTO,
    DocumentUpdateDTO,
    build_document_registry,
)
from forze_kits.aggregates.search import bind_search_sync
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_kits.integrations.search import SearchRebuildReport, rebuild_search_index
from forze_mock import MockDepsModule, MockStateDepKey

# ----------------------- #

_TX = "mock"


class Gadget(DocWithSoftDeletion):
    name: str = ""


class GadgetCreate(CreateDocumentCmd):
    name: str


class GadgetUpdate(UpdateCmdWithSoftDeletion):
    name: str | None = None


class GadgetRead(ReadDocument):
    name: str
    is_deleted: bool = False


GADGET_SPEC = DocumentSpec(
    name="gadgets",
    read=GadgetRead,
    write=DocumentWriteTypes(
        domain=Gadget, create_cmd=GadgetCreate, update_cmd=GadgetUpdate
    ),
)
GADGET_INDEX = SearchSpec(name="gadgets_search", model_type=GadgetRead, fields=["name"])
_GADGET_BUCKET = "gadgets_search"


# A plain aggregate whose read model has no soft-delete flag at all — the sweep's
# ``getattr`` guard must be inert for it, exactly as the sync path's is.
class Widget(Document):
    name: str = ""


class WidgetCreate(CreateDocumentCmd):
    name: str


class WidgetUpdate(BaseDTO):
    name: str | None = None


class WidgetRead(ReadDocument):
    name: str


WIDGET_SPEC = DocumentSpec(
    name="widgets",
    read=WidgetRead,
    write=DocumentWriteTypes(
        domain=Widget, create_cmd=WidgetCreate, update_cmd=WidgetUpdate
    ),
)
WIDGET_INDEX = SearchSpec(name="widgets_search", model_type=WidgetRead, fields=["name"])
_WIDGET_BUCKET = "widgets_search"


# ....................... #


def _index(runtime, bucket: str) -> dict:
    return runtime.get_context().deps.provide(MockStateDepKey).documents.get(bucket, {})


async def _rebuild(ctx, *, document, search, **kw) -> SearchRebuildReport:
    return await rebuild_search_index(
        ctx.doc.query(document),
        ctx.search.command(search),
        document=document,
        search=search,
        **kw,
    )


# ....................... #


class TestEquivalenceWithIncrementalSync:
    """A rebuilt index equals the index an unbroken incremental sync would have produced.

    Same runtime, same rows, same ids: the incremental sync builds the index, the bucket is
    snapshotted, the index is wiped, and the sweep rebuilds it from the document plane. The
    two must be identical — including for the rows whose *correct* index state is "absent".
    """

    async def test_rebuild_reproduces_the_synced_index_exactly(self) -> None:
        reg = build_document_registry(GADGET_SPEC)
        reg = bind_search_sync(
            reg, document=GADGET_SPEC, search=GADGET_INDEX, tx_route=_TX
        )
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(runtime, reg.freeze(), GADGET_SPEC)

        async with runtime.scope():
            # Three rows, covering every state whose index representation differs:
            # a live row (indexed), a soft-deleted row (must be absent), and a killed
            # row (gone from the source entirely, so absent both ways).
            live = await gadgets().create(GadgetCreate(name="alpha"))
            soft = await gadgets().create(GadgetCreate(name="beta"))
            killed = await gadgets().create(GadgetCreate(name="gamma"))

            await gadgets().update(
                DocumentUpdateDTO(
                    id=soft.id, rev=soft.rev, dto=GadgetUpdate(is_deleted=True)
                )
            )
            await gadgets().kill(DocumentIdDTO(id=killed.id))

            synced = dict(_index(runtime, _GADGET_BUCKET))
            assert set(synced) == {live.id}  # the sync's own answer, for the record

            ctx = runtime.get_context()
            await ctx.search.management(GADGET_INDEX).delete_all()
            assert _index(runtime, _GADGET_BUCKET) == {}

            report = await _rebuild(ctx, document=GADGET_SPEC, search=GADGET_INDEX)

            # The whole contract, in one assertion.
            assert _index(runtime, _GADGET_BUCKET) == synced
            assert report == SearchRebuildReport(indexed=1, removed=1)


# ....................... #


class TestBackfill:
    async def test_fills_an_index_that_was_never_synced(self) -> None:
        # The motivating case: rows written before search existed. No sync is bound, so
        # nothing ever carried them, and the index is correct-looking and empty.
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(
            runtime, build_document_registry(WIDGET_SPEC).freeze(), WIDGET_SPEC
        )

        async with runtime.scope():
            created = [
                await widgets().create(WidgetCreate(name=n)) for n in ("a", "b", "c")
            ]
            assert _index(runtime, _WIDGET_BUCKET) == {}

            report = await _rebuild(
                runtime.get_context(), document=WIDGET_SPEC, search=WIDGET_INDEX
            )

            bucket = _index(runtime, _WIDGET_BUCKET)
            assert set(bucket) == {row.id for row in created}
            assert {bucket[row.id]["name"] for row in created} == {"a", "b", "c"}
            assert report == SearchRebuildReport(indexed=3, removed=0)

    async def test_a_read_model_without_the_flag_is_upserted_unconditionally(
        self,
    ) -> None:
        # WidgetRead has no ``is_deleted`` at all; the guard must be inert, not raise.
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(
            runtime, build_document_registry(WIDGET_SPEC).freeze(), WIDGET_SPEC
        )

        async with runtime.scope():
            await widgets().create(WidgetCreate(name="a"))
            report = await _rebuild(
                runtime.get_context(), document=WIDGET_SPEC, search=WIDGET_INDEX
            )

        assert report == SearchRebuildReport(indexed=1, removed=0)

    async def test_rerunning_converges_rather_than_duplicating(self) -> None:
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(
            runtime, build_document_registry(WIDGET_SPEC).freeze(), WIDGET_SPEC
        )

        async with runtime.scope():
            await widgets().create(WidgetCreate(name="a"))
            ctx = runtime.get_context()

            first = await _rebuild(ctx, document=WIDGET_SPEC, search=WIDGET_INDEX)
            after_first = dict(_index(runtime, _WIDGET_BUCKET))
            second = await _rebuild(ctx, document=WIDGET_SPEC, search=WIDGET_INDEX)

            # An interrupted sweep is re-run, not repaired: applying a row twice is
            # applying it once.
            assert second == first
            assert _index(runtime, _WIDGET_BUCKET) == after_first

    async def test_filters_scope_the_sweep(self) -> None:
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(
            runtime, build_document_registry(WIDGET_SPEC).freeze(), WIDGET_SPEC
        )

        async with runtime.scope():
            keep = await widgets().create(WidgetCreate(name="keep"))
            await widgets().create(WidgetCreate(name="drop"))

            report = await _rebuild(
                runtime.get_context(),
                document=WIDGET_SPEC,
                search=WIDGET_INDEX,
                filters={"$values": {"name": "keep"}},
            )

            assert set(_index(runtime, _WIDGET_BUCKET)) == {keep.id}
            assert report == SearchRebuildReport(indexed=1, removed=0)


# ....................... #


class TestGhosts:
    """A soft-deleted row must never end up searchable — the failure the sweep exists to avoid."""

    async def test_a_soft_deleted_row_is_never_indexed(self) -> None:
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(
            runtime, build_document_registry(GADGET_SPEC).freeze(), GADGET_SPEC
        )

        async with runtime.scope():
            row = await gadgets().create(GadgetCreate(name="alpha"))
            await gadgets().update(
                DocumentUpdateDTO(
                    id=row.id, rev=row.rev, dto=GadgetUpdate(is_deleted=True)
                )
            )

            report = await _rebuild(
                runtime.get_context(), document=GADGET_SPEC, search=GADGET_INDEX
            )

            # A sweep that merely upserted everything it read would put this row back —
            # a hit that GET then 404s.
            assert _index(runtime, _GADGET_BUCKET) == {}
            assert report == SearchRebuildReport(indexed=0, removed=1)

    async def test_a_soft_deleted_row_already_in_the_index_is_evicted(self) -> None:
        # The drifted-index case: the row was indexed while live, the sync then broke, and
        # the soft-delete never reached the index. The sweep must *remove* it — which is why
        # the sweep is a rebuild and not a fill.
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(
            runtime, build_document_registry(GADGET_SPEC).freeze(), GADGET_SPEC
        )

        async with runtime.scope():
            row = await gadgets().create(GadgetCreate(name="alpha"))
            ctx = runtime.get_context()

            await _rebuild(ctx, document=GADGET_SPEC, search=GADGET_INDEX)
            assert row.id in _index(runtime, _GADGET_BUCKET)

            await gadgets().update(
                DocumentUpdateDTO(
                    id=row.id, rev=row.rev, dto=GadgetUpdate(is_deleted=True)
                )
            )
            report = await _rebuild(ctx, document=GADGET_SPEC, search=GADGET_INDEX)

            assert _index(runtime, _GADGET_BUCKET) == {}
            assert report == SearchRebuildReport(indexed=0, removed=1)


# ....................... #


class _FakeQuery:
    """A document query port that records how it was streamed."""

    def __init__(self, *batches: list[Any]) -> None:
        self._batches = batches
        self.chunk_sizes: list[int] = []
        self.streamed = False

    async def find_stream(self, filters=None, *, sorts=None, chunk_size=500):  # type: ignore[no-untyped-def]
        self.streamed = True
        self.chunk_sizes.append(chunk_size)
        for batch in self._batches:
            yield batch


class _FakeCommand:
    """A search command port that records its calls (batched, not per row)."""

    def __init__(self) -> None:
        self.upserts: list[list[Any]] = []
        self.deletes: list[list[str]] = []

    async def upsert_many(self, documents) -> None:  # type: ignore[no-untyped-def]
        self.upserts.append(list(documents))

    async def delete(self, ids) -> None:  # type: ignore[no-untyped-def]
        self.deletes.append(list(ids))


def _row(name: str, *, deleted: bool | None = None) -> Any:
    model: dict[str, Any] = {"id": uuid4(), "name": name}

    if deleted is not None:
        model["is_deleted"] = deleted

    return type("Row", (), model)()


# ....................... #


class TestBatching:
    """One index call per chunk — a per-row round-trip would make a large rebuild unusable."""

    async def test_each_chunk_is_one_upsert_and_one_delete(self) -> None:
        query = _FakeQuery(
            [_row("a"), _row("b", deleted=True), _row("c")],
            [_row("d", deleted=True), _row("e")],
        )
        command = _FakeCommand()

        report = await rebuild_search_index(
            query,  # type: ignore[arg-type]
            command,  # type: ignore[arg-type]
            document=WIDGET_SPEC,
            search=WIDGET_INDEX,
            chunk_size=3,
        )

        assert [len(batch) for batch in command.upserts] == [2, 1]
        assert [len(batch) for batch in command.deletes] == [1, 1]
        assert report == SearchRebuildReport(indexed=3, removed=2)
        assert report.scanned == 5
        assert query.chunk_sizes == [3]  # threaded through, not silently defaulted

    async def test_a_chunk_with_no_deletes_costs_no_delete_call(self) -> None:
        query = _FakeQuery([_row("a"), _row("b")])
        command = _FakeCommand()

        await rebuild_search_index(
            query,  # type: ignore[arg-type]
            command,  # type: ignore[arg-type]
            document=WIDGET_SPEC,
            search=WIDGET_INDEX,
        )

        assert len(command.upserts) == 1
        assert command.deletes == []


# ....................... #


class TestEncryptionParity:
    """The sweep holds both specs, so it is one more seam the plaintext-to-Meili leak can open at."""

    async def test_a_document_sealing_a_field_the_index_does_not_is_refused(
        self,
    ) -> None:
        sealed = DocumentSpec(
            name="sealed",
            read=WidgetRead,
            write=DocumentWriteTypes(
                domain=Widget, create_cmd=WidgetCreate, update_cmd=WidgetUpdate
            ),
            encryption=FieldEncryption(encrypted=frozenset({"name"})),
        )
        # The index declares no encryption — so the document's decrypted read model would be
        # written to it in clear.
        plain_index = SearchSpec(
            name="sealed_search", model_type=WidgetRead, fields=["name"]
        )
        query = _FakeQuery([_row("secret")])
        command = _FakeCommand()

        with pytest.raises(CoreException) as exc_info:
            await rebuild_search_index(
                query,  # type: ignore[arg-type]
                command,  # type: ignore[arg-type]
                document=sealed,
                search=plain_index,
            )

        assert exc_info.value.kind is ExceptionKind.CONFIGURATION
        assert exc_info.value.code == "search_encryption_parity_mismatch"

        # Refused *before* the first row is read: a mismatch can never half-fill an index
        # with plaintext, and the sweep costs nothing when it is going to fail.
        assert not query.streamed
        assert command.upserts == []


# ....................... #


class TestAggregateKitFrontDoor:
    async def test_kit_rebuilds_its_own_index(self) -> None:
        kit = AggregateKit(spec=WIDGET_SPEC, search=WIDGET_INDEX)
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(runtime, kit.registry(tx_route=_TX), WIDGET_SPEC)

        async with runtime.scope():
            created = await widgets().create(WidgetCreate(name="alpha"))
            ctx = runtime.get_context()

            # The kit syncs on write, so wipe first to prove the rebuild is what refills it.
            await ctx.search.management(WIDGET_INDEX).delete_all()
            assert _index(runtime, _WIDGET_BUCKET) == {}

            report = await kit.rebuild_search(ctx)

            assert set(_index(runtime, _WIDGET_BUCKET)) == {created.id}
            assert report == SearchRebuildReport(indexed=1, removed=0)

    async def test_a_kit_without_search_refuses(self) -> None:
        kit = AggregateKit(spec=WIDGET_SPEC)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            with pytest.raises(CoreException) as exc_info:
                await kit.rebuild_search(runtime.get_context())

        assert exc_info.value.kind is ExceptionKind.PRECONDITION
