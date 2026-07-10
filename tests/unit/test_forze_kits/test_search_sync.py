"""`bind_search_sync` keeps an external search index in step with a document's writes (mock).

The mock's search command mutates an in-memory bucket keyed by the **search** spec's name —
distinct from the document's bucket when the names differ, exactly as a real Meilisearch index
name differs from the backing table. These tests point search at a separate name and assert that
bucket tracks create / update / kill, proving the after-commit sync fires post-commit.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits import document_facade
from forze_kits.aggregates.document import (
    DocumentIdDTO,
    DocumentUpdateDTO,
    build_document_registry,
)
from forze_kits.aggregates.document.dto import DocumentUpdateRes, written_read_model
from forze_kits.aggregates.search import bind_search_sync
from forze_mock import MockDepsModule, MockStateDepKey

# ----------------------- #

_TX = "mock"


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
# A separate index name — the mock keeps it in its own bucket, like a real Meili index.
WIDGET_INDEX = SearchSpec(name="widgets_search", model_type=WidgetRead, fields=["name"])
_INDEX_BUCKET = "widgets_search"


def _synced_registry():
    reg = build_document_registry(WIDGET_SPEC)
    reg = bind_search_sync(reg, document=WIDGET_SPEC, search=WIDGET_INDEX, tx_route=_TX)
    return reg.freeze()


def _index_bucket(runtime):
    return runtime.get_context().deps.provide(MockStateDepKey).documents.get(
        _INDEX_BUCKET, {}
    )


# ....................... #


class TestWrittenModelExtraction:
    def test_create_result_passes_through_update_result_is_unwrapped(self) -> None:
        class _Tiny(BaseModel):
            id: str = "x"

        row = _Tiny()
        assert written_read_model(row) is row  # CREATE returns the read model directly
        assert written_read_model(DocumentUpdateRes(data=row, diff={})) is row  # UPDATE unwraps


# ....................... #


class TestBindSearchSyncShape:
    def test_patches_in_place_without_conjuring_ops(self) -> None:
        reg = build_document_registry(WIDGET_SPEC)
        before = reg.operation_keys()

        bound = bind_search_sync(
            reg, document=WIDGET_SPEC, search=WIDGET_INDEX, tx_route=_TX
        )

        # No new (handlerless) op keys are introduced; every patched write op keeps its handler.
        assert bound.operation_keys() == before
        bound.freeze()  # freezes cleanly


# ....................... #


class TestSearchSyncEndToEnd:
    async def test_create_upserts_into_the_index(self) -> None:
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(runtime, _synced_registry(), WIDGET_SPEC)

        async with runtime.scope():
            created = await widgets().create(WidgetCreate(name="alpha"))

            bucket = _index_bucket(runtime)
            assert created.id in bucket
            assert bucket[created.id]["name"] == "alpha"

    async def test_update_re_syncs_the_index(self) -> None:
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(runtime, _synced_registry(), WIDGET_SPEC)

        async with runtime.scope():
            created = await widgets().create(WidgetCreate(name="alpha"))
            await widgets().update(
                DocumentUpdateDTO(
                    id=created.id, rev=created.rev, dto=WidgetUpdate(name="beta")
                )
            )

            # The index reflects the update — proving DocumentUpdateRes is unwrapped and upserted.
            assert _index_bucket(runtime)[created.id]["name"] == "beta"

    async def test_kill_removes_the_index_entry(self) -> None:
        runtime = build_runtime(MockDepsModule())
        widgets = document_facade(runtime, _synced_registry(), WIDGET_SPEC)

        async with runtime.scope():
            created = await widgets().create(WidgetCreate(name="alpha"))
            assert created.id in _index_bucket(runtime)

            await widgets().kill(DocumentIdDTO(id=created.id))

            assert created.id not in _index_bucket(runtime)
