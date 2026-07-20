"""`bind_search_sync` keeps an external search index in step with a document's writes (mock).

The mock's search command mutates an in-memory bucket keyed by the **search** spec's name —
distinct from the document's bucket when the names differ, exactly as a real Meilisearch index
name differs from the backing table. These tests point search at a separate name and assert that
bucket tracks create / update / kill, proving the after-commit sync fires post-commit.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
import structlog.testing
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
from forze_kits.aggregates.search import SearchSyncSteps, bind_search_sync
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
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
    return (
        runtime.get_context()
        .deps.provide(MockStateDepKey)
        .documents.get(_INDEX_BUCKET, {})
    )


# ....................... #


class TestWrittenModelExtraction:
    def test_create_result_passes_through_update_result_is_unwrapped(self) -> None:
        class _Tiny(BaseModel):
            id: str = "x"

        row = _Tiny()
        assert written_read_model(row) is row  # CREATE returns the read model directly
        assert (
            written_read_model(DocumentUpdateRes(data=row, diff={})) is row
        )  # UPDATE unwraps


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


# ....................... #


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


def _gadget_registry():
    reg = build_document_registry(GADGET_SPEC)
    reg = bind_search_sync(reg, document=GADGET_SPEC, search=GADGET_INDEX, tx_route=_TX)
    return reg.freeze()


def _gadget_index(runtime):
    return (
        runtime.get_context()
        .deps.provide(MockStateDepKey)
        .documents.get(_GADGET_BUCKET, {})
    )


class TestSearchSyncSoftDelete:
    """A soft-deleted row must leave the index via the generic UPDATE path — never re-enter it."""

    async def test_update_that_soft_deletes_removes_the_index_entry(self) -> None:
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(runtime, _gadget_registry(), GADGET_SPEC)

        async with runtime.scope():
            created = await gadgets().create(GadgetCreate(name="alpha"))
            assert created.id in _gadget_index(runtime)

            await gadgets().update(
                DocumentUpdateDTO(
                    id=created.id, rev=created.rev, dto=GadgetUpdate(is_deleted=True)
                )
            )

            # The ghost is gone — search cannot return a row that GET would 404.
            assert created.id not in _gadget_index(runtime)

    async def test_update_that_restores_re_upserts_the_row(self) -> None:
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(runtime, _gadget_registry(), GADGET_SPEC)

        async with runtime.scope():
            created = await gadgets().create(GadgetCreate(name="alpha"))
            deleted = await gadgets().update(
                DocumentUpdateDTO(
                    id=created.id, rev=created.rev, dto=GadgetUpdate(is_deleted=True)
                )
            )
            assert created.id not in _gadget_index(runtime)

            await gadgets().update(
                DocumentUpdateDTO(
                    id=created.id,
                    rev=deleted.data.rev,
                    dto=GadgetUpdate(is_deleted=False),
                )
            )

            assert created.id in _gadget_index(runtime)

    async def test_live_row_update_still_upserts(self) -> None:
        runtime = build_runtime(MockDepsModule())
        gadgets = document_facade(runtime, _gadget_registry(), GADGET_SPEC)

        async with runtime.scope():
            created = await gadgets().create(GadgetCreate(name="alpha"))
            await gadgets().update(
                DocumentUpdateDTO(
                    id=created.id, rev=created.rev, dto=GadgetUpdate(name="beta")
                )
            )

            assert _gadget_index(runtime)[created.id]["name"] == "beta"


# ....................... #


def _read_row(cls: type[Any], **fields: Any) -> Any:
    now = datetime.now(UTC)
    return cls(id=uuid4(), rev=1, created_at=now, last_update_at=now, **fields)


class _FlakyCommand:
    """Search command stub failing the first *fail_times* calls, then recording."""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls: list[tuple[str, object]] = []

    async def _record(self, action: str, payload: object) -> None:
        self.calls.append((action, payload))

        if len(self.calls) <= self.fail_times:
            raise ConnectionError("index unreachable")

    async def upsert(self, models: object) -> None:
        await self._record("upsert", models)

    async def delete(self, ids: object) -> None:
        await self._record("delete", ids)


class _FakeSearchDeps:
    def __init__(self, command: _FlakyCommand) -> None:
        self._command = command

    def command(self, search: object) -> _FlakyCommand:
        return self._command


class _FakeCtx:
    def __init__(self, command: _FlakyCommand) -> None:
        self.search = _FakeSearchDeps(command)


class TestSearchSyncRetry:
    """A transient index failure is retried in-place; exhaustion warns with reconcilable identity."""

    async def test_transient_failure_is_retried_then_succeeds(self) -> None:
        command = _FlakyCommand(fail_times=1)
        steps = SearchSyncSteps(search=WIDGET_INDEX, retry_base_delay=0.0)
        hook = steps.upsert_on_write().factory(_FakeCtx(command))

        await hook(None, _read_row(WidgetRead, name="alpha"))

        assert [action for action, _ in command.calls] == ["upsert", "upsert"]

    async def test_exhausted_retries_warn_and_reraise(self) -> None:
        command = _FlakyCommand(fail_times=100)
        steps = SearchSyncSteps(
            search=WIDGET_INDEX, retry_attempts=1, retry_base_delay=0.0
        )
        row = _read_row(WidgetRead, name="alpha")
        hook = steps.upsert_on_write().factory(_FakeCtx(command))

        with structlog.testing.capture_logs() as logs, pytest.raises(ConnectionError):
            await hook(None, row)

        assert len(command.calls) == 2  # the first call plus one bounded retry

        warnings = [log for log in logs if log["log_level"] == "warning"]
        assert len(warnings) == 1
        # Enough identity to reconcile the stale row by hand.
        assert warnings[0]["index"] == "widgets_search"
        assert warnings[0]["document_id"] == str(row.id)

    async def test_soft_deleted_row_goes_through_the_delete_path(self) -> None:
        command = _FlakyCommand(fail_times=0)
        steps = SearchSyncSteps(search=GADGET_INDEX, retry_base_delay=0.0)
        row = _read_row(GadgetRead, name="alpha", is_deleted=True)
        hook = steps.upsert_on_write().factory(_FakeCtx(command))

        await hook(None, row)

        assert command.calls == [("delete", [str(row.id)])]
