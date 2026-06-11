"""Smoke tests for ``forze_kits.dto.ImportTimestamps`` import/restore semantics."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter
from forze_mock.state import MockState

from forze_kits.dto import ImportTimestamps

# ----------------------- #


class _NoteDoc(Document):
    body: str


class _NoteImportCreate(ImportTimestamps):
    body: str


class _NoteUpdate(BaseDTO):
    body: str | None = None


class _NoteRead(ReadDocument):
    body: str


def _adapter(
    state: MockState,
) -> MockDocumentAdapter[_NoteRead, _NoteDoc, _NoteImportCreate, _NoteUpdate]:
    spec = DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(
            domain=_NoteDoc,
            create_cmd=_NoteImportCreate,
            update_cmd=_NoteUpdate,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="notes",
        read_model=_NoteRead,
        domain_model=_NoteDoc,
    )


_CREATED = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
_UPDATED = datetime(2021, 6, 7, 8, 9, 10, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_ensure_preserves_import_timestamps_but_not_rev() -> None:
    """Explicit timestamps on an import-style create flow through ``ensure``."""

    doc = _adapter(MockState())
    payload = _NoteImportCreate(
        body="restored",
        created_at=_CREATED,
        last_update_at=_UPDATED,
    )

    restored = await doc.ensure(uuid4(), payload)
    read = await doc.get(restored.id)

    assert read.created_at == _CREATED
    assert read.last_update_at == _UPDATED
    # ``rev`` is intentionally not part of the import surface: the server
    # assigns the initial revision regardless of the source document.
    assert read.rev == 1


@pytest.mark.asyncio
async def test_ensure_omitted_timestamps_fall_back_to_server_stamp() -> None:
    """``None`` timestamps let the server stamp them (non-import create path)."""

    doc = _adapter(MockState())

    restored = await doc.ensure(uuid4(), _NoteImportCreate(body="fresh"))
    read = await doc.get(restored.id)

    now = datetime.now(timezone.utc)
    assert abs(now - read.created_at) < timedelta(minutes=1)
    assert abs(now - read.last_update_at) < timedelta(minutes=1)
