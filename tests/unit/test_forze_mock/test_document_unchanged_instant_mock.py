"""The mock finds a resent unchanged instant unchanged, as the real stores must."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.document_unchanged_instant import (
    InstantCreate,
    InstantDoc,
    InstantRead,
    InstantUpdate,
    assert_an_unchanged_instant_is_not_a_change,
)

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_an_unchanged_instant_is_not_a_change() -> None:
    spec = DocumentSpec(
        name="instant",
        read=InstantRead,
        write=DocumentWriteTypes(
            domain=InstantDoc, create_cmd=InstantCreate, update_cmd=InstantUpdate
        ),
    )
    doc: Any = MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="instant",
        read_model=InstantRead,
        domain_model=InstantDoc,
    )

    await assert_an_unchanged_instant_is_not_a_change(doc, doc)
