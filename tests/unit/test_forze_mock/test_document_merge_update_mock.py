"""The mock stores an update merged into a mapping or nested model, as the real stores must."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.document_merge_update import (
    MergeCreate,
    MergeDoc,
    MergeRead,
    MergeUpdate,
    assert_update_matching_refuses_a_merge,
    assert_updates_merge,
)

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_an_update_merges_into_a_stored_mapping_and_model() -> None:
    spec = DocumentSpec(
        name="merge",
        read=MergeRead,
        write=DocumentWriteTypes(domain=MergeDoc, create_cmd=MergeCreate, update_cmd=MergeUpdate),
    )
    doc: Any = MockDocumentAdapter(
        spec=spec, state=MockState(), namespace="merge", read_model=MergeRead, domain_model=MergeDoc
    )

    await assert_updates_merge(doc, doc)
    await assert_update_matching_refuses_a_merge(doc, doc)
