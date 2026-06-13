"""The in-memory oracle for the hierarchy operators (label-aware, inclusive)."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.hierarchy import (
    CASES,
    TreeCreate,
    TreeDoc,
    TreeRead,
    assert_hierarchy_parity,
    seed_tree_corpus,
)

pytestmark = pytest.mark.unit


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="tree",
        read=TreeRead,
        write=DocumentWriteTypes(domain=TreeDoc, create_cmd=TreeCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="tree",
        read_model=TreeRead,
        domain_model=TreeDoc,
    )


@pytest.mark.asyncio
async def test_hierarchy_cases_pin_oracle() -> None:
    # The oracle is its own reference here: parity of the mock against itself, which also
    # asserts every case matches its hand-pinned expected label set.
    doc = _mock()
    await seed_tree_corpus(doc)

    await assert_hierarchy_parity(doc, doc)


@pytest.mark.asyncio
async def test_every_case_has_a_nonempty_expectation() -> None:
    # Guard against a vacuously-passing parity case (all filters select something).
    assert all(expected for _, expected in CASES)
