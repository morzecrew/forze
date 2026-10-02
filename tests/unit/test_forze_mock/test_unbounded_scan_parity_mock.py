"""A read with no limit on the mock: the order every real backend's scan must reproduce."""

from __future__ import annotations

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.unbounded_scan_parity import (
    ScanCreate,
    ScanDoc,
    ScanRead,
    run_unbounded_scan_parity,
)

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


def _doc() -> MockDocumentAdapter:
    return MockDocumentAdapter(
        spec=DocumentSpec(
            name="scan",
            read=ScanRead,
            write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
        ),
        state=MockState(),
        namespace="scan",
        read_model=ScanRead,
        domain_model=ScanDoc,
    )


async def test_a_read_without_a_limit_orders_ties_by_id() -> None:
    doc = _doc()

    await run_unbounded_scan_parity(doc, doc)


@pytest.mark.parametrize("pagination", [{"offset": -1}, {"offset": -1, "limit": 5}])
async def test_a_read_refuses_a_negative_offset(pagination: dict[str, int]) -> None:
    with pytest.raises(CoreException, match="negative"):
        await _doc().find_many(pagination=pagination)
