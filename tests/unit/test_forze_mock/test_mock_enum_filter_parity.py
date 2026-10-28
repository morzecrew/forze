"""The mock matches a filter on a plain ``Enum`` field by the member's value, as a store does.

The Postgres twin is ``tests/integration/test_forze_postgres/test_pg_enum_filter_parity.py``.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.enum_filter_parity import (
    ROWS,
    StatusCreate,
    StatusDoc,
    StatusRead,
    assert_enum_cases,
)

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_enum_filters_match_on_mock() -> None:
    doc = MockDocumentAdapter(
        spec=DocumentSpec(
            name="enum_rows",
            read=StatusRead,
            write=DocumentWriteTypes(domain=StatusDoc, create_cmd=StatusCreate),
        ),
        state=MockState(),
        namespace="enum_rows",
        read_model=StatusRead,
        domain_model=StatusDoc,
    )
    # Created through the port, each row keeps the member, not the value a filter names.
    ids = {key: (await doc.create(StatusCreate.model_validate(row))).id for key, row in ROWS.items()}

    await assert_enum_cases(doc, ids, backend="mock")
