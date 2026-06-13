"""Mongo opt-in non-native null ordering: an explicit NULLS FIRST/LAST override.

Mongo natively orders null as the smallest value (asc → nulls first, desc → nulls last).
With ``computed_null_ordering`` enabled, an *override* of that placement is honored for
offset reads via an aggregation pipeline (a computed null-rank sort key) — verified here
against the in-memory oracle on a real Mongo. Without the opt-in the override is rejected.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.cursor_parity import (
    SEED,
    CursorCreate,
    CursorDoc,
    CursorRead,
)
from tests.support.execution_context import context_from_deps

# Explicit overrides of Mongo's native null-as-smallest order; ``seq`` keeps the order
# total so the mock and Mongo agree exactly.
_OVERRIDE_CASES = [
    {"score": {"dir": "asc", "nulls": "last"}, "seq": "asc"},
    {"score": {"dir": "desc", "nulls": "first"}, "seq": "asc"},
    {"grp": "asc", "score": {"dir": "asc", "nulls": "last"}, "seq": "asc"},
]

_NATIVE_CASE = {"score": "asc", "seq": "asc"}  # canonical default — native sort path


def _mock_port() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="nullord",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="nullord",
        read_model=CursorRead,
        domain_model=CursorDoc,
    )


async def _seq_order(query: Any, sorts: Any) -> list[int]:
    page = await query.find_many(filters=None, sorts=sorts, pagination={"limit": 100})
    return [h.seq for h in page.hits]


async def _setup(
    mongo_client: MongoClient, *, computed_null_ordering: bool
) -> tuple[Any, Any]:
    collection = f"nullord_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="nullord",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
            computed_null_ordering=computed_null_ordering,
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    for create in SEED:
        await cmd.create(create)

    return cmd, query


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("sorts", _OVERRIDE_CASES)
async def test_computed_null_ordering_matches_oracle(
    mongo_client: MongoClient, sorts: dict[str, Any]
) -> None:
    _cmd, query = await _setup(mongo_client, computed_null_ordering=True)

    oracle = _mock_port()
    for create in SEED:
        await oracle.create(create)

    mongo_order = await _seq_order(query, sorts)
    oracle_order = await _seq_order(oracle, sorts)

    assert mongo_order == oracle_order
    assert sorted(mongo_order) == list(range(len(SEED)))  # no rows dropped


@pytest.mark.integration
@pytest.mark.asyncio
async def test_default_null_ordering_uses_native_path(
    mongo_client: MongoClient,
) -> None:
    # The canonical placement is unaffected by the flag — still the native indexed sort.
    _cmd, query = await _setup(mongo_client, computed_null_ordering=True)

    oracle = _mock_port()
    for create in SEED:
        await oracle.create(create)

    assert await _seq_order(query, _NATIVE_CASE) == await _seq_order(oracle, _NATIVE_CASE)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_override_rejected_without_opt_in(mongo_client: MongoClient) -> None:
    _cmd, query = await _setup(mongo_client, computed_null_ordering=False)

    with pytest.raises(CoreException, match="does not support"):
        await _seq_order(query, {"score": {"dir": "asc", "nulls": "last"}, "seq": "asc"})
