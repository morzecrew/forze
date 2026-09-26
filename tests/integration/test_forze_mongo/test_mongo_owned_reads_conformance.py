"""Mongo document reads with ``owned_by`` — the shared battery, uncached and cached.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.owned_reads_conformance import (
    OWNED_READS_BATTERY,
    Check,
    OwnedReadsHarness,
    owned_spec,
)

# ----------------------- #


@pytest_asyncio.fixture(params=["uncached", "cached"])
async def harness(request: pytest.FixtureRequest, mongo_client: MongoClient) -> OwnedReadsHarness:
    cached = request.param == "cached"
    db = (await mongo_client.db()).name
    collection = f"owned_{uuid4().hex[:12]}"
    cache_spec = CacheSpec(name=f"cache_{collection}")
    spec = owned_spec(f"doc_{collection}", **({"cache": cache_spec} if cached else {}))
    factory = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    state = MockState()

    def _cache(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=cspec.name)

    ctx = context_from_deps(
        Deps.plain(
            {
                MockStateDepKey: state,
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: factory,
                DocumentCommandDepKey: factory,
                CacheDepKey: _cache,
            }
        )
    )

    async def _cache_holds(pk: UUID) -> bool:
        return any(key[0] == str(pk) for key in state.cache_bodies.get(cache_spec.name, {}))

    return OwnedReadsHarness(
        query=ctx.doc.query(spec),
        command=ctx.doc.command(spec),
        spec_name=str(spec.name),
        cache_holds=_cache_holds if cached else None,
    )


@pytest.mark.conformance(plane="owned_reads", engine="mongo")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)
