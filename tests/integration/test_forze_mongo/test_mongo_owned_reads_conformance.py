"""Mongo document reads with ``owned_by`` — the shared battery, uncached and cached.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.owned_reads_conformance import (
    OWNED_READS_BATTERY,
    Check,
    OwnedReadsHarness,
    mock_read_cache,
    owned_spec,
)

# ----------------------- #


@pytest_asyncio.fixture(params=["uncached", "cached"])
async def harness(request: pytest.FixtureRequest, mongo_client: MongoClient) -> OwnedReadsHarness:
    cached = request.param == "cached"
    db = (await mongo_client.db()).name
    collection = f"owned_{uuid4().hex[:12]}"
    cache = CacheSpec(name=f"cache_{collection}")
    cache_deps, cache_holds = mock_read_cache(cache)
    spec = owned_spec(f"doc_{collection}", **({"cache": cache} if cached else {}))
    factory: ConfigurableMongoDocument[Any, Any, Any, Any] = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                **cache_deps,
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: factory,
                DocumentCommandDepKey: factory,
            }
        )
    )

    return OwnedReadsHarness(
        query=ctx.doc.query(spec),
        command=ctx.doc.command(spec),
        spec_name=str(spec.name),
        cache_holds=cache_holds if cached else None,
    )


@pytest.mark.conformance(plane="owned_reads", engine="mongo")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)
