"""Firestore document reads with ``owned_by`` — the shared battery, uncached and cached.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

from uuid import UUID

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from tests.support.execution_context import context_from_deps
from tests.support.owned_reads_conformance import (
    OWNED_READS_BATTERY,
    Check,
    OwnedReadsHarness,
    owned_spec,
)

# ----------------------- #


@pytest.fixture(params=["uncached", "cached"])
def harness(
    request: pytest.FixtureRequest,
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> OwnedReadsHarness:
    cached = request.param == "cached"
    cache_spec = CacheSpec(name=f"cache_{unique_collection}")
    spec = owned_spec(f"doc_{unique_collection}", **({"cache": cache_spec} if cached else {}))
    factory = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", unique_collection),
            write=("(default)", unique_collection),
        ),
    )
    state = MockState()

    def _cache(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=cspec.name)

    ctx = context_from_deps(
        Deps.plain(
            {
                MockStateDepKey: state,
                FirestoreClientDepKey: firestore_client,
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


@pytest.mark.conformance(plane="owned_reads", engine="firestore")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)
