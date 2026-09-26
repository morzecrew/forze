"""Firestore document reads with ``owned_by`` — the shared battery, uncached and cached.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps
from tests.support.owned_reads_conformance import (
    OWNED_READS_BATTERY,
    Check,
    OwnedReadsHarness,
    mock_read_cache,
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
    cache = CacheSpec(name=f"cache_{unique_collection}")
    cache_deps, cache_holds = mock_read_cache(cache)
    spec = owned_spec(f"doc_{unique_collection}", **({"cache": cache} if cached else {}))
    factory: ConfigurableFirestoreDocument[Any, Any, Any, Any] = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", unique_collection),
            write=("(default)", unique_collection),
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                **cache_deps,
                FirestoreClientDepKey: firestore_client,
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


@pytest.mark.conformance(plane="owned_reads", engine="firestore")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)
