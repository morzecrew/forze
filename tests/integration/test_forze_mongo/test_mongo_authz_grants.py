"""Grant resolution on Mongo reads in batches and grants what the row-by-row reads granted."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.authz_grants import GRANT_SPECS, resolve_both_ways, wide_catalog
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_principal_past_one_in_batch_resolves_in_full(mongo_client: MongoClient) -> None:
    db = f"authz_{uuid4().hex[:8]}"
    routes = {
        spec.name: ConfigurableMongoDocument(
            config=MongoDocumentConfig(read=(db, spec.name), write=(db, spec.name)),
        )
        for spec in GRANT_SPECS
    }
    ctx = context_from_deps(
        Deps.plain({MongoClientDepKey: mongo_client}).merge(
            Deps.routed({DocumentQueryDepKey: routes, DocumentCommandDepKey: routes})
        )
    )
    principal_id, roles, permissions = await wide_catalog(ctx)

    assert await resolve_both_ways(ctx, principal_id) == (roles, permissions)
