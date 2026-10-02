"""A principal's tenants on Mongo: read in one batch, listed as the row-by-row reads did."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.tenant_memberships import (
    TENANCY_SPECS,
    list_both_ways,
    refuses_a_missing_tenant,
    wide_memberships,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ctx(mongo_client: MongoClient) -> Any:
    db = f"tenancy_{uuid4().hex[:8]}"
    routes = {
        spec.name: ConfigurableMongoDocument(
            config=MongoDocumentConfig(read=(db, spec.name), write=(db, spec.name)),
        )
        for spec in TENANCY_SPECS
    }

    return context_from_deps(
        Deps.plain({MongoClientDepKey: mongo_client}).merge(
            Deps.routed({DocumentQueryDepKey: routes, DocumentCommandDepKey: routes})
        )
    )


async def test_a_principal_in_more_tenants_than_one_batch_lists_them_all(
    mongo_client: MongoClient,
) -> None:
    ctx = _ctx(mongo_client)
    principal_id, active = await wide_memberships(ctx)

    listed = await list_both_ways(ctx, principal_id)

    assert sorted(t.tenant_key for t in listed) == sorted([*active, "tenant-34"])


async def test_a_membership_naming_no_tenant_fails_the_listing(mongo_client: MongoClient) -> None:
    assert await refuses_a_missing_tenant(_ctx(mongo_client)) == "core.not_found"
