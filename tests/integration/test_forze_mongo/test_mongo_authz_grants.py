"""Grant resolution on Mongo reads in batches and grants what the row-by-row reads granted."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.authz_grants import (
    GRANT_SPECS,
    TENANT_ROLE_SPECS,
    resolve_both_ways,
    seeded_role_assignment,
    tenant_held_roles,
    wide_catalog,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _context(mongo_client: MongoClient, specs: tuple[Any, ...]) -> Any:
    db = f"authz_{uuid4().hex[:8]}"
    routes = {
        spec.name: ConfigurableMongoDocument(
            config=MongoDocumentConfig(read=(db, spec.name), write=(db, spec.name)),
        )
        for spec in specs
    }

    return context_from_deps(
        Deps.plain({MongoClientDepKey: mongo_client}).merge(
            Deps.routed({DocumentQueryDepKey: routes, DocumentCommandDepKey: routes})
        )
    )


async def test_tenant_held_role_bindings(mongo_client: MongoClient) -> None:
    ctx = _context(mongo_client, TENANT_ROLE_SPECS)
    ra, principal_id = await seeded_role_assignment(ctx)

    await tenant_held_roles(ctx, ra, principal_id)


async def test_a_principal_past_one_in_batch_resolves_in_full(mongo_client: MongoClient) -> None:
    ctx = _context(mongo_client, GRANT_SPECS)
    principal_id, roles, permissions = await wide_catalog(ctx)

    assert await resolve_both_ways(ctx, principal_id) == (roles, permissions)
