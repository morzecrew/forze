"""Postgres stores an update merged into a ``jsonb`` mapping or nested model, not the patch."""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.document_merge_update import (
    SEED,
    MergeCreate,
    MergeDoc,
    MergeRead,
    MergeUpdate,
    assert_update_matching_refuses_a_merge,
    assert_updates_merge,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SPEC = DocumentSpec(
    name="merge",
    read=MergeRead,
    write=DocumentWriteTypes(domain=MergeDoc, create_cmd=MergeCreate, update_cmd=MergeUpdate),
)


async def _table(pg_client: PostgresClient) -> str:
    t = f"pg_merge_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            meta jsonb NOT NULL,
            address jsonb NOT NULL,
            items jsonb NOT NULL,
            extra jsonb
        );
        """
    )

    return t


async def test_an_update_merges_into_a_stored_mapping_and_model(pg_client: PostgresClient) -> None:
    ctx = document_context(pg_client, await _table(pg_client))

    await assert_updates_merge(ctx.document.command(SPEC), ctx.document.query(SPEC))
    await assert_update_matching_refuses_a_merge(
        ctx.document.command(SPEC), ctx.document.query(SPEC)
    )


async def test_the_update_route_answers_with_the_merge_patch(pg_client: PostgresClient) -> None:
    pytest.importorskip("fastapi")
    from fastapi import APIRouter, FastAPI

    from forze_fastapi.exceptions import register_exception_handlers
    from forze_fastapi.routes import attach_document_routes
    from forze_kits.aggregates.document import build_document_registry

    ctx = document_context(pg_client, await _table(pg_client))
    created = await ctx.document.command(SPEC).create(SEED)

    router = APIRouter(prefix="/merge")
    attach_document_routes(
        router,
        registry=build_document_registry(SPEC).freeze(),
        ns=SPEC.default_namespace,
        ctx_dep=lambda: ctx,
        style="rest",
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    # Over ASGI on this loop: TestClient would drive pg_client's pool from a second loop.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.patch(
            f"/merge/{created.id}",
            params={"rev": created.rev},
            json={"meta": {"k": 2}, "address": {"city": "B"}, "items": [{"a": None}]},
        )

    assert res.status_code == 200, res.text
    diff = res.json()["diff"]
    assert {k: diff[k] for k in ("meta", "address", "items")} == {
        "meta": {"k": 2},
        "address": {"city": "B"},
        "items": [{"a": None}],
    }, diff
