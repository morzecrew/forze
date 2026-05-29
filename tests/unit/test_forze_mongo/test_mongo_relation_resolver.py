"""Unit tests for Mongo RelationSpec resolution."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_mongo.kernel.relation import resolve_mongo_collection


@pytest.mark.asyncio
async def test_resolve_static_mongo_collection() -> None:
    database, collection = await resolve_mongo_collection(("app", "items"), None)
    assert database == "app"
    assert collection == "items"


@pytest.mark.asyncio
async def test_resolve_callable_mongo_collection() -> None:
    tid = uuid4()

    def resolver(tenant_id: object) -> tuple[str, str]:
        assert tenant_id == tid
        return (f"tenant_{tid.hex[:8]}", "items")

    database, collection = await resolve_mongo_collection(resolver, tid)
    assert database == f"tenant_{tid.hex[:8]}"
    assert collection == "items"
