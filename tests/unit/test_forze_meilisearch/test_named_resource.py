"""Unit tests for Meilisearch NamedResourceSpec index_uid resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_meilisearch.adapters.search._command import (
    MeilisearchSearchManagementAdapter,
)
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.module import MeilisearchDepsModule
from forze_meilisearch.kernel.relation import resolve_meilisearch_index_uid


@pytest.mark.asyncio
async def test_resolve_static_index_uid() -> None:
    assert await resolve_meilisearch_index_uid("products", None) == "products"


@pytest.mark.asyncio
async def test_resolve_callable_index_uid() -> None:
    tid = uuid4()

    def resolver(tenant_id: object) -> str:
        assert tenant_id == tid
        return f"idx-{tid.hex[:8]}"

    assert await resolve_meilisearch_index_uid(resolver, tid) == f"idx-{tid.hex[:8]}"


@pytest.mark.asyncio
async def test_management_adapter_resolves_dynamic_index_uid() -> None:
    class _Row(BaseModel):
        id: str
        title: str

    tid = uuid4()
    spec = SearchSpec(name="items", model_type=_Row, fields=["title"])
    config = MeilisearchSearchConfig(
        index_uid=lambda t: f"idx-{t}" if t else "shared",
    )
    client = MagicMock()
    index = MagicMock()
    index.update_settings = AsyncMock(return_value=MagicMock())
    client.get_or_create_index = AsyncMock(return_value=index)
    client.wait_for_task = AsyncMock(return_value=MagicMock(status="succeeded"))

    adapter = MeilisearchSearchManagementAdapter(
        spec=spec,
        config=config,
        client=client,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.ensure_index()

    client.get_or_create_index.assert_awaited_once()
    assert client.get_or_create_index.await_args.args[0] == f"idx-{tid}"


def test_meilisearch_deps_module_warns_dynamic_index_with_tenant_aware() -> None:
    def _resolver(_tenant_id: object) -> str:
        return "tenant-index"

    with patch(
        "forze_meilisearch.execution.deps.module.warn_integration_routes",
    ) as mock_warn:
        MeilisearchDepsModule(
            client=MagicMock(),
            searches={
                "items": MeilisearchSearchConfig(
                    index_uid=_resolver,
                    tenant_aware=True,
                ),
            },
        )

    mock_warn.assert_called_once()
    kwargs = mock_warn.call_args.kwargs
    assert kwargs["integration"] == "Meilisearch"
    assert kwargs["routes"] is not None
    assert kwargs["routes"]["items"].index_uid is _resolver


def test_meilisearch_federated_deps_warns_member_index() -> None:
    def _resolver(_tenant_id: object) -> str:
        return "leg-index"

    with patch(
        "forze_meilisearch.execution.deps.module.warn_dynamic_relation_with_tenant_aware",
    ) as mock_warn:
        MeilisearchDepsModule(
            client=MagicMock(),
            federated_searches={
                "fed": MeilisearchFederatedSearchConfig(
                    members={
                        "a": MeilisearchSearchConfig(
                            index_uid=_resolver,
                            tenant_aware=True,
                        ),
                        "b": MeilisearchSearchConfig(index_uid="static-b"),
                    },
                ),
            },
        )

    dynamic_calls = [
        c
        for c in mock_warn.call_args_list
        if c.kwargs.get("route_name") == "fed.a"
    ]
    assert len(dynamic_calls) == 1
    assert dynamic_calls[0].kwargs["named_fields"] == [("index_uid", _resolver)]
