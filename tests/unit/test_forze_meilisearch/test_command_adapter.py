"""Unit tests for MeilisearchSearchCommandAdapter."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.adapters.search._command import (
    MeilisearchSearchCommandAdapter,
    MeilisearchSearchManagementAdapter,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

# ----------------------- #


class _Doc(BaseModel):
    id: str
    title: str


def _adapter(
    client: MagicMock,
    *,
    wait_for_tasks: bool = True,
    tenant: object | None = None,
) -> MeilisearchSearchCommandAdapter[_Doc]:
    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    provider = (lambda: tenant) if tenant is not None else None
    return MeilisearchSearchCommandAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(index_uid="items_idx", wait_for_tasks=wait_for_tasks),
        client=client,
        tenant_aware=tenant is not None,
        tenant_provider=provider,
    )


def _client_with_index(index: MagicMock) -> MagicMock:
    client = MagicMock()
    client.index = MagicMock(return_value=index)
    client.get_or_create_index = AsyncMock(return_value=index)
    return client


@pytest.mark.asyncio
async def test_upsert_calls_add_documents() -> None:
    client = MagicMock()
    index = MagicMock()
    task = MagicMock(task_uid=1)
    index.add_documents = AsyncMock(return_value=task)
    index.update_settings = AsyncMock(return_value=task)
    client.get_or_create_index = AsyncMock(return_value=index)
    client.wait_for_task = AsyncMock()
    client.index = MagicMock(return_value=index)

    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    adapter = MeilisearchSearchCommandAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(
            index_uid="items_idx",
            wait_for_tasks=False,
        ),
        client=client,
    )

    await adapter.upsert([_Doc(id="1", title="A")])

    index.add_documents.assert_awaited_once()
    client.wait_for_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_await_task_raises_on_failed_status() -> None:
    """A completed-but-failed Meilisearch task surfaces as an error, not silent success."""

    index = MagicMock()
    index.add_documents = AsyncMock(return_value=MagicMock(task_uid=7))
    client = _client_with_index(index)
    client.wait_for_task = AsyncMock(
        return_value=MagicMock(status="failed", error="bad doc")
    )

    adapter = _adapter(client, wait_for_tasks=True)

    with pytest.raises(CoreException) as ei:
        await adapter.upsert([_Doc(id="1", title="A")])

    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE


@pytest.mark.asyncio
async def test_await_task_succeeds_and_bounds_the_wait() -> None:
    """A succeeded task passes, and the wait is bounded by the configured timeout."""

    index = MagicMock()
    index.add_documents = AsyncMock(return_value=MagicMock(task_uid=7))
    client = _client_with_index(index)
    client.wait_for_task = AsyncMock(return_value=MagicMock(status="succeeded"))

    adapter = _adapter(client, wait_for_tasks=True)
    await adapter.upsert([_Doc(id="1", title="A")])

    client.wait_for_task.assert_awaited_once_with(7, timeout=timedelta(seconds=60))


@pytest.mark.asyncio
async def test_delete_all_is_tenant_scoped_when_tagged() -> None:
    """``delete_all`` under tagged tenancy filters by tenant, never wiping the shared index."""

    index = MagicMock()
    index.delete_documents_by_filter = AsyncMock(return_value=MagicMock(task_uid=1))
    index.delete_all_documents = AsyncMock(return_value=MagicMock(task_uid=1))
    client = _client_with_index(index)
    client.wait_for_task = AsyncMock(return_value=MagicMock(status="succeeded"))

    tenant_id = uuid4()
    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    adapter = MeilisearchSearchManagementAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(index_uid="items_idx", wait_for_tasks=False),
        client=client,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    await adapter.delete_all()

    index.delete_all_documents.assert_not_awaited()
    index.delete_documents_by_filter.assert_awaited_once()
    flt = index.delete_documents_by_filter.await_args[0][0]
    assert str(tenant_id) in flt and "tenant_id" in flt


@pytest.mark.asyncio
async def test_delete_by_id_is_tenant_scoped_when_tagged() -> None:
    """``delete(ids)`` under tagged tenancy scopes the delete to this tenant's rows."""

    index = MagicMock()
    index.delete_documents_by_filter = AsyncMock(return_value=MagicMock(task_uid=1))
    index.delete_documents = AsyncMock(return_value=MagicMock(task_uid=1))
    client = _client_with_index(index)
    client.wait_for_task = AsyncMock(return_value=MagicMock(status="succeeded"))

    tenant_id = uuid4()
    adapter = _adapter(
        client, wait_for_tasks=False, tenant=TenantIdentity(tenant_id=tenant_id)
    )

    await adapter.delete(["a", "b"])

    index.delete_documents.assert_not_awaited()
    flt = index.delete_documents_by_filter.await_args[0][0]
    assert str(tenant_id) in flt and '"a"' in flt and '"b"' in flt


def test_to_index_document_stamps_tenant_when_tagged() -> None:
    """Tagged-tenancy writes carry the tenant discriminator so reads can isolate them."""

    tenant_id = uuid4()
    adapter = _adapter(
        MagicMock(), wait_for_tasks=False, tenant=TenantIdentity(tenant_id=tenant_id)
    )

    doc = adapter.to_index_document(_Doc(id="1", title="A"))

    assert doc["tenant_id"] == str(tenant_id)


def test_untenanted_adapter_does_not_stamp_tenant() -> None:
    adapter = _adapter(MagicMock(), wait_for_tasks=False)
    doc = adapter.to_index_document(_Doc(id="1", title="A"))
    assert "tenant_id" not in doc
