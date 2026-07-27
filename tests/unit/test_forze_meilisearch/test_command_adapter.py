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
async def test_ensure_index_provisions_max_total_hits() -> None:
    """ensure_index sets the index pagination cap to the route's max_total_hits."""

    index = MagicMock()
    index.update_settings = AsyncMock(return_value=MagicMock(task_uid=1))
    client = _client_with_index(index)
    client.wait_for_task = AsyncMock(return_value=MagicMock(status="succeeded"))

    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    adapter = MeilisearchSearchManagementAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(
            index_uid="items_idx", max_total_hits=2500, wait_for_tasks=False
        ),
        client=client,
    )

    await adapter.ensure_index()

    settings = index.update_settings.await_args[0][0]
    assert settings.pagination.max_total_hits == 2500


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


# ....................... #


def _failed_task(error: object) -> MagicMock:
    task = MagicMock()
    task.status = "failed"
    task.error = error
    return task


class TestFailedTaskClassification:
    """A failed Meilisearch task is not automatically an infrastructure fault.

    Meilisearch tags a rejected request ``invalid_request`` — a missing index, a document
    the schema refuses, a filter on an unfilterable attribute. Reporting those as
    infrastructure implied a retryable server fault for something that will fail identically
    forever, and the circuit breaker counted them against the engine's health. The engine's
    own message rides along too: "task 7 did not succeed" gives a caller nothing to act on.
    """

    @pytest.mark.asyncio
    async def test_a_rejected_request_is_a_precondition_carrying_the_engine_message(
        self,
    ) -> None:
        index = MagicMock()
        index.add_documents = AsyncMock(return_value=MagicMock(task_uid=7))
        client = _client_with_index(index)
        client.wait_for_task = AsyncMock(
            return_value=_failed_task(
                {
                    "code": "invalid_document_id",
                    "type": "invalid_request",
                    "message": "Document identifier `bad id` is invalid.",
                }
            )
        )

        with pytest.raises(CoreException) as ei:
            await _adapter(client).upsert([_Doc(id="x", title="t")])

        assert ei.value.kind == ExceptionKind.PRECONDITION
        assert "invalid_document_id" in str(ei.value)
        assert "Document identifier" in str(ei.value)
        assert ei.value.details["meili_code"] == "invalid_document_id"

    @pytest.mark.asyncio
    async def test_an_engine_side_failure_stays_infrastructure(self) -> None:
        """The other branch: a genuine server fault is still retryable."""

        index = MagicMock()
        index.add_documents = AsyncMock(return_value=MagicMock(task_uid=8))
        client = _client_with_index(index)
        client.wait_for_task = AsyncMock(
            return_value=_failed_task(
                {"code": "internal", "type": "internal", "message": "index corrupted"}
            )
        )

        with pytest.raises(CoreException) as ei:
            await _adapter(client).upsert([_Doc(id="x", title="t")])

        assert ei.value.kind == ExceptionKind.INFRASTRUCTURE
        assert "index corrupted" in str(ei.value)

    @pytest.mark.asyncio
    async def test_an_unexpected_error_shape_still_reaches_the_caller(self) -> None:
        """The SDK hands back a mapping; anything else is stringified rather than dropped,
        so a shape change cannot turn a real failure into an empty message."""

        index = MagicMock()
        index.add_documents = AsyncMock(return_value=MagicMock(task_uid=9))
        client = _client_with_index(index)
        client.wait_for_task = AsyncMock(return_value=_failed_task("a bare string failure"))

        with pytest.raises(CoreException) as ei:
            await _adapter(client).upsert([_Doc(id="x", title="t")])

        assert ei.value.kind == ExceptionKind.INFRASTRUCTURE
        assert "a bare string failure" in str(ei.value)

    @pytest.mark.asyncio
    async def test_delete_all_tolerates_a_missing_index_but_nothing_else(self) -> None:
        """The tolerate list is per call, not global.

        Wiping an index that was never provisioned already satisfies "the index holds no
        documents". The same code from any other operation still means the write went
        nowhere, so it must not be swallowed there.
        """

        missing = _failed_task(
            {
                "code": "index_not_found",
                "type": "invalid_request",
                "message": "Index `items_idx` not found.",
            }
        )

        index = MagicMock()
        index.delete_all_documents = AsyncMock(return_value=MagicMock(task_uid=10))
        index.add_documents = AsyncMock(return_value=MagicMock(task_uid=11))
        client = _client_with_index(index)
        client.wait_for_task = AsyncMock(return_value=missing)

        spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
        management = MeilisearchSearchManagementAdapter(
            spec=spec,
            config=MeilisearchSearchConfig(index_uid="items_idx"),
            client=client,
        )

        # Tolerated here...
        await management.delete_all()

        # ...and not tolerated on a write, where it means the documents were not indexed.
        with pytest.raises(CoreException) as ei:
            await _adapter(client).upsert([_Doc(id="x", title="t")])

        assert ei.value.kind == ExceptionKind.PRECONDITION

    @pytest.mark.asyncio
    async def test_a_failed_task_is_not_awaited_when_waiting_is_off(self) -> None:
        """``wait_for_tasks=False`` is fire-and-forget by construction: nothing is inspected,
        so nothing can be classified."""

        index = MagicMock()
        index.add_documents = AsyncMock(return_value=MagicMock(task_uid=12))
        client = _client_with_index(index)
        client.wait_for_task = AsyncMock()

        await _adapter(client, wait_for_tasks=False).upsert([_Doc(id="x", title="t")])

        client.wait_for_task.assert_not_awaited()
