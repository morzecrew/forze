"""A tenant-aware Postgres document route is refused where no tenant is bound.

The document adapter keeps its tenancy on its gateways, not on itself, so the steps that
sweep a document collection unbound must look there to refuse it at startup.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.tenancy import require_tenant_global
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.progress import job_record_spec
from forze_postgres import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresReadOnlyDocumentConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.mark.parametrize("tenant_aware", [False, True])
async def test_the_guard_reads_the_document_gateways(tenant_aware: bool) -> None:
    spec = job_record_spec()
    module = PostgresDepsModule(
        client=PostgresClient(),
        ro_documents={
            str(spec.name): PostgresReadOnlyDocumentConfig(
                read=("public", "jobs"), tenant_aware=tenant_aware
            )
        },
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        port = runtime.get_context().document.query(spec)

        if not tenant_aware:
            require_tenant_global(port, what="The sweep")
            return

        with pytest.raises(CoreException) as caught:
            require_tenant_global(port, what="The sweep")

    assert caught.value.code == "tenant_global_required"
