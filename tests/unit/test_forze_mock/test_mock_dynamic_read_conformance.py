"""The in-memory dynamic-read adapter against the shared governance battery.

The mock's handler is deliberately dumb: it looks up rows by container and slices them to the
``row_probe`` the shell asked for, which is all a real ``_fetch_rows`` does. Anything smarter
here would be the mock re-implementing the shell it is supposed to be compared against.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from uuid import UUID

import pytest

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.integrations.dynamic_read import DynamicReadRequest
from forze.base.primitives import JsonDict
from forze_mock.adapters import (
    MockDynamicReadAdapter,
    MockDynamicReadRegistry,
    MockState,
)
from tests.support.dynamic_read_conformance import (
    DYNAMIC_READ_BATTERY,
    ROUTE,
    Check,
    DynamicReadHarness,
)

pytestmark = pytest.mark.asyncio

ROWS_STATEMENT = "SELECT n FROM items ORDER BY n"
TENANT_STATEMENT = "SELECT %(tenant)s AS t"


@pytest.fixture
def harness() -> DynamicReadHarness:
    containers: dict[str | None, list[JsonDict]] = {}

    def handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
        _ = state
        key = str(request.tenant_id) if request.tenant_id is not None else None

        if request.statement == TENANT_STATEMENT:
            # The shell merges the tenant id only when the statement references the
            # placeholder — reading it back out of the bound params is how the mock observes
            # the same merge Postgres observes by binding it.
            return [{"t": request.params["tenant"]}]

        return list(containers.get(key, ()))[: request.row_probe]

    registry = MockDynamicReadRegistry().on(ROUTE, handler)

    async def seed(tenant: UUID | None, count: int) -> None:
        key = str(tenant) if tenant is not None else None
        containers[key] = [{"n": index} for index in range(count)]

    def build(
        spec: DynamicReadSpec,
        tenant_provider: TenantProviderPort | None,
        tenant_aware: bool,
    ) -> DynamicReadPort:
        return MockDynamicReadAdapter(
            state=MockState(),
            spec=spec,
            registry=registry,
            statement_timeout=timedelta(seconds=5),
            tenant_aware=tenant_aware,
            tenant_provider=tenant_provider,
        )

    return DynamicReadHarness(
        backend="mock",
        build=build,
        seed=seed,
        rows_statement=ROWS_STATEMENT,
        tenant_statement=TENANT_STATEMENT,
    )


@pytest.mark.conformance(plane="dynamic_read", engine="mock")
@pytest.mark.parametrize("check", DYNAMIC_READ_BATTERY, ids=lambda check: check.__name__)
async def test_dynamic_read_battery(check: Check, harness: DynamicReadHarness) -> None:
    await check(harness)
