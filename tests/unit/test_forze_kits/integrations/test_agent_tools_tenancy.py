"""The tenancy scenario on the in-memory mock — one half of the mock-equals-real pair.

The scenario itself lives in :mod:`tests.support.agent_tools_tenancy` and is driven
identically by the Postgres leg, so a divergence between the two engines fails here rather
than drifting. What this module owns is the mock's provisioning: a tenant-aware route and a
document registry over it.
"""

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import build_document_registry
from forze_mock import MockDepsModule, MockRouteConfig
from tests.support.agent_tools_tenancy import (
    AGENT_TOOLS_TENANCY_BATTERY,
    AgentToolsTenancyHarness,
    Check,
    battery_is_populated,
)

pytestmark = pytest.mark.unit

# ----------------------- #

_NS = StrKeyNamespace(prefix="notes")


class _Note(Document):
    title: str


class _NoteRead(ReadDocument):
    title: str


class _CreateNote(CreateDocumentCmd):
    title: str


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write={"domain": _Note, "create_cmd": _CreateNote},
    )


# ....................... #


@pytest.mark.parametrize("check", AGENT_TOOLS_TENANCY_BATTERY, ids=lambda c: c.__name__)
async def test_agent_tools_tenancy_battery(check: Check) -> None:
    # tenant_aware: the mock partitions storage and filters rows, mirroring the tenant
    # predicate a real relation carries.
    module = MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        await check(
            AgentToolsTenancyHarness(
                ctx=runtime.get_context(),
                registry=build_document_registry(_spec(), ns=_NS).freeze(),
                ns=_NS,
                backend="mock",
            )
        )


# ....................... #


def test_the_battery_still_has_its_checks() -> None:
    # parametrize over an emptied tuple collects nothing and reports green, on both legs
    # at once. This is the floor under that.
    battery_is_populated()


# ....................... #


class TestTheMocksOwnWiring:
    """Provisioning facts that are the mock's, not the shared scenario's."""

    def test_the_route_is_tenant_aware(self) -> None:
        # If this ever wired tenant-free, every isolation check above would pass for the
        # wrong reason: nothing to isolate, nothing to leak.
        module = MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})

        assert module.routes is not None
        assert module.routes["notes"].tenant_aware is True

    def test_the_registry_exposes_the_operations_the_scenario_drives(self) -> None:
        catalog = build_document_registry(_spec(), ns=_NS).freeze().catalog()

        assert str(_NS.key("create")) in {str(key) for key in catalog}
        assert str(_NS.key("list")) in {str(key) for key in catalog}


# ....................... #


def test_each_harness_tenant_is_freshly_generated() -> None:
    # A default that handed out one shared tenant would make every isolation assertion in
    # the battery vacuous without failing anything. Read off the field rather than built
    # from a half-populated harness: the property under test is the factory's.
    fields = attrs.fields(AgentToolsTenancyHarness)

    for field in (fields.tenant_a, fields.tenant_b):
        first = field.default.factory()
        second = field.default.factory()

        assert first.tenant_id != second.tenant_id, f"{field.name} is not freshly generated"
