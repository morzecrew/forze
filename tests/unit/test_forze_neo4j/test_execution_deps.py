"""Unit tests for forze_neo4j execution deps (module + factory)."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel, SecretStr

from forze.application.contracts.graph import (
    GraphCommandDepKey,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jGraphAdapter
from forze_neo4j.execution.deps import (
    ConfigurableNeo4jGraph,
    Neo4jClientDepKey,
    Neo4jDepsModule,
    Neo4jGraphConfig,
)
from forze_neo4j.kernel.client import (
    Neo4jClient,
    Neo4jRoutingCredentials,
    RoutedNeo4jClient,
)
from tests.support.execution_context import context_from_deps


class _R(BaseModel):
    id: str


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(name="g", nodes=(GraphNodeSpec(name="N", read=_R),), edges=())


def _ctx():
    return context_from_deps(
        Deps.plain({Neo4jClientDepKey: MagicMock(spec=Neo4jClient)})
    )


def test_module_registers_client_only() -> None:
    module = Neo4jDepsModule(client=MagicMock(spec=Neo4jClient))
    deps = module()
    assert isinstance(deps, Deps)
    assert deps.exists(Neo4jClientDepKey)


def test_module_registers_all_graph_keys() -> None:
    module = Neo4jDepsModule(
        client=MagicMock(spec=Neo4jClient),
        graphs={"g": Neo4jGraphConfig()},
    )
    deps = module()
    assert deps.exists(GraphQueryDepKey, route="g")
    assert deps.exists(GraphCommandDepKey, route="g")
    assert deps.exists(GraphRawQueryDepKey, route="g")


def test_factory_builds_adapter() -> None:
    factory = ConfigurableNeo4jGraph(config=Neo4jGraphConfig(tenant_property="tid"))
    adapter = factory(_ctx(), _spec())
    assert isinstance(adapter, Neo4jGraphAdapter)
    assert adapter.tenant_property == "tid"
    assert adapter.tenant_aware is False


def test_factory_passes_tenant_aware() -> None:
    factory = ConfigurableNeo4jGraph(config=Neo4jGraphConfig(tenant_aware=True))
    adapter = factory(_ctx(), _spec())
    assert adapter.tenant_aware is True


def test_config_rejects_mapping() -> None:
    with pytest.raises(TypeError, match="Neo4jGraphConfig"):
        ConfigurableNeo4jGraph(config={"tenant_aware": True})


def test_resolved_via_context() -> None:
    module = Neo4jDepsModule(
        client=MagicMock(spec=Neo4jClient),
        graphs={"g": Neo4jGraphConfig()},
    )
    ctx = context_from_deps(module())
    spec = _spec()
    assert isinstance(ctx.graph.query(spec), Neo4jGraphAdapter)
    assert isinstance(ctx.graph.command(spec), Neo4jGraphAdapter)
    assert isinstance(ctx.graph.raw(spec), Neo4jGraphAdapter)


# ----------------------- #
# tenant-isolation floor (Neo4j now spans tagged → namespace → dedicated)


def test_namespace_floor_satisfied_by_per_tenant_database() -> None:
    # A dynamic per-tenant `database` resolver reaches the namespace tier.
    Neo4jDepsModule(
        client=MagicMock(spec=Neo4jClient),
        graphs={"g": Neo4jGraphConfig(database=lambda t: f"t_{t}")},
        required_tenant_isolation="namespace",
    )


def test_namespace_floor_rejects_static_database() -> None:
    # A static database name is only `tagged` — below a `namespace` floor.
    with pytest.raises(CoreException, match="neo4j_tenancy_validation_failed"):
        Neo4jDepsModule(
            client=MagicMock(spec=Neo4jClient),
            graphs={"g": Neo4jGraphConfig(database="static", tenant_aware=True)},
            required_tenant_isolation="namespace",
        )


def test_dedicated_floor_satisfied_by_routed_client() -> None:
    # A RoutedNeo4jClient routes per-tenant connections → dedicated.
    Neo4jDepsModule(
        client=MagicMock(spec=RoutedNeo4jClient),
        graphs={"g": Neo4jGraphConfig(tenant_aware=True)},
        required_tenant_isolation="dedicated",
    )


def test_dedicated_floor_rejects_shared_client() -> None:
    # A shared (non-routed) client cannot reach dedicated.
    with pytest.raises(CoreException, match="neo4j_tenancy_validation_failed"):
        Neo4jDepsModule(
            client=MagicMock(spec=Neo4jClient),
            graphs={"g": Neo4jGraphConfig(tenant_aware=True)},
            required_tenant_isolation="dedicated",
        )


def test_raw_query_disabled_by_default() -> None:
    # The whole-query raw hatch is fail-closed by default (opt-in only).
    assert Neo4jGraphConfig().allow_raw_query is False


async def test_routed_client_rejects_partial_credentials() -> None:
    # Fail closed: a username without a password (or vice versa) is a misconfigured secret,
    # never a silent downgrade to anonymous auth.
    client = RoutedNeo4jClient(
        secrets=MagicMock(),
        secret_ref_for_tenant={},
        tenant_provider=lambda: None,
    )
    creds = Neo4jRoutingCredentials(uri=SecretStr("neo4j://h:7687"), username="u")

    with pytest.raises(CoreException, match="neo4j_partial_credentials"):
        await client.initialize_client(uuid4(), creds)
