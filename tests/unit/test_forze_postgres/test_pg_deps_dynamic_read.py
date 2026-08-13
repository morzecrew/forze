"""Wiring guards for the Postgres dynamic-read plane — the refusals that fire at freeze.

Each of these is a wiring an author could plausibly write and a reviewer could plausibly miss,
so each fails where it is cheapest: at module construction, with a code naming what went wrong.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import pytest

from forze.application.contracts.dynamic_read import DynamicReadDepKey, DynamicReadSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
from forze_postgres.execution.deps.module import PostgresDepsModule
from forze_postgres.kernel.client import PostgresClient, RoutedPostgresClient

ROUTE = "widgets"


def _module(config: PostgresDynamicReadConfig, *, routed: bool = False) -> PostgresDepsModule:
    client: Any = Mock(spec=RoutedPostgresClient) if routed else Mock(spec=PostgresClient)
    return PostgresDepsModule(
        client=client,
        introspector_cache_partition_key=(lambda: "t") if routed else None,
        dynamic_reads={ROUTE: config},
    )


# ....................... #


def test_a_tenant_aware_route_on_the_tagged_tier_is_refused() -> None:
    """The tagged tier is where a missing predicate leaks silently, so it is refused outright.

    On namespace or dedicated the identical authoring mistake either fails loudly (undefined
    relation) or stays inside the tenant's container. On tagged it *succeeds*, with another
    tenant's rows in a correctly-rendered widget — and for a runtime statement there is nothing
    that could have caught it earlier.
    """

    with pytest.raises(CoreException) as ei:
        _module(PostgresDynamicReadConfig(provenance="trusted", tenant_aware=True))

    assert ei.value.code == "dynamic_read_tagged_refused"
    assert ei.value.kind == ExceptionKind.CONFIGURATION


def test_a_static_query_schema_does_not_satisfy_the_tenant_aware_guard() -> None:
    """One fixed schema for every tenant is not namespace routing, whatever it is named."""

    with pytest.raises(CoreException) as ei:
        _module(
            PostgresDynamicReadConfig(
                provenance="trusted",
                tenant_aware=True,
                query_schema="reporting",
            )
        )

    assert ei.value.code == "dynamic_read_tagged_refused"


def test_a_per_tenant_query_schema_satisfies_the_tenant_aware_guard() -> None:
    """The namespace tier: the container the statement runs in does the scoping."""

    module = _module(
        PostgresDynamicReadConfig(
            provenance="trusted",
            tenant_aware=True,
            query_schema=lambda tenant_id: f"t_{tenant_id}",
        )
    )

    assert DynamicReadDepKey in module().routed_deps


def test_a_routed_client_satisfies_the_tenant_aware_guard() -> None:
    """The dedicated tier answers with credentials rather than with anything in the config."""

    module = _module(
        PostgresDynamicReadConfig(provenance="trusted", tenant_aware=True),
        routed=True,
    )

    assert DynamicReadDepKey in module().routed_deps


def test_untrusted_provenance_without_confinement_is_refused() -> None:
    """A generator nobody reviews per-statement needs a container, not just a read-only txn."""

    with pytest.raises(CoreException) as ei:
        _module(PostgresDynamicReadConfig(provenance="untrusted"))

    assert ei.value.code == "dynamic_read_untrusted_unconfined"
    assert ei.value.kind == ExceptionKind.CONFIGURATION


def test_untrusted_provenance_is_satisfied_by_a_role() -> None:
    """``SET LOCAL ROLE`` is the tier-B confinement the guard is asking for."""

    module = _module(PostgresDynamicReadConfig(provenance="untrusted", role="widget_reader"))

    assert DynamicReadDepKey in module().routed_deps


def test_untrusted_provenance_is_satisfied_by_a_routed_client() -> None:
    """Per-tenant credentials are the stronger answer, and the guard accepts it."""

    module = _module(PostgresDynamicReadConfig(provenance="untrusted"), routed=True)

    assert DynamicReadDepKey in module().routed_deps


def test_the_wired_route_builds_an_adapter_carrying_the_config() -> None:
    """Registration is not resolution — the factory has to hand back a usable port.

    A route can be registered and still produce an adapter with, say, no timeout threaded
    through: the wiring guards above would all pass and every statement on it would run
    unbounded. So the built adapter is inspected, not just the key.
    """

    from forze.application.execution import DepsRegistry, ExecutionContext
    from forze_postgres.adapters.dynamic_read import PostgresDynamicReadAdapter

    config = PostgresDynamicReadConfig(
        provenance="trusted",
        query_schema="reporting",
        statement_timeout=timedelta(seconds=3),
    )
    client = Mock(spec=PostgresClient)
    module = PostgresDepsModule(client=client, dynamic_reads={ROUTE: config})
    ctx = ExecutionContext(deps=DepsRegistry.from_modules(module).freeze().resolve())

    port = ctx.dynamic_read.query(DynamicReadSpec(name=ROUTE))

    assert isinstance(port, PostgresDynamicReadAdapter)
    assert port.client is client
    assert port.config is config
    # The route's ceiling reaches the shared shell, which is what actually clamps a call.
    assert port.statement_timeout == timedelta(seconds=3)
    assert port.tenant_aware is False


def test_provenance_has_no_default() -> None:
    """An author must name their threat tier to wire the route at all."""

    with pytest.raises(TypeError):
        PostgresDynamicReadConfig()  # type: ignore[call-arg]


@pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
def test_a_non_positive_statement_timeout_is_refused(timeout: timedelta) -> None:
    """There is no spelling for "no timeout" on this plane, including by arithmetic."""

    with pytest.raises(CoreException) as ei:
        PostgresDynamicReadConfig(provenance="trusted", statement_timeout=timeout)

    assert ei.value.code == "dynamic_read_timeout_invalid"


def test_a_tenant_aware_route_participates_in_the_isolation_floor() -> None:
    """A declared floor is enforced on this route like on every other.

    The namespace tier clears ``namespace`` and not ``dedicated`` — so a deployment that
    declared the strongest floor cannot get a shared-schema dynamic-read route past it.
    """

    with pytest.raises(CoreException) as ei:
        PostgresDepsModule(
            client=Mock(spec=PostgresClient),
            required_tenant_isolation="dedicated",
            dynamic_reads={
                ROUTE: PostgresDynamicReadConfig(
                    provenance="trusted",
                    tenant_aware=True,
                    query_schema=lambda tenant_id: f"t_{tenant_id}",
                )
            },
        )

    assert ei.value.code == "postgres_tenancy_validation_failed"
    assert ei.value.details == {
        "required_isolation": "dedicated",
        "derived_isolation": "namespace",
    }
    # The route is named in the message, so a deployment with many routes learns which one.
    assert ROUTE in ei.value.summary
    assert "dynamic_read" in ei.value.summary
