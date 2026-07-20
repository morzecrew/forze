"""Tests for the ConfigurableHttpService dep factory.

Drives every ``_resolve_client`` branch (static client, tenant-aware via
per-tenant secret refs, tenant-aware over an already-routed dep client, and the
misconfiguration error) plus the adapter-wiring done in ``__call__``.
"""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import Deps
from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import (
    BaseHttpIntegration,
    async_http_op,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_deps
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.execution.deps.factories import ConfigurableHttpService
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.kernel.client import HttpClient, RoutedHttpClient

# ----------------------- #

TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")


class _PingResponse(BaseModel):
    ok: bool


class _PingClient(BaseHttpIntegration):
    ping = async_http_op(
        request=None,
        response=_PingResponse,
        method="GET",
        path="/ping",
    )


class _SecretsStub:
    async def resolve_str(self, ref: SecretRef) -> str:
        return "{}"

    async def exists(self, ref: SecretRef) -> bool:
        return True


def _spec(name: str = "svc"):
    return build_http_service_spec(_PingClient, name=name)


# ....................... #


def test_static_factory_returns_dep_client_wired_into_adapter() -> None:
    client = HttpClient()
    config = HttpServiceConfig(base_url="https://example.com")
    ctx = context_from_deps(Deps.plain({HttpClientDepKey: client}))
    spec = _spec()

    adapter = ConfigurableHttpService(config=config)(ctx, spec)

    assert isinstance(adapter, HttpServiceAdapter)
    # __call__ wires the resolved client, the config, and the spec verbatim.
    assert adapter.client is client
    assert adapter.config is config
    assert adapter.spec is spec


def test_tenant_aware_with_secret_ref_builds_routed_client() -> None:
    config = HttpServiceConfig(
        tenant_aware=True,
        secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/svc")},
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                HttpClientDepKey: HttpClient(),
                SecretsDepKey: _SecretsStub(),
            },
        ),
    )

    adapter = ConfigurableHttpService(config=config)(ctx, _spec())

    # A per-tenant secret_ref ignores the plain dep client and builds a routed one.
    assert isinstance(adapter.client, RoutedHttpClient)


def test_tenant_aware_with_secret_ref_caches_routed_client() -> None:
    config = HttpServiceConfig(
        tenant_aware=True,
        secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/svc")},
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                HttpClientDepKey: HttpClient(),
                SecretsDepKey: _SecretsStub(),
            },
        ),
    )
    factory = ConfigurableHttpService(config=config)
    spec = _spec()

    first = factory(ctx, spec)
    second = factory(ctx, spec)

    # The routed client is constructed once and reused across calls.
    assert first.client is second.client


def test_tenant_aware_over_routed_dep_client_returns_it() -> None:
    routed = RoutedHttpClient(
        secrets=_SecretsStub(),
        secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/svc")},
        tenant_provider=lambda: TENANT_ID,
    )
    config = HttpServiceConfig(tenant_aware=True)
    ctx = context_from_deps(
        Deps.plain(
            {
                HttpClientDepKey: routed,
                SecretsDepKey: _SecretsStub(),
            },
        ),
    )

    adapter = ConfigurableHttpService(config=config)(ctx, _spec())

    # No per-service secret_ref, but the dep client is already routed: reuse it.
    assert adapter.client is routed


def test_tenant_aware_without_routing_raises_configuration() -> None:
    config = HttpServiceConfig(tenant_aware=True)
    ctx = context_from_deps(
        Deps.plain(
            {
                HttpClientDepKey: HttpClient(),
                SecretsDepKey: _SecretsStub(),
            },
        ),
    )

    with pytest.raises(CoreException, match="secret_ref_for_tenant"):
        ConfigurableHttpService(config=config)(ctx, _spec())
