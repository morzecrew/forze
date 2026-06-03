"""Tenant-aware HTTP adapter invoke tests."""

from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef
from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import BaseHttpIntegration, async_http_op
from forze_http.adapters.http_service import HttpxHttpServiceAdapter
from forze_http.execution.deps.configs import HttpxHttpServiceConfig
from forze_http.kernel.client import HttpxClient, RoutedHttpxClient
from forze_http.kernel.client.credentials import credential_auth_headers
from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

# ----------------------- #

TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")


class PingResponse(BaseModel):
    ok: bool


class PingClient(BaseHttpIntegration):
    ping = async_http_op(
        request=None,
        response=PingResponse,
        method="GET",
        path="/ping",
    )


class _SecretsStub:
    async def resolve_str(self, ref: SecretRef) -> str:
        return (
            '{"base_url": "https://tenant.example.com", '
            '"headers": {"X-Tenant": "abc"}, '
            '"bearer_token": "tok"}'
        )

    async def exists(self, ref: SecretRef) -> bool:
        return True


@pytest.mark.asyncio
async def test_tenant_invoke_uses_routed_client() -> None:
    spec = build_http_service_spec(PingClient, name="ping")
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    routed = RoutedHttpxClient(
        secrets=_SecretsStub(),
        secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
        tenant_provider=lambda: TENANT_ID,
    )

    async def initialize_client(
        tenant_id: UUID,
        creds: HttpRoutingCredentials,
    ) -> HttpxClient:
        client = HttpxClient()
        await client.initialize(
            creds.base_url,
            default_headers=credential_auth_headers(creds),
            transport=transport,
        )

        return client

    routed.initialize_client = initialize_client  # type: ignore[method-assign]
    await routed.startup()

    adapter = HttpxHttpServiceAdapter(
        client=routed,
        config=HttpxHttpServiceConfig(tenant_aware=True),
        spec=spec,
    )

    result = await adapter.invoke("ping")

    assert result.ok is True
    assert captured["url"] == "https://tenant.example.com/ping"
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers.get("x-tenant") == "abc" or headers.get("X-Tenant") == "abc"

    await routed.close()
