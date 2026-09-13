"""Tenant-aware HTTP adapter invoke tests."""

from unittest.mock import MagicMock
from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef
from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import BaseHttpIntegration, async_http_op
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClient, RoutedHttpClient, routed_client
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

    routed = RoutedHttpClient(
        secrets=_SecretsStub(),
        secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
        tenant_provider=lambda: TENANT_ID,
    )

    async def initialize_client(
        tenant_id: UUID,
        creds: HttpRoutingCredentials,
    ) -> HttpClient:
        client = HttpClient()
        await client.initialize(
            creds.base_url,
            default_headers=credential_auth_headers(creds),
            transport=transport,
        )

        return client

    routed.initialize_client = initialize_client  # type: ignore[method-assign]
    await routed.startup()

    adapter = HttpServiceAdapter(
        client=routed,
        config=HttpServiceConfig(tenant_aware=True),
        spec=spec,
    )

    # `finally`, so a failed assertion still closes the pool — an open client leaks into
    # whatever runs next, and the leak is harder to read than the assertion that caused it.
    try:
        result = await adapter.invoke("ping")

        assert result.ok is True
        assert captured["url"] == "https://tenant.example.com/ping"
        headers = captured["headers"]
        assert isinstance(headers, dict)
        assert headers.get("x-tenant") == "abc" or headers.get("X-Tenant") == "abc"

    finally:
        await routed.close()


# ....................... #


class TestFormBodyThroughTheRoutedClient:
    """A tenant-routed form POST. The routed client forwards every parameter by hand, which
    is exactly where a newly added one gets dropped — and a dropped `data` would send an
    empty body that reads as a provider rejecting the credential."""

    @pytest.mark.asyncio
    async def test_the_form_body_reaches_the_tenant_client(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["type"] = request.headers.get("content-type")
            seen["body"] = request.content.decode()

            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)

        routed = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
            tenant_provider=lambda: TENANT_ID,
        )

        async def initialize_client(
            tenant_id: UUID,
            creds: HttpRoutingCredentials,
        ) -> HttpClient:
            client = HttpClient()
            await client.initialize(creds.base_url, transport=transport)

            return client

        routed.initialize_client = initialize_client  # type: ignore[method-assign]
        await routed.startup()

        try:
            response = await routed.request(
                "POST",
                "/oauth/token",
                data={"grant_type": "authorization_code"},
            )

            assert response.status_code == 200
            assert seen["type"] == "application/x-www-form-urlencoded"
            assert seen["body"] == "grant_type=authorization_code"

        finally:
            await routed.close()


# ....................... #


class TestCleartextTenantRoute:
    """A tenant-routed service has no `base_url` at wiring, so the config's cleartext check
    can never see it — the tenant's URL arrives with the tenant's secret. The warning has
    to be asked once per tenant client, which is also where a mistake is quietest: it
    affects one tenant rather than the deployment."""

    @staticmethod
    def _spy(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        spy = MagicMock()
        monkeypatch.setattr(routed_client, "logger", spy)

        return spy

    @pytest.mark.asyncio
    async def test_a_plaintext_tenant_url_with_credentials_warns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = self._spy(monkeypatch)
        client = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
            tenant_provider=lambda: TENANT_ID,
        )

        made = await client.initialize_client(
            TENANT_ID,
            HttpRoutingCredentials(base_url="http://tenant.example.com", bearer_token="tok"),
        )

        try:
            spy.warning.assert_called_once()
            call = str(spy.warning.call_args)
            assert "cleartext_credentials" in call
            assert str(TENANT_ID) in call
            # The tenant to fix, never the credential itself.
            assert "tok" not in call

        finally:
            await made.aclose()

    @pytest.mark.asyncio
    async def test_a_sensitive_route_warns_with_no_credential_headers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The half the first version of this check missed: a declared-sensitive route can
        # send a JSON or form body to a plaintext tenant URL carrying no credential
        # headers at all, and the payload is the thing worth protecting.
        spy = self._spy(monkeypatch)
        client = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
            tenant_provider=lambda: TENANT_ID,
            egress_sensitive=True,
        )

        made = await client.initialize_client(
            TENANT_ID, HttpRoutingCredentials(base_url="http://tenant.example.com")
        )

        try:
            spy.warning.assert_called_once()
            assert str(TENANT_ID) in str(spy.warning.call_args)

        finally:
            await made.aclose()

    @pytest.mark.asyncio
    async def test_a_sensitive_route_over_https_is_quiet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = self._spy(monkeypatch)
        client = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
            tenant_provider=lambda: TENANT_ID,
            egress_sensitive=True,
        )

        made = await client.initialize_client(
            TENANT_ID, HttpRoutingCredentials(base_url="https://tenant.example.com")
        )

        try:
            spy.warning.assert_not_called()

        finally:
            await made.aclose()

    @pytest.mark.asyncio
    async def test_https_or_no_credentials_is_quiet(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self._spy(monkeypatch)
        client = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/ping")},
            tenant_provider=lambda: TENANT_ID,
        )

        quiet = [
            HttpRoutingCredentials(base_url="https://tenant.example.com", bearer_token="tok"),
            HttpRoutingCredentials(base_url="http://localhost:8080", bearer_token="tok"),
            # No credential to leak and nothing declared sensitive: a plaintext URL
            # alone is the application's own business.
            HttpRoutingCredentials(base_url="http://tenant.example.com"),
        ]

        for creds in quiet:
            made = await client.initialize_client(TENANT_ID, creds)
            await made.aclose()

        spy.warning.assert_not_called()
