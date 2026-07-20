"""Tenant-routed inference clients — the mechanism behind ``dedicated`` isolation.

``namespace`` isolation gives each tenant its own *model name* behind one shared client, so
every tenant's features still traverse the same connection and the same credential.
``dedicated`` resolves a whole client per tenant from that tenant's own secret: a separate
endpoint (HTTP) or a separate AWS identity (SageMaker). These tests pin the routing,
per-tenant caching, rotation, and the fail-closed behavior when no tenant is bound.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    RoutedInferenceHttpClient,
)
from forze_inference.sagemaker import (
    RoutedSageMakerRuntimeClient,
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
)

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


def _ref(tenant_id: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tenant_id}/inference")


class _MemSecrets:
    """Minimal ``SecretsPort`` serving per-tenant JSON credential blobs."""

    def __init__(self, payloads: dict[UUID, dict[str, Any]]) -> None:
        self.payloads = payloads
        self.reads = 0

    async def resolve_str(self, ref: SecretRef) -> str:
        self.reads += 1

        for tenant_id, payload in self.payloads.items():
            if ref.path == _ref(tenant_id).path:
                return json.dumps(payload)

        raise RuntimeError(f"missing secret: {ref.path}")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == _ref(t).path for t in self.payloads)


def _mock_transport(
    monkeypatch: pytest.MonkeyPatch,
    handler: Any,
) -> None:
    """Route every per-tenant client the router builds through a mock transport.

    The router constructs its inner clients itself, so there is no seam to pass a transport
    through — patching ``initialize`` is how a routed client gets tested without a server.
    """

    original = InferenceHttpClient.initialize

    async def patched(self: InferenceHttpClient, base_url: str, **kwargs: Any) -> None:
        kwargs["transport"] = httpx.MockTransport(handler)
        await original(self, base_url, **kwargs)

    monkeypatch.setattr(InferenceHttpClient, "initialize", patched)


# ....................... #


class TestRoutedHttpInference:
    @staticmethod
    def _routed(
        secrets: _MemSecrets,
        tenant: Any,
        *,
        max_cached_tenants: int = 100,
    ) -> RoutedInferenceHttpClient:
        return RoutedInferenceHttpClient(
            secrets=secrets,  # type: ignore[arg-type]
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: tenant(),
            max_cached_tenants=max_cached_tenants,
        )

    @pytest.mark.asyncio
    async def test_each_tenant_reaches_its_own_endpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The isolation claim itself: two tenants scoring the same spec must hit two
        different servers, not one server with two model names."""

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url.host))
            return httpx.Response(200, json={"predictions": [{"y": 1.0}]})

        secrets = _MemSecrets(
            {
                _T1: {"base_url": "http://tenant-one.invalid"},
                _T2: {"base_url": "http://tenant-two.invalid"},
            }
        )
        current: UUID | None = _T1
        routed = self._routed(secrets, lambda: current)
        _mock_transport(monkeypatch, handler)

        await routed.startup()

        try:
            module = HttpInferenceDepsModule(
                client=routed,
                models={
                    "doubler": HttpInferenceConfig(
                        protocol="mlflow",
                        model_name="doubler",
                        acknowledge_data_egress=True,
                    )
                },
                required_tenant_isolation="dedicated",
            )
            port = context_from_modules(module).inference.model(_spec())

            await port.predict(_Features(x=1.0))
            current = _T2
            await port.predict(_Features(x=1.0))

        finally:
            await routed.close()

        assert seen == ["tenant-one.invalid", "tenant-two.invalid"]

    @pytest.mark.asyncio
    async def test_unbound_tenant_fails_closed(self) -> None:
        secrets = _MemSecrets({_T1: {"base_url": "http://tenant-one.invalid"}})
        routed = self._routed(secrets, lambda: None)

        await routed.startup()

        try:
            with pytest.raises(CoreException, match="Tenant ID is required"):
                await routed.post_json("/invocations", {"instances": []})
        finally:
            await routed.close()

    @pytest.mark.asyncio
    async def test_tenant_client_is_cached_across_calls(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A per-tenant client is built once — otherwise every prediction would pay a
        secret read plus a fresh connection pool."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [{"y": 1.0}]})

        secrets = _MemSecrets({_T1: {"base_url": "http://tenant-one.invalid"}})
        routed = self._routed(secrets, lambda: _T1)
        _mock_transport(monkeypatch, handler)

        await routed.startup()

        try:
            await routed.post_json("/invocations", {"instances": [{"x": 1.0}]})
            reads_after_first = secrets.reads
            await routed.post_json("/invocations", {"instances": [{"x": 2.0}]})
        finally:
            await routed.close()

        assert secrets.reads == reads_after_first


# ....................... #


class TestTenancyCeilingWithRoutedClients:
    """``dedicated`` is reachable only with a routed client — the ceiling moves with it."""

    def test_http_dedicated_floor_accepted_with_a_routed_client(self) -> None:
        secrets = _MemSecrets({_T1: {"base_url": "http://tenant-one.invalid"}})
        routed = RoutedInferenceHttpClient(
            secrets=secrets,  # type: ignore[arg-type]
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
        )

        HttpInferenceDepsModule(
            client=routed,
            models={
                "doubler": HttpInferenceConfig(
                    protocol="mlflow",
                    model_name="doubler",
                    acknowledge_data_egress=True,
                )
            },
            required_tenant_isolation="dedicated",
        )

    def test_http_dedicated_floor_still_refused_with_a_single_client(self) -> None:
        with pytest.raises(CoreException) as ei:
            HttpInferenceDepsModule(
                client=InferenceHttpClient(),
                models={
                    "doubler": HttpInferenceConfig(
                        protocol="mlflow",
                        model_name="doubler",
                        acknowledge_data_egress=True,
                    )
                },
                required_tenant_isolation="dedicated",
            )

        assert ei.value.code == "inference_http_tenancy_validation_failed"

    def test_sagemaker_dedicated_floor_accepted_with_a_routed_client(self) -> None:
        secrets = _MemSecrets(
            {
                _T1: {
                    "region_name": "eu-west-1",
                    "access_key_id": "AKIA",
                    "secret_access_key": "secret",
                }
            }
        )
        routed = RoutedSageMakerRuntimeClient(
            secrets=secrets,  # type: ignore[arg-type]
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
        )

        SageMakerInferenceDepsModule(
            client=routed,
            models={
                "doubler": SageMakerInferenceConfig(
                    endpoint_name="doubler-prod",
                    acknowledge_data_egress=True,
                )
            },
            required_tenant_isolation="dedicated",
        )

    @pytest.mark.asyncio
    async def test_sagemaker_unbound_tenant_fails_closed(self) -> None:
        secrets = _MemSecrets(
            {
                _T1: {
                    "region_name": "eu-west-1",
                    "access_key_id": "AKIA",
                    "secret_access_key": "secret",
                }
            }
        )
        routed = RoutedSageMakerRuntimeClient(
            secrets=secrets,  # type: ignore[arg-type]
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: None,
        )

        await routed.startup()

        try:
            with pytest.raises(CoreException, match="Tenant ID is required"):
                await routed.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await routed.close()
