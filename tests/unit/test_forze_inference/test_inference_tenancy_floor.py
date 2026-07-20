"""Declared tenant-isolation floors are enforced at wiring for the remote inference modules.

The tiers an inference route can reach are ``none`` (one shared model), ``tagged`` (a bound
tenant is required, still one shared model), ``namespace`` (a per-tenant ``model_name`` /
``endpoint_name`` resolver — a model per tenant on the serving side) and ``dedicated`` (a
routed per-tenant client).

These tests cover the single-client module, where ``dedicated`` is refused as a wiring gap.
The routed clients that satisfy it live in ``test_routed_inference_clients.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
)
from forze_inference.sagemaker import (
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerRuntimeClient,
)

# ----------------------- #

HTTP_CODE = "inference_http_tenancy_validation_failed"
SAGEMAKER_CODE = "sagemaker_inference_tenancy_validation_failed"


def _http(**overrides: Any) -> HttpInferenceConfig:
    values: dict[str, Any] = {
        "protocol": "mlflow",
        "model_name": "doubler",
        "acknowledge_data_egress": True,
    }
    values.update(overrides)
    return HttpInferenceConfig(**values)


def _sagemaker(**overrides: Any) -> SageMakerInferenceConfig:
    values: dict[str, Any] = {
        "endpoint_name": "doubler-prod",
        "acknowledge_data_egress": True,
    }
    values.update(overrides)
    return SageMakerInferenceConfig(**values)


def _http_module(config: HttpInferenceConfig, floor: Any) -> HttpInferenceDepsModule:
    return HttpInferenceDepsModule(
        client=InferenceHttpClient(),
        models={"doubler": config},
        required_tenant_isolation=floor,
    )


def _sagemaker_module(config: SageMakerInferenceConfig, floor: Any) -> SageMakerInferenceDepsModule:
    return SageMakerInferenceDepsModule(
        client=SageMakerRuntimeClient(),
        models={"doubler": config},
        required_tenant_isolation=floor,
    )


# ....................... #


class TestHttpInferenceTenancyFloor:
    def test_no_declared_floor_accepts_any_wiring(self) -> None:
        _http_module(_http(), None)

    def test_namespace_floor_accepts_a_per_tenant_model_resolver(self) -> None:
        _http_module(_http(model_name=lambda tenant_id: f"doubler-{tenant_id}"), "namespace")

    def test_namespace_floor_refuses_a_shared_model(self) -> None:
        with pytest.raises(CoreException) as ei:
            _http_module(_http(), "namespace")

        assert ei.value.code == HTTP_CODE
        assert "weaker" in ei.value.summary  # wiring gap, not a capability ceiling

    def test_tagged_floor_accepts_a_tenant_aware_route(self) -> None:
        _http_module(_http(tenant_aware=True), "tagged")

    def test_tagged_floor_refuses_a_tenant_blind_route(self) -> None:
        with pytest.raises(CoreException) as ei:
            _http_module(_http(), "tagged")

        assert ei.value.code == HTTP_CODE

    def test_dedicated_floor_refuses_a_single_client(self) -> None:
        """``dedicated`` needs a routed per-tenant client. With a single client it is a
        *wiring gap* (fixable — swap the client), not a capability ceiling; the routed
        counterpart is covered in ``test_routed_inference_clients.py``."""

        with pytest.raises(CoreException) as ei:
            _http_module(_http(model_name=lambda tenant_id: f"doubler-{tenant_id}"), "dedicated")

        assert ei.value.code == HTTP_CODE
        assert "weaker" in ei.value.summary
        assert "route the client per tenant" in ei.value.summary


# ....................... #


class TestSageMakerInferenceTenancyFloor:
    def test_namespace_floor_accepts_a_per_tenant_endpoint_resolver(self) -> None:
        _sagemaker_module(
            _sagemaker(endpoint_name=lambda tenant_id: f"doubler-{tenant_id}"), "namespace"
        )

    def test_namespace_floor_refuses_a_shared_endpoint(self) -> None:
        with pytest.raises(CoreException) as ei:
            _sagemaker_module(_sagemaker(), "namespace")

        assert ei.value.code == SAGEMAKER_CODE

    def test_dedicated_floor_refuses_a_single_client(self) -> None:
        with pytest.raises(CoreException) as ei:
            _sagemaker_module(
                _sagemaker(endpoint_name=lambda tenant_id: f"doubler-{tenant_id}"), "dedicated"
            )

        assert ei.value.code == SAGEMAKER_CODE
        assert "weaker" in ei.value.summary
