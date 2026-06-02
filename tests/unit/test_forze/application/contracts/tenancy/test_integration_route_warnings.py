"""Unit tests for batch integration route tenancy warnings."""

from __future__ import annotations

from unittest.mock import patch

from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.contracts.tenancy.wiring import (
    IntegrationRouteWarning,
    warn_integration_routes,
)

# ----------------------- #


class _Config:
    def __init__(
        self,
        *,
        tenant_aware: bool = False,
        bucket: NamedResourceSpec | str = "static-bucket",
    ) -> None:
        self.tenant_aware = tenant_aware
        self.bucket = bucket


_STORAGE_WARNING = IntegrationRouteWarning(
    kind="storage",
    tenant_aware=lambda config: config.tenant_aware,
    named_fields=lambda config: [("bucket", config.bucket)],
)


def test_warn_integration_routes_skips_none_mapping() -> None:
    with patch(
        "forze.application.contracts.tenancy.wiring.warn_dynamic_relation_with_tenant_aware",
    ) as mock_warn:
        warn_integration_routes(
            integration="GCS",
            routes=None,
            warning=_STORAGE_WARNING,
        )

    mock_warn.assert_not_called()


def test_warn_integration_routes_delegates_per_route() -> None:
    bucket_resolver = lambda _tenant_id: "tenant-bucket"

    with patch(
        "forze.application.contracts.tenancy.wiring.warn_dynamic_relation_with_tenant_aware",
    ) as mock_warn:
        warn_integration_routes(
            integration="GCS",
            routes={
                "files": _Config(tenant_aware=True, bucket=bucket_resolver),
            },
            warning=_STORAGE_WARNING,
            log_warning=print,
        )

    mock_warn.assert_called_once_with(
        integration="GCS",
        route_name="files",
        kind="storage",
        tenant_aware=True,
        relation_fields=(),
        named_fields=[("bucket", bucket_resolver)],
        log_warning=print,
    )
