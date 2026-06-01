"""Tenancy helpers for mock adapters."""

from __future__ import annotations

import attrs

from forze.application.contracts.tenancy import TenancyMixin

from .partition import partition_namespace

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MockTenancyMixin(TenancyMixin):
    """Apply tenant key prefixes to resolved mock namespaces."""

    def _partitioned_namespace(self, resolved: str) -> str:
        tenant_id = self.require_tenant_if_aware()
        return partition_namespace(tenant_id, resolved)
