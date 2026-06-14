"""Tests for the canonical tenancy fail-closed semantics on TenancyMixin.

Phase F: every adapter inherits one `_tenant_id_for_resolve` (relation-tier passthrough +
fail-closed `authentication`/`tenant_required`), so the same condition can't raise different
exceptions on different backends.
"""

from __future__ import annotations

from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.tenancy import TenantIdentity, TenancyMixin
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Adapter(TenancyMixin):
    pass


def _adapter(*, tenant_aware: bool, tenant: TenantIdentity | None | object = ...) -> _Adapter:
    if tenant is ...:
        return _Adapter(tenant_aware=tenant_aware)
    return _Adapter(tenant_aware=tenant_aware, tenant_provider=lambda: tenant)  # type: ignore[arg-type]


def test_not_tenant_aware_passes_bound_tenant_through() -> None:
    # Relation-tier: a bound tenant is returned even when NOT tenant_aware, so a dynamic
    # per-tenant resolver can scope itself without row-level filtering.
    tid = uuid4()
    adapter = _adapter(tenant_aware=False, tenant=TenantIdentity(tenant_id=tid))
    assert adapter._tenant_id_for_resolve() == tid


def test_not_tenant_aware_no_tenant_returns_none() -> None:
    adapter = _adapter(tenant_aware=False, tenant=None)
    assert adapter._tenant_id_for_resolve() is None


def test_not_tenant_aware_no_provider_returns_none() -> None:
    adapter = _adapter(tenant_aware=False)
    assert adapter._tenant_id_for_resolve() is None


def test_tenant_aware_missing_tenant_fails_closed_with_authentication() -> None:
    adapter = _adapter(tenant_aware=True, tenant=None)

    with pytest.raises(CoreException, match="tenant_required") as ei:
        adapter._tenant_id_for_resolve()

    # Consistent egress: an auth failure (401), not an internal error (500).
    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.code == "tenant_required"


def test_tenant_aware_no_provider_is_configuration_error() -> None:
    adapter = _adapter(tenant_aware=True)

    with pytest.raises(CoreException) as ei:
        adapter._tenant_id_for_resolve()

    assert ei.value.kind is ExceptionKind.CONFIGURATION


def test_tenant_aware_with_tenant_returns_id() -> None:
    tid = uuid4()
    adapter = _adapter(tenant_aware=True, tenant=TenantIdentity(tenant_id=tid))
    assert adapter._tenant_id_for_resolve() == tid
