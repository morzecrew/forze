"""Tests for :mod:`forze.application.contracts.tenancy.deps`."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.tenancy.deps import (
    TenancyDeps,
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.base.exceptions import CoreException


class TestTenancyDeps:
    def test_resolver_returns_none_when_not_registered(self) -> None:
        ctx = MagicMock()
        ctx.deps.exists.return_value = False
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.resolver() is None
        ctx.deps.exists.assert_called_once_with(TenantResolverDepKey)

    def test_manager_returns_none_when_not_registered(self) -> None:
        ctx = MagicMock()
        ctx.deps.exists.return_value = False
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.manager() is None
        ctx.deps.exists.assert_called_once_with(TenantManagementDepKey)

    def test_resolver_resolves_when_registered(self) -> None:
        port = object()
        ctx = MagicMock()
        ctx.deps.exists.return_value = True
        ctx.deps.resolve_simple.return_value = port
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.resolver() is port
        ctx.deps.resolve_simple.assert_called_once()

    def test_manager_resolves_when_registered(self) -> None:
        port = object()
        ctx = MagicMock()
        ctx.deps.exists.return_value = True
        ctx.deps.resolve_simple.return_value = port
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.manager() is port

    def test_current_returns_bound_tenant(self) -> None:
        tenant = TenantIdentity(tenant_id=uuid4())
        ctx = MagicMock()
        ctx.inv_ctx.get_tenant.return_value = tenant
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.current() is tenant

    def test_require_current_id_returns_id(self) -> None:
        tid = uuid4()
        ctx = MagicMock()
        ctx.inv_ctx.get_tenant.return_value = TenantIdentity(tenant_id=tid)
        deps = TenancyDeps()
        deps.lock(ctx)

        assert deps.require_current_id() == tid

    def test_require_current_id_raises_when_no_tenant(self) -> None:
        ctx = MagicMock()
        ctx.inv_ctx.get_tenant.return_value = None
        deps = TenancyDeps()
        deps.lock(ctx)

        with pytest.raises(CoreException):
            deps.require_current_id()
