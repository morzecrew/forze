"""Tests for :mod:`forze.application.contracts.tenancy.deps`."""

from __future__ import annotations

from unittest.mock import MagicMock

from forze.application.contracts.tenancy.deps import (
    TenancyDeps,
    TenantManagementDepKey,
    TenantResolverDepKey,
)


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
