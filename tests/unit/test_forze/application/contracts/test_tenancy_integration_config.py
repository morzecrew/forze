"""Tests for TenantAwareIntegrationConfig."""

from __future__ import annotations

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig


@attrs.define(slots=True, kw_only=True, frozen=True)
class _ExampleConfig(TenantAwareIntegrationConfig):
    name: str


def test_tenant_aware_defaults_false() -> None:
    cfg = _ExampleConfig(name="x")
    assert cfg.tenant_aware is False


def test_tenant_aware_can_be_set() -> None:
    cfg = _ExampleConfig(name="x", tenant_aware=True)
    assert cfg.tenant_aware is True
