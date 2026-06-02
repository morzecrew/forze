"""Tests for :mod:`forze.application.contracts.tenancy.routed_client_base`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.tenancy.registry import TenantClientRegistry
from forze.application.contracts.tenancy.routed_client_base import (
    DsnRoutedTenantClientBase,
    RoutedTenantClientBase,
)
from forze.base.exceptions import exc


class _Client:
    closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_routed_base_evict_clears_fingerprint() -> None:
  tid = uuid4()

  @attrs.define(slots=True, kw_only=True)
  class _Routed(RoutedTenantClientBase[_Client]):
      async def resolve_credentials(self, tenant_id):  # noqa: ANN001
          return "creds"

      async def initialize_client(self, tenant_id, creds):  # noqa: ANN001
          return _Client()

      async def ensure_access_fingerprint(self, tenant_id) -> None:  # noqa: ANN001
          self._pool.set_fingerprint(tenant_id, "fp")

  routed = _Routed(
      secrets=MagicMock(),
      secret_ref_for_tenant={},
      tenant_provider=lambda: tid,
      max_cached_tenants=2,
  )
  await routed.startup()
  await routed._get_client()
  assert routed._pool.get_fingerprint(tid) == "fp"
  await routed.evict_tenant(tid)
  assert routed._pool.get_fingerprint(tid) is None


@pytest.mark.asyncio
async def test_dsn_routed_sets_fingerprint_before_create() -> None:
  tid = uuid4()
  secrets = MagicMock()
  secrets.resolve_str = AsyncMock(return_value="postgres://u:p@h/db")

  @attrs.define(slots=True, kw_only=True)
  class _Routed(DsnRoutedTenantClientBase[_Client]):
      async def initialize_client(self, tenant_id, creds: str) -> _Client:  # noqa: ANN001
          return _Client()

  routed = _Routed(
      secrets=secrets,
      secret_ref_for_tenant=lambda _t: MagicMock(),
      tenant_provider=lambda: tid,
      dsn_backend="database",
  )
  await routed.startup()
  await routed._get_client()
  assert routed._pool.get_fingerprint(tid) is not None
