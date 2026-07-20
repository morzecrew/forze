"""Tests for :class:`~forze_identity.authz.adapters.principal_registry.PrincipalRegistryAdapter`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec
from forze_identity.authz.adapters.principal_registry import PrincipalRegistryAdapter
from forze_identity.authz.domain.models.policy_principal import (
    ReadPolicyPrincipal,
)


def _adapter() -> PrincipalRegistryAdapter:
    principal_qry = MagicMock()
    principal_qry.spec = DocumentSpec(name="principals", read=ReadPolicyPrincipal)
    principal_qry.find = AsyncMock(return_value=None)
    principal_qry.get = AsyncMock()

    principal_cmd = MagicMock()
    principal_cmd.spec = DocumentSpec(name="principals", read=ReadPolicyPrincipal)
    principal_cmd.create = AsyncMock()
    principal_cmd.update = AsyncMock()

    return PrincipalRegistryAdapter(
        principal_qry=principal_qry,
        principal_cmd=principal_cmd,
    )


@pytest.mark.asyncio
async def test_ensure_principal_creates_when_missing() -> None:
    adapter = _adapter()
    pid = uuid4()
    now = datetime.now(tz=UTC)
    created = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=True,
    )
    adapter.principal_cmd.create = AsyncMock(return_value=created)
    ref = await adapter.ensure_principal(pid, "user")
    assert ref.principal_id == pid
    adapter.principal_cmd.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_principal_inactive_on_create() -> None:
    adapter = _adapter()
    pid = uuid4()
    now = datetime.now(tz=UTC)
    created = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="service",
        is_active=True,
    )
    inactive = ReadPolicyPrincipal(
        id=pid,
        rev=2,
        created_at=now,
        last_update_at=now,
        kind="service",
        is_active=False,
    )
    adapter.principal_cmd.create = AsyncMock(return_value=created)
    adapter.principal_qry.get = AsyncMock(return_value=inactive)
    ref = await adapter.ensure_principal(pid, "service", is_active=False)
    assert ref.is_active is False
    adapter.principal_cmd.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_principal() -> None:
    adapter = _adapter()
    pid = uuid4()
    now = datetime.now(tz=UTC)
    created = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=True,
    )
    adapter.principal_cmd.create = AsyncMock(return_value=created)
    ref = await adapter.create_principal("user")
    assert ref.principal_id == pid


@pytest.mark.asyncio
async def test_get_principal_none_when_missing() -> None:
    adapter = _adapter()
    assert await adapter.get_principal(uuid4()) is None


@pytest.mark.asyncio
async def test_deactivate_principal_noop_when_missing() -> None:
    adapter = _adapter()
    await adapter.deactivate_principal(uuid4())
    adapter.principal_cmd.update.assert_not_awaited()
