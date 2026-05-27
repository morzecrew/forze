"""Tests for :class:`~forze_identity.authz.adapters.effective_grants.GrantQueryAdapter`."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authz import AuthzSpec, EffectiveGrants, PrincipalRef
from forze.application.contracts.document import DocumentSpec
from forze_identity.authz.adapters.effective_grants import GrantQueryAdapter
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal
from forze_identity.authz.services.grants import AuthzGrantResolver, AuthzGrantResolverDeps

# ----------------------- #


def _document_qry() -> MagicMock:
    qry = MagicMock()
    qry.spec = DocumentSpec(name="catalog", read=ReadPolicyPrincipal)
    return qry


def _resolver() -> MagicMock:
    deps = AuthzGrantResolverDeps(
        permission_qry=_document_qry(),
        role_qry=_document_qry(),
        group_qry=_document_qry(),
        rp_binding_qry=_document_qry(),
        pr_binding_qry=_document_qry(),
        pp_binding_qry=_document_qry(),
        gp_binding_qry=_document_qry(),
        gr_binding_qry=_document_qry(),
        gperm_binding_qry=_document_qry(),
    )
    resolver = MagicMock(spec=AuthzGrantResolver)
    resolver.deps = deps
    resolver.resolve_effective_grants = AsyncMock(
        return_value=EffectiveGrants(permissions=(), roles=()),
    )
    return resolver


@pytest.mark.asyncio
async def test_resolve_effective_grants_delegates_to_resolver() -> None:
    pid = uuid4()
    now = datetime.now(tz=timezone.utc)
    principal_qry = _document_qry()
    principal_qry.find = AsyncMock(
        return_value=ReadPolicyPrincipal(
            id=pid,
            rev=1,
            created_at=now,
            last_update_at=now,
            kind="user",
            is_active=True,
        ),
    )
    resolver = _resolver()
    adapter = GrantQueryAdapter(
        spec=AuthzSpec(name="main"),
        principal_qry=principal_qry,
        resolver=resolver,
    )

    grants = await adapter.resolve_effective_grants(
        PrincipalRef(principal_id=pid, kind="user"),
    )

    assert grants.permissions == ()
    resolver.resolve_effective_grants.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_raises_when_principal_missing() -> None:
    principal_qry = _document_qry()
    principal_qry.find = AsyncMock(return_value=None)
    adapter = GrantQueryAdapter(
        spec=AuthzSpec(name="main"),
        principal_qry=principal_qry,
        resolver=_resolver(),
    )

    with pytest.raises(Exception, match="Policy principal not found"):
        await adapter.resolve_effective_grants(uuid4())
