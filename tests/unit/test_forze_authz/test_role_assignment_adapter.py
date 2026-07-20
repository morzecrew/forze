"""Tests for :class:`~forze_identity.authz.adapters.role_assignment.RoleAssignmentAdapter`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzScope, RoleRef
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.base import Page
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze_identity.authz.adapters.role_assignment import RoleAssignmentAdapter
from forze_identity.authz.domain.models.bindings import ReadPrincipalRoleBinding
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal
from forze_identity.authz.domain.models.role_definition import ReadRoleDefinition
from forze_identity.authz.services.grants import AuthzGrantResolver


def _secure_spec(name: str, model: type) -> DocumentSpec:
    return DocumentSpec(name=name, read=model)


def _adapter(**kwargs: object) -> RoleAssignmentAdapter:
    pid = uuid4()
    rid = uuid4()
    now = datetime.now(tz=UTC)
    principal = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=True,
    )
    role = ReadRoleDefinition(
        id=rid,
        rev=1,
        created_at=now,
        last_update_at=now,
        role_key="admin",
    )

    principal_qry = MagicMock()
    principal_qry.spec = _secure_spec("principals", ReadPolicyPrincipal)
    principal_qry.find = AsyncMock(return_value=principal)

    role_qry = MagicMock()
    role_qry.spec = _secure_spec("roles", ReadRoleDefinition)
    role_qry.find = AsyncMock(return_value=role)

    pr_binding_qry = MagicMock()
    pr_binding_qry.spec = _secure_spec("pr_bindings", ReadPrincipalRoleBinding)
    pr_binding_qry.find_many = AsyncMock(
        return_value=Page(hits=[], count=0, page=1, size=500),
    )

    pr_binding_cmd = MagicMock()
    pr_binding_cmd.spec = _secure_spec("pr_bindings_cmd", ReadPrincipalRoleBinding)
    pr_binding_cmd.create = AsyncMock()
    pr_binding_cmd.kill = AsyncMock()

    resolver = MagicMock(spec=AuthzGrantResolver)
    for dep_name in (
        "permission_qry",
        "group_qry",
        "rp_binding_qry",
        "pp_binding_qry",
        "gp_binding_qry",
        "gr_binding_qry",
        "gperm_binding_qry",
    ):
        dep = MagicMock()
        dep.spec = _secure_spec(dep_name, ReadPolicyPrincipal)
        setattr(resolver.deps, dep_name, dep)
    resolver.list_assigned_roles = AsyncMock(
        return_value=frozenset({RoleRef(role_id=rid, role_key="admin")}),
    )

    defaults: dict[str, object] = {
        "spec": AuthzSpec(name="main", tenancy_mode="optional"),
        "principal_qry": principal_qry,
        "role_qry": role_qry,
        "pr_binding_qry": pr_binding_qry,
        "pr_binding_cmd": pr_binding_cmd,
        "resolver": resolver,
    }
    defaults.update(kwargs)
    return RoleAssignmentAdapter(**defaults)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_assign_role_creates_binding() -> None:
    adapter = _adapter()
    subject = AuthnIdentity(principal_id=uuid4())
    await adapter.assign_role(subject, "admin")
    adapter.pr_binding_cmd.create.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_assign_role_idempotent_when_binding_exists() -> None:
    adapter = _adapter()
    rid = uuid4()
    binding = ReadPrincipalRoleBinding(
        id=uuid4(),
        rev=1,
        created_at=datetime.now(tz=UTC),
        last_update_at=datetime.now(tz=UTC),
        principal_id=uuid4(),
        role_id=rid,
    )
    adapter.role_qry.find = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(id=rid, role_key="admin"),
    )
    adapter.pr_binding_qry.find_many = AsyncMock(  # type: ignore[method-assign]
        return_value=Page(hits=[binding], count=1, page=1, size=500),
    )
    await adapter.assign_role(AuthnIdentity(principal_id=binding.principal_id), "admin")
    adapter.pr_binding_cmd.create.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_assign_role_unknown_principal_raises() -> None:
    adapter = _adapter()
    adapter.principal_qry.find = AsyncMock(return_value=None)  # type: ignore[method-assign]
    with pytest.raises(CoreException, match="Policy principal not found"):
        await adapter.assign_role(uuid4(), "admin")


@pytest.mark.asyncio
async def test_revoke_role_kills_binding() -> None:
    adapter = _adapter()
    rid = uuid4()
    binding = ReadPrincipalRoleBinding(
        id=uuid4(),
        rev=1,
        created_at=datetime.now(tz=UTC),
        last_update_at=datetime.now(tz=UTC),
        principal_id=uuid4(),
        role_id=rid,
    )
    adapter.role_qry.find = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(id=rid, role_key="admin"),
    )
    adapter.pr_binding_qry.find_many = AsyncMock(  # type: ignore[method-assign]
        return_value=Page(hits=[binding], count=1, page=1, size=500),
    )
    await adapter.revoke_role(binding.principal_id, "admin")
    adapter.pr_binding_cmd.kill.assert_awaited_once_with(binding.id)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_list_roles_delegates_to_resolver() -> None:
    adapter = _adapter()
    tid = uuid4()
    roles = await adapter.list_roles(
        uuid4(),
        scope=AuthzScope(tenant_id=tid),
    )
    assert len(roles) == 1
    adapter.resolver.list_assigned_roles.assert_awaited_once()  # type: ignore[attr-defined]
