"""Unit tests for document scoping and sensitive-resource checks."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authz import (
    AuthzSubject,
    AuthzDocumentScopeRequest,
    EffectiveGrants,
    PermissionRef,
    AuthzScope,
    AuthzSensitiveAccessRequest,
)
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_identity.authz.adapters.scoping import AuthzScopeAdapter
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal
from forze_identity.authz.services.grants import AuthzGrantResolver
from forze_identity.authz.services.policy import AuthzPolicyService

pytestmark = pytest.mark.unit


def _adapter(
    *,
    grants: EffectiveGrants,
    principal_active: bool = True,
) -> AuthzScopeAdapter:
    pid = uuid4()
    now = datetime.now(tz=timezone.utc)
    row = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=principal_active,
    )
    principal_qry = MagicMock()
    principal_qry.spec = DocumentSpec(name="policy_principals", read=ReadPolicyPrincipal)
    principal_qry.find = AsyncMock(return_value=row)

    resolver = MagicMock(spec=AuthzGrantResolver)
    resolver.resolve_effective_grants = AsyncMock(return_value=grants)

    return AuthzScopeAdapter(
        spec=AuthzSpec(name="main", tenancy_mode="optional"),
        principal_qry=principal_qry,
        resolver=resolver,
        policy=AuthzPolicyService(),
    )


@pytest.mark.asyncio
async def test_scope_document_injects_tenant_filter() -> None:
    tid = uuid4()
    perm_id = uuid4()
    adapter = _adapter(
        grants=EffectiveGrants(
            permissions=frozenset(
                {PermissionRef(permission_id=perm_id, permission_key="widgets.list")},
            ),
        ),
    )
    subject = AuthzSubject(principal_id=uuid4())

    scope = await adapter.scope_document(
        AuthzDocumentScopeRequest(
            subject=subject,
            scope=AuthzScope(tenant_id=tid),
            document_name="widgets",
            operation="list",
            action="widgets.list",
            base_filters={"$values": {"status": "open"}},
        ),
    )

    assert scope.deny_all is False
    assert scope.filters == {
        "$and": [
            {"$values": {"status": "open"}},
            {"$values": {"tenant_id": tid}},
        ],
    }


@pytest.mark.asyncio
async def test_scope_document_denies_without_permission() -> None:
    adapter = _adapter(grants=EffectiveGrants(permissions=frozenset()))

    scope = await adapter.scope_document(
        AuthzDocumentScopeRequest(
            subject=AuthzSubject(principal_id=uuid4()),
            scope=AuthzScope(),
            document_name="widgets",
            operation="list",
            action="widgets.list",
        ),
    )

    assert scope.deny_all is True
    assert scope.filters is None


@pytest.mark.asyncio
async def test_authorize_sensitive_resource_without_grant() -> None:
    adapter = _adapter(grants=EffectiveGrants(permissions=frozenset()))

    allowed = await adapter.authorize_sensitive_resource(
        AuthzSensitiveAccessRequest(
            subject=AuthzSubject(principal_id=uuid4()),
            scope=AuthzScope(),
            resource_type="invoice",
            resource_id=uuid4(),
            action="invoice.read",
        ),
    )

    assert allowed is False


@pytest.mark.asyncio
async def test_wrap_document_scope_merges_filters_on_ctx() -> None:
    from forze.application.hooks.authz import AuthzDocumentScopeWrap

    tid = uuid4()
    perm_id = uuid4()
    adapter = _adapter(
        grants=EffectiveGrants(
            permissions=frozenset(
                {PermissionRef(permission_id=perm_id, permission_key="widgets.list")},
            ),
        ),
    )

    ctx = ExecutionContext(deps=Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    @attrs.define
    class _ListArgs:
        filters: dict[str, object] | None = None

    args = _ListArgs(filters={"$values": {"status": "open"}})

    with patch.object(ctx.authz, "scope", return_value=adapter):
        with ctx.inv_ctx.bind(
            metadata=metadata,
            authn=ident,
            tenant=TenantIdentity(tenant_id=tid),
        ):
            wrap = AuthzDocumentScopeWrap(
                spec=AuthzSpec(name="main"),
                document_name="widgets",
                operation="list",
                action="widgets.list",
            )(ctx)

            async def _next(a: object) -> object:
                return a

            out = await wrap(_next, args)

    assert out.filters == {
        "$and": [
            {"$values": {"status": "open"}},
            {"$values": {"tenant_id": tid}},
        ],
    }
