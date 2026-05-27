"""Unit tests for :class:`~forze_identity.authz.services.policy.AuthzPolicyService`."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzSubject,
    EffectiveGrants,
    PermissionRef,
    AuthzResource,
)

pytestmark = pytest.mark.unit

from forze_identity.authz.services.policy import AuthzPolicyService


def test_decide_matches_permission_key() -> None:
    pid = uuid4()
    grants = EffectiveGrants(
        permissions=frozenset(
            {
                PermissionRef(permission_id=pid, permission_key="documents.read"),
            },
        ),
    )

    svc = AuthzPolicyService()

    decision = svc.decide(
        grants,
        AuthzRequest(
            subject=AuthzSubject(principal_id=uuid4()),
            action="documents.read",
        ),
    )

    assert decision.allowed is True
    assert decision.matched_permission_key == "documents.read"

    denied = svc.decide(
        grants,
        AuthzRequest(
            subject=AuthzSubject(principal_id=uuid4()),
            action="documents.write",
        ),
    )

    assert denied.allowed is False


def test_decide_owner_attribute_requires_admin() -> None:
    owner = uuid4()
    other = uuid4()
    perm_id = uuid4()

    grants = EffectiveGrants(
        permissions=frozenset(
            {
                PermissionRef(permission_id=perm_id, permission_key="invoice.read"),
            },
        ),
    )

    svc = AuthzPolicyService()

    denied = svc.decide(
        grants,
        AuthzRequest(
            subject=AuthzSubject(principal_id=other),
            action="invoice.read",
            resource=AuthzResource(
                resource_type="invoice",
                resource_id=uuid4(),
                attributes={"owner_id": str(owner)},
            ),
        ),
    )

    assert denied.allowed is False

    admin_grants = EffectiveGrants(
        permissions=frozenset(
            {
                PermissionRef(permission_id=perm_id, permission_key="invoice.read"),
                PermissionRef(permission_id=uuid4(), permission_key="admin"),
            },
        ),
    )

    allowed = svc.decide(
        admin_grants,
        AuthzRequest(
            subject=AuthzSubject(principal_id=other),
            action="invoice.read",
            resource=AuthzResource(
                resource_type="invoice",
                resource_id=uuid4(),
                attributes={"owner_id": str(owner)},
            ),
        ),
    )

    assert allowed.allowed is True
