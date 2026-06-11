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


def _owner_mismatch_request(*, subject: AuthzSubject | None = None) -> AuthzRequest:
    return AuthzRequest(
        subject=subject or AuthzSubject(principal_id=uuid4()),
        action="invoice.read",
        resource=AuthzResource(
            resource_type="invoice",
            resource_id=uuid4(),
            attributes={"owner_id": str(uuid4())},
        ),
    )


def _grants(*keys: str) -> EffectiveGrants:
    return EffectiveGrants(
        permissions=frozenset(
            PermissionRef(permission_id=uuid4(), permission_key=key) for key in keys
        ),
    )


def test_decide_scoped_admin_overrides_owner_by_default() -> None:
    # `<resource_type>.admin` is a reserved owner-override key out of the box.
    svc = AuthzPolicyService()

    allowed = svc.decide(
        _grants("invoice.read", "invoice.admin"),
        _owner_mismatch_request(),
    )

    assert allowed.allowed is True

    # The scoped override is bound to the resource type — another type's
    # admin key must not bypass ownership.
    denied = svc.decide(
        _grants("invoice.read", "orders.admin"),
        _owner_mismatch_request(),
    )

    assert denied.allowed is False


def test_decide_empty_override_set_enforces_owner_even_for_admin() -> None:
    svc = AuthzPolicyService(owner_override_permissions=frozenset())

    for extra in ("admin", "invoice.admin"):
        denied = svc.decide(
            _grants("invoice.read", extra),
            _owner_mismatch_request(),
        )

        assert denied.allowed is False
        assert denied.reason == "Resource owner does not match subject"


def test_decide_custom_override_keys_honored() -> None:
    svc = AuthzPolicyService(
        owner_override_permissions=frozenset(
            {"superuser", "{resource_type}.owner_override"},
        ),
    )

    # Custom literal key bypasses ownership.
    assert svc.decide(
        _grants("invoice.read", "superuser"),
        _owner_mismatch_request(),
    ).allowed is True

    # Custom templated key is expanded with the resource type.
    assert svc.decide(
        _grants("invoice.read", "invoice.owner_override"),
        _owner_mismatch_request(),
    ).allowed is True

    # The default reserved keys lose their special meaning once renamed.
    assert svc.decide(
        _grants("invoice.read", "admin", "invoice.admin"),
        _owner_mismatch_request(),
    ).allowed is False


def test_decide_owner_match_needs_no_override() -> None:
    subject_id = uuid4()
    svc = AuthzPolicyService(owner_override_permissions=frozenset())

    allowed = svc.decide(
        _grants("invoice.read"),
        AuthzRequest(
            subject=AuthzSubject(principal_id=subject_id),
            action="invoice.read",
            resource=AuthzResource(
                resource_type="invoice",
                resource_id=uuid4(),
                attributes={"owner_id": str(subject_id)},
            ),
        ),
    )

    assert allowed.allowed is True
