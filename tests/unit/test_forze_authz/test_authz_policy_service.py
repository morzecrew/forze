"""Unit tests for :class:`~forze_authz.services.policy.AuthzPolicyService`."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authz.value_objects import (
    EffectiveGrants,
    PermissionRef,
)

pytestmark = pytest.mark.unit

from forze_authz.services.policy import AuthzPolicyService


def test_permits_matches_permission_key() -> None:
    pid = uuid4()
    grants = EffectiveGrants(
        permissions=frozenset(
            {
                PermissionRef(permission_id=pid, permission_key="documents.read"),
            },
        ),
    )

    svc = AuthzPolicyService()

    assert svc.permits(grants, "documents.read") is True
    assert svc.permits(grants, "documents.write") is False
