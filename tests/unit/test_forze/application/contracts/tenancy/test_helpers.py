"""Unit tests for :mod:`forze.application.contracts.tenancy.helpers`."""

from uuid import UUID

import pytest

from forze.application.contracts.tenancy import TenantIdentity, require_tenant_id
from forze.base.exceptions import CoreException

# ----------------------- #

_TID = UUID("11111111-1111-1111-1111-111111111111")


class TestRequireTenantId:
    def test_returns_uuid(self) -> None:
        assert require_tenant_id(lambda: _TID, message="need tenant") == _TID

    def test_returns_identity_tenant_id(self) -> None:
        identity = TenantIdentity(tenant_id=_TID)

        assert (
            require_tenant_id(lambda: identity, message="need tenant") == _TID
        )

    def test_raises_when_none(self) -> None:
        with pytest.raises(CoreException, match="need tenant") as ei:
            require_tenant_id(lambda: None, message="need tenant", code="tenant_required")

        assert ei.value.code == "tenant_required"
