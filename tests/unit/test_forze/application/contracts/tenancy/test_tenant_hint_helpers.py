"""Unit tests for tenant hint parsing helpers."""

from uuid import UUID

import pytest

from forze.application.contracts.tenancy import (
    coalesce_tenant_request_hints,
    parse_tenant_hint,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_TID = UUID("11111111-1111-1111-1111-111111111111")
_OTHER = UUID("22222222-2222-2222-2222-222222222222")


class TestParseTenantHint:
    def test_none_and_empty(self) -> None:
        assert parse_tenant_hint(None) is None
        assert parse_tenant_hint("") is None
        assert parse_tenant_hint("   ") is None

    def test_valid_uuid(self) -> None:
        assert parse_tenant_hint(str(_TID)) == _TID
        assert parse_tenant_hint(f"  { _TID }  ") == _TID

    def test_invalid_returns_none(self) -> None:
        assert parse_tenant_hint("tenant-7") is None
        assert parse_tenant_hint("not-a-uuid") is None


class TestCoalesceTenantRequestHints:
    def test_none_when_no_hints(self) -> None:
        assert coalesce_tenant_request_hints() is None

    def test_issuer_precedence_when_header_unparseable(self) -> None:
        assert (
            coalesce_tenant_request_hints(
                issuer_hint=str(_TID),
                header_hint="not-a-uuid",
            )
            == _TID
        )

    def test_same_uuid_from_both(self) -> None:
        assert (
            coalesce_tenant_request_hints(
                issuer_hint=str(_TID),
                header_hint=str(_TID),
            )
            == _TID
        )

    def test_header_only(self) -> None:
        assert coalesce_tenant_request_hints(header_hint=str(_TID)) == _TID

    def test_issuer_only(self) -> None:
        assert coalesce_tenant_request_hints(issuer_hint=str(_TID)) == _TID

    def test_conflict_raises(self) -> None:
        with pytest.raises(CoreException, match="Conflicting") as ei:
            coalesce_tenant_request_hints(
                issuer_hint=str(_TID),
                header_hint=str(_OTHER),
            )

        assert ei.value.code == "tenant_conflict"

    def test_malformed_hints_ignored(self) -> None:
        assert coalesce_tenant_request_hints(issuer_hint="acme", header_hint="x") is None


class TestRequireTenantId:
    def test_returns_bound_tenant_id(self) -> None:
        from forze.application.contracts.tenancy import require_tenant_id

        assert require_tenant_id(lambda: _TID, message="need tenant") == _TID

    def test_unwraps_tenant_identity(self) -> None:
        from forze.application.contracts.tenancy import TenantIdentity, require_tenant_id

        got = require_tenant_id(lambda: TenantIdentity(tenant_id=_TID), message="need")
        assert got == _TID

    def test_missing_tenant_raises_authentication_not_internal(self) -> None:
        # A missing bound tenant is caller-caused (401-class), matching the sibling
        # ``require_tenant_if_aware`` — not a server fault (500-class ``internal``).
        from forze.base.exceptions import ExceptionKind

        from forze.application.contracts.tenancy import require_tenant_id

        with pytest.raises(CoreException) as ei:
            require_tenant_id(lambda: None, message="Tenant ID is required")

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
        assert ei.value.code == "tenant_required"
