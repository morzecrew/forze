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
