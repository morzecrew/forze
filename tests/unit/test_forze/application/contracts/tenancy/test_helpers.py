"""Unit tests for :mod:`forze.application.contracts.tenancy.helpers`."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import (
    TenantIdentity,
    ensure_dsn_fingerprint,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_dsn_for_tenant,
    resolve_structured_for_tenant,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_TID = UUID("11111111-1111-1111-1111-111111111111")


class _MemSecrets:
    def __init__(
        self,
        *,
        strings: dict[str, str] | None = None,
        structured: dict[str, str] | None = None,
    ) -> None:
        self.strings = strings or {}
        self.structured = structured or {}

    async def resolve_str(self, ref: SecretRef) -> str:
        if ref.path in self.strings:
            return self.strings[ref.path]

        if ref.path in self.structured:
            return self.structured[ref.path]

        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self.strings or ref.path in self.structured


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/backend")


class _Creds(BaseModel):
    host: str
    token: str


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


class TestResolveDsnForTenant:
    @pytest.mark.asyncio
    async def test_resolves_dsn(self) -> None:
        secrets = _MemSecrets(strings={_ref(_TID).path: "redis://localhost:6379/0"})

        dsn = await resolve_dsn_for_tenant(
            tenant_id=_TID,
            secrets=secrets,
            ref_for_tenant=_ref,
            backend="Redis",
        )

        assert dsn == "redis://localhost:6379/0"


class TestEnsureDsnFingerprint:
    @pytest.mark.asyncio
    async def test_caches_fingerprint(self) -> None:
        secrets = _MemSecrets(strings={_ref(_TID).path: "redis://localhost:6379/0"})
        stored: dict[UUID, str] = {}

        fp1 = await ensure_dsn_fingerprint(
            stored.get,
            stored.__setitem__,
            tenant_id=_TID,
            secrets=secrets,
            ref_for_tenant=_ref,
            backend="Redis",
        )
        fp2 = await ensure_dsn_fingerprint(
            stored.get,
            stored.__setitem__,
            tenant_id=_TID,
            secrets=secrets,
            ref_for_tenant=_ref,
            backend="Redis",
        )

        assert fp1 == fp2
        assert stored[_TID] == fp1


class TestResolveStructuredForTenant:
    @pytest.mark.asyncio
    async def test_resolves_model(self) -> None:
        secrets = _MemSecrets(
            structured={_ref(_TID).path: '{"host":"h","token":"t"}'},
        )

        creds = await resolve_structured_for_tenant(
            _Creds,
            tenant_id=_TID,
            secrets=secrets,
            ref_for_tenant=_ref,
            backend="Test",
        )

        assert creds == _Creds(host="h", token="t")


class TestEnsureStructuredFingerprint:
    @pytest.mark.asyncio
    async def test_caches_via_callable(self) -> None:
        stored: dict[UUID, str] = {}
        calls = 0

        async def fingerprint() -> str:
            nonlocal calls
            calls += 1
            return "fp"

        fp1 = await ensure_structured_fingerprint(
            stored.get,
            stored.__setitem__,
            tenant_id=_TID,
            fingerprint=fingerprint,
        )
        fp2 = await ensure_structured_fingerprint(
            stored.get,
            stored.__setitem__,
            tenant_id=_TID,
            fingerprint=fingerprint,
        )

        assert fp1 == fp2 == "fp"
        assert calls == 1
