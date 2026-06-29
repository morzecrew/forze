"""Unit tests for :mod:`forze.application.contracts.secrets.resolution`."""

from uuid import UUID

import pytest

from forze.application.contracts.secrets import (
    SecretRef,
    resolve_str_for_tenant,
    secret_ref_for_tenant,
)
from forze.application.contracts.secrets.resolution import resolve_structured
from forze.base.exceptions import CoreException, exc
from pydantic import BaseModel

# ----------------------- #

_TID = UUID("11111111-1111-1111-1111-111111111111")
_REF = SecretRef(path="tenants/x/dsn")


class TestSecretRefForTenant:
    def test_callable(self) -> None:
        assert secret_ref_for_tenant(lambda tid: SecretRef(path=str(tid)), _TID) == SecretRef(
            path=str(_TID),
        )

    def test_mapping(self) -> None:
        assert secret_ref_for_tenant({_TID: _REF}, _TID) == _REF


class TestResolveStrForTenant:
    @pytest.mark.asyncio
    async def test_resolves(self) -> None:
        class _Secrets:
            async def resolve_str(self, ref: SecretRef) -> str:
                return "dsn"

            async def exists(self, ref: SecretRef) -> bool:
                return True

        out = await resolve_str_for_tenant(
            _Secrets(),
            _REF,
            tenant_id=_TID,
            backend="Redis",
        )
        assert out == "dsn"

    @pytest.mark.asyncio
    async def test_reraises_exc(self) -> None:
        class _Secrets:
            async def resolve_str(self, ref: SecretRef) -> str:
                raise exc.not_found("missing")

            async def exists(self, ref: SecretRef) -> bool:
                return False

        with pytest.raises(CoreException, match="missing"):
            await resolve_str_for_tenant(
                _Secrets(),
                _REF,
                tenant_id=_TID,
                backend="Redis",
            )

    @pytest.mark.asyncio
    async def test_wraps_other_errors(self) -> None:
        class _Secrets:
            async def resolve_str(self, ref: SecretRef) -> str:
                raise RuntimeError("boom")

            async def exists(self, ref: SecretRef) -> bool:
                return False

        with pytest.raises(CoreException, match="Failed to resolve Mongo secret"):
            await resolve_str_for_tenant(
                _Secrets(),
                _REF,
                tenant_id=_TID,
                backend="Mongo",
            )


class TestResolveStructured:
    @pytest.mark.asyncio
    async def test_valid_json(self) -> None:
        class _Model(BaseModel):
            x: int

        class _Secrets:
            async def resolve_str(self, ref: SecretRef) -> str:
                return '{"x": 1}'

            async def exists(self, ref: SecretRef) -> bool:
                return True

        m = await resolve_structured(_Secrets(), _REF, _Model)
        assert m.x == 1
