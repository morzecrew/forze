"""Unit tests for :class:`~forze_kits.adapters.secrets.EnvSecrets`."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, resolve_structured
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import EnvSecrets

# ----------------------- #

class _Sample(BaseModel):
    dsn: str

@pytest.mark.asyncio
async def test_resolve_str_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORZE_TEST_DSN", "postgresql://localhost/x")
    sec = EnvSecrets()
    assert await sec.resolve_str(SecretRef(path="FORZE_TEST_DSN")) == "postgresql://localhost/x"

@pytest.mark.asyncio
async def test_resolve_structured_json_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORZE_TEST_JSON", '{"dsn": "postgresql://localhost/x"}')
    sec = EnvSecrets()
    model = await resolve_structured(sec, SecretRef(path="FORZE_TEST_JSON"), _Sample)
    assert model.dsn == "postgresql://localhost/x"

@pytest.mark.asyncio
async def test_missing_env_var() -> None:
    sec = EnvSecrets()
    with pytest.raises(CoreException):
        await sec.resolve_str(SecretRef(path="FORZE_TEST_MISSING_VAR"))

@pytest.mark.asyncio
async def test_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORZE_TEST_EXISTS", "1")
    sec = EnvSecrets()
    assert await sec.exists(SecretRef(path="FORZE_TEST_EXISTS")) is True
    assert await sec.exists(SecretRef(path="FORZE_TEST_NOT_SET")) is False
