"""Unit tests for :class:`~forze_kits.adapters.secrets.MappingSecrets`."""

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import MappingSecrets

# ----------------------- #


@pytest.mark.asyncio
async def test_resolve_str_ok() -> None:
    sec = MappingSecrets(data={"db/1": "postgresql://localhost/x"})
    assert await sec.resolve_str(SecretRef(path="db/1")) == "postgresql://localhost/x"


@pytest.mark.asyncio
async def test_resolve_str_missing() -> None:
    sec = MappingSecrets(data={})
    with pytest.raises(CoreException):
        await sec.resolve_str(SecretRef(path="missing"))


@pytest.mark.asyncio
async def test_exists() -> None:
    sec = MappingSecrets(data={"a": "1"})
    assert await sec.exists(SecretRef(path="a")) is True
    assert await sec.exists(SecretRef(path="b")) is False
