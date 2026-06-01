"""Unit tests for :class:`~forze_kits.secrets.DirectorySecrets`."""

from forze.base.exceptions import CoreException, exc
from pathlib import Path

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, resolve_structured
from forze_kits.secrets import DirectorySecrets

# ----------------------- #

class _Sample(BaseModel):
    dsn: str

@pytest.mark.asyncio
async def test_resolve_str_from_file(tmp_path: Path) -> None:
    (tmp_path / "tenants").mkdir()
    (tmp_path / "tenants" / "dsn.txt").write_text(
        "postgresql://localhost/x", encoding="utf-8"
    )

    sec = DirectorySecrets(root=tmp_path)
    assert (
        await sec.resolve_str(SecretRef(path="tenants/dsn.txt"))
        == "postgresql://localhost/x"
    )

@pytest.mark.asyncio
async def test_resolve_structured_json_file(tmp_path: Path) -> None:
    (tmp_path / "cfg.json").write_text(
        '{"dsn": "postgresql://localhost/x"}',
        encoding="utf-8",
    )

    sec = DirectorySecrets(root=tmp_path)
    model = await resolve_structured(sec, SecretRef(path="cfg.json"), _Sample)
    assert model.dsn == "postgresql://localhost/x"

@pytest.mark.asyncio
async def test_missing_file(tmp_path: Path) -> None:
    sec = DirectorySecrets(root=tmp_path)
    with pytest.raises(CoreException):
        await sec.resolve_str(SecretRef(path="missing.txt"))

@pytest.mark.asyncio
async def test_path_traversal_rejected(tmp_path: Path) -> None:
    sec = DirectorySecrets(root=tmp_path)
    with pytest.raises(CoreException, match="escapes"):
        await sec.resolve_str(SecretRef(path="../outside.txt"))

@pytest.mark.asyncio
async def test_exists(tmp_path: Path) -> None:
    (tmp_path / "ok.txt").write_text("x", encoding="utf-8")
    sec = DirectorySecrets(root=tmp_path)
    assert await sec.exists(SecretRef(path="ok.txt")) is True
    assert await sec.exists(SecretRef(path="nope.txt")) is False
    assert await sec.exists(SecretRef(path="../escape.txt")) is False
