"""Contract-compat tests across canonical secrets adapters."""

from pathlib import Path

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, resolve_structured
from forze.base.exceptions import SecretNotFoundError
from forze_secrets import DirectorySecrets, EnvSecrets, MappingSecrets

# ----------------------- #


class _Sample(BaseModel):
    dsn: str


@pytest.mark.asyncio
async def test_resolve_structured_ok_mapping() -> None:
    sec = MappingSecrets({"db/1": '{"dsn": "postgres://localhost/x"}'})
    model = await resolve_structured(sec, SecretRef(path="db/1"), _Sample)
    assert model.dsn == "postgres://localhost/x"


@pytest.mark.asyncio
async def test_resolve_structured_ok_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_1", '{"dsn": "postgres://localhost/x"}')
    sec = EnvSecrets()
    model = await resolve_structured(sec, SecretRef(path="DB_1"), _Sample)
    assert model.dsn == "postgres://localhost/x"


@pytest.mark.asyncio
async def test_resolve_structured_ok_directory(tmp_path: Path) -> None:
    (tmp_path / "db").mkdir()
    (tmp_path / "db" / "1").write_text(
        '{"dsn": "postgres://localhost/x"}', encoding="utf-8"
    )
    sec = DirectorySecrets(root=tmp_path)
    model = await resolve_structured(sec, SecretRef(path="db/1"), _Sample)
    assert model.dsn == "postgres://localhost/x"


@pytest.mark.asyncio
async def test_resolve_structured_invalid_mapping() -> None:
    sec = MappingSecrets({"db/1": '{"dsn": 1}'})
    with pytest.raises(exc.internal, match="not valid") as exc_info:
        await resolve_structured(sec, SecretRef(path="db/1"), _Sample)

    details = exc_info.value.details
    assert details is not None
    errors = details["errors"]
    assert errors
    assert "input" not in errors[0]


@pytest.mark.asyncio
async def test_secret_not_found_mapping() -> None:
    sec = MappingSecrets({})
    with pytest.raises(SecretNotFoundError):
        await sec.resolve_str(SecretRef(path="missing"))
