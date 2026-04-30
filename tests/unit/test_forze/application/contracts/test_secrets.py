"""Unit tests for secrets contracts."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, resolve_structured
from forze.base.errors import CoreError, SecretNotFoundError

# ----------------------- #


class _Sample(BaseModel):
    dsn: str


class _MemSecrets:
    """Minimal :class:`~forze.application.contracts.secrets.SecretsPort` for tests."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    async def resolve_str(self, ref: SecretRef) -> str:
        try:
            return self._data[ref.path]

        except KeyError as e:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._data


@pytest.mark.asyncio
async def test_resolve_structured_ok() -> None:
    sec = _MemSecrets({"db/1": '{"dsn": "postgres://localhost/x"}'})
    ref = SecretRef(path="db/1")
    m = await resolve_structured(sec, ref, _Sample)
    assert m.dsn == "postgres://localhost/x"


@pytest.mark.asyncio
async def test_resolve_structured_invalid() -> None:
    sec = _MemSecrets({"db/1": '{"dsn": 1}'})
    ref = SecretRef(path="db/1")
    with pytest.raises(CoreError, match="not valid"):
        await resolve_structured(sec, ref, _Sample)


@pytest.mark.asyncio
async def test_secret_not_found() -> None:
    sec = _MemSecrets({})
    ref = SecretRef(path="missing")
    with pytest.raises(SecretNotFoundError):
        await sec.resolve_str(ref)


@pytest.mark.asyncio
async def test_exists() -> None:
    sec = _MemSecrets({"a": "1"})
    assert await sec.exists(SecretRef(path="a")) is True
    assert await sec.exists(SecretRef(path="b")) is False
