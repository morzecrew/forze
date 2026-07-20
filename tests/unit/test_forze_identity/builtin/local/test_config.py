"""Unit tests for forze_identity.builtin.local configuration."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any
from uuid import UUID

import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.local import from_json_path, from_mapping

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")
_TID = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def test_from_mapping_merges_tenant_from_api_key() -> None:
    config = from_mapping(
        {
            "api_keys": {
                "dev-key": {
                    "principal_id": str(_PID),
                    "tenant_id": str(_TID),
                },
            },
        },
    )

    assert config.api_keys["dev-key"].principal_id == _PID
    assert config.principal_tenants[_PID] == _TID


def test_from_mapping_rejects_invalid_principal_uuid() -> None:
    with pytest.raises((CoreException, ValueError)):
        from_mapping(
            {"api_keys": {"k": {"principal_id": "not-a-uuid"}}},
        )


def test_from_mapping_rejects_invalid_default_tenant_uuid() -> None:
    with pytest.raises(CoreException, match="default_tenant_id"):
        from_mapping(
            {
                "api_keys": {"k": {"principal_id": str(_PID)}},
                "default_tenant_id": "not-a-uuid",
            },
        )


def test_repr_does_not_leak_api_keys() -> None:
    config = from_mapping(
        {"api_keys": {"super-secret-key": {"principal_id": str(_PID)}}},
    )

    rendered = repr(config)

    assert "super-secret-key" not in rendered


class _DuplicateKeyMapping(Mapping[str, Any]):
    """Mapping whose iteration yields the same key twice (e.g. a multidict)."""

    def __init__(self, pairs: list[tuple[str, Any]]) -> None:
        self._pairs = pairs

    def __getitem__(self, key: str) -> Any:
        for k, v in self._pairs:
            if k == key:
                return v

        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(k for k, _ in self._pairs)

    def __len__(self) -> int:
        return len(self._pairs)

    def items(self):  # type: ignore[override]
        return list(self._pairs)


def test_duplicate_api_key_error_does_not_leak_key() -> None:
    entry = {"principal_id": str(_PID)}
    raw_keys = _DuplicateKeyMapping(
        [("super-secret-key", entry), ("super-secret-key", entry)],
    )

    with pytest.raises(CoreException, match="duplicate api_keys entry") as ei:
        from_mapping({"api_keys": raw_keys})  # type: ignore[dict-item]

    assert "super-secret-key" not in str(ei.value)


def test_from_json_path(tmp_path) -> None:
    path = tmp_path / "identity.json"
    path.write_text(
        json.dumps(
            {
                "api_keys": {
                    "k": {"principal_id": str(_PID), "tenant_id": str(_TID)},
                },
            },
        ),
        encoding="utf-8",
    )

    config = from_json_path(path)
    assert config.principal_tenants[_PID] == _TID
