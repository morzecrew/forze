"""Unit tests for forze_identity.builtin.local configuration."""

from __future__ import annotations

import json
from uuid import UUID

import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.local import LocalIdentityConfig, from_json_path, from_mapping

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
