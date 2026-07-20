"""Unit tests for forze_identity.builtin.local env loading."""

from __future__ import annotations

import json
from uuid import UUID

import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.local.load import from_env

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")


def test_from_env_inline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "FORZE_IDENTITY_LOCAL_CONFIG",
        json.dumps(
            {"api_keys": {"k": {"principal_id": str(_PID)}}},
        ),
    )
    monkeypatch.delenv("FORZE_IDENTITY_LOCAL_FILE", raising=False)

    config = from_env()
    assert config.api_keys["k"].principal_id == _PID


def test_from_env_requires_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORZE_IDENTITY_LOCAL_FILE", raising=False)
    monkeypatch.delenv("FORZE_IDENTITY_LOCAL_CONFIG", raising=False)

    with pytest.raises(CoreException):
        from_env()
