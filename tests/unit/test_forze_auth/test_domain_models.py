"""Smoke tests for :mod:`forze_auth.domain` models (validation & structure)."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from forze_auth.domain.enums.iam import IamPrincipalKind
from forze_auth.domain.models.account import (
    ApiKeyAccount,
    PasswordAccount,
    ReadPasswordAccount,
)
from forze_auth.domain.models.iam import IamPermission, IamPrincipal, IamRole
from forze_auth.domain.models.session import ReadRefreshGrant, RefreshGrant

# ----------------------- #


def test_iam_principal_kind_enum() -> None:
    assert IamPrincipalKind.USER == "user"


def test_password_account_constructible() -> None:
    pid = uuid4()
    acc = PasswordAccount(
        principal_id=pid,
        username="alice",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$fake",
    )
    assert acc.principal_id == pid
    assert acc.is_active is True


def test_read_password_account_round_trip_fields() -> None:
    now = datetime.now(UTC)
    row = ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=uuid4(),
        username="bob",
        password_hash="hash",
        is_active=False,
    )
    assert row.is_active is False


def test_api_key_account_minimal() -> None:
    acc = ApiKeyAccount(
        principal_id=uuid4(),
        key_hash="kh",
        prefix="pk_live",
    )
    assert acc.prefix == "pk_live"


def test_iam_principal_and_role_have_names() -> None:
    p = IamPrincipal(name="user-1", kind=IamPrincipalKind.USER)
    assert p.kind == IamPrincipalKind.USER
    r = IamRole(name="admin")
    assert r.name == "admin"


def test_iam_permission_resource_action() -> None:
    perm = IamPermission(name="doc.read", resource="document", action="read")
    assert perm.resource == "document"


def test_refresh_grant_requires_hash_and_expiry() -> None:
    exp = datetime.now(UTC) + timedelta(days=1)
    g = RefreshGrant(
        principal_id=uuid4(),
        refresh_hash=b"\x01" * 32,
        expires_at=exp,
    )
    assert len(g.refresh_hash) == 32


def test_read_refresh_grant_with_revocation_fields() -> None:
    now = datetime.now(UTC)
    rg = ReadRefreshGrant(
        id=uuid4(),
        rev=2,
        created_at=now,
        last_update_at=now,
        principal_id=uuid4(),
        refresh_hash=b"\x02" * 16,
        expires_at=now + timedelta(hours=1),
        revoked_at=now,
    )
    assert rg.revoked_at == now


def test_password_account_rejects_bad_email() -> None:
    with pytest.raises(ValidationError):
        PasswordAccount(
            principal_id=uuid4(),
            username="u",
            password_hash="h",
            email="not-an-email",
        )
