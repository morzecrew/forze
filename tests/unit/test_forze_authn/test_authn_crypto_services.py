"""Unit tests for authn cryptography and token helpers."""

from __future__ import annotations

from forze.base.exceptions import CoreException
import secrets
from datetime import timedelta
from uuid import uuid4

import jwt
import pytest

pytestmark = pytest.mark.unit

from forze.base.primitives import utcnow
from forze_identity.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    ApiKeyConfig,
    ApiKeyService,
    InviteTokenService,
    PasswordConfig,
    PasswordService,
    RefreshTokenService,
)

def _slow_password_config() -> PasswordConfig:
    return PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)

def test_api_key_digest_round_trip_and_reject_wrong() -> None:
    pepper = secrets.token_bytes(32)
    svc = ApiKeyService(pepper=pepper)
    plain = svc.generate_key()
    assert isinstance(plain, str)

    hashed = svc.calculate_key_digest(plain)
    assert svc.verify_key(plain, hashed)

    svc2 = ApiKeyService(pepper=secrets.token_bytes(32))
    assert not svc2.verify_key(plain, hashed)

    assert not svc.verify_key("", hashed)
    assert not svc.verify_key(plain + "!", hashed)

def test_api_key_prefix_tuple() -> None:
    pepper = secrets.token_bytes(32)
    svc = ApiKeyService(pepper=pepper, config=ApiKeyConfig(prefix="pfx"))
    raw = svc.generate_key()
    assert isinstance(raw, tuple)
    prefix, secret = raw
    assert prefix == "pfx"

def test_password_hash_and_verify_fast_config() -> None:
    pwd = PasswordService(config=_slow_password_config())
    h = pwd.hash_password("hunter2")
    assert pwd.verify_password(h, "hunter2")
    assert not pwd.verify_password(h, "hunter3")
    assert not pwd.verify_password("$invalid", "x")

def test_refresh_digest_round_trip() -> None:
    pepper = secrets.token_bytes(32)
    svc = RefreshTokenService(pepper=pepper)
    tok = svc.generate_token()
    digest = svc.calculate_token_digest(tok)
    assert svc.verify_token(tok, digest)

    other = RefreshTokenService(pepper=secrets.token_bytes(32))
    assert not other.verify_token(tok, digest)

    assert not svc.verify_token("not-base64%%%", digest)

def test_invite_digest_round_trip() -> None:
    pepper = secrets.token_bytes(32)
    svc = InviteTokenService(pepper=pepper)
    tok = svc.generate_token()
    digest = svc.calculate_token_digest(tok)
    assert svc.verify_token(tok, digest)

    other = InviteTokenService(pepper=secrets.token_bytes(32))
    assert not other.verify_token(tok, digest)

    assert not svc.verify_token("not-base64%%%", digest)

# Digest stability fixed vectors: digests of live tokens are persisted in
# databases, so any refactor of the token services must keep them byte-identical.
# Values were computed with the pre-refactor implementation.
_FIXED_PEPPER = b"0123456789abcdef0123456789abcdef"

def test_refresh_and_invite_digest_fixed_vector_unpadded_43() -> None:
    # 43 b64url chars (len % 4 == 3) — the shape generate_token() emits for
    # the default 32-byte token; also covers the b64 padding formula.
    token = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    expected = "da7164a2bac13505f97e4d5e630fa9e59f47e8ea74fb7b30c59a1ec4cb355363"

    refresh = RefreshTokenService(pepper=_FIXED_PEPPER)
    invite = InviteTokenService(pepper=_FIXED_PEPPER)

    for svc in (refresh, invite):
        assert svc.calculate_token_digest(token) == expected
        assert svc.verify_token(token, expected)

def test_refresh_and_invite_digest_fixed_vector_unpadded_22() -> None:
    # 22 b64url chars (len % 4 == 2) — exercises the other padding branch.
    token = "AAECAwQFBgcICQoLDA0ODw"
    expected = "8e832f79b12634f0595cb176f678a4ab97787bdc8ceaef5494dfb79220a745f9"

    refresh = RefreshTokenService(pepper=_FIXED_PEPPER)
    invite = InviteTokenService(pepper=_FIXED_PEPPER)

    for svc in (refresh, invite):
        assert svc.calculate_token_digest(token) == expected
        assert svc.verify_token(token, expected)

def test_access_token_issue_and_verify() -> None:
    secret = secrets.token_bytes(32)
    svc = AccessTokenService(
        secret_key=secret,
        config=AccessTokenConfig(issuer="it", audience="api"),
    )
    pid = uuid4()
    token = svc.issue_token(principal_id=pid)
    claims = svc.verify_token(token)
    assert claims["sub"] == str(pid)
    assert claims["iss"] == "it"
    assert claims["aud"] == "api"

def test_access_token_optional_sid_claim() -> None:
    secret = secrets.token_bytes(32)
    svc = AccessTokenService(
        secret_key=secret,
        config=AccessTokenConfig(issuer="it", audience="api"),
    )
    pid = uuid4()
    sid = uuid4()
    token = svc.issue_token(principal_id=pid, session_id=sid)
    claims = svc.verify_token(token)
    assert claims["sid"] == str(sid)


def test_access_token_optional_tid_claim() -> None:
    secret = secrets.token_bytes(32)
    svc = AccessTokenService(
        secret_key=secret,
        config=AccessTokenConfig(issuer="it", audience="api"),
    )
    pid = uuid4()
    tid = uuid4()
    token = svc.issue_token(principal_id=pid, tenant_id=tid)
    claims = svc.verify_token(token)
    assert claims["tid"] == str(tid)

def test_access_token_rejects_bad_signature() -> None:
    svc_a = AccessTokenService(secret_key=secrets.token_bytes(32))
    token = svc_a.issue_token(principal_id=uuid4())
    svc_b = AccessTokenService(secret_key=secrets.token_bytes(32))
    with pytest.raises(CoreException) as ei:
        svc_b.verify_token(token)
    assert ei.value.code == "invalid_access_token"

def test_access_token_detects_expiry() -> None:
    secret = secrets.token_bytes(32)
    cfg = AccessTokenConfig(issuer="it", audience="api")
    svc = AccessTokenService(secret_key=secret, config=cfg)
    stale = utcnow() - timedelta(hours=3)
    exp = utcnow() - timedelta(hours=2)
    payload = {
        "iss": cfg.issuer,
        "aud": cfg.audience,
        "sub": str(uuid4()),
        "iat": int(stale.timestamp()),
        "exp": int(exp.timestamp()),
    }
    expired_token = jwt.encode(
        payload,
        secret,
        algorithm=cfg.algorithm,
    )
    with pytest.raises(CoreException) as ei:
        svc.verify_token(expired_token)
    assert ei.value.code == "access_token_expired"
