"""Unit tests for authn cryptography and token helpers."""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import uuid4

import jwt
import pytest

pytestmark = pytest.mark.unit

from forze.base.errors import AuthenticationError
from forze.base.primitives import utcnow
from forze_authnz.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    ApiKeyConfig,
    ApiKeyService,
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


def test_password_needs_rehash_after_tune() -> None:
    weak_cfg = PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)
    pwd_weak = PasswordService(config=weak_cfg)
    hashed = pwd_weak.hash_password("quiet")

    assert not pwd_weak.password_needs_rehash(hashed)

    pwd_strong = PasswordService(config=PasswordService().config)
    assert pwd_strong.password_needs_rehash(hashed)


def test_refresh_digest_round_trip() -> None:
    pepper = secrets.token_bytes(32)
    svc = RefreshTokenService(pepper=pepper)
    tok = svc.generate_token()
    digest = svc.calculate_token_digest(tok)
    assert svc.verify_token(tok, digest)

    other = RefreshTokenService(pepper=secrets.token_bytes(32))
    assert not other.verify_token(tok, digest)

    assert not svc.verify_token("not-base64%%%", digest)


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


def test_access_token_rejects_bad_signature() -> None:
    svc_a = AccessTokenService(secret_key=secrets.token_bytes(32))
    token = svc_a.issue_token(principal_id=uuid4())
    svc_b = AccessTokenService(secret_key=secrets.token_bytes(32))
    with pytest.raises(AuthenticationError) as ei:
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
    with pytest.raises(AuthenticationError) as ei:
        svc.verify_token(expired_token)
    assert ei.value.code == "access_token_expired"


def test_try_decode_returns_none_for_garbage() -> None:
    svc = AccessTokenService(secret_key=secrets.token_bytes(32))
    assert svc.try_decode_token("") is None
    assert svc.try_decode_token("not.a.token") is None
