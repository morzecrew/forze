"""Tests for :class:`~forze_auth.kernel.access.AccessTokenGateway`."""

import time
from datetime import timedelta

import pytest

from forze.base.errors import AuthenticationError
from forze_auth.kernel.access import AccessTokenConfig, AccessTokenGateway

# ----------------------- #

_SECRET = b"x" * 32


def test_access_token_secret_must_be_long_enough() -> None:
    with pytest.raises(ValueError):
        AccessTokenGateway(secret_key=b"short")


def test_issue_and_verify_round_trip() -> None:
    gw = AccessTokenGateway(secret_key=_SECRET)
    tok = gw.issue_token(subject="user-1", scopes=("read",), extras={"k": "v"})
    claims = gw.verify_token(tok)
    assert claims["sub"] == "user-1"
    assert claims["scp"] == ["read"]
    assert claims["xtr"] == {"k": "v"}


def test_verify_expired_token_raises() -> None:
    gw = AccessTokenGateway(
        secret_key=_SECRET,
        config=AccessTokenConfig(expires_in=timedelta(seconds=1)),
    )
    tok = gw.issue_token(subject="subj")
    time.sleep(2.0)
    with pytest.raises(AuthenticationError, match="expired"):
        gw.verify_token(tok, leeway=timedelta(seconds=0))


def test_verify_tampered_token_raises() -> None:
    gw = AccessTokenGateway(secret_key=_SECRET)
    tok = gw.issue_token(subject="u")
    broken = tok[:-4] + "xxxx"
    with pytest.raises(AuthenticationError, match="Invalid access token"):
        gw.verify_token(broken)


def test_try_decode_invalid_returns_none() -> None:
    gw = AccessTokenGateway(secret_key=_SECRET)
    assert gw.try_decode_token("not-a-jwt") is None


def test_try_decode_wrong_key_returns_none() -> None:
    gw1 = AccessTokenGateway(secret_key=_SECRET)
    gw2 = AccessTokenGateway(secret_key=b"y" * 32)
    tok = gw1.issue_token(subject="u")
    assert gw2.try_decode_token(tok) is None
