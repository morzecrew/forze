"""Tests for :class:`~forze_auth.kernel.refresh.RefreshTokenGateway`."""

from unittest.mock import patch

import pytest

from forze_auth.kernel.refresh import RefreshTokenConfig, RefreshTokenGateway

# ----------------------- #

_PEPPER = b"p" * 32


def test_refresh_pepper_must_be_long_enough() -> None:
    with pytest.raises(ValueError):
        RefreshTokenGateway(pepper=b"short")


def test_generate_token_verify_round_trip() -> None:
    gw = RefreshTokenGateway(pepper=_PEPPER)
    tok = gw.generate_token()
    digest = gw.calculate_token_digest(tok)
    assert gw.verify_token(tok, digest) is True


def test_verify_rejects_wrong_digest() -> None:
    gw = RefreshTokenGateway(pepper=_PEPPER)
    tok = gw.generate_token()
    assert gw.verify_token(tok, b"\x00" * 32) is False


def test_verify_rejects_garbage_token() -> None:
    gw = RefreshTokenGateway(pepper=_PEPPER)
    assert gw.verify_token("not-valid-base64!!!", b"\x00" * 32) is False


def test_custom_token_length() -> None:
    gw = RefreshTokenGateway(
        pepper=_PEPPER,
        config=RefreshTokenConfig(length=8),
    )
    raw = gw.generate_token()
    digest = gw.calculate_token_digest(raw)
    assert gw.verify_token(raw, digest) is True


def test_verify_returns_false_when_hmac_fails() -> None:
    gw = RefreshTokenGateway(pepper=_PEPPER)
    tok = gw.generate_token()
    with patch(
        "forze_auth.kernel.refresh.hmac.new",
        side_effect=RuntimeError("boom"),
    ):
        assert gw.verify_token(tok, b"\x00" * 32) is False
