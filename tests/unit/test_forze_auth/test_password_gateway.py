"""Tests for :class:`~forze_auth.kernel.password.PasswordHasherGateway`."""

import pytest

from forze_auth.kernel.password import PasswordHasherConfig, PasswordHasherGateway

# ----------------------- #


def test_hash_and_verify_round_trip() -> None:
    gw = PasswordHasherGateway()
    h = gw.hash_password("correct horse battery staple")
    assert gw.verify_password(h, "correct horse battery staple") is True


def test_verify_rejects_wrong_password() -> None:
    gw = PasswordHasherGateway()
    h = gw.hash_password("secret")
    assert gw.verify_password(h, "other") is False


def test_verify_rejects_malformed_hash() -> None:
    gw = PasswordHasherGateway()
    assert gw.verify_password("not-a-valid-argon2-hash", "x") is False


def test_password_needs_rehash_on_invalid_hash() -> None:
    gw = PasswordHasherGateway()
    assert gw.password_needs_rehash("%%%invalid%%%") is True


def test_custom_lightweight_config_still_verifies() -> None:
    gw = PasswordHasherGateway(
        config=PasswordHasherConfig(
            time_cost=1,
            memory_cost=8192,
            parallelism=1,
            hash_length=32,
            salt_length=16,
        )
    )
    digest = gw.hash_password("light-config")
    assert gw.verify_password(digest, "light-config") is True
