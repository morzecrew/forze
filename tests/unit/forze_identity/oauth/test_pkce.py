"""PKCE helper tests."""

from __future__ import annotations

import base64
import hashlib

import pytest

from forze_identity.oauth import generate_pkce

pytestmark = pytest.mark.unit


def test_generate_pkce_s256_challenge() -> None:
    pair = generate_pkce()

    assert len(pair.code_verifier) >= 43
    digest = hashlib.sha256(pair.code_verifier.encode()).digest()
    expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    assert pair.code_challenge == expected
