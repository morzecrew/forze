"""Nonce value binding tests for :func:`forze_identity.oidc.verify_id_token_nonce`."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("jwt")

pytestmark = pytest.mark.unit

import jwt

from forze_identity.oidc import nonce as nonce_module
from forze_identity.oidc import verify_id_token_nonce

# ----------------------- #


def _token(payload: dict[str, object]) -> str:
    return jwt.encode(payload, "k" * 32, algorithm="HS256")


# ....................... #


class TestVerifyIdTokenNonce:
    def test_matching_claims_pass(self) -> None:
        verify_id_token_nonce({"nonce": "n-1"}, "n-1")

    def test_matching_raw_token_passes(self) -> None:
        verify_id_token_nonce(_token({"sub": "u", "nonce": "n-1"}), "n-1")

    def test_mismatch_raises_authentication(self) -> None:
        with pytest.raises(CoreException) as ei:
            verify_id_token_nonce({"nonce": "other"}, "n-1")
        assert ei.value.code == "oidc_nonce_mismatch"

    def test_missing_claim_raises_authentication(self) -> None:
        with pytest.raises(CoreException) as ei:
            verify_id_token_nonce(_token({"sub": "u"}), "n-1")
        assert ei.value.code == "oidc_nonce_mismatch"

    def test_non_string_claim_raises_authentication(self) -> None:
        with pytest.raises(CoreException) as ei:
            verify_id_token_nonce({"nonce": 123}, "123")
        assert ei.value.code == "oidc_nonce_mismatch"

    def test_empty_expected_nonce_fails_closed(self) -> None:
        with pytest.raises(CoreException) as ei:
            verify_id_token_nonce({"nonce": ""}, "")
        assert ei.value.code == "oidc_nonce_mismatch"

    def test_undecodable_token_raises_authentication(self) -> None:
        with pytest.raises(CoreException) as ei:
            verify_id_token_nonce("not-a-jwt", "n-1")
        assert ei.value.code == "invalid_oidc_token"

    def test_uses_constant_time_comparison(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[bytes, bytes]] = []

        def _recording_compare(a: bytes, b: bytes) -> bool:
            calls.append((a, b))
            return a == b

        monkeypatch.setattr(
            nonce_module.hmac,
            "compare_digest",
            _recording_compare,
        )

        verify_id_token_nonce({"nonce": "n-1"}, "n-1")

        assert calls == [(b"n-1", b"n-1")]
