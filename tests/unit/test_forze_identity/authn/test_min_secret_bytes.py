"""The exported minimum-secret length is the one the validators enforce.

An application validating its own settings — so a short secret fails at boot naming the
environment variable rather than later inside an attrs validator naming a field — reads
the constant instead of copying the number. These tests are what stop the two from
drifting: they pin the exported value to the boundary the kernel actually rejects at.
"""

from typing import Any

import pytest

import forze_identity
from forze_identity.authn import MIN_SECRET_BYTES, AuthnKernelConfig
from forze_identity.authn.services import ApiKeyConfig, ApiKeyService, Hs256Signer

# ----------------------- #

SECRET_FIELDS = (
    "access_token_secret",
    "refresh_token_pepper",
    "invite_token_pepper",
    "reset_token_pepper",
    "api_key_pepper",
)


# ....................... #


class TestMinSecretBytes:
    def test_reachable_from_the_package_front_door(self) -> None:
        assert forze_identity.MIN_SECRET_BYTES == MIN_SECRET_BYTES

    # ....................... #

    def test_the_floor_is_thirty_two_bytes(self) -> None:
        """The one literal in this file, and deliberately so.

        Every other assertion here is written relative to the constant, which pins the
        validators to it but not it to anything — lowering the constant moves the test
        inputs with it and the suite stays green. This is the assertion that makes
        weakening the floor a failing test rather than a silent security regression.
        """

        assert MIN_SECRET_BYTES == 32

    # ....................... #

    @pytest.mark.parametrize("field", SECRET_FIELDS)
    def test_kernel_rejects_one_byte_short(self, field: str) -> None:
        kwargs: dict[str, Any] = {field: b"x" * (MIN_SECRET_BYTES - 1)}

        with pytest.raises(ValueError):
            AuthnKernelConfig(**kwargs)

    # ....................... #

    @pytest.mark.parametrize("field", SECRET_FIELDS)
    def test_kernel_accepts_exactly_the_minimum(self, field: str) -> None:
        kwargs: dict[str, Any] = {field: b"x" * MIN_SECRET_BYTES}
        config = AuthnKernelConfig(**kwargs)

        assert getattr(config, field) == b"x" * MIN_SECRET_BYTES

    # ....................... #

    def test_services_enforce_the_same_boundary(self) -> None:
        """Not only the kernel config — the signer and the peppered-HMAC base too."""

        with pytest.raises(ValueError):
            Hs256Signer(secret=b"x" * (MIN_SECRET_BYTES - 1))

        with pytest.raises(ValueError):
            ApiKeyService(pepper=b"x" * (MIN_SECRET_BYTES - 1), config=ApiKeyConfig())
