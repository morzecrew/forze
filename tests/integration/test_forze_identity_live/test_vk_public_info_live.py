"""Live smoke tests against VK ID's real ``public_info`` endpoint.

These tests hit ``https://id.vk.ru/oauth2/public_info`` with dummy credentials to
prove the request shape and error parsing against the live vendor, without an app
registration. They are network tests (no Docker), gated behind
``FORZE_LIVE_IDP_TESTS=1``::

    FORZE_LIVE_IDP_TESTS=1 uv run pytest tests/integration/test_forze_identity_live -q

Live responses observed on 2026-06-10 (HTTP 200 in both cases — VK signals
failure in the payload, not the status code):

- ``client_id=00000`` (malformed app id) →
  ``{"error":"invalid_request","error_description":"client_id is invalid"}``
- ``client_id=51812345&id_token=dummytoken`` (well-formed but unregistered) →
  ``{"error":"invalid_client","error_description":"invalid app"}``
"""

from __future__ import annotations

import os

import pytest

from forze.application.contracts.authn import AccessTokenCredentials
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.builtin.idp.vk import VkPublicInfoTokenVerifier

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("FORZE_LIVE_IDP_TESTS") != "1",
        reason="live IdP smoke tests run only with FORZE_LIVE_IDP_TESTS=1",
    ),
]

# ----------------------- #


async def test_live_public_info_rejects_dummy_token_as_authentication() -> None:
    verifier = VkPublicInfoTokenVerifier(client_id="51812345")

    with pytest.raises(CoreException) as ei:
        await verifier.verify_token(AccessTokenCredentials(token="dummytoken"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.code == "vk_id_token_rejected"
    # VK's live answer for an unregistered app id is an OAuth-style error object;
    # the verifier surfaces the vendor error code (never the token) in details.
    assert ei.value.details is not None
    assert ei.value.details["vendor_error"] in {
        "invalid_client",
        "invalid_request",
        "invalid_id_token",
    }


async def test_live_public_info_rejects_malformed_client_id() -> None:
    verifier = VkPublicInfoTokenVerifier(client_id="00000")

    with pytest.raises(CoreException) as ei:
        await verifier.verify_token(AccessTokenCredentials(token="dummytoken"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.details is not None
    assert ei.value.details["vendor_error"] in {"invalid_request", "invalid_client"}
