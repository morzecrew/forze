"""Unit tests for LocalApiKeyVerifier."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.contracts.authn import ApiKeyCredentials
from forze.base.exceptions import CoreException
from forze_identity.authn.domain.constants import ISSUER_FORZE_LOCAL_API_KEY
from forze_identity.builtin.local import LocalApiKeyVerifier, LocalIdentityConfig

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")


@pytest.mark.asyncio
async def test_verify_api_key_match() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"secret": {"principal_id": str(_PID)}}},
    )
    verifier = LocalApiKeyVerifier(config=config)

    assertion = await verifier.verify_api_key(ApiKeyCredentials(key="secret"))

    assert assertion.issuer == ISSUER_FORZE_LOCAL_API_KEY
    assert assertion.subject == str(_PID)


@pytest.mark.asyncio
async def test_verify_api_key_miss() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"secret": {"principal_id": str(_PID)}}},
    )
    verifier = LocalApiKeyVerifier(config=config)

    with pytest.raises(CoreException, match="Invalid API key"):
        await verifier.verify_api_key(ApiKeyCredentials(key="wrong"))


@pytest.mark.asyncio
async def test_verify_api_key_non_ascii_is_clean_authentication_error() -> None:
    """Non-ASCII input must yield a clean 401, not a TypeError from compare_digest."""

    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"secret": {"principal_id": str(_PID)}}},
    )
    verifier = LocalApiKeyVerifier(config=config)

    with pytest.raises(CoreException, match="Invalid API key"):
        await verifier.verify_api_key(ApiKeyCredentials(key="pässwörd-ключ"))
