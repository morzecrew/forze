"""HmacApiKeyVerifier: a delegation key emits its agent as an ``act`` claim."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import ACT_CLAIM, ApiKeyCredentials
from forze.application.contracts.authn.value_objects import VerifiedAssertion
from forze_identity.authn.domain.constants import ISSUER_FORZE_API_KEY
from forze_identity.authn.domain.models.account import ReadApiKeyAccount
from forze_identity.authn.services import ApiKeyConfig, ApiKeyService
from forze_identity.authn.verifiers.hmac_api_key import HmacApiKeyVerifier

pytestmark = pytest.mark.unit

_KEY = "raw-key"


def _verifier(account: ReadApiKeyAccount) -> HmacApiKeyVerifier:
    svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
    ak_qry = MagicMock()
    ak_qry.spec = MagicMock(cache=None, history_enabled=False)
    ak_qry.find = AsyncMock(return_value=account)

    return HmacApiKeyVerifier(api_key_svc=svc, ak_qry=ak_qry)


def _account(*, actor_principal_id=None) -> ReadApiKeyAccount:
    svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
    now = datetime.now(tz=UTC)

    return ReadApiKeyAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=uuid4(),
        actor_principal_id=actor_principal_id,
        key_hash=svc.calculate_key_digest(_KEY),
        is_active=True,
    )


# ....................... #


class TestHmacApiKeyVerifier:
    @pytest.mark.asyncio
    async def test_delegation_key_carries_act_claim(self) -> None:
        agent = uuid4()
        account = _account(actor_principal_id=agent)

        assertion = await _verifier(account).verify_api_key(ApiKeyCredentials(key=_KEY))

        assert isinstance(assertion, VerifiedAssertion)
        assert assertion.issuer == ISSUER_FORZE_API_KEY
        assert assertion.subject == str(account.principal_id)
        assert assertion.claims[ACT_CLAIM] == {"sub": str(agent)}

    @pytest.mark.asyncio
    async def test_plain_key_carries_no_claims(self) -> None:
        assertion = await _verifier(_account()).verify_api_key(ApiKeyCredentials(key=_KEY))

        assert assertion.claims == {}
