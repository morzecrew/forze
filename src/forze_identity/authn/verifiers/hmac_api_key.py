from typing import Any, final

import attrs

from forze.application.contracts.authn import (
    ACT_CLAIM,
    ApiKeyCredentials,
    ApiKeyVerifierPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze_identity._secure_spec import forbid_cache_and_history

from ..adapters._utils import find_api_key_account_by_key_hash
from ..domain.constants import ISSUER_FORZE_API_KEY
from ..domain.models.account import ReadApiKeyAccount
from ..services import ApiKeyService

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HmacApiKeyVerifier(ApiKeyVerifierPort):
    """Verify API key credentials against a document-backed account using HMAC-SHA256."""

    api_key_svc: ApiKeyService
    """API key digest/verification service."""

    ak_qry: DocumentQueryPort[ReadApiKeyAccount]
    """Query port for API key accounts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.ak_qry.spec

        forbid_cache_and_history(spec, label="API key account")

    # ....................... #

    async def verify_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> VerifiedAssertion:
        digest = self.api_key_svc.calculate_key_digest(credentials.key)
        account = await find_api_key_account_by_key_hash(self.ak_qry, digest)

        if account is None or not account.is_active:
            raise exc.authentication("API key account not found")

        if account.expires_at is not None and account.expires_at <= utcnow():
            raise exc.authentication("API key account not found")

        ok = self.api_key_svc.verify_key(
            key=credentials.key,
            expected_digest=account.key_hash,
        )

        if not ok:
            raise exc.authentication("Invalid API key")

        claims: dict[str, Any] = {}

        # A delegation key carries its agent as an RFC 8693 ``act`` claim; the
        # orchestrator resolves it into AuthnIdentity.actor (intrinsic, trusted).
        if account.actor_principal_id is not None:
            claims[ACT_CLAIM] = {"sub": str(account.actor_principal_id)}

        return VerifiedAssertion(
            issuer=ISSUER_FORZE_API_KEY,
            subject=str(account.principal_id),
            claims=claims,
        )
