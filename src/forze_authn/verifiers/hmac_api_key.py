from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyVerifierPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError

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

        if spec.cache is not None:
            raise CoreError(
                "API key account caching is forbidden by security reasons"
            )

        if spec.history_enabled:
            raise CoreError(
                "API key account history is forbidden by security reasons"
            )

    # ....................... #

    async def verify_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> VerifiedAssertion:
        digest = self.api_key_svc.calculate_key_digest(credentials.key)
        account = await find_api_key_account_by_key_hash(self.ak_qry, digest)

        if account is None or not account.is_active:
            raise AuthenticationError("API key account not found")

        ok = self.api_key_svc.verify_key(
            key=credentials.key,
            expected_digest=account.key_hash,
        )

        if not ok:
            raise AuthenticationError("Invalid API key")

        return VerifiedAssertion(
            issuer=ISSUER_FORZE_API_KEY,
            subject=str(account.principal_id),
        )
