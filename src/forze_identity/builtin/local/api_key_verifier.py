"""API-key verifier backed by a static local identity config (demo/MVP only)."""

import secrets
from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyVerifierPort,
    VerifiedAssertion,
)
from forze.base.exceptions import exc
from forze_identity.authn.domain.constants import ISSUER_FORZE_LOCAL_API_KEY

from .config import LocalIdentityConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalApiKeyVerifier(ApiKeyVerifierPort):
    """Verify API keys against a frozen :class:`LocalIdentityConfig`.

    Intended for local development and demos only — no rotation, audit trail, or
    revocation. Use :class:`~forze_identity.authn.HmacApiKeyVerifier` in production.
    """

    config: LocalIdentityConfig
    """Static key → principal mapping."""

    # ....................... #

    async def verify_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> VerifiedAssertion:
        presented = credentials.key

        # Compare UTF-8 bytes: compare_digest raises TypeError on non-ASCII str,
        # which would turn a bad credential (401) into an internal error (500).
        presented_bytes = presented.encode()

        for stored_key, entry in self.config.api_keys.items():
            if secrets.compare_digest(presented_bytes, stored_key.encode()):
                return VerifiedAssertion(
                    issuer=ISSUER_FORZE_LOCAL_API_KEY,
                    subject=str(entry.principal_id),
                )

        raise exc.authentication("Invalid API key")
