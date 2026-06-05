from typing import Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyLifecyclePort,
    AuthnIdentity,
    CredentialLifetime,
    IssuedApiKey,
    PrincipalEligibilityPort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history
from forze.base.primitives import utcnow

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    ReadApiKeyAccount,
    UpdateApiKeyAccountCmd,
)
from ..services import ApiKeyService
from ._utils import find_api_key_account_by_id, find_api_key_account_by_key_hash

# ----------------------- #
#! TODO: configurable prefix


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyLifecycleAdapter(ApiKeyLifecyclePort):
    """API key lifecycle adapter."""

    api_key_svc: ApiKeyService
    """API key service."""

    ak_qry: DocumentQueryPort[ReadApiKeyAccount]
    """API key account query port."""

    ak_cmd: DocumentCommandPort[
        ReadApiKeyAccount,
        ApiKeyAccount,
        CreateApiKeyAccountCmd,
        UpdateApiKeyAccountCmd,
    ]
    """API key account command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.ak_qry.spec
        cmd_spec = self.ak_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="API key account")

    # ....................... #

    async def issue_api_key(self, identity: AuthnIdentity) -> IssuedApiKey:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        return await self._issue_for_principal(identity.principal_id)

    # ....................... #

    async def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> IssuedApiKey:
        """Rotate the presented API key: mint a fresh key, then retire the old one.

        Two writes (create the new key, then deactivate the presented one). Run this
        within a transaction scope so both commit or roll back together — the document
        gateways join the ambient transaction when one is open. The order is
        recovery-safe even without a transaction: a failed retire leaves the old key
        briefly valid alongside the new one rather than losing access, and the retire is
        rev-conditional (optimistic concurrency) against concurrent rotation.
        """

        digest = self.api_key_svc.calculate_key_digest(credentials.key)
        account = await find_api_key_account_by_key_hash(self.ak_qry, digest)

        if account is None or not account.is_active:
            raise exc.authentication("API key not found")

        if account.expires_at is not None and account.expires_at <= utcnow():
            raise exc.authentication("API key not found")

        if not self.api_key_svc.verify_key(
            key=credentials.key,
            expected_digest=account.key_hash,
        ):
            raise exc.authentication("Invalid API key")

        await self.eligibility.require_authentication_allowed(account.principal_id)

        # Rotate: issue a fresh key, then retire the presented one. Account fields
        # (prefix/expires_at/key_hash) are immutable, so refresh mints a new document
        # rather than mutating the existing key in place.
        issued = await self._issue_for_principal(account.principal_id)

        await self.ak_cmd.update(
            account.id,
            account.rev,
            UpdateApiKeyAccountCmd(is_active=False),
            return_new=False,
        )

        return issued

    # ....................... #

    async def _issue_for_principal(self, principal_id: UUID) -> IssuedApiKey:
        now = utcnow()
        expires_in = self.api_key_svc.config.expires_in
        expires_at = (now + expires_in) if expires_in is not None else None

        res = self.api_key_svc.generate_key()

        if isinstance(res, tuple):
            prefix, key = res

        else:
            key = res
            prefix = None

        key_hash = self.api_key_svc.calculate_key_digest(key)

        create_cmd = CreateApiKeyAccountCmd(
            principal_id=principal_id,
            key_hash=key_hash,
            prefix=prefix,
            expires_at=expires_at,
        )

        created_key = await self.ak_cmd.create(create_cmd)

        creds = ApiKeyCredentials(key=key, prefix=prefix)

        return IssuedApiKey(
            key=creds,
            key_id=str(created_key.id),
            lifetime=CredentialLifetime(
                expires_in=expires_in,
                issued_at=created_key.created_at,
                expires_at=expires_at,
            ),
        )

    # ....................... #

    async def revoke_api_key(self, identity: AuthnIdentity, key_id: str) -> None:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        try:
            parsed_id = UUID(key_id)

        except ValueError as e:
            raise exc.authentication("API key not found") from e

        account = await find_api_key_account_by_id(self.ak_qry, parsed_id)

        if account is None or account.principal_id != identity.principal_id:
            raise exc.authentication("API key not found")

        if not account.is_active:
            return

        await self.ak_cmd.update(
            account.id,
            account.rev,
            UpdateApiKeyAccountCmd(is_active=False),
            return_new=False,
        )

    # ....................... #

    async def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: Sequence[str],
    ) -> None:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        for key_id in key_ids:
            await self.revoke_api_key(identity, key_id)
