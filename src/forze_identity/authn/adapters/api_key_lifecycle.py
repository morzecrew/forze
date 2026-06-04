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
from ._utils import find_api_key_account_by_id

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
            principal_id=identity.principal_id,
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

    async def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> IssuedApiKey:
        raise NotImplementedError("Not implemented")

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
