from typing import Sequence, final

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyLifecyclePort,
    ApiKeyResponse,
    AuthnIdentity,
    CredentialLifetime,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    ReadApiKeyAccount,
    ReadPrincipal,
    UpdateApiKeyAccountCmd,
)
from ..services import ApiKeyService
from ._utils import find_api_key_account_by_authn_identity

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

    principal_qry: DocumentQueryPort[ReadPrincipal]
    """Principal query port."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.ak_qry.spec
        cmd_spec = self.ak_cmd.spec
        principal_spec = self.principal_qry.spec

        if qry_spec.cache is not None:
            raise CoreError("API key account caching is forbidden by security reasons")

        if cmd_spec.cache is not None:
            raise CoreError("API key account caching is forbidden by security reasons")

        if qry_spec.history_enabled:
            raise CoreError("API key account history is forbidden by security reasons")

        if cmd_spec.history_enabled:
            raise CoreError("API key account history is forbidden by security reasons")

        if principal_spec.cache is not None:
            raise CoreError("Principal caching is forbidden by security reasons")

        if principal_spec.history_enabled:
            raise CoreError("Principal history is forbidden by security reasons")

    # ....................... #

    async def issue_api_key(self, identity: AuthnIdentity) -> ApiKeyResponse:
        ak = await find_api_key_account_by_authn_identity(self.ak_qry, identity)

        if ak is None or not ak.is_active:
            raise AuthenticationError("API key account not found")

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
        )

        created_key = await self.ak_cmd.create(create_cmd)

        creds = ApiKeyCredentials(key=key, prefix=prefix)

        return ApiKeyResponse(
            key=creds,
            key_id=str(created_key.id),
            lifetime=CredentialLifetime(
                expires_in=self.api_key_svc.config.expires_in,
            ),
        )

    # ....................... #

    async def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> ApiKeyResponse:
        raise NotImplementedError("Not implemented")

    # ....................... #

    async def revoke_api_key(self, key_id: str) -> None:  # noqa: F841
        raise NotImplementedError("Not implemented")

    # ....................... #

    async def revoke_many_api_keys(self, key_ids: Sequence[str]) -> None:  # noqa: F841
        raise NotImplementedError("Not implemented")
